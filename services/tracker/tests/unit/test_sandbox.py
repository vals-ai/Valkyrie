"""Tests for tracker sandbox orchestration.

Run: pytest services/tracker/tests/unit/test_sandbox.py
"""

import asyncio
import shlex
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import suppress
from typing import Any, Never, cast
from unittest.mock import AsyncMock, Mock, call

import pytest
from benchmark_service import (
    ComposeSandbox,
    ComposeSource,
    ExecResult,
    ImageSource,
    Resources,
    SandboxNotFoundError,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
)
from benchmark_service.sandbox import SandboxCommandError as ProviderSandboxCommandError
from benchmark_service.sandbox import SandboxError as ProviderSandboxError

from tracker.external_service_gateway import (
    AccountingSessionSnapshot,
    AccountingSessionState,
    ExternalServiceAccountingSummary,
    ExternalServiceDeadlineController,
)
from tracker import sandbox as sandbox_module
from tracker.aws.runtime import AWSRuntime
from tracker.runtime.storage import ObjectStore
from tracker.database.models import (
    AgentCausedExitReason,
    AgentContractRequest,
    GenerationContainment,
    MAX_OUTPUT_ARTIFACT_BYTES,
    OutputArtifact,
)
from tracker.exceptions import (
    AgentRunFailedError,
    ControlledGenerationError,
    ControlledGenerationTerminationUnconfirmedError,
    DependencySetupExhaustedError,
    GenerationTerminationUnconfirmedError,
    InvalidSandboxConfigurationError,
    OutputArtifactError,
    SSLConnectionError,
    SandboxError,
    SandboxSetupError,
)
from tracker.sandbox import (
    OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES,
    _controlled_completion_precedes_deadline,  # pyright: ignore[reportPrivateUsage]
    _controlled_generation_selected,  # pyright: ignore[reportPrivateUsage]
    _stream_controlled_output,  # pyright: ignore[reportPrivateUsage]
    _stream_controlled_output_with_egress_allowlist,  # pyright: ignore[reportPrivateUsage]
    create_sandbox,
    run_agent,
    upload_agent_artifacts,
    upload_output_artifacts,
)


def _ignore_output(_message: str) -> None:
    pass


def _mock_object_store() -> Mock:
    store = Mock(spec=ObjectStore)
    store.put_bytes = AsyncMock()
    store.put_stream = AsyncMock(return_value=0)
    store.temporary_download_url = AsyncMock(return_value="https://example.com/presigned")
    return store


def _capture_put_bytes(store: Mock, upload: Callable[..., Any]) -> None:
    async def put_bytes(key: str, content: bytes) -> None:
        await upload(content, key, None)

    store.put_bytes.side_effect = put_bytes


def _capture_put_stream(store: Mock, upload: Callable[..., Any]) -> None:
    async def put_stream(key: str, chunks: AsyncIterator[bytes], **kwargs: Any) -> int:
        return await upload(chunks, key, None, **kwargs)

    store.put_stream.side_effect = put_stream


def _collect_put_stream(store: Mock, uploaded: list[tuple[bytes, str]]) -> None:
    async def put_stream(key: str, chunks: AsyncIterator[bytes], **_kwargs: Any) -> int:
        content = b"".join([chunk async for chunk in chunks])
        uploaded.append((content, key))
        return len(content)

    store.put_stream.side_effect = put_stream


def _fake_stream_download(content_for: Callable[[str], bytes]) -> Callable[[str], AsyncIterator[bytes]]:
    def stream_download(remote_path: str) -> AsyncIterator[bytes]:
        async def chunks() -> AsyncIterator[bytes]:
            yield content_for(remote_path)

        return chunks()

    return stream_download


_create_sandbox = getattr(sandbox_module, "_create_sandbox")
_delete_sandbox = getattr(sandbox_module, "delete_sandbox")
_exec = getattr(sandbox_module, "_exec")
_apply_egress_allowlist = getattr(sandbox_module, "_apply_egress_allowlist")
_install_agent_dependencies = getattr(sandbox_module, "install_agent_dependencies")
_install_agent_dependencies_with_retries = getattr(sandbox_module, "_install_agent_dependencies_with_retries")
_stream_command_output_with_egress_allowlist = getattr(sandbox_module, "_stream_command_output_with_egress_allowlist")
_upload_agent_artifacts = getattr(sandbox_module, "upload_agent_artifacts")
_upload_output_artifact = getattr(sandbox_module, "_upload_output_artifact")


class TestOutputArtifacts:
    """Declared output artifact collection and size validation."""

    async def test_upload_output_artifacts_streams_file_without_exec_output(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        """Artifact contents stream through the sandbox file-transfer API, never command output.

        Test cases:
        - A 288,928-byte sidecar is streamed without a base64 exec call.
        - The exact bytes are uploaded to the task-scoped S3 key.
        - The authority check is handed to the store so a revoked run aborts mid-upload.
        """
        store = _mock_object_store()
        artifact = "artifacts/turns.jsonl"
        artifact_content = b"x" * 288_928
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output=str(len(artifact_content)))
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _collect_put_stream(store, uploaded)

        execution_is_current = Mock(return_value=True)
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.stream_download = _fake_stream_download(lambda _path: artifact_content)

        await upload_output_artifacts(
            mock_sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            store,
            execution_is_current=execution_is_current,
        )

        assert uploaded == [(artifact_content, "benchmarks/benchmark-123/task_0/artifacts/turns.jsonl")]
        assert store.put_stream.await_args.kwargs["should_continue"] is execution_is_current

    async def test_upload_output_artifacts_skips_upload_when_authority_revoked(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        """A run that lost authority before transfer uploads nothing."""
        store = _mock_object_store()
        artifact = "artifacts/turns.jsonl"

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="3")
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        mock_sandbox = Mock()

        await upload_output_artifacts(
            mock_sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            store,
            execution_is_current=lambda: False,
        )

        store.put_stream.assert_not_awaited()

    async def test_upload_output_artifacts_can_upload_explicit_glob_sources(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "find /logs -type f -path '/logs/*/turns/init/config.json' | sort | head -n 1":
                return ExecResult(exit_code=0, output="/logs/task/turns/init/config.json\n")
            if command == "stat -c%s /logs/task/turns/init/config.json":
                return ExecResult(exit_code=0, output="11")
            if command == "find /logs -type f -path '/logs/*/result.json' | sort | head -n 1":
                return ExecResult(exit_code=0, output="/logs/task/result.json\n")
            if command == "stat -c%s /logs/task/result.json":
                return ExecResult(exit_code=0, output="13")
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _collect_put_stream(store, uploaded)

        streamed_contents = {
            "/logs/task/turns/init/config.json": b'{"llm":{}}\n',
            "/logs/task/result.json": b'{"turns":[]}\n',
        }
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.stream_download = _fake_stream_download(lambda path: streamed_contents[path])

        await upload_output_artifacts(
            mock_sandbox,
            [
                OutputArtifact(path="artifacts/config.json", source="/logs/*/turns/init/config.json"),
                OutputArtifact(path="artifacts/result.json", source="/logs/*/result.json"),
            ],
            "benchmark-123",
            "task_0",
            store,
        )

        assert uploaded == [
            (b'{"llm":{}}\n', "benchmarks/benchmark-123/task_0/artifacts/config.json"),
            (b'{"turns":[]}\n', "benchmarks/benchmark-123/task_0/artifacts/result.json"),
        ]

    async def test_upload_output_artifacts_uses_result_paired_with_model_library_config(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /logs/model-library-run/result.json":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /logs/model-library-run/result.json":
                return ExecResult(exit_code=0, output="13")
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _collect_put_stream(store, uploaded)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.stream_download = _fake_stream_download(lambda _path: b'{"turns":[]}\n')

        await upload_output_artifacts(
            mock_sandbox,
            [OutputArtifact(path="artifacts/result.json", source="/logs/model-library-run/result.json")],
            "benchmark-123",
            "task_0",
            store,
        )

        assert uploaded == [(b'{"turns":[]}\n', "benchmarks/benchmark-123/task_0/artifacts/result.json")]

    async def test_upload_output_artifacts_fails_when_declared_file_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        artifact = "artifacts/missing.json"

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            assert command == "test -f /tmp/valkyrie/artifacts/missing.json"
            return ExecResult(exit_code=1, output="")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)

        with pytest.raises(OutputArtifactError, match="Required output artifact missing"):
            await upload_output_artifacts(Mock(), [artifact], "benchmark-123", "task_0", store)

    async def test_upload_output_artifacts_skips_missing_optional_model_patch(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        artifact = OutputArtifact(
            path="artifacts/model.patch",
            source="/logs/artifacts/model.patch",
            required=False,
        )

        sandbox = Mock()
        exec_mock = AsyncMock(return_value=ExecResult(exit_code=1, output=""))
        monkeypatch.setattr(sandbox_module, "_exec", exec_mock)

        await upload_output_artifacts(
            sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            store,
        )

        exec_mock.assert_awaited_once_with(
            sandbox,
            "test -f /logs/artifacts/model.patch && ! test -L /logs/artifacts/model.patch",
        )
        store.put_stream.assert_not_awaited()

    @pytest.mark.parametrize(
        ("required", "expected_uploads"),
        [(True, [b"secret"]), (False, [])],
        ids=["required-collected", "optional-skipped"],
    )
    async def test_upload_output_artifacts_handles_non_glob_symlinks_by_requiredness(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        required: bool,
        expected_uploads: list[bytes],
    ) -> None:
        store = _mock_object_store()
        source = "/logs/symlink result.json"
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f '/logs/symlink result.json'":
                return ExecResult(exit_code=0, output="")
            if command == "test -f '/logs/symlink result.json' && ! test -L '/logs/symlink result.json'":
                return ExecResult(exit_code=1, output="")
            if command == "stat -c%s '/logs/symlink result.json'":
                return ExecResult(exit_code=0, output="6")
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _collect_put_stream(store, uploaded)

        sandbox = Mock()
        sandbox.id = "sandbox-123"
        sandbox.name = "task-alias"
        sandbox.stream_download = _fake_stream_download(lambda _path: b"secret")
        artifact = OutputArtifact(path="artifacts/result.json", source=source, required=required)

        await upload_output_artifacts(
            sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            store,
        )

        assert [content for content, _key in uploaded] == expected_uploads

    async def test_upload_output_artifacts_prioritizes_required_artifacts_for_total_size_limit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        optional_source = "/logs/optional.json"
        required_source = "/logs/required.json"
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command in {
                f"test -f {optional_source} && ! test -L {optional_source}",
                f"test -f {required_source}",
            }:
                return ExecResult(exit_code=0, output="")
            if command == f"stat -c%s {optional_source}":
                return ExecResult(exit_code=0, output=str(OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES))
            if command == f"stat -c%s {required_source}":
                return ExecResult(exit_code=0, output="1")
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _collect_put_stream(store, uploaded)

        sandbox = Mock()
        sandbox.id = "sandbox-123"
        sandbox.name = "task-alias"
        sandbox.stream_download = _fake_stream_download(lambda path: path.encode())

        await upload_output_artifacts(
            sandbox,
            [
                OutputArtifact(path="telemetry/optional.json", source=optional_source, required=False),
                OutputArtifact(path="scoring/required.json", source=required_source),
            ],
            "benchmark-123",
            "task_0",
            store,
        )

        assert uploaded == [
            (
                required_source.encode(),
                "benchmarks/benchmark-123/task_0/scoring/required.json",
            )
        ]

    async def test_upload_output_artifacts_fails_when_file_exceeds_tracker_limit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        artifact = "artifacts/large.json"

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/large.json":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/large.json":
                return ExecResult(exit_code=0, output=str(MAX_OUTPUT_ARTIFACT_BYTES + 1))
            raise AssertionError(f"unexpected command: {command}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)

        with pytest.raises(OutputArtifactError, match="too large"):
            await upload_output_artifacts(Mock(), [artifact], "benchmark-123", "task_0", store)

        store.put_stream.assert_not_awaited()

    @pytest.mark.parametrize(
        ("stat_result", "total_bytes", "error"),
        (
            (ExecResult(exit_code=1, output=""), 0, "Failed to stat"),
            (ExecResult(exit_code=0, output="not-a-size"), 0, "Failed to parse"),
            (
                ExecResult(exit_code=0, output=str(MAX_OUTPUT_ARTIFACT_BYTES)),
                1,
                "Output artifacts are too large",
            ),
        ),
    )
    async def test_upload_output_artifact_rejects_invalid_sizes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        stat_result: ExecResult,
        total_bytes: int,
        error: str,
    ) -> None:
        store = _mock_object_store()
        sandbox = Mock()
        exec_mock = AsyncMock(
            side_effect=[
                ExecResult(exit_code=0, output=""),
                stat_result,
            ]
        )
        monkeypatch.setattr(sandbox_module, "_exec", exec_mock)

        with pytest.raises(OutputArtifactError, match=error):
            await _upload_output_artifact(
                sandbox,
                "artifacts/result.json",
                "benchmark-123",
                "task_0",
                store,
                total_bytes,
            )

        store.put_stream.assert_not_awaited()

    async def test_upload_output_artifacts_skips_invalid_optional_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        artifact = OutputArtifact(
            path="atif/trajectory.json",
            source="/logs/trajectory_atif.json",
            required=False,
        )

        sandbox = Mock()
        exec_mock = AsyncMock(
            side_effect=[
                ExecResult(exit_code=0, output=""),
                ExecResult(exit_code=0, output=str(MAX_OUTPUT_ARTIFACT_BYTES + 1)),
            ]
        )
        monkeypatch.setattr(sandbox_module, "_exec", exec_mock)

        await upload_output_artifacts(
            sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            store,
        )

        assert exec_mock.await_args_list == [
            call(
                sandbox,
                "test -f /logs/trajectory_atif.json && ! test -L /logs/trajectory_atif.json",
            ),
            call(sandbox, "stat -c%s /logs/trajectory_atif.json"),
        ]
        store.put_stream.assert_not_awaited()


class TestArchiveAndUploadOutput:
    """Streaming of the agent output archive from the sandbox to S3."""

    async def test_archive_and_upload_output_streams_archive_to_s3(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        """
        Test cases:
        - The tar.gz archive is streamed chunk-by-chunk to S3 without a full in-memory download.
        - The temporary archive is removed from the sandbox afterwards.
        """
        exec_commands: list[str] = []
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            exec_commands.append(command)
            return ExecResult(exit_code=0, output="")

        async def fake_upload_stream_to_s3(
            chunks: Any,
            s3_key: str,
            _aws_runtime: AWSRuntime,
            should_continue: Any = None,
        ) -> int:
            data = b"".join([chunk async for chunk in chunks])
            uploaded.append((data, s3_key))
            return len(data)

        def fake_stream_download(remote_path: str) -> AsyncIterator[bytes]:
            assert remote_path.endswith(".tar.gz")

            async def chunks() -> AsyncIterator[bytes]:
                yield b"chunk-1"
                yield b"chunk-2"

            return chunks()

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _capture_put_stream(store, fake_upload_stream_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.stream_download = fake_stream_download

        await sandbox_module.archive_and_upload_output(
            mock_sandbox,
            "/logs",
            "benchmarks/benchmark-123/task_0/output.tar.gz",
            store,
        )

        assert uploaded == [(b"chunk-1chunk-2", "benchmarks/benchmark-123/task_0/output.tar.gz")]
        assert exec_commands[0].startswith("tar -czf ")
        assert exec_commands[-1].startswith("rm -f ")


class _CompletedControlledWorkload:
    def __init__(self, exit_code: int, absence_confirmed_at: float) -> None:
        self.result = ExecResult(exit_code=exit_code)
        self.absence_confirmed_at = absence_confirmed_at


class _FakeControlledWorkload:
    def __init__(
        self,
        *,
        exit_code: int = 0,
        wait_error: BaseException | None = None,
        wait_release: asyncio.Event | None = None,
        output_release: asyncio.Event | None = None,
        kill_release: asyncio.Event | None = None,
        natural: bool = True,
        absence_confirmed_at: float | None = None,
    ) -> None:
        self.exit_code = exit_code
        self.wait_error = wait_error
        self.wait_release = wait_release
        self.output_release = output_release
        self.kill_release = kill_release
        self.absence_confirmed_at = absence_confirmed_at
        self.closed = asyncio.Event()
        self.wait_finished = asyncio.Event()
        self.output_started = asyncio.Event()
        self.output_drain_started = asyncio.Event()
        self.output_finished = asyncio.Event()
        self.kill_started = asyncio.Event()
        self.kill_calls = 0
        self.kill_error: BaseException | None = None
        self.output_error: BaseException | None = None
        self.block_kill = False
        if natural:
            self.closed.set()

    async def output(self) -> AsyncIterator[str]:
        try:
            self.output_started.set()
            yield "agent output"
            await self.closed.wait()
            self.output_drain_started.set()
            if self.output_release is not None:
                await self.output_release.wait()
            if self.output_error is not None:
                raise self.output_error
        finally:
            self.output_finished.set()

    async def wait(self) -> _CompletedControlledWorkload:
        try:
            if self.wait_release is not None:
                await self.wait_release.wait()
            if self.wait_error is not None:
                raise self.wait_error
            confirmed_at = self.absence_confirmed_at
            if confirmed_at is None:
                confirmed_at = asyncio.get_running_loop().time()
            return _CompletedControlledWorkload(self.exit_code, confirmed_at)
        finally:
            self.wait_finished.set()

    async def kill(self) -> None:
        self.kill_calls += 1
        self.kill_started.set()
        if self.block_kill:
            await asyncio.Event().wait()
        if self.kill_release is not None:
            await self.kill_release.wait()
        if self.kill_error is not None:
            raise self.kill_error
        self.closed.set()
        if self.wait_release is not None:
            self.wait_release.set()


class _FakeControlledSandbox:
    id = "sandbox-123"
    name = "task-alias"
    generation_containment: GenerationContainment | None

    def __init__(self, workload: _FakeControlledWorkload) -> None:
        self.workload = workload
        self.generation_containment = GenerationContainment(type="linux_pid_namespace", version=1)
        self.probe_calls = 0
        self.controlled_calls: list[tuple[str, str | None]] = []

    async def probe_generation_containment(self) -> None:
        self.probe_calls += 1

    def controlled_workload(self, command: str, *, cwd: str | None = None) -> _FakeControlledWorkload:
        self.controlled_calls.append((command, cwd))
        return self.workload


def _accounting_snapshot(
    *,
    state: AccountingSessionState = AccountingSessionState.OPEN,
    overhead_ms: int = 0,
    revision: int = 0,
    epoch: int = 0,
) -> AccountingSessionSnapshot:
    return AccountingSessionSnapshot(
        session_id="session-1",
        state=state,
        cumulative_neutral_overhead_ms=overhead_ms,
        revision=revision,
        accounting_epoch=epoch,
    )


class _FakeAccountingClient:
    def __init__(
        self,
        *,
        overhead_ms: int = 0,
        read_error: BaseException | None = None,
        begin_overhead_ms: int | None = None,
    ) -> None:
        self.overhead_ms = overhead_ms
        self.begin_overhead_ms = begin_overhead_ms
        self.read_error = read_error
        self.decisions: list[str] = []
        self.read_calls = 0
        self.begin_calls = 0

    async def read_session(self, _session_id: str) -> AccountingSessionSnapshot:
        self.read_calls += 1
        if self.read_error is not None:
            raise self.read_error
        return _accounting_snapshot(overhead_ms=self.overhead_ms, revision=1)

    async def begin_arbitration(self, _session_id: str) -> AccountingSessionSnapshot:
        self.begin_calls += 1
        if self.begin_overhead_ms is not None:
            self.overhead_ms = self.begin_overhead_ms
            self.begin_overhead_ms = None
        return _accounting_snapshot(
            state=AccountingSessionState.ARBITRATING,
            overhead_ms=self.overhead_ms,
            revision=1,
            epoch=1,
        )

    async def resolve_arbitration(self, _session_id: str, decision: Any) -> AccountingSessionSnapshot:
        self.decisions.append(str(decision))
        state = AccountingSessionState.OPEN if str(decision) == "RESUME" else AccountingSessionState.SEALED
        return _accounting_snapshot(
            state=state,
            overhead_ms=self.overhead_ms,
            revision=1,
            epoch=1,
        )


def _controlled_contract(*, version: int = 1) -> AgentContractRequest:
    return AgentContractRequest(
        name="test-agent",
        install_cmd="",
        run_cmd="echo done",
        generation_containment=GenerationContainment(type="linux_pid_namespace", version=version),
    )


class TestRunAgent:
    """Agent execution, output collection, and runtime command construction."""

    async def test_run_agent_uploads_declared_output_artifacts(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd="echo done",
            final_output="/logs",
            output_artifacts=["artifacts/result.json"],
        )
        artifact_calls: list[str] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command.startswith("mkdir -p") or command == "test -e /logs":
                return ExecResult(exit_code=0, output="")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_stream_command_output(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            return None, 0.0

        async def fake_upload_output_artifacts(
            _sandbox: Any,
            artifacts: list[str],
            benchmark_id: str,
            task_id: str,
            _aws_runtime: AWSRuntime,
            _execution_is_current: Any,
        ) -> None:
            artifact_calls.append(f"{benchmark_id}:{task_id}:{artifacts[0]}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "stream_command_output", fake_stream_command_output)
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", fake_upload_output_artifacts)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        await run_agent(
            mock_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            lambda _msg: None,
            "/testbed",
            object_store=store,
            benchmark_id="benchmark-123",
        )

        assert artifact_calls == ["benchmark-123:task_0:artifacts/result.json"]

    async def test_run_agent_collects_outputs_before_reraising_agent_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd="exit 23",
            final_output="/logs",
            output_artifacts=["artifacts/result.json"],
        )
        archive_output = AsyncMock()
        upload_artifacts = AsyncMock()

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command.startswith("mkdir -p") or command == "test -e /logs":
                return ExecResult(exit_code=0, output="")
            raise AssertionError(f"unexpected command: {command}")

        async def fail_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            raise AgentRunFailedError("agent exited 23")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "stream_command_output", fail_agent)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive_output)
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", upload_artifacts)

        sandbox = Mock(id="sandbox-123", name="task-alias")
        with pytest.raises(AgentRunFailedError, match="agent exited 23"):
            await run_agent(
                sandbox,
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/testbed",
                object_store=store,
                agent_output_s3_key="benchmarks/benchmark-123/task_0/agent_output.tar.gz",
                benchmark_id="benchmark-123",
            )

        archive_output.assert_awaited_once()
        upload_artifacts.assert_awaited_once()

    async def test_run_agent_preserves_agent_error_when_terminal_upload_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd="exit 23",
            final_output="/logs",
            output_artifacts=["artifacts/result.json"],
        )
        upload_artifacts = AsyncMock()

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command.startswith("mkdir -p") or command == "test -e /logs":
                return ExecResult(exit_code=0, output="")
            raise AssertionError(f"unexpected command: {command}")

        async def fail_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            raise AgentRunFailedError("agent exited 23")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "stream_command_output", fail_agent)
        monkeypatch.setattr(
            sandbox_module,
            "archive_and_upload_output",
            AsyncMock(side_effect=OutputArtifactError("terminal upload failed")),
        )
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", upload_artifacts)

        sandbox = Mock(id="sandbox-123", name="task-alias")
        with pytest.raises(AgentRunFailedError, match="agent exited 23"):
            await run_agent(
                sandbox,
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/testbed",
                object_store=store,
                agent_output_s3_key="benchmarks/benchmark-123/task_0/agent_output.tar.gz",
                benchmark_id="benchmark-123",
            )

        upload_artifacts.assert_awaited_once()

    async def test_run_agent_threads_benchmark_id_to_archive_and_upload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd="echo done",
            final_output="/tmp/agent_output",
        )
        archive_calls: list[str] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command.startswith("mkdir -p") or command.startswith("test -e"):
                return ExecResult(exit_code=0)
            raise AssertionError(f"unexpected command: {command}")

        async def fake_stream_command_output(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            return None, 0.0

        async def fake_archive_and_upload_output(
            _sandbox: Any,
            output_path: str,
            _s3_key: str,
            _aws_runtime: AWSRuntime,
            *,
            benchmark_id: str | None = None,
            task_id: str | None = None,
            execution_is_current: Callable[[], bool] | None = None,
        ) -> None:
            archive_calls.append(f"{benchmark_id}:{task_id}:{output_path}")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "stream_command_output", fake_stream_command_output)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", fake_archive_and_upload_output)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        await run_agent(
            mock_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            lambda _msg: None,
            "/testbed",
            object_store=store,
            agent_output_s3_key="benchmarks/run/task/agent_output.tar.gz",
            benchmark_id="benchmark-123",
        )

        assert archive_calls == ["benchmark-123:task_0:/tmp/agent_output"]

        archive_calls.clear()
        await run_agent(
            mock_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            lambda _msg: None,
            "/testbed",
            object_store=store,
            agent_output_s3_key="benchmarks/run/task/agent_output.tar.gz",
            benchmark_id="benchmark-123",
            execution_is_current=lambda: False,
        )
        assert archive_calls == []

    async def test_run_agent_wraps_compose_runtime_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        """Compose runtime sources should route agent setup and execution through the wrapper.

        Test cases:
        - The mkdir and stream execution helpers receive a ComposeSandbox when runtime_source is compose.
        """
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd="echo done",
        )
        observed_sandboxes: list[Any] = []

        async def fake_exec(sandbox: Any, command: str) -> ExecResult:
            observed_sandboxes.append(sandbox)
            assert command == "mkdir -p /workspace"
            return ExecResult(exit_code=0)

        async def fake_stream_command_output(sandbox: Any, command: str, _log_output: Any) -> tuple[None, float]:
            observed_sandboxes.append(sandbox)
            assert command == "cd /workspace && PYTHONSAFEPATH=1 echo done"
            return None, 0.0

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "stream_command_output", fake_stream_command_output)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.state = "started"

        await run_agent(
            mock_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            lambda _msg: None,
            "/workspace",
            object_store=store,
            runtime_source=ComposeSource(
                outer=ImageSource(image="docker:28.3.3-dind"),
                compose_command="docker compose -f /harbor/compose.yaml",
            ),
        )

        assert observed_sandboxes
        assert all(isinstance(sandbox, ComposeSandbox) for sandbox in observed_sandboxes)

    async def test_run_agent_shell_wraps_agent_timeout_command(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        store = _mock_object_store()
        """Task timeouts should apply to the full shell-form agent command.

        Test cases:
        - A command with environment assignment and shell chaining is passed to timeout through sh -c.
        """
        run_cmd = "FOO=bar python run.py && echo done"
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd=run_cmd,
        )
        observed_commands: list[str] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            assert command == "mkdir -p /workspace"
            return ExecResult(exit_code=0)

        async def fake_stream_command_output(_sandbox: Any, command: str, _log_output: Any) -> tuple[None, float]:
            observed_commands.append(command)
            return None, 0.0

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "stream_command_output", fake_stream_command_output)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        await run_agent(
            mock_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            lambda _msg: None,
            "/workspace",
            object_store=store,
            agent_timeout=2.5,
        )

        assert observed_commands == [f"cd /workspace && PYTHONSAFEPATH=1 timeout 2.5 sh -c {shlex.quote(run_cmd)}"]

    async def test_run_agent_none_timeout_keeps_controlled_capability_dormant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class LegacyOnlySandbox:
            id = "sandbox-123"
            name = "task-alias"

            @property
            def generation_containment(self) -> Never:
                raise AssertionError("runtime capability must stay dormant")

            async def probe_generation_containment(self) -> Never:
                raise AssertionError("runtime probe must stay dormant")

            def controlled_workload(self, *_args: Any, **_kwargs: Any) -> Never:
                raise AssertionError("controlled factory must stay dormant")

        legacy_stream = AsyncMock(return_value=(None, 0.0))
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "_stream_command_output_with_egress_allowlist", legacy_stream)

        await run_agent(
            cast(Any, LegacyOnlySandbox()),
            _controlled_contract(),
            "/tmp/problem.txt",
            "task_0",
            _ignore_output,
            "/workspace",
            object_store=_mock_object_store(),
            agent_timeout=None,
            task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
        )

        legacy_stream.assert_awaited_once()

    @pytest.mark.parametrize(
        ("agent_version", "task_version", "expected"),
        [
            (None, 1, False),
            (1, None, False),
            (1, 2, False),
            (2, 2, False),
            (1, 1, True),
        ],
    )
    def test_controlled_generation_requires_exact_bilateral_v1(
        self, agent_version: int | None, task_version: int | None, expected: bool
    ) -> None:
        contract = (
            _controlled_contract(version=agent_version)
            if agent_version is not None
            else AgentContractRequest(name="test-agent", install_cmd="", run_cmd="echo done")
        )
        task_containment = (
            GenerationContainment(type="linux_pid_namespace", version=task_version)
            if task_version is not None
            else None
        )

        assert _controlled_generation_selected(contract, task_containment, agent_timeout=10.0) is expected

    async def test_mismatched_declarations_preserve_legacy_timeout_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        legacy_stream = AsyncMock(return_value=(AgentCausedExitReason.TIMEOUT, 2.5))
        controlled_stream = AsyncMock(side_effect=AssertionError("controlled path selected"))
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "_stream_command_output_with_egress_allowlist", legacy_stream)
        monkeypatch.setattr(sandbox_module, "_stream_controlled_output_with_egress_allowlist", controlled_stream)
        sandbox = Mock(id="sandbox-123", name="task-alias")

        reason, _ = await run_agent(
            sandbox,
            _controlled_contract(),
            "/tmp/problem.txt",
            "task_0",
            _ignore_output,
            "/workspace",
            object_store=_mock_object_store(),
            agent_timeout=2.5,
            task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=2),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        controlled_stream.assert_not_awaited()
        assert legacy_stream.await_args is not None
        assert "timeout 2.5 sh -c" in legacy_stream.await_args.args[1]

    @pytest.mark.parametrize("timeout", [0.0, -1.0, float("inf"), float("nan")])
    def test_controlled_generation_rejects_invalid_matched_timeout(self, timeout: float) -> None:
        with pytest.raises(InvalidSandboxConfigurationError, match="positive finite"):
            _controlled_generation_selected(
                _controlled_contract(),
                GenerationContainment(type="linux_pid_namespace", version=1),
                timeout,
            )

    async def test_run_agent_selects_supported_controlled_workload(self, monkeypatch: pytest.MonkeyPatch) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))

        reason, duration = await run_agent(
            cast(Any, sandbox),
            _controlled_contract(),
            "/tmp/problem.txt",
            "task_0",
            _ignore_output,
            "/workspace",
            object_store=_mock_object_store(),
            agent_timeout=10.0,
            task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
        )

        assert reason is None
        assert duration >= 0
        assert sandbox.probe_calls == 1
        assert sandbox.controlled_calls == [("PYTHONSAFEPATH=1 echo done", "/workspace")]

    async def test_run_agent_timeout_collects_only_after_confirmed_kill(self, monkeypatch: pytest.MonkeyPatch) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={"final_output": "/logs", "egress_allowlist": ["example.com"]}
        )
        archive = AsyncMock()

        async def fake_exec(_sandbox: Any, _command: str) -> ExecResult:
            if _command == "test -e /logs":
                assert workload.kill_calls == 1
                assert workload.closed.is_set()
            return ExecResult(exit_code=0)

        async def fake_clear(_sandbox: Any, fail_on_error: bool) -> None:
            assert fail_on_error
            assert workload.kill_calls == 1
            assert workload.closed.is_set()

        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_apply_egress_allowlist", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_clear_egress_allowlist", fake_clear)
        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)

        reason, _ = await run_agent(
            cast(Any, sandbox),
            contract,
            "/tmp/problem.txt",
            "task_0",
            _ignore_output,
            "/workspace",
            object_store=_mock_object_store(),
            agent_output_s3_key="benchmarks/run/task/output.tar.gz",
            agent_timeout=0.001,
            task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        archive.assert_awaited_once()

    async def test_run_agent_unconfirmed_timeout_suppresses_collection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.block_kill = True
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={
                "final_output": "/logs",
                "egress_allowlist": ["example.com"],
            }
        )
        archive = AsyncMock()
        apply_egress = AsyncMock()
        clear_egress = AsyncMock()
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)
        monkeypatch.setattr(sandbox_module, "_apply_egress_allowlist", apply_egress)
        monkeypatch.setattr(sandbox_module, "_clear_egress_allowlist", clear_egress)
        monkeypatch.setattr(sandbox_module, "GENERATION_TERMINATION_GRACE_SECONDS", 0.01)

        with pytest.raises(GenerationTerminationUnconfirmedError):
            await run_agent(
                cast(Any, sandbox),
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                agent_output_s3_key="benchmarks/run/task/output.tar.gz",
                agent_timeout=0.001,
                task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
            )

        archive.assert_not_awaited()
        apply_egress.assert_awaited_once()
        clear_egress.assert_not_awaited()

    async def test_accounting_persistence_failure_skips_output_collection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(update={"final_output": "/logs"})
        archive = AsyncMock()
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persistence_error = RuntimeError("summary persistence failed")

        async def fail_persistence(
            _summary: ExternalServiceAccountingSummary,
        ) -> None:
            raise persistence_error

        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)

        with pytest.raises(RuntimeError, match="summary persistence failed") as raised:
            await run_agent(
                cast(Any, sandbox),
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                agent_output_s3_key="benchmarks/run/task/output.tar.gz",
                agent_timeout=10.0,
                task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
                external_service_deadline=controller,
                on_external_service_sealed=fail_persistence,
            )

        assert raised.value is persistence_error
        archive.assert_not_awaited()

    async def test_run_agent_rejects_unsupported_effective_sandbox_before_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = _FakeControlledSandbox(_FakeControlledWorkload())
        sandbox.generation_containment = None
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())

        with pytest.raises(InvalidSandboxConfigurationError, match="does not support"):
            await run_agent(
                cast(Any, sandbox),
                _controlled_contract(),
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                agent_timeout=10.0,
                task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
            )

        assert sandbox.controlled_calls == []

    async def test_run_agent_probe_failure_prevents_controlled_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = _FakeControlledSandbox(_FakeControlledWorkload())
        probe = AsyncMock(side_effect=ProviderSandboxError("probe failed"))
        monkeypatch.setattr(sandbox, "probe_generation_containment", probe)
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())

        with pytest.raises(ProviderSandboxError, match="probe failed"):
            await run_agent(
                cast(Any, sandbox),
                _controlled_contract(),
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                agent_timeout=10.0,
                task_generation_containment=GenerationContainment(type="linux_pid_namespace", version=1),
            )

        assert sandbox.controlled_calls == []

    def test_controlled_completion_at_deadline_belongs_to_timeout(self) -> None:
        result = _CompletedControlledWorkload(exit_code=0, absence_confirmed_at=10.0)

        assert not _controlled_completion_precedes_deadline(result, 10.0)
        assert _controlled_completion_precedes_deadline(result, 10.1)

    async def test_controlled_completion_at_deadline_runs_timeout_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real_loop = asyncio.get_running_loop()
        started_at = real_loop.time()
        clock = Mock()
        clock.time.return_value = started_at
        monkeypatch.setattr(sandbox_module.asyncio, "get_running_loop", lambda: clock)

        workload = _FakeControlledWorkload(absence_confirmed_at=started_at + 1.0)
        sandbox = _FakeControlledSandbox(workload)

        reason, duration = await _stream_controlled_output(
            cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 1.0
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert duration == 1.0
        assert workload.kill_calls == 1

    async def test_controlled_delayed_arbitration_uses_recorded_confirmation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        real_wait = asyncio.wait

        async def delayed_wait(
            tasks: set[asyncio.Task[Any]], *, return_when: str
        ) -> tuple[set[asyncio.Task[Any]], set[asyncio.Task[Any]]]:
            await asyncio.sleep(0.01)
            return await real_wait(tasks, timeout=0, return_when=return_when)

        monkeypatch.setattr(asyncio, "wait", delayed_wait)

        reason, _ = await _stream_controlled_output(
            cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 0.005
        )

        assert reason is None
        assert workload.kill_calls == 0

    async def test_controlled_timeout_freezes_cause_over_losing_os_kill(self) -> None:
        workload = _FakeControlledWorkload(exit_code=137, wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)

        reason, duration = await _stream_controlled_output(
            cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 0.001
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert duration == 0.001
        assert workload.kill_calls == 1

    @pytest.mark.parametrize(
        ("exit_code", "expected"),
        [(0, None), (137, AgentCausedExitReason.OS_KILLED)],
    )
    async def test_controlled_predeadline_exit_classification(
        self, exit_code: int, expected: AgentCausedExitReason | None
    ) -> None:
        workload = _FakeControlledWorkload(exit_code=exit_code)
        sandbox = _FakeControlledSandbox(workload)

        reason, _ = await _stream_controlled_output(cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 10.0)

        assert reason == expected
        assert workload.kill_calls == 0

    async def test_controlled_natural_exit_124_is_not_timeout(self) -> None:
        workload = _FakeControlledWorkload(exit_code=124)
        sandbox = _FakeControlledSandbox(workload)

        with pytest.raises(AgentRunFailedError, match="exit code 124"):
            await _stream_controlled_output(cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 10.0)

    async def test_controlled_creation_consumes_generation_allowance(self) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        original_factory = sandbox.controlled_workload

        def delayed_factory(command: str, *, cwd: str | None = None) -> _FakeControlledWorkload:
            time.sleep(0.02)
            return original_factory(command, cwd=cwd)

        sandbox.controlled_workload = delayed_factory  # type: ignore[method-assign]

        reason, _ = await _stream_controlled_output(
            cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 0.001
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert workload.kill_calls == 1

    async def test_controlled_cancellation_during_natural_output_join_restores_egress(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload(output_release=asyncio.Event())
        sandbox = _FakeControlledSandbox(workload)
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()
        real_wait = asyncio.wait
        join_started = asyncio.Event()

        async def observed_wait(
            tasks: set[asyncio.Task[Any]],
            *,
            timeout: float | None = None,
            return_when: str = asyncio.ALL_COMPLETED,
        ) -> tuple[set[asyncio.Task[Any]], set[asyncio.Task[Any]]]:
            if len(tasks) == 1 and return_when == asyncio.ALL_COMPLETED:
                join_started.set()
            return await real_wait(tasks, timeout=timeout, return_when=return_when)

        monkeypatch.setattr(sandbox_module.asyncio, "wait", observed_wait)
        task = asyncio.create_task(
            _stream_controlled_output_with_egress_allowlist(
                controlled_sandbox, "echo done", "/workspace", _ignore_output, ["example.com"], 10.0
            )
        )
        await asyncio.wait_for(join_started.wait(), timeout=1)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        assert workload.kill_calls == 1
        controlled_sandbox.clear_egress_rules.assert_awaited_once_with()

    async def test_controlled_cancellation_during_timeout_output_drain_restores_egress(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), output_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()
        real_wait = asyncio.wait
        join_started = asyncio.Event()

        async def observed_wait(
            tasks: set[asyncio.Task[Any]],
            *,
            timeout: float | None = None,
            return_when: str = asyncio.ALL_COMPLETED,
        ) -> tuple[set[asyncio.Task[Any]], set[asyncio.Task[Any]]]:
            if len(tasks) == 1 and return_when == asyncio.ALL_COMPLETED:
                join_started.set()
            return await real_wait(tasks, timeout=timeout, return_when=return_when)

        monkeypatch.setattr(sandbox_module.asyncio, "wait", observed_wait)
        task = asyncio.create_task(
            _stream_controlled_output_with_egress_allowlist(
                controlled_sandbox, "echo done", "/workspace", _ignore_output, ["example.com"], 0.001
            )
        )
        await asyncio.wait_for(join_started.wait(), timeout=1)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        assert workload.kill_calls == 2
        controlled_sandbox.clear_egress_rules.assert_awaited_once_with()

    async def test_controlled_deadline_kill_succeeds_within_absolute_grace(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), kill_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        real_timeout_at = asyncio.timeout_at
        timeout_calls: list[tuple[float, float]] = []

        def observed_timeout_at(when: float | None) -> asyncio.Timeout:
            assert when is not None
            timeout_calls.append((when, asyncio.get_running_loop().time()))
            return real_timeout_at(when)

        monkeypatch.setattr(sandbox_module.asyncio, "timeout_at", observed_timeout_at)
        task = asyncio.create_task(
            _stream_controlled_output(cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 0.001)
        )
        await workload.kill_started.wait()
        assert timeout_calls
        absolute_deadline, kill_started_at = timeout_calls[0]
        generation_deadline = absolute_deadline - sandbox_module.GENERATION_TERMINATION_GRACE_SECONDS
        while asyncio.get_running_loop().time() <= generation_deadline:
            await asyncio.sleep(0)
        assert workload.kill_release is not None
        workload.kill_release.set()
        reason, duration = await task
        assert reason == AgentCausedExitReason.TIMEOUT
        assert duration == 0.001
        assert absolute_deadline > kill_started_at
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

    async def test_controlled_deadline_requires_kill_confirmation_within_grace(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.block_kill = True
        sandbox = _FakeControlledSandbox(workload)
        original_factory = sandbox.controlled_workload
        real_timeout_at = asyncio.timeout_at
        timeout_calls: list[tuple[float, float]] = []

        def delayed_factory(command: str, *, cwd: str | None = None) -> _FakeControlledWorkload:
            time.sleep(0.02)
            return original_factory(command, cwd=cwd)

        def observed_timeout_at(when: float | None) -> asyncio.Timeout:
            assert when is not None
            timeout_calls.append((when, asyncio.get_running_loop().time()))
            return real_timeout_at(when)

        sandbox.controlled_workload = delayed_factory  # type: ignore[method-assign]
        monkeypatch.setattr(sandbox_module.asyncio, "timeout_at", observed_timeout_at)
        monkeypatch.setattr(sandbox_module, "GENERATION_TERMINATION_GRACE_SECONDS", 0.01)

        with pytest.raises(GenerationTerminationUnconfirmedError):
            await _stream_controlled_output(cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 0.001)

        assert len(timeout_calls) == 1
        absolute_deadline, kill_started_at = timeout_calls[0]
        assert absolute_deadline < kill_started_at
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

    async def test_external_credit_at_deadline_resumes_then_seals(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real_timeout_at = asyncio.timeout_at
        timeout_deadlines: list[float] = []

        def capture_timeout_at(when: float | None) -> asyncio.Timeout:
            assert when is not None
            timeout_deadlines.append(when)
            return real_timeout_at(when)

        monkeypatch.setattr(sandbox_module.asyncio, "timeout_at", capture_timeout_at)
        started_before = asyncio.get_running_loop().time()
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        client = _FakeAccountingClient(overhead_ms=0, begin_overhead_ms=20)
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.01,
            credit_cap_seconds=0.02,
        )
        persisted_after_kill: list[tuple[ExternalServiceAccountingSummary, int, bool]] = []

        async def persist(summary: ExternalServiceAccountingSummary) -> None:
            persisted_after_kill.append((summary, workload.kill_calls, workload.wait_finished.is_set()))

        reason, duration = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            0.01,
            controller,
            persist,
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert abs(duration - 0.03) < 1e-9
        effective_kill_deadline = timeout_deadlines[-1] - sandbox_module.GENERATION_TERMINATION_GRACE_SECONDS
        assert 0.03 <= effective_kill_deadline - started_before < 0.05
        assert client.decisions == ["RESUME", "SEAL"]
        assert workload.kill_calls == 1
        assert persisted_after_kill[0][1] == 1
        assert persisted_after_kill[0][2] is True
        assert persisted_after_kill[0][0].external_service_credit_applied_seconds == 0.02

    async def test_delayed_arbitration_with_expired_credit_seals_at_frozen_deadline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        events: list[str] = []

        class OrderedWorkload(_FakeControlledWorkload):
            async def kill(self) -> None:
                events.append("kill")
                await super().kill()

        workload = OrderedWorkload(wait_release=asyncio.Event(), natural=False)

        class DelayedCreditClient(_FakeAccountingClient):
            async def begin_arbitration(self, _session_id: str) -> AccountingSessionSnapshot:
                self.begin_calls += 1
                await asyncio.sleep(0.02)
                self.overhead_ms = 10
                return _accounting_snapshot(
                    state=AccountingSessionState.ARBITRATING,
                    overhead_ms=10,
                    revision=1,
                    epoch=1,
                )

            async def resolve_arbitration(self, session_id: str, decision: Any) -> AccountingSessionSnapshot:
                events.append(str(decision))
                return await super().resolve_arbitration(session_id, decision)

        client = DelayedCreditClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.005,
            credit_cap_seconds=0.01,
        )
        real_timeout_at = asyncio.timeout_at
        timeout_deadlines: list[float] = []

        def capture_timeout_at(when: float | None) -> asyncio.Timeout:
            assert when is not None
            timeout_deadlines.append(when)
            return real_timeout_at(when)

        monkeypatch.setattr(sandbox_module.asyncio, "timeout_at", capture_timeout_at)
        started_before = asyncio.get_running_loop().time()

        reason, duration = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            0.005,
            controller,
            AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert abs(duration - 0.015) < 1e-9
        assert client.decisions == ["SEAL"]
        assert events == ["SEAL", "kill"]
        grace_deadlines = [value for value in timeout_deadlines if value - started_before > 1.0]
        assert len(grace_deadlines) == 1
        recomputed_deadline = grace_deadlines[0] - sandbox_module.GENERATION_TERMINATION_GRACE_SECONDS
        assert 0.015 <= recomputed_deadline - started_before < 0.025

    async def test_refresh_credit_extends_from_immutable_start(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        client = _FakeAccountingClient(overhead_ms=20)
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.01,
            credit_cap_seconds=0.02,
        )

        reason, duration = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            0.01,
            controller,
            AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert abs(duration - 0.03) < 1e-9
        assert client.read_calls >= 1
        assert client.decisions == ["SEAL"]

    async def test_transient_refresh_error_retries_within_lead_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)

        class TransientRefreshClient(_FakeAccountingClient):
            async def read_session(self, _session_id: str) -> AccountingSessionSnapshot:
                self.read_calls += 1
                if self.read_calls == 1:
                    raise RuntimeError("transient refresh")
                return _accounting_snapshot(revision=1)

        client = TransientRefreshClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.01,
            credit_cap_seconds=1.0,
        )
        monkeypatch.setattr(sandbox_module, "EXTERNAL_SERVICE_REFRESH_RETRY_SECONDS", 0.0)

        reason, _ = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            0.01,
            controller,
            AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert client.read_calls == 2
        assert client.begin_calls == 1

    async def test_exhausted_refresh_window_proceeds_to_arbitration(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)

        class BlockingRefreshClient(_FakeAccountingClient):
            async def read_session(self, _session_id: str) -> AccountingSessionSnapshot:
                self.read_calls += 1
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        client = BlockingRefreshClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.005,
            credit_cap_seconds=1.0,
        )

        reason, _ = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            0.005,
            controller,
            AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert client.read_calls == 1
        assert client.begin_calls == 1
        assert client.decisions == ["SEAL"]
        assert workload.kill_calls == 1

    async def test_failed_refresh_task_cannot_defeat_natural_completion(self) -> None:
        workload = _FakeControlledWorkload()
        client = _FakeAccountingClient(read_error=asyncio.CancelledError())
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted = AsyncMock()

        reason, _ = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            10.0,
            controller,
            persisted,
        )

        assert reason is None
        assert workload.kill_calls == 0
        assert client.decisions == ["SEAL"]
        persisted.assert_awaited_once()

    async def test_completion_during_arbitration_within_credit_is_natural(self) -> None:
        wait_release = asyncio.Event()
        workload = _FakeControlledWorkload(wait_release=wait_release, natural=False)

        class CompletingArbitrationClient(_FakeAccountingClient):
            async def begin_arbitration(self, _session_id: str) -> AccountingSessionSnapshot:
                self.begin_calls += 1
                self.overhead_ms = 20
                wait_release.set()
                workload.closed.set()
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                return _accounting_snapshot(
                    state=AccountingSessionState.ARBITRATING,
                    overhead_ms=20,
                    revision=1,
                    epoch=1,
                )

        client = CompletingArbitrationClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.005,
            credit_cap_seconds=0.02,
        )
        persisted = AsyncMock()

        reason, _ = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            0.005,
            controller,
            persisted,
        )

        assert reason is None
        assert workload.kill_calls == 0
        assert client.decisions == ["SEAL"]
        persisted.assert_awaited_once()

    async def test_accounting_termination_error_is_not_masked_by_cleanup(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.kill_error = ProviderSandboxError("kill failed")
        client = _FakeAccountingClient(read_error=RuntimeError("refresh failed"))
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.005,
            credit_cap_seconds=1.0,
        )

        with pytest.raises(GenerationTerminationUnconfirmedError) as raised:
            await _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                0.005,
                controller,
                AsyncMock(),
            )

        assert isinstance(raised.value.__cause__, ProviderSandboxError)

    async def test_natural_completion_seals_before_return(self) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        client = _FakeAccountingClient(overhead_ms=5)
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted: list[ExternalServiceAccountingSummary] = []

        async def persist_natural(
            summary: ExternalServiceAccountingSummary,
        ) -> None:
            persisted.append(summary)

        reason, _ = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            10.0,
            controller,
            persist_natural,
        )

        assert reason is None
        assert client.decisions == ["SEAL"]
        assert workload.wait_finished.is_set()
        assert workload.kill_calls == 0
        assert persisted[0].external_service_overhead_seconds == 0.005

    async def test_deadline_control_error_kills_then_propagates_raw_error(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        control_error = RuntimeError("gateway unavailable")

        class FailingArbitrationClient(_FakeAccountingClient):
            async def begin_arbitration(self, _session_id: str) -> AccountingSessionSnapshot:
                raise control_error

        client = FailingArbitrationClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.001,
            credit_cap_seconds=1.0,
        )

        with pytest.raises(RuntimeError, match="gateway unavailable") as raised:
            await _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                0.001,
                controller,
                AsyncMock(),
            )

        assert raised.value is control_error
        assert workload.kill_calls == 1
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

    async def test_controlled_wait_error_kills_before_nonretryable_failure(self) -> None:
        workload = _FakeControlledWorkload(wait_error=SandboxNotFoundError("lost"), natural=False)
        sandbox = _FakeControlledSandbox(workload)

        with pytest.raises(ControlledGenerationError):
            await _stream_controlled_output(cast(Any, sandbox), "echo done", "/workspace", _ignore_output, 10.0)

        assert workload.kill_calls == 1
        assert workload.closed.is_set()

    async def test_controlled_output_error_after_start_is_nonretryable(self) -> None:
        workload = _FakeControlledWorkload()
        workload.output_error = SandboxNotFoundError("output lost")
        sandbox = _FakeControlledSandbox(workload)
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()

        with pytest.raises(ControlledGenerationError):
            await _stream_controlled_output_with_egress_allowlist(
                controlled_sandbox,
                "echo done",
                "/workspace",
                _ignore_output,
                ["example.com"],
                10.0,
            )

        assert workload.kill_calls == 1
        controlled_sandbox.clear_egress_rules.assert_awaited_once_with()

    async def test_controlled_output_error_preserves_egress_when_kill_fails(
        self,
    ) -> None:
        workload = _FakeControlledWorkload()
        workload.output_error = SandboxNotFoundError("output lost")
        workload.kill_error = ProviderSandboxError("kill unavailable")
        sandbox = _FakeControlledSandbox(workload)
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()

        with pytest.raises(ControlledGenerationTerminationUnconfirmedError):
            await _stream_controlled_output_with_egress_allowlist(
                controlled_sandbox,
                "echo done",
                "/workspace",
                _ignore_output,
                ["example.com"],
                10.0,
            )

        assert workload.kill_calls == 1
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        controlled_sandbox.modify_egress_rules.assert_awaited_once_with(["example.com"])
        controlled_sandbox.clear_egress_rules.assert_not_awaited()

    async def test_controlled_cancellation_kills_before_propagation(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output_with_egress_allowlist(
                controlled_sandbox,
                "echo done",
                "/workspace",
                _ignore_output,
                ["example.com"],
                10.0,
            )
        )
        await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert workload.kill_calls == 1
        assert workload.closed.is_set()
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        controlled_sandbox.modify_egress_rules.assert_awaited_once_with(["example.com"])
        controlled_sandbox.clear_egress_rules.assert_awaited_once_with()

    async def test_controlled_cancellation_joins_children_when_kill_fails(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.kill_error = ProviderSandboxError("kill unavailable")
        sandbox = _FakeControlledSandbox(workload)
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output_with_egress_allowlist(
                controlled_sandbox,
                "echo done",
                "/workspace",
                _ignore_output,
                ["example.com"],
                10.0,
            )
        )
        await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(ControlledGenerationTerminationUnconfirmedError):
            await task

        assert workload.kill_calls == 1
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        controlled_sandbox.modify_egress_rules.assert_awaited_once_with(["example.com"])
        controlled_sandbox.clear_egress_rules.assert_not_awaited()


class TestSandboxRetry:
    """Sandbox retry callbacks and dependency-install retries."""

    def test_sandbox_retry_decorators_use_observability_retry_callbacks(self) -> None:
        upload_before_sleep = _upload_agent_artifacts.retry.before_sleep
        deps_before_sleep = _install_agent_dependencies_with_retries.retry.before_sleep

        assert upload_before_sleep is not None
        assert callable(upload_before_sleep)
        assert deps_before_sleep is not None
        assert callable(deps_before_sleep)

    async def test_install_agent_dependencies_retries_after_setup_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dependency setup should be bounded and retried when it hangs.

        Test cases:
        - The install command is wrapped in the 10 minute shell timeout.
        - A timed out setup attempt is retried by the existing dependency retry policy.
        """
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="apt-get update -qq && echo done",
            run_cmd="echo done",
        )
        observed_commands: list[str] = []
        setup_results: deque[tuple[AgentCausedExitReason | None, float]] = deque(
            [(AgentCausedExitReason.TIMEOUT, 600.0), (None, 2.0)]
        )

        async def fake_stream_command_output(
            _sandbox: Any,
            command: str,
            _log_output: Any,
        ) -> tuple[AgentCausedExitReason | None, float]:
            observed_commands.append(command)

            return setup_results.popleft()

        def log_output(_message: str) -> None:
            pass

        monkeypatch.setattr(sandbox_module, "stream_command_output", fake_stream_command_output)

        await _install_agent_dependencies(Mock(), contract, log_output)

        expected_command = "cd /bundle/test-agent && timeout 600 sh -c 'apt-get update -qq && echo done'"
        assert observed_commands == [expected_command, expected_command]

    async def test_install_agent_dependencies_uses_fixed_retry_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="bash setup.sh",
            run_cmd="echo done",
        )
        stream_command = AsyncMock(
            side_effect=[
                AgentRunFailedError("setup failed 1"),
                AgentRunFailedError("setup failed 2"),
                AgentRunFailedError("setup failed 3"),
                (None, 2.0),
            ]
        )
        sleep = AsyncMock()
        monkeypatch.setattr(sandbox_module, "stream_command_output", stream_command)
        monkeypatch.setattr(asyncio, "sleep", sleep)

        await _install_agent_dependencies(Mock(), contract, _ignore_output)

        assert stream_command.await_count == 4
        assert [call.args[0] for call in sleep.await_args_list] == [0.0, 10.0, 60.0]

    async def test_install_agent_dependencies_exhaustion_raises_and_final_mode_runs_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="bash setup.sh",
            run_cmd="echo done",
        )
        stream_command = AsyncMock(side_effect=AgentRunFailedError("setup failed"))
        sleep = AsyncMock()
        monkeypatch.setattr(sandbox_module, "stream_command_output", stream_command)
        monkeypatch.setattr(asyncio, "sleep", sleep)

        with pytest.raises(DependencySetupExhaustedError) as exc_info:
            await _install_agent_dependencies(Mock(), contract, _ignore_output)

        assert isinstance(exc_info.value.__cause__, AgentRunFailedError)
        assert stream_command.await_count == 4
        assert [call.args[0] for call in sleep.await_args_list] == [0.0, 10.0, 60.0]

        stream_command.reset_mock()
        sleep.reset_mock()
        mode = getattr(sandbox_module, "DependencySetupMode")

        with pytest.raises(AgentRunFailedError):
            await _install_agent_dependencies(
                Mock(),
                contract,
                _ignore_output,
                mode=mode.FINAL_FRESH_SANDBOX,
            )

        assert stream_command.await_count == 1
        sleep.assert_not_awaited()


class TestSandboxLifecycle:
    """Sandbox creation, execution, deletion, and telemetry behavior."""

    def test_metric_source_name_drops_high_cardinality_tag_and_digest(self) -> None:
        metric_source_name = getattr(sandbox_module, "_metric_source_name")

        assert metric_source_name(ImageSource(image="ghcr.io/vals/swebench:latest")) == "ghcr.io/vals/swebench"
        assert (
            metric_source_name(ImageSource(image="registry.local:5000/vals/swebench@sha256:abcdef"))
            == "registry.local:5000/vals/swebench"
        )
        assert (
            metric_source_name(
                ComposeSource(
                    outer=ImageSource(image="public.ecr.aws/vals/harbor:task@sha256:abcdef"),
                    compose_command="docker compose -f /harbor/compose.yaml",
                )
            )
            == "public.ecr.aws/vals/harbor"
        )
        assert metric_source_name(SnapshotSource(snapshot="base-python")) == "snapshot"
        with pytest.raises(AssertionError, match="Expected code to be unreachable"):
            metric_source_name(cast(SandboxSource, object()))

    def test_compose_runtime_sandbox_wraps_only_compose_sources(self) -> None:
        """Compose sources should adapt only the runtime sandbox surface.

        Test cases:
        - ComposeSource returns a ComposeSandbox wrapper around the outer sandbox.
        - ImageSource returns the original sandbox unchanged.
        """
        runtime_sandbox = getattr(sandbox_module, "runtime_sandbox")
        outer_sandbox = Mock()
        compose_source = ComposeSource(
            outer=ImageSource(image="docker:28.3.3-dind"),
            compose_command="docker compose -f /harbor/compose.yaml",
        )

        wrapped = runtime_sandbox(outer_sandbox, compose_source)
        unwrapped = runtime_sandbox(outer_sandbox, compose_source.outer)

        assert isinstance(wrapped, ComposeSandbox)
        assert unwrapped is outer_sandbox

    def test_sandbox_span_helpers_set_safe_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        span_attributes: dict[str, str | int] = {}

        mock_span = Mock()

        def mock_set_attribute(key: str, value: str | int) -> None:
            span_attributes[key] = value

        mock_span.set_attribute.side_effect = mock_set_attribute
        monkeypatch.setattr("tracker.sandbox.trace.get_current_span", lambda: mock_span)

        create_span_attrs = getattr(sandbox_module, "_set_sandbox_create_span_attributes")
        sandbox_span_attrs = getattr(sandbox_module, "_set_sandbox_span_attributes")
        resources = Resources(vcpu=2, memory=4, disk=5)

        create_span_attrs("task-alias", ImageSource(image="ghcr.io/vals/swebench:latest"), resources)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.state = "started"
        sandbox_span_attrs(mock_sandbox)

        assert span_attributes == {
            "valkyrie.sandbox_name": "task-alias",
            "valkyrie.image": "ghcr.io/vals/swebench:latest",
            "valkyrie.resources.vcpu": 2,
            "valkyrie.resources.memory": 4,
            "valkyrie.resources.disk": 5,
            "valkyrie.sandbox_id": "sandbox-123",
            "valkyrie.sandbox_state": "started",
        }

    async def test_create_sandbox_passes_request_and_records_returned_identity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        span_calls: list[tuple[str, str, int]] = []

        def fake_create_span_attrs(sandbox_name: str, source: Any, resources: Any) -> None:
            span_calls.append((sandbox_name, source.image, resources.vcpu))

        monkeypatch.setattr(sandbox_module, "_set_sandbox_create_span_attributes", fake_create_span_attrs)

        span_attributes: dict[str, str] = {}
        span = Mock(spec=sandbox_module.trace.Span)
        span.get_span_context.return_value = sandbox_module.trace.INVALID_SPAN_CONTEXT
        span.set_attribute.side_effect = lambda key, value: span_attributes.update({key: value})
        monkeypatch.setattr(sandbox_module.trace, "get_current_span", lambda context=None: span)

        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-created-123"
        mock_sandbox.name = "provider-returned-name"
        mock_sandbox.state = "started"
        provider = AsyncMock()
        provider.create_sandbox = AsyncMock(return_value=mock_sandbox)

        resources = Resources(vcpu=2, memory=4, disk=5)
        volumes = [
            VolumeMount(
                name="shared-fixtures",
                mount_path="/fixtures",
                read_only=True,
                subpath="{run_id}",
            )
        ]
        sandbox_secrets = {"TAVILY_API_KEY": "daytona-tavily"}
        sandbox = await _create_sandbox(
            provider,
            "task-alias",
            ImageSource(image="ghcr.io/vals/swebench:latest"),
            resources,
            labels={"run-id": "run-123"},
            sandbox_secrets=sandbox_secrets,
            volumes=volumes,
        )

        assert sandbox is mock_sandbox
        assert span_calls == [("task-alias", "ghcr.io/vals/swebench:latest", 2)]
        assert span_attributes == {
            "valkyrie.sandbox_id": "sandbox-created-123",
            "valkyrie.sandbox_name": "provider-returned-name",
            "valkyrie.sandbox_state": "started",
        }
        request = provider.create_sandbox.await_args.args[0]
        assert request.name == "task-alias"
        assert request.resources == resources
        assert request.labels == {"run-id": "run-123"}
        assert request.sandbox_secrets == sandbox_secrets
        assert request.volumes == volumes
        assert request.auto_stop_interval == sandbox_module.SANDBOX_AUTO_STOP_INTERVAL
        assert request.create_timeout == sandbox_module.SANDBOX_CREATE_TIMEOUT

    async def test_create_sandbox_rejects_plaintext_and_secret_environment_collision(self) -> None:
        provider = AsyncMock()

        with pytest.raises(InvalidSandboxConfigurationError, match="TAVILY_API_KEY"):
            await _create_sandbox(
                provider,
                "task-alias",
                ImageSource(image="ghcr.io/vals/swebench:latest"),
                Resources(vcpu=1, memory=2, disk=3),
                env_vars={"TAVILY_API_KEY": "plaintext-value"},
                sandbox_secrets={"TAVILY_API_KEY": "daytona-tavily"},
            )

        provider.create_sandbox.assert_not_awaited()

    async def test_create_sandbox_unwraps_compose_source_before_provider_create(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Compose sources should create the outer sandbox through the provider.

        Test cases:
        - Provider receives the compose outer image source instead of ComposeSource.
        - Sandbox create span attributes use the provider source.
        """
        span_calls: list[tuple[str, str, int]] = []

        def fake_create_span_attrs(sandbox_name: str, source: Any, resources: Any) -> None:
            span_calls.append((sandbox_name, source.image, resources.vcpu))

        monkeypatch.setattr(sandbox_module, "_set_sandbox_create_span_attributes", fake_create_span_attrs)

        mock_sandbox = AsyncMock()
        provider = AsyncMock()
        provider.create_sandbox = AsyncMock(return_value=mock_sandbox)

        resources = Resources(vcpu=2, memory=4, disk=5)
        compose_source = ComposeSource(
            outer=ImageSource(image="docker:28.3.3-dind"),
            compose_command="docker compose -f /harbor/compose.yaml",
        )
        sandbox = await _create_sandbox(provider, "task-alias", compose_source, resources)

        assert sandbox is mock_sandbox
        assert span_calls == [("task-alias", "docker:28.3.3-dind", 2)]
        request = provider.create_sandbox.await_args.args[0]
        assert request.source == compose_source.outer
        assert request.resources == resources

    async def test_delete_sandbox_raises_provider_errors(self) -> None:
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.labels = None

        provider = AsyncMock()
        provider.delete_sandbox = AsyncMock(side_effect=ProviderSandboxError("state change"))

        with pytest.raises(ProviderSandboxError, match="state change"):
            await _delete_sandbox(mock_sandbox, provider, initiated_by="force_stop")

        provider.delete_sandbox.assert_awaited_once_with("sandbox-123")

    async def test_exec_wraps_provider_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sandbox_module, "_set_sandbox_span_attributes", Mock(), raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.exec = AsyncMock(side_effect=ProviderSandboxError("exec failed"))

        with pytest.raises(SandboxError, match="exec failed"):
            await _exec(mock_sandbox, "echo hi")

    async def test_create_sandbox_emits_create_duration_and_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        distributions: list[tuple[str, float, dict[str, str]]] = []
        context_calls: list[tuple[str, str]] = []

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return mock_sandbox

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_set_sandbox_context(sandbox: Any, *, image: str | None = None) -> None:
            context_calls.append((sandbox.id, image or ""))

        monotonic_values: deque[float] = deque([10.0, 13.5])

        def fake_monotonic() -> float:
            if monotonic_values:
                return monotonic_values.popleft()
            return 13.5

        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", AsyncMock())
        monkeypatch.setattr(sandbox_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(sandbox_module, "set_sandbox_context", fake_set_sandbox_context, raising=False)
        monkeypatch.setattr("tracker.sandbox.time.monotonic", fake_monotonic)

        resources = Resources(vcpu=2, memory=4, disk=5)
        async with create_sandbox(
            provider=AsyncMock(),
            sandbox_name="task-alias",
            source=ImageSource(image="ghcr.io/vals/swebench:latest"),
            resources=resources,
            creation_semaphore=asyncio.Semaphore(1),
        ) as sandbox:
            assert sandbox is mock_sandbox

        assert distributions == [
            (
                "valkyrie.sandbox.create.duration",
                3.5,
                {"image": "ghcr.io/vals/swebench"},
            )
        ]
        assert context_calls == [("sandbox-123", "ghcr.io/vals/swebench:latest")]

    async def test_create_sandbox_randomizes_only_direct_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        created_names: list[str] = []
        mock_sandbox = AsyncMock(id="sandbox-123", name="task-alias")

        async def mock_create_sandbox(
            _provider: Any,
            sandbox_name: str,
            *_args: Any,
            **_kwargs: Any,
        ) -> AsyncMock:
            created_names.append(sandbox_name)

            return mock_sandbox

        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", AsyncMock())

        common_arguments = {
            "provider": AsyncMock(),
            "sandbox_name": "task-alias",
            "source": ImageSource(image="ghcr.io/vals/swebench:latest"),
            "resources": Resources(vcpu=2, memory=4, disk=5),
            "creation_semaphore": asyncio.Semaphore(1),
        }
        async with create_sandbox(**common_arguments):
            pass
        async with create_sandbox(**common_arguments, unique_name=False):
            pass

        assert created_names[0].startswith("task-alias_")
        assert created_names[0] != "task-alias"
        assert created_names[1] == "task-alias"

    async def test_targeted_snapshot_is_measured_and_deleted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.labels = None
        provider = AsyncMock()
        provider.create_sandbox.return_value = mock_sandbox
        distribution = Mock()
        set_sandbox_context = Mock()
        monkeypatch.setattr(sandbox_module, "distribution", distribution)
        monkeypatch.setattr(sandbox_module, "set_sandbox_context", set_sandbox_context)

        source = TargetedSnapshotSource(snapshot="masscan-linux-vm", target="us-west-3")
        async with create_sandbox(
            provider=provider,
            sandbox_name="task-alias",
            source=source,
            resources=Resources(vcpu=4, memory=16, disk=30),
            creation_semaphore=asyncio.Semaphore(1),
        ) as sandbox:
            assert sandbox is mock_sandbox

        request = provider.create_sandbox.await_args.args[0]
        assert request.source == source
        assert distribution.call_args.kwargs["tags"] == {"image": "snapshot"}
        set_sandbox_context.assert_called_once_with(mock_sandbox, image="snapshot")
        provider.delete_sandbox.assert_awaited_once_with(mock_sandbox.id)

    async def test_create_sandbox_emits_error_metric(self, monkeypatch: pytest.MonkeyPatch) -> None:
        create_error = RuntimeError("create failed")
        increments: list[tuple[str, dict[str, str]]] = []

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Never:
            raise create_error

        def fake_incr(name: str, _value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)

        resources = Resources(vcpu=2, memory=4, disk=5)
        with pytest.raises(RuntimeError):
            async with create_sandbox(
                provider=AsyncMock(),
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=resources,
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        assert increments == [("valkyrie.sandbox.create.errors", {"error_class": "RuntimeError"})]

    async def test_create_sandbox_deletes_resource_when_cancelled_during_creation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cancellation during remote creation must not leave a sandbox running.

        Test cases:
        - Remote creation completes after the caller is cancelled.
        - The completed sandbox is deleted before cancellation reaches the caller.
        """
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        active_sandbox_ids: set[str] = set()
        creation_started = asyncio.Event()
        release_creation = asyncio.Event()
        remote_creation_task: asyncio.Task[AsyncMock] | None = None

        async def remote_create() -> AsyncMock:
            creation_started.set()
            await release_creation.wait()
            active_sandbox_ids.add(mock_sandbox.id)

            return mock_sandbox

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncMock:
            nonlocal remote_creation_task
            remote_creation_task = asyncio.create_task(remote_create())

            return await asyncio.shield(remote_creation_task)

        deletion_initiators: list[Any] = []

        async def mock_delete_sandbox(sandbox: AsyncMock, _provider: Any, **kwargs: Any) -> None:
            active_sandbox_ids.remove(sandbox.id)
            deletion_initiators.append(kwargs.get("initiated_by"))

        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", mock_delete_sandbox)

        async def use_sandbox() -> None:
            async with create_sandbox(
                provider=AsyncMock(),
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=Resources(vcpu=2, memory=4, disk=5),
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        context_task = asyncio.create_task(use_sandbox())
        await creation_started.wait()

        context_task.cancel()
        release_creation.set()

        with pytest.raises(asyncio.CancelledError):
            await context_task

        assert remote_creation_task is not None
        await remote_creation_task
        assert active_sandbox_ids == set()
        assert deletion_initiators == ["create_cancelled"]

    async def test_create_sandbox_teardown_names_initiator(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Normal context-manager exit attributes the deletion to task_teardown in the audit trail."""
        mock_sandbox = Mock()

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Mock:
            return mock_sandbox

        delete_mock = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", delete_mock)
        monkeypatch.setattr(sandbox_module, "distribution", Mock())
        monkeypatch.setattr(sandbox_module, "set_sandbox_context", Mock())

        provider = Mock()
        async with create_sandbox(
            provider=provider,
            sandbox_name="task-alias",
            source=ImageSource(image="ghcr.io/vals/swebench:latest"),
            resources=Resources(vcpu=2, memory=4, disk=5),
            creation_semaphore=asyncio.Semaphore(1),
        ):
            pass

        delete_mock.assert_awaited_once_with(mock_sandbox, provider, initiated_by="task_teardown")

    async def test_create_sandbox_preserves_success_when_task_teardown_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.labels = {"Benchmark": "swebench", "Id": "bench-1", "Task": "task_0"}

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Mock:
            return mock_sandbox

        provider = Mock()
        provider.delete_sandbox = AsyncMock(side_effect=ProviderSandboxError("cleanup failed"))
        logger_mock = Mock()
        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "distribution", Mock())
        monkeypatch.setattr(sandbox_module, "set_sandbox_context", Mock())
        monkeypatch.setattr(sandbox_module, "logger", logger_mock)

        async with create_sandbox(
            provider=provider,
            sandbox_name="task-alias",
            source=ImageSource(image="ghcr.io/vals/swebench:latest"),
            resources=Resources(vcpu=2, memory=4, disk=5),
            creation_semaphore=asyncio.Semaphore(1),
        ):
            pass

        audit_call = logger_mock.info.call_args_list[-1]
        assert audit_call.args == ("sandbox.delete",)
        assert audit_call.kwargs["extra"] == {
            "sandbox_id": "sandbox-123",
            "sandbox_name": "task-alias",
            "benchmark_id": "bench-1",
            "benchmark_name": "swebench",
            "task_id": "task_0",
            "org_id": None,
            "initiated_by": "task_teardown",
            "outcome": "failed",
            "error": "SandboxError: cleanup failed",
        }

    async def test_create_sandbox_preserves_body_failure_when_task_teardown_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.labels = None

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Mock:
            return mock_sandbox

        provider = Mock()
        provider.delete_sandbox = AsyncMock(side_effect=ProviderSandboxError("cleanup failed"))
        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "distribution", Mock())
        monkeypatch.setattr(sandbox_module, "set_sandbox_context", Mock())

        primary_error = RuntimeError("primary failure")
        with pytest.raises(RuntimeError) as exc_info:
            async with create_sandbox(
                provider=provider,
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=Resources(vcpu=2, memory=4, disk=5),
                creation_semaphore=asyncio.Semaphore(1),
            ):
                raise primary_error

        assert exc_info.value is primary_error

    async def test_create_sandbox_propagates_provider_error_during_cancelled_creation_cleanup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.labels = None
        creation_started = asyncio.Event()
        release_creation = asyncio.Event()

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Mock:
            creation_started.set()
            await release_creation.wait()
            return mock_sandbox

        provider = Mock()
        provider.delete_sandbox = AsyncMock(side_effect=ProviderSandboxError("cleanup failed"))
        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)

        async def use_sandbox() -> None:
            async with create_sandbox(
                provider=provider,
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=Resources(vcpu=2, memory=4, disk=5),
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        context_task = asyncio.create_task(use_sandbox())
        await creation_started.wait()
        context_task.cancel()
        release_creation.set()

        with pytest.raises(ProviderSandboxError, match="cleanup failed"):
            await context_task

    async def test_create_sandbox_propagates_cancelled_task_teardown_and_audits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.labels = {"Benchmark": "swebench", "Id": "bench-1", "Task": "task_0"}

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Mock:
            return mock_sandbox

        provider = Mock()
        provider.delete_sandbox = AsyncMock(side_effect=asyncio.CancelledError())
        logger_mock = Mock()
        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "distribution", Mock())
        monkeypatch.setattr(sandbox_module, "set_sandbox_context", Mock())
        monkeypatch.setattr(sandbox_module, "logger", logger_mock)

        with pytest.raises(asyncio.CancelledError):
            async with create_sandbox(
                provider=provider,
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=Resources(vcpu=2, memory=4, disk=5),
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        audit_call = logger_mock.info.call_args_list[-1]
        assert audit_call.args == ("sandbox.delete",)
        assert audit_call.kwargs["extra"] == {
            "sandbox_id": "sandbox-123",
            "sandbox_name": "task-alias",
            "benchmark_id": "bench-1",
            "benchmark_name": "swebench",
            "task_id": "task_0",
            "org_id": None,
            "initiated_by": "task_teardown",
            "outcome": "cancelled",
            "error": None,
        }


class TestDeleteSandboxAudit:
    """Structured `sandbox.delete` audit records."""

    @staticmethod
    def _sandbox() -> AsyncMock:
        sandbox = AsyncMock()
        sandbox.id = "sandbox-123"
        sandbox.name = "task-alias"
        sandbox.labels = {"Benchmark": "swebench", "Id": "bench-1", "Task": "task_0"}
        return sandbox

    @pytest.mark.parametrize(
        ("provider_error", "outcome", "error"),
        [
            (None, "deleted", None),
            (SandboxNotFoundError("gone"), "already_gone", None),
            (ProviderSandboxError("state change"), "failed", "SandboxError: state change"),
            (RuntimeError("boom"), "failed", "RuntimeError: boom"),
        ],
        ids=["deleted", "already-gone", "provider-error", "unexpected-error"],
    )
    async def test_delete_sandbox_audits_every_outcome(
        self,
        monkeypatch: pytest.MonkeyPatch,
        provider_error: Exception | None,
        outcome: str,
        error: str | None,
    ) -> None:
        """Every attempt emits one record naming the initiator, the sandbox, and what happened to it."""
        logger_mock = Mock()
        monkeypatch.setattr(sandbox_module, "logger", logger_mock)
        provider = AsyncMock()
        provider.delete_sandbox = AsyncMock(side_effect=provider_error)

        # Of these outcomes only ProviderSandboxError propagates; test_delete_sandbox_raises_provider_errors pins that.
        with suppress(ProviderSandboxError):
            await _delete_sandbox(self._sandbox(), provider, initiated_by="force_stop", org_id="org-1")

        logger_mock.info.assert_called_once_with(
            "sandbox.delete",
            extra={
                "sandbox_id": "sandbox-123",
                "sandbox_name": "task-alias",
                "benchmark_id": "bench-1",
                "benchmark_name": "swebench",
                "task_id": "task_0",
                "org_id": "org-1",
                "initiated_by": "force_stop",
                "outcome": outcome,
                "error": error,
            },
        )

    async def test_delete_sandbox_audits_unlabelled_sandbox(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unlabelled sandboxes still audit; the other fields are pinned by the `deleted` case above."""
        logger_mock = Mock()
        monkeypatch.setattr(sandbox_module, "logger", logger_mock)
        sandbox = self._sandbox()
        sandbox.labels = None

        await _delete_sandbox(sandbox, AsyncMock(), initiated_by="task_teardown")

        logger_mock.info.assert_called_once()
        extra = logger_mock.info.call_args.kwargs["extra"]
        assert (extra["benchmark_id"], extra["benchmark_name"], extra["task_id"]) == (None, None, None)

    async def test_delete_sandbox_audits_cancelled_delete(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A delete cancelled mid-flight still leaves a record: the provider may have acted on it."""
        logger_mock = Mock()
        monkeypatch.setattr(sandbox_module, "logger", logger_mock)
        provider = AsyncMock()
        provider.delete_sandbox = AsyncMock(side_effect=asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await _delete_sandbox(self._sandbox(), provider, initiated_by="task_teardown")

        logger_mock.info.assert_called_once_with(
            "sandbox.delete",
            extra={
                "sandbox_id": "sandbox-123",
                "sandbox_name": "task-alias",
                "benchmark_id": "bench-1",
                "benchmark_name": "swebench",
                "task_id": "task_0",
                "org_id": None,
                "initiated_by": "task_teardown",
                "outcome": "cancelled",
                "error": None,
            },
        )


class TestUploadAgentArtifacts:
    """Agent artifact upload failure classification."""

    @pytest.mark.parametrize(
        "exit_code,retryable",
        [
            # Curl SSL failures are transient and need a new sandbox.
            (35, True),
            # Generic failures are deterministic and fail the task.
            (1, False),
        ],
    )
    async def test_exit_code_maps_to_retryable_exception(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        exit_code: int,
        retryable: bool,
    ) -> None:
        store = _mock_object_store()
        """Exit code 35 (curl SSL/TLS) raises SandboxSetupError so process_task retries
        with a fresh sandbox. All other non-zero exit codes raise the base SandboxError,
        which marks the task as failed without a sandbox retry.

        Test Cases:
            - Exit code 35 raises SandboxSetupError (retryable — triggers a new sandbox)
            - Other non-zero exit codes raise SandboxError but not SandboxSetupError (non-retryable)
        """
        mock_sandbox = AsyncMock()
        mock_sandbox.name = "test-sandbox"

        monkeypatch.setattr(
            sandbox_module,
            "_exec",
            AsyncMock(return_value=ExecResult(exit_code=exit_code, output="error output")),
        )
        store.temporary_download_url = AsyncMock(return_value="https://example.com/presigned")

        expected = SSLConnectionError if retryable else SandboxError
        with pytest.raises(expected) as exc_info:
            await upload_agent_artifacts(mock_sandbox, contract, "bench-123", store)

        if not retryable:
            assert not isinstance(exc_info.value, SandboxSetupError)


class TestEgressAllowlist:
    """Tracker-side egress rule handling around the agent command."""

    async def test_stream_command_output_scopes_egress_rules(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Apply egress rules only around a command that has an allowlist.

        Test cases:
        - A non-empty allowlist applies rules before streaming output.
        - Egress rules are cleared after the command completes.
        """
        events: list[str] = []

        async def mock_stream_command_output(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            events.append("stream")

            return None, 2.5

        async def mock_modify_egress_rules(allowed_addresses: list[str]) -> None:
            events.append(f"modify:{','.join(allowed_addresses)}")

        async def mock_clear_egress_rules() -> None:
            events.append("clear")

        monkeypatch.setattr(sandbox_module, "stream_command_output", mock_stream_command_output)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.modify_egress_rules = mock_modify_egress_rules
        mock_sandbox.clear_egress_rules = mock_clear_egress_rules

        def ignore_output(_message: str) -> None:
            pass

        result = await _stream_command_output_with_egress_allowlist(
            mock_sandbox,
            "run-agent.sh",
            on_output=ignore_output,
            allowed_addresses=["https://api.openai.com"],
        )

        assert result == (None, 2.5)
        assert events == ["modify:https://api.openai.com", "stream", "clear"]

    async def test_stream_command_output_skips_egress_rules_without_allowlist(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run commands normally when the contract has no egress allowlist.

        Test cases:
        - Empty allowlists call the existing stream_command_output path.
        - Provider egress methods are not called.
        """

        async def mock_stream_command_output(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            return None, 1.0

        monkeypatch.setattr(sandbox_module, "stream_command_output", mock_stream_command_output)

        mock_sandbox = Mock()
        mock_sandbox.modify_egress_rules = AsyncMock()
        mock_sandbox.clear_egress_rules = AsyncMock()

        def ignore_output(_message: str) -> None:
            pass

        result = await _stream_command_output_with_egress_allowlist(
            mock_sandbox,
            "run-agent.sh",
            on_output=ignore_output,
            allowed_addresses=[],
        )

        assert result == (None, 1.0)
        mock_sandbox.modify_egress_rules.assert_not_awaited()
        mock_sandbox.clear_egress_rules.assert_not_awaited()

    @pytest.mark.parametrize(
        ("provider_error", "expected_error", "message"),
        [
            (ValueError("bad allowlist"), SandboxSetupError, "Failed to apply egress rules: bad allowlist"),
            (ProviderSandboxError("provider failed"), SandboxError, "provider failed"),
        ],
    )
    async def test_apply_egress_allowlist_maps_provider_errors(
        self,
        provider_error: Exception,
        expected_error: type[Exception],
        message: str,
    ) -> None:
        """Map provider egress failures onto tracker sandbox exceptions.

        Test cases:
        - Provider validation errors become SandboxSetupError.
        - Provider sandbox errors become SandboxError.
        """
        mock_sandbox = Mock()
        mock_sandbox.modify_egress_rules = AsyncMock(side_effect=provider_error)

        with pytest.raises(expected_error, match=message):
            await _apply_egress_allowlist(mock_sandbox, ["https://api.openai.com"])


class TestStreamCommandOutputAgentFailure:
    """Agent command failure cleanup and error classification."""

    async def test_stream_command_output_uses_sandbox_timing_and_removes_files(self) -> None:
        observed_commands: list[str] = []

        async def stream_command(command: str) -> AsyncIterator[str]:
            observed_commands.append(command)
            yield "done\n"

        exec_commands: list[str] = []

        async def exec_command(command: str) -> ExecResult:
            exec_commands.append(command)
            if command.startswith("cat ") and command.endswith(".start_ns"):
                return ExecResult(exit_code=0, output="1000000000")
            if command.startswith("cat ") and command.endswith(".end_ns"):
                return ExecResult(exit_code=0, output="3000000000")
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.state = "started"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command

        exit_reason, duration = await sandbox_module.stream_command_output(
            mock_sandbox, "run-agent.sh", on_output=lambda _: None
        )

        assert exit_reason is None
        assert duration == 2
        # Timing is embedded in the sandbox command, not run raw.
        assert observed_commands and "run-agent.sh" in observed_commands[0]
        assert ".start_ns" in observed_commands[0]
        assert exec_commands[-1].startswith("rm -f ")
        assert ".start_ns" in exec_commands[-1]
        assert ".end_ns" in exec_commands[-1]

    async def test_stream_command_output_falls_back_when_timing_files_missing(self) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield "done\n"

        async def exec_command(command: str) -> ExecResult:
            if command.startswith("cat "):
                # `cat` on a missing file: non-zero exit and an error string on stdout.
                return ExecResult(
                    exit_code=1,
                    output="cat: /tmp/.valkyrie/abc.end_ns: No such file or directory",
                )
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.state = "started"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command

        exit_reason, duration = await sandbox_module.stream_command_output(
            mock_sandbox, "run-agent.sh", on_output=lambda _: None
        )

        # No crash on int() of the cat error text; duration degrades to the monotonic fallback.
        assert exit_reason is None
        assert duration >= 0

    @pytest.mark.parametrize("exit_code", [1, 2, 127])
    async def test_non_zero_exit_raises_prompt_free_agent_error_and_tags_exit_code(
        self, monkeypatch: pytest.MonkeyPatch, exit_code: int
    ) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield "last line\n"
            raise ProviderSandboxCommandError(exit_code)

        tagged: dict[str, str] = {}
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = AsyncMock()

        def fake_set_tag(key: str, value: object) -> None:
            tagged[key] = str(value)

        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", fake_set_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(mock_sandbox, "run-agent.sh", on_output=lambda _: None)

        assert isinstance(exc_info.value, SandboxError)
        assert not isinstance(exc_info.value, SandboxSetupError)
        assert str(exc_info.value) == f"Sandbox error: Agent command failed with exit code {exit_code}"
        assert "last line" not in str(exc_info.value)
        assert "run-agent.sh" not in str(exc_info.value)
        assert tagged == {"agent_exit_code": str(exit_code)}

    async def test_arbitrary_agent_output_is_not_persisted_in_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield "prompt secret and attacker-controlled output\n"
            raise ProviderSandboxCommandError(1)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = AsyncMock()

        def fake_set_tag(_key: str, _value: object) -> None:
            pass

        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", fake_set_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(
                mock_sandbox,
                "run-agent.sh --secret value",
                on_output=lambda _: None,
            )

        assert str(exc_info.value) == "Sandbox error: Agent command failed with exit code 1"
        assert "prompt" not in str(exc_info.value)
        assert "secret" not in str(exc_info.value)
        assert "run-agent.sh" not in str(exc_info.value)
