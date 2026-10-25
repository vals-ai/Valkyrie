"""Tests for tracker sandbox orchestration.

Run: pytest services/tracker/tests/unit/test_sandbox.py
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any, Literal, Never, cast
from unittest.mock import AsyncMock, Mock, call

import pytest
from benchmark_service import (
    ComposeSandbox,
    ComposeSource,
    Episode,
    ExecResult,
    GenerationContainment,
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
from tenacity import wait_none

from tracker.external_service_gateway import (
    AccountingSnapshot,
    GenerationAccountingSummary,
    ExternalServiceDeadlineController,
)
from tracker import sandbox as sandbox_module
from tracker.generation_plan import GenerationPlan
from tracker.aws.runtime import AWSRuntime
from tracker.runtime.storage import ObjectStore
from tracker.database.models import (
    AgentCausedExitReason,
    AgentContractRequest,
    AgentTurns,
    MAX_OUTPUT_ARTIFACT_BYTES,
    OutputArtifact,
)
from tracker.egress import EgressPolicy
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
    sandbox_resolves,
    OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES,
    _stream_controlled_output,  # pyright: ignore[reportPrivateUsage]
    SandboxCreationState,
    create_sandbox,
    run_agent,
    upload_agent_artifacts,
    upload_output_artifacts,
)


def _ignore_output(_message: str) -> None:
    pass


def _ignore_tag(_key: str, _value: object) -> None:
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
_apply_egress_policy = getattr(sandbox_module, "apply_egress_policy")
_run_egress_operation = getattr(sandbox_module, "_run_egress_operation")
_install_agent_dependencies = getattr(sandbox_module, "install_agent_dependencies")
_install_agent_dependencies_with_retries = getattr(sandbox_module, "_install_agent_dependencies_with_retries")
_install_agent_dependencies_once = getattr(sandbox_module, "_install_agent_dependencies_once")
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

    async def test_upload_output_artifacts_uploads_empty_file_without_streaming(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        """A zero-byte required artifact lands as an empty object under the same authority check.

        Daytona's streaming download raises "No file data received" on an empty file,
        which used to fail the required artifact and leave it missing from S3.
        """
        store = _mock_object_store()
        artifact = "artifacts/turns.jsonl"
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="0")
            raise AssertionError(f"unexpected command: {command}")

        def stream_download(remote_path: str) -> AsyncIterator[bytes]:
            async def chunks() -> AsyncIterator[bytes]:
                raise ProviderSandboxError(f"No file data received for: {remote_path}")
                yield b""

            return chunks()

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        _collect_put_stream(store, uploaded)

        execution_is_current = Mock(return_value=True)
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.stream_download = stream_download

        await upload_output_artifacts(
            mock_sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            store,
            execution_is_current=execution_is_current,
        )

        assert uploaded == [(b"", "benchmarks/benchmark-123/task_0/artifacts/turns.jsonl")]
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


def _signed_stage_frame(key: bytes, seq: int, event: str, container: str | None) -> str:
    payload = json.dumps({"seq": seq, "event": event, "container": container}, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    mac = hmac.new(key, encoded.encode(), hashlib.sha256).hexdigest()
    return f"VALKYRIE-STAGE/1 {encoded} {mac}\n"


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
        kill_release: asyncio.Event | None = None,
        natural: bool = True,
        absence_confirmed_at: float | None = None,
    ) -> None:
        self.exit_code = exit_code
        self.wait_error = wait_error
        self.wait_release = wait_release
        self.kill_release = kill_release
        self.absence_confirmed_at = absence_confirmed_at
        self.agent_absence_confirmed_at: float | None = None
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
            if self.agent_absence_confirmed_at is None:
                self.agent_absence_confirmed_at = confirmed_at
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
        if self.agent_absence_confirmed_at is None:
            self.agent_absence_confirmed_at = asyncio.get_running_loop().time()
        if self.wait_release is not None:
            self.wait_release.set()


class _FakeControlledSandbox:
    id = "sandbox-123"
    name = "task-alias"
    state = "started"
    generation_containment: GenerationContainment | None

    def __init__(self, workload: _FakeControlledWorkload) -> None:
        self.workload = workload
        self.generation_containment = GenerationContainment(type="linux_cgroup_v2", version=1)
        self.probe_calls = 0
        self.controlled_calls: list[tuple[str, str | None, Mapping[str, str] | None]] = []

    async def probe_generation_containment(self) -> None:
        self.probe_calls += 1

    async def exec(self, command: str) -> ExecResult:
        assert command == "cat /proc/$$/task/$$/children >/dev/null"
        return ExecResult(exit_code=0)

    def controlled_workload(
        self, command: str, *, cwd: str | None = None, env_vars: Mapping[str, str] | None = None
    ) -> _FakeControlledWorkload:
        self.controlled_calls.append((command, cwd, env_vars))
        return self.workload


class _StageFrameWorkload(_FakeControlledWorkload):
    def __init__(self, frame: str) -> None:
        super().__init__(wait_release=asyncio.Event(), natural=False)
        self.frame = frame

    async def output(self) -> AsyncIterator[str]:
        try:
            self.output_started.set()
            yield self.frame
            await self.closed.wait()
            self.output_drain_started.set()
        finally:
            self.output_finished.set()


def _accounting_snapshot(*, overhead_ms: int = 0, revision: int = 0) -> AccountingSnapshot:
    return AccountingSnapshot(accounting_id="session-1", cumulative_neutral_overhead_ms=overhead_ms, revision=revision)


class _FakeAccountingClient:
    def __init__(self, *, overhead_ms: int = 0, read_error: BaseException | None = None) -> None:
        self.overhead_ms = overhead_ms
        self.read_error = read_error
        self.read_calls = 0

    async def read_counter(self, _accounting_id: str) -> AccountingSnapshot:
        self.read_calls += 1
        if self.read_error is not None:
            raise self.read_error
        return _accounting_snapshot(overhead_ms=self.overhead_ms, revision=self.read_calls)


def _controlled_contract() -> AgentContractRequest:
    return AgentContractRequest(
        name="test-agent",
        install_cmd="",
        run_cmd="echo done",
    )


class TestRunAgent:
    """Agent execution, output collection, and runtime command construction."""

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
        monkeypatch.setattr(sandbox_module, "_stream_controlled_output", fail_agent)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive_output)
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", upload_artifacts)

        sandbox = Mock(id="sandbox-123", name="task-alias")
        sandbox.generation_containment = GenerationContainment(type="linux_process_group", version=1)
        sandbox.probe_generation_containment = AsyncMock()
        with pytest.raises(AgentRunFailedError, match="agent exited 23"):
            await run_agent(
                sandbox,
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/testbed",
                object_store=store,
                plan=GenerationPlan(allowance_seconds=None, staged=False, episode=None),
                agent_output_s3_key="benchmarks/benchmark-123/task_0/agent_output.tar.gz",
                benchmark_id="benchmark-123",
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=None),
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
        monkeypatch.setattr(sandbox_module, "_stream_controlled_output", fail_agent)
        monkeypatch.setattr(
            sandbox_module,
            "archive_and_upload_output",
            AsyncMock(side_effect=OutputArtifactError("terminal upload failed")),
        )
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", upload_artifacts)

        sandbox = Mock(id="sandbox-123", name="task-alias")
        sandbox.generation_containment = GenerationContainment(type="linux_process_group", version=1)
        sandbox.probe_generation_containment = AsyncMock()
        with pytest.raises(AgentRunFailedError, match="agent exited 23"):
            await run_agent(
                sandbox,
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/testbed",
                object_store=store,
                plan=GenerationPlan(allowance_seconds=None, staged=False, episode=None),
                agent_output_s3_key="benchmarks/benchmark-123/task_0/agent_output.tar.gz",
                benchmark_id="benchmark-123",
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=None),
            )

        upload_artifacts.assert_awaited_once()

    async def test_run_agent_revoked_authority_skips_archive(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        store = _mock_object_store()
        contract = AgentContractRequest(
            name="test-agent",
            install_cmd="",
            run_cmd="echo done",
            final_output="/tmp/agent_output",
        )

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command.startswith("mkdir -p") or command.startswith("test -e"):
                return ExecResult(exit_code=0)
            raise AssertionError(f"unexpected command: {command}")

        async def fake_controlled_output(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            return None, 0.0

        archive_output = AsyncMock()

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "_stream_controlled_output", fake_controlled_output)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive_output)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.generation_containment = GenerationContainment(type="linux_process_group", version=1)
        mock_sandbox.probe_generation_containment = AsyncMock()

        await run_agent(
            mock_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            lambda _msg: None,
            "/testbed",
            object_store=store,
            plan=GenerationPlan(allowance_seconds=None, staged=False, episode=None),
            agent_output_s3_key="benchmarks/run/task/agent_output.tar.gz",
            benchmark_id="benchmark-123",
            execution_is_current=lambda: False,
            deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=None),
        )
        archive_output.assert_not_awaited()

    async def test_run_agent_without_deadline_uses_controlled_wait_output_and_finalize(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event())
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={
                "run_cmd": "FOO=bar echo {model} && echo {task_id} {problem_statement_path} && echo finished",
                "kwargs": {"model": "gpt-4o"},
            }
        )
        output: list[str] = []
        finalized = AsyncMock()
        controller = ExternalServiceDeadlineController(base_allowance_seconds=None)
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))

        async def release_after_output() -> None:
            await workload.output_started.wait()
            await asyncio.sleep(0.02)
            workload.wait_release.set()

        release_task = asyncio.create_task(release_after_output())

        reason, duration = await run_agent(
            cast(Any, sandbox),
            contract,
            "/tmp/problem.txt",
            "task_0",
            output.append,
            "/workspace",
            object_store=_mock_object_store(),
            plan=GenerationPlan(allowance_seconds=None, staged=False, episode=None),
            deadline_controller=controller,
            on_external_service_finalized=finalized,
        )

        await release_task
        assert reason is None
        assert duration >= 0.02
        assert output == ["Running agent test-agent", "agent output"]
        assert workload.kill_calls == 0
        assert workload.closed.is_set() and workload.output_finished.is_set()
        assert controller.deadline(asyncio.get_running_loop().time()) is None
        finalized.assert_awaited_once()
        assert sandbox.controlled_calls == [
            (
                "PYTHONSAFEPATH=1 FOO=bar echo gpt-4o && echo task_0 /tmp/problem.txt && echo finished",
                "/workspace",
                None,
            )
        ]

    async def test_ordinary_generation_uses_ssp_only_in_controlled_child(self, monkeypatch: pytest.MonkeyPatch) -> None:
        requests: list[str] = []

        async def endpoint(label: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.readuntil(b"\r\n\r\n")
            requests.append(label)
            body = label.encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + body
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        native = await asyncio.start_server(lambda r, w: endpoint("native", r, w), "127.0.0.1", 0)
        ssp = await asyncio.start_server(lambda r, w: endpoint("ssp", r, w), "127.0.0.1", 0)
        native_url = f"http://127.0.0.1:{native.sockets[0].getsockname()[1]}"
        ssp_url = f"http://127.0.0.1:{ssp.sockets[0].getsockname()[1]}"
        monkeypatch.setenv("MODEL_GATEWAY_URL", native_url)
        child_code = (
            "import os, urllib.request; print(urllib.request.urlopen(os.environ['MODEL_GATEWAY_URL']).read().decode())"
        )
        command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
        messages: list[str] = []

        class LocalWorkload:
            def __init__(self, env_vars: Mapping[str, str]) -> None:
                self.agent_absence_confirmed_at: float | None = None
                self.process = subprocess.Popen(
                    ["sh", "-c", command],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env={**os.environ, **env_vars},
                )

            async def output(self) -> AsyncIterator[str]:
                assert self.process.stdout is not None
                while line := await asyncio.to_thread(self.process.stdout.readline):
                    yield line.decode()

            async def wait(self) -> _CompletedControlledWorkload:
                exit_code = await asyncio.to_thread(self.process.wait)
                self.agent_absence_confirmed_at = asyncio.get_running_loop().time()
                return _CompletedControlledWorkload(exit_code, self.agent_absence_confirmed_at)

            async def kill(self) -> None:
                self.process.kill()
                await asyncio.to_thread(self.process.wait)
                self.agent_absence_confirmed_at = asyncio.get_running_loop().time()

        class LocalSandbox:
            def controlled_workload(
                self, _command: str, *, cwd: str | None = None, env_vars: Mapping[str, str]
            ) -> LocalWorkload:
                return LocalWorkload(env_vars)

        try:
            reason, _ = await _stream_controlled_output(
                cast(Any, LocalSandbox()),
                command,
                "/workspace",
                messages.append,
                ExternalServiceDeadlineController(base_allowance_seconds=None),
                generation_gateway_url=ssp_url,
                on_accounting_failure=AsyncMock(),
            )
            assert reason is None
            assert messages == ["ssp\n"]
            evaluation = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                child_code,
                stdout=asyncio.subprocess.PIPE,
            )
            output, _ = await evaluation.communicate()
            assert evaluation.returncode == 0
            assert output == b"native\n"
            assert requests == ["ssp", "native"]
        finally:
            native.close()
            ssp.close()
            await native.wait_closed()
            await ssp.wait_closed()

    async def test_run_agent_timeout_collects_only_after_confirmed_kill(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={"final_output": "/logs", "egress_allowlist": ["example.com"]}
        )
        archive = AsyncMock()
        staged_policy = ["example.com"]
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()
        await _apply_egress_policy(controlled_sandbox, staged_policy)

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -e /logs":
                assert workload.kill_calls == 1
                assert workload.closed.is_set()
                controlled_sandbox.modify_egress_rules.assert_awaited_once_with(staged_policy)
                controlled_sandbox.clear_egress_rules.assert_not_awaited()
            return ExecResult(exit_code=0)

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)

        reason, _ = await run_agent(
            controlled_sandbox,
            contract,
            "/tmp/problem.txt",
            "task_0",
            _ignore_output,
            "/workspace",
            object_store=_mock_object_store(),
            agent_output_s3_key="benchmarks/run/task/output.tar.gz",
            plan=GenerationPlan(
                allowance_seconds=0.001,
                staged=False,
                episode=None,
            ),
            deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=0.001),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        archive.assert_awaited_once()
        controlled_sandbox.modify_egress_rules.assert_awaited_once_with(staged_policy)
        controlled_sandbox.clear_egress_rules.assert_not_awaited()

    async def test_run_agent_unconfirmed_timeout_preserves_run_policy_and_suppresses_collection(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.block_kill = True
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={"final_output": "/logs", "egress_allowlist": ["example.com"]}
        )
        archive = AsyncMock()
        staged_policy = ["example.com"]
        controlled_sandbox = cast(Any, sandbox)
        controlled_sandbox.modify_egress_rules = AsyncMock()
        controlled_sandbox.clear_egress_rules = AsyncMock()
        await _apply_egress_policy(controlled_sandbox, staged_policy)
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)
        monkeypatch.setattr(sandbox_module, "GENERATION_TERMINATION_GRACE_SECONDS", 0.01)

        with pytest.raises(GenerationTerminationUnconfirmedError):
            await run_agent(
                controlled_sandbox,
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                agent_output_s3_key="benchmarks/run/task/output.tar.gz",
                plan=GenerationPlan(
                    allowance_seconds=0.001,
                    staged=False,
                    episode=None,
                ),
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=0.001),
            )

        archive.assert_not_awaited()
        controlled_sandbox.modify_egress_rules.assert_awaited_once_with(staged_policy)
        controlled_sandbox.clear_egress_rules.assert_not_awaited()

    async def test_controlled_nonzero_exit_collects_outputs_after_finalize(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workload = _FakeControlledWorkload(exit_code=23)
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={"final_output": "/logs", "output_artifacts": ["artifacts/result.json"]}
        )
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        finalized: list[GenerationAccountingSummary] = []

        async def save_summary(summary: GenerationAccountingSummary) -> None:
            finalized.append(summary)

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -e /logs":
                assert workload.closed.is_set()
                assert len(finalized) == 1
            return ExecResult(exit_code=0)

        async def collect_after_finalize(*_args: Any, **_kwargs: Any) -> None:
            assert len(finalized) == 1
            assert workload.closed.is_set()

        archive = AsyncMock(side_effect=collect_after_finalize)
        artifacts = AsyncMock(side_effect=collect_after_finalize)
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", artifacts)

        with pytest.raises(AgentRunFailedError, match="exit code 23"):
            await run_agent(
                cast(Any, sandbox),
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                agent_output_s3_key="benchmarks/run/task/output.tar.gz",
                benchmark_id="benchmark-123",
                plan=GenerationPlan(
                    allowance_seconds=10.0,
                    staged=False,
                    episode=None,
                ),
                deadline_controller=controller,
                on_external_service_finalized=save_summary,
            )

        assert len(finalized) == 1
        archive.assert_awaited_once()
        artifacts.assert_awaited_once()

    async def test_accounting_persistence_failure_collects_available_output(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        contract = _controlled_contract().model_copy(
            update={"final_output": "/logs", "output_artifacts": ["artifacts/result.json"]}
        )

        async def collect_after_stop(*_args: Any, **_kwargs: Any) -> None:
            assert workload.closed.is_set()

        archive = AsyncMock(side_effect=collect_after_stop)
        artifacts = AsyncMock(side_effect=collect_after_stop)
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persistence_error = RuntimeError("summary persistence failed")

        async def fail_persistence(
            _summary: GenerationAccountingSummary,
        ) -> None:
            raise persistence_error

        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)
        monkeypatch.setattr(sandbox_module, "upload_output_artifacts", artifacts)

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
                benchmark_id="benchmark-123",
                plan=GenerationPlan(
                    allowance_seconds=10.0,
                    staged=False,
                    episode=None,
                ),
                deadline_controller=controller,
                on_external_service_finalized=fail_persistence,
            )

        assert raised.value is persistence_error
        archive.assert_awaited_once()
        artifacts.assert_awaited_once()

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
                plan=GenerationPlan(
                    allowance_seconds=10.0,
                    staged=False,
                    episode=None,
                ),
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=10.0),
            )

        assert sandbox.controlled_calls == []

    @pytest.mark.parametrize(
        ("updates", "message"),
        [
            ({}, "Parallel episode agent run_cmd requires {slot_dir}"),
            (
                {"run_cmd": "echo {slot_dir}", "final_output": "/logs"},
                "Parallel episode final_output requires {slot_dir}",
            ),
            (
                {"run_cmd": "echo {slot_dir}", "turns": AgentTurns(container_name="worker")},
                "Parallel episode does not support container_name",
            ),
        ],
    )
    async def test_run_agent_rejects_parallel_episode_contract_before_provisioning(
        self, monkeypatch: pytest.MonkeyPatch, updates: dict[str, Any], message: str
    ) -> None:
        sandbox = _FakeControlledSandbox(_FakeControlledWorkload())
        upload = AsyncMock()
        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        contract = _controlled_contract().model_copy(update=updates)

        with pytest.raises(InvalidSandboxConfigurationError) as captured:
            await run_agent(
                cast(Any, sandbox),
                contract,
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                plan=GenerationPlan(
                    allowance_seconds=10.0,
                    staged=True,
                    episode=Episode(command="stage", parallel_agents=2),
                ),
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=10.0),
            )

        assert str(captured.value) == f"Sandbox error: {message}"
        assert sandbox.probe_calls == 0
        assert sandbox.controlled_calls == []
        upload.assert_not_awaited()

    async def test_run_agent_runs_two_cbs_slots_and_resumes_one(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        stage = tmp_path / "stage"
        slots = tmp_path / "slots"
        stage.mkdir()
        orchestrator = """
import json
import os
import time
from pathlib import Path
from benchmark_service.valkyrie_stage import Agents
agents = Agents()
agents.start("a", "/tmp/first-observation")
agents.start("b", "/tmp/first-observation")
for slot in ("a", "b"):
    while not (Path(os.environ["TEST_SLOTS_ROOT"]) / slot / "agent" / "ready").exists():
        time.sleep(0.02)
# CBS stop signals release both ready turns only after overlap is established.
first = agents.stop("a")
second = agents.stop("b")
agents.start("a", "/tmp/next-observation")
continued = agents.wait_any()
print(json.dumps({"slots": [first.slot, second.slot, continued.slot], "reasons": [first.reason, second.reason, continued.reason]}), flush=True)
"""

        class LocalWorkload:
            def __init__(self) -> None:
                self.started = asyncio.create_task(
                    asyncio.create_subprocess_exec(
                        sys.executable,
                        "-c",
                        orchestrator,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        env=dict(os.environ, VALKYRIE_STAGE_DIR=str(stage), TEST_SLOTS_ROOT=str(slots)),
                        start_new_session=True,
                    )
                )

            async def output(self) -> AsyncIterator[str]:
                process = await asyncio.shield(self.started)
                assert process.stdout is not None
                async for line in process.stdout:
                    yield line.decode()

            async def wait(self) -> _CompletedControlledWorkload:
                process = await asyncio.shield(self.started)
                exit_code = await process.wait()
                return _CompletedControlledWorkload(exit_code, asyncio.get_running_loop().time())

            async def kill(self) -> None:
                process = await asyncio.shield(self.started)
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                await process.wait()

        class LocalSandbox:
            id = "local-episode"
            name = "local-episode"
            state = "started"
            generation_containment = GenerationContainment(type="linux_cgroup_v2", version=1)
            workload: LocalWorkload | None = None
            probe_calls = 0

            async def probe_generation_containment(self) -> None:
                self.probe_calls += 1

            async def exec(self, command: str) -> ExecResult:
                assert command == "cat /proc/$$/task/$$/children >/dev/null"
                process = await asyncio.create_subprocess_shell(command)
                return ExecResult(exit_code=await process.wait())

            async def upload_file(self, path: str, content: bytes) -> None:
                if path.startswith(f"{sandbox_module._STAGE_DIR}/ack/"):
                    acknowledgements.append(path.rsplit("/", 1)[1])
                target = stage / Path(path).relative_to(sandbox_module._STAGE_DIR)
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.name == "agent.json":
                    config = json.loads(content)
                    assert config["slots_root"] == "/workspace/.valkyrie/slots"
                    # Translate the sandbox's slot root into this task-owned host directory.
                    config["slots_root"] = str(slots)
                    content = json.dumps(config).encode()
                target.write_bytes(content)

            def controlled_workload(
                self, command: str, *, cwd: str | None = None, env_vars: Mapping[str, str] | None = None
            ) -> LocalWorkload:
                self.workload = LocalWorkload()
                return self.workload

        acknowledgements: list[str] = []
        sandbox = LocalSandbox()
        archive = AsyncMock()
        persisted = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)
        contract = _controlled_contract().model_copy(
            update={
                "run_cmd": (
                    "echo $$ > {slot_dir}/group.pid; echo task-{task_id} > {slot_dir}/first; "
                    "touch {slot_dir}/ready; timeout 30 sh -c 'while :; do sleep 0.05; done'"
                ),
                "turns": AgentTurns(continue_cmd="echo resumed > {slot_dir}/continued"),
            }
        )
        output: list[str] = []
        controller = ExternalServiceDeadlineController(base_allowance_seconds=10.0)
        completed = False
        try:
            async with asyncio.timeout(20):
                reason, _duration = await run_agent(
                    cast(Any, sandbox),
                    contract,
                    "/tmp/problem.txt",
                    "task_0",
                    output.append,
                    "/workspace",
                    object_store=_mock_object_store(),
                    agent_output_s3_key="benchmarks/run/task/output.tar.gz",
                    plan=GenerationPlan(
                        allowance_seconds=10.0,
                        staged=True,
                        episode=Episode(command="python3 /run/valkyrie-stage/orchestrator.py", parallel_agents=2),
                    ),
                    deadline_controller=controller,
                    on_external_service_finalized=persisted,
                )
                completed = True
        finally:
            if not completed and sandbox.workload is not None:
                try:
                    for group_file in slots.glob("*/agent/group.pid"):
                        with suppress(ProcessLookupError):
                            os.killpg(int(group_file.read_text()), signal.SIGKILL)
                finally:
                    await sandbox.workload.kill()

        assert reason is None
        assert (slots / "a" / "agent" / "first").read_text().strip() == "task-task_0"
        assert (slots / "b" / "agent" / "first").read_text().strip() == "task-task_0"
        assert (slots / "a" / "agent" / "continued").read_text().strip() == "resumed"
        assert any(
            json.loads(message) == {"slots": ["a", "b", "a"], "reasons": ["stopped", "stopped", "exited"]}
            for message in output
            if message.startswith("{")
        )
        assert acknowledgements == ["1", "2", "3", "4"]
        assert sandbox.probe_calls == 1
        assert controller.elapsed_seconds(asyncio.get_running_loop().time()) > 0.0
        persisted.assert_awaited_once()
        archive.assert_awaited_once()
        assert archive.await_args.args[1] == "/workspace/.valkyrie/slots"

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
                plan=GenerationPlan(
                    allowance_seconds=10.0,
                    staged=False,
                    episode=None,
                ),
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=10.0),
            )

        assert sandbox.controlled_calls == []

    async def test_controlled_completion_at_deadline_runs_timeout_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real_loop = asyncio.get_running_loop()
        started_at = real_loop.time()
        clock = Mock()
        clock.time.return_value = started_at
        monkeypatch.setattr(sandbox_module.asyncio, "get_running_loop", lambda: clock)

        workload = _FakeControlledWorkload(absence_confirmed_at=started_at + 1.0)
        sandbox = _FakeControlledSandbox(workload)
        persisted: list[GenerationAccountingSummary] = []

        async def persist(summary: GenerationAccountingSummary) -> None:
            persisted.append(summary)

        reason, duration = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            ExternalServiceDeadlineController(base_allowance_seconds=1.0),
            persist,
            on_accounting_failure=AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert duration == 1.0
        assert persisted[0].generation_elapsed_seconds == duration

    async def test_controlled_timeout_freezes_cause_over_losing_os_kill(self) -> None:
        workload = _FakeControlledWorkload(exit_code=137, wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)

        reason, duration = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            ExternalServiceDeadlineController(base_allowance_seconds=0.001),
            on_accounting_failure=AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert duration > 0
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

        reason, _ = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            ExternalServiceDeadlineController(base_allowance_seconds=10.0),
            on_accounting_failure=AsyncMock(),
        )

        assert reason == expected
        assert workload.kill_calls == 0

    async def test_controlled_natural_exit_124_is_not_timeout(self) -> None:
        workload = _FakeControlledWorkload(exit_code=124)
        sandbox = _FakeControlledSandbox(workload)

        with pytest.raises(AgentRunFailedError, match="exit code 124"):
            await _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                on_accounting_failure=AsyncMock(),
            )

    async def test_controlled_report_preparation_does_not_consume_generation_allowance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=10.0)
        prep_entered = asyncio.Event()
        release_prep = asyncio.Event()
        messages: list[str] = []
        finalized = AsyncMock()

        async def gated_exec(_sandbox: Any, command: str) -> ExecResult:
            if command.startswith("mkdir -p "):
                prep_entered.set()
                await release_prep.wait()
            return ExecResult(exit_code=0)

        monkeypatch.setattr(sandbox_module, "_exec", gated_exec)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                messages.append,
                controller,
                finalized,
                on_accounting_failure=AsyncMock(),
                redact_error=lambda error: error,
            )
        )
        try:
            async with asyncio.timeout(1):
                await prep_entered.wait()
                assert controller.active_since is None
                assert controller.active_elapsed_seconds == 0
                release_prep.set()
                reason, _ = await task
        finally:
            release_prep.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert reason is None
        assert workload.kill_calls == 0
        assert messages == ["agent output"]
        finalized.assert_awaited_once()
        assert controller.active_since is None

    async def test_controlled_creation_consumes_generation_allowance(self) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        original_factory = sandbox.controlled_workload

        def delayed_factory(
            command: str, *, cwd: str | None = None, env_vars: Mapping[str, str] | None = None
        ) -> _FakeControlledWorkload:
            time.sleep(0.02)
            return original_factory(command, cwd=cwd, env_vars=env_vars)

        sandbox.controlled_workload = delayed_factory  # type: ignore[method-assign]

        reason, _ = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            ExternalServiceDeadlineController(base_allowance_seconds=0.001),
            on_accounting_failure=AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT

    @pytest.mark.parametrize("exit_code", [0, 23], ids=["success", "agent-error"])
    async def test_predeadline_agent_absence_collects_delayed_real_result(
        self, monkeypatch: pytest.MonkeyPatch, exit_code: int
    ) -> None:
        workload = _FakeControlledWorkload(exit_code=exit_code, wait_release=asyncio.Event(), natural=False)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=0.08)
        cutoff_seen = asyncio.Event()
        original_freeze = controller.freeze_credit

        def freeze_at_cutoff() -> None:
            original_freeze()
            cutoff_seen.set()

        monkeypatch.setattr(controller, "freeze_credit", freeze_at_cutoff)
        persisted: list[GenerationAccountingSummary] = []

        async def persist(summary: GenerationAccountingSummary) -> None:
            persisted.append(summary)

        sandbox = _FakeControlledSandbox(workload)
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        task = asyncio.create_task(
            run_agent(
                cast(Any, sandbox),
                _controlled_contract(),
                "/tmp/problem.txt",
                "task_0",
                _ignore_output,
                "/workspace",
                object_store=_mock_object_store(),
                plan=GenerationPlan(allowance_seconds=0.08, staged=False, episode=None),
                deadline_controller=controller,
                on_external_service_finalized=persist,
            )
        )
        try:
            await asyncio.wait_for(workload.output_started.wait(), 1)
            started_at = controller.active_since
            assert started_at is not None
            receipt_at = asyncio.get_running_loop().time()
            workload.agent_absence_confirmed_at = receipt_at
            await asyncio.wait_for(cutoff_seen.wait(), 1)
            assert not task.done()
            assert workload.kill_calls == 0
            assert workload.wait_release is not None
            workload.wait_release.set()
            workload.closed.set()
            if exit_code:
                with pytest.raises(AgentRunFailedError, match="exit code 23"):
                    await asyncio.wait_for(task, 1)
            else:
                reason, duration = await asyncio.wait_for(task, 1)
                assert reason is None
                assert duration == pytest.approx(receipt_at - started_at, abs=0.005)
            assert len(persisted) == 1
            assert persisted[0].generation_elapsed_seconds == receipt_at - started_at
            assert workload.kill_calls == 0
            assert workload.wait_finished.is_set() and workload.output_finished.is_set()
        finally:
            workload.closed.set()
            if workload.wait_release is not None:
                workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    @pytest.mark.parametrize("overhead_ms", [100, 0], ids=["extended", "late-after-cutoff"])
    async def test_credit_refresh_observes_agent_absence_during_counter_read(self, overhead_ms: int) -> None:
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()

        class DelayedCredit(_FakeAccountingClient):
            async def read_counter(self, _accounting_id: str) -> AccountingSnapshot:
                self.read_calls += 1
                if self.read_calls == 1:
                    return _accounting_snapshot(revision=1)
                refresh_started.set()
                await release_refresh.wait()
                return _accounting_snapshot(overhead_ms=overhead_ms, revision=self.read_calls)

        client = DelayedCredit()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.05,
            credit_cap_seconds=0.1,
        )
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        persisted: list[GenerationAccountingSummary] = []

        async def persist(summary: GenerationAccountingSummary) -> None:
            persisted.append(summary)

        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persist,
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(refresh_started.wait(), 1)
            started_at = controller.active_since
            assert started_at is not None
            receipt_at = asyncio.get_running_loop().time()
            workload.agent_absence_confirmed_at = receipt_at
            workload.closed.set()
            release_refresh.set()
            assert workload.wait_release is not None
            workload.wait_release.set()
            reason, duration = await asyncio.wait_for(task, 1)
            assert reason == (None if overhead_ms else AgentCausedExitReason.TIMEOUT)
            assert duration == pytest.approx(receipt_at - started_at, abs=0.005)
            assert persisted[0].generation_elapsed_seconds == receipt_at - started_at
            assert persisted[0].external_service_credit_applied_seconds == overhead_ms / 1000
            assert client.read_calls >= 2
            assert workload.kill_calls == (0 if overhead_ms else 1)
        finally:
            release_refresh.set()
            if workload.wait_release is not None:
                workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_predeadline_absence_without_full_result_fails_at_collection_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sandbox_module, "GENERATION_TERMINATION_GRACE_SECONDS", 0.02)
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=0.04)
        persisted = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(workload.output_started.wait(), 1)
            workload.agent_absence_confirmed_at = asyncio.get_running_loop().time()
            workload.closed.set()
            with pytest.raises(
                ControlledGenerationTerminationUnconfirmedError, match="result, output, or workload cleanup"
            ):
                await asyncio.wait_for(task, 1)
            persisted.assert_not_awaited()
            assert workload.kill_calls == 0
            assert workload.wait_finished.is_set() and workload.output_finished.is_set()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_during_result_collection_joins_owned_work(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=0.04)
        cutoff_seen = asyncio.Event()
        original_freeze = controller.freeze_credit

        def freeze_at_cutoff() -> None:
            original_freeze()
            cutoff_seen.set()

        monkeypatch.setattr(controller, "freeze_credit", freeze_at_cutoff)
        persisted: list[GenerationAccountingSummary] = []

        async def persist(summary: GenerationAccountingSummary) -> None:
            persisted.append(summary)

        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persist,
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(workload.output_started.wait(), 1)
            started_at = controller.active_since
            assert started_at is not None
            receipt_at = asyncio.get_running_loop().time()
            workload.agent_absence_confirmed_at = receipt_at
            workload.closed.set()
            await asyncio.wait_for(cutoff_seen.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
            assert workload.kill_calls == 1
            assert workload.wait_finished.is_set() and workload.output_finished.is_set()
            assert len(persisted) == 1
            assert persisted[0].generation_elapsed_seconds == receipt_at - started_at
        finally:
            if workload.wait_release is not None:
                workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_natural_completion_drains_buffered_tail_output(self) -> None:
        class CompletedProducerWithTail(_FakeControlledWorkload):
            def __init__(self) -> None:
                super().__init__()
                self.buffered_output: asyncio.Queue[str] = asyncio.Queue()

            async def output(self) -> AsyncIterator[str]:
                await self.wait_finished.wait()
                await asyncio.sleep(0.02)
                while not self.buffered_output.empty():
                    yield self.buffered_output.get_nowait()

        workload = CompletedProducerWithTail()
        workload.buffered_output.put_nowait("already-buffered tail")
        messages: list[str] = []
        reason, _ = await asyncio.wait_for(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                messages.append,
                ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                on_accounting_failure=AsyncMock(),
            ),
            timeout=0.2,
        )

        assert reason is None
        assert messages == ["already-buffered tail"]
        assert workload.kill_calls == 0

    async def test_confirmed_timeout_drains_buffered_output_before_finalize(self) -> None:
        class BufferedOutputAfterKill(_FakeControlledWorkload):
            def __init__(self) -> None:
                super().__init__(wait_release=asyncio.Event(), natural=False)
                self.buffered_output: asyncio.Queue[str] = asyncio.Queue()

            async def output(self) -> AsyncIterator[str]:
                self.output_started.set()
                try:
                    await self.closed.wait()
                    await asyncio.sleep(0.02)
                    while not self.buffered_output.empty():
                        yield self.buffered_output.get_nowait()
                finally:
                    self.output_finished.set()

        workload = BufferedOutputAfterKill()
        workload.buffered_output.put_nowait("queued before confirmed kill")
        messages: list[str] = []
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.001,
            credit_cap_seconds=0.0,
        )
        finalized: list[GenerationAccountingSummary] = []

        async def persist_summary(summary: GenerationAccountingSummary) -> None:
            assert messages == ["queued before confirmed kill"]
            finalized.append(summary)

        reason, _ = await asyncio.wait_for(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                messages.append,
                controller,
                persist_summary,
                on_accounting_failure=AsyncMock(),
            ),
            timeout=0.2,
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert workload.kill_calls == 1
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        assert len(finalized) == 1

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
            _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=0.001),
                on_accounting_failure=AsyncMock(),
            )
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
        assert duration > 0
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

        def delayed_factory(
            command: str, *, cwd: str | None = None, env_vars: Mapping[str, str] | None = None
        ) -> _FakeControlledWorkload:
            time.sleep(0.02)
            return original_factory(command, cwd=cwd, env_vars=env_vars)

        def observed_timeout_at(when: float | None) -> asyncio.Timeout:
            assert when is not None
            timeout_calls.append((when, asyncio.get_running_loop().time()))
            return real_timeout_at(when)

        sandbox.controlled_workload = delayed_factory  # type: ignore[method-assign]
        monkeypatch.setattr(sandbox_module.asyncio, "timeout_at", observed_timeout_at)
        monkeypatch.setattr(sandbox_module, "GENERATION_TERMINATION_GRACE_SECONDS", 0.01)

        with pytest.raises(GenerationTerminationUnconfirmedError):
            await _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=0.001),
                on_accounting_failure=AsyncMock(),
            )

        assert len(timeout_calls) == 1
        absolute_deadline, kill_started_at = timeout_calls[0]
        assert absolute_deadline < kill_started_at
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

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
            controller,
            AsyncMock(),
            on_accounting_failure=AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert duration > 0.02
        assert client.read_calls >= 1

    async def test_timeout_persists_cutoff_counter_even_if_ssp_grows_after_stop(self) -> None:
        client = _FakeAccountingClient()
        reads_at_stop: list[int] = []

        class LaterCounterGrowth(_FakeControlledWorkload):
            async def kill(self) -> None:
                reads_at_stop.append(client.read_calls)
                await super().kill()
                client.overhead_ms = 9000

        workload = LaterCounterGrowth(wait_release=asyncio.Event(), natural=False)
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.005,
            credit_cap_seconds=10.0,
        )
        persisted: list[GenerationAccountingSummary] = []

        async def persist(summary: GenerationAccountingSummary) -> None:
            persisted.append(summary)

        reason, _ = await _stream_controlled_output(
            cast(Any, _FakeControlledSandbox(workload)),
            "echo done",
            "/workspace",
            _ignore_output,
            controller,
            persist,
            on_accounting_failure=AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert client.read_calls == reads_at_stop[0]
        assert persisted[0].external_service_overhead_seconds == 0.0
        assert persisted[0].external_service_credit_applied_seconds == 0.0
        assert persisted[0].external_service_credit_revision == reads_at_stop[0]

    async def test_transient_refresh_error_retries_within_lead_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)

        class TransientRefreshClient(_FakeAccountingClient):
            async def read_counter(self, _accounting_id: str) -> AccountingSnapshot:
                self.read_calls += 1
                if self.read_calls == 1:
                    raise RuntimeError("transient refresh")
                return _accounting_snapshot(revision=self.read_calls)

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
            controller,
            AsyncMock(),
            on_accounting_failure=AsyncMock(),
        )

        assert reason == AgentCausedExitReason.TIMEOUT
        assert client.read_calls >= 2

    async def test_failed_refresh_task_cannot_defeat_natural_completion(self) -> None:
        workload = _FakeControlledWorkload()

        class CancelledRefreshClient(_FakeAccountingClient):
            async def read_counter(self, accounting_id: str) -> AccountingSnapshot:
                if self.read_calls == 0:
                    self.read_calls += 1
                    raise asyncio.CancelledError()
                return await super().read_counter(accounting_id)

        client = CancelledRefreshClient()
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
            controller,
            persisted,
            on_accounting_failure=AsyncMock(),
        )

        assert reason is None
        assert workload.kill_calls == 0
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

        with pytest.raises(ControlledGenerationTerminationUnconfirmedError) as raised:
            await _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                AsyncMock(),
                on_accounting_failure=AsyncMock(),
            )

        assert isinstance(raised.value.__cause__, GenerationTerminationUnconfirmedError)

    async def test_natural_completion_finalizes_before_return(self) -> None:
        workload = _FakeControlledWorkload()
        sandbox = _FakeControlledSandbox(workload)
        client = _FakeAccountingClient(overhead_ms=5)
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted: list[GenerationAccountingSummary] = []

        async def persist_natural(
            summary: GenerationAccountingSummary,
        ) -> None:
            persisted.append(summary)

        reason, _ = await _stream_controlled_output(
            cast(Any, sandbox),
            "echo done",
            "/workspace",
            _ignore_output,
            controller,
            persist_natural,
            on_accounting_failure=AsyncMock(),
        )

        assert reason is None
        assert workload.wait_finished.is_set()
        assert workload.kill_calls == 0
        assert persisted[0].external_service_overhead_seconds == 0.005

    async def test_deadline_control_error_kills_then_propagates_raw_error(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        control_error = RuntimeError("gateway unavailable")

        class FailingCounterClient(_FakeAccountingClient):
            async def read_counter(self, _accounting_id: str) -> AccountingSnapshot:
                raise control_error

        client = FailingCounterClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.001,
            credit_cap_seconds=1.0,
        )

        with pytest.raises(RuntimeError) as raised:
            await _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                AsyncMock(),
                on_accounting_failure=AsyncMock(),
            )

        assert raised.value is control_error
        assert workload.closed.is_set()
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

    @pytest.mark.parametrize("frame_case", ["begin", "end", "invalid", "success"])
    async def test_staged_frame_handling_and_normal_tail(
        self,
        frame_case: Literal["begin", "end", "invalid", "success"],
    ) -> None:
        key = b"stage-test-key"
        release_frame = asyncio.Event()
        frame_sent = asyncio.Event()
        output_release = asyncio.Event()
        wait_release = asyncio.Event()
        acknowledgements: list[str] = []
        messages: list[str] = []
        loss = SandboxNotFoundError("wait lost")
        end_sent_at: float | None = None
        end_fed_at: float | None = None

        class FramedWorkload(_FakeControlledWorkload):
            async def output(self) -> AsyncIterator[str]:
                nonlocal end_sent_at, end_fed_at
                self.output_started.set()
                try:
                    if frame_case == "begin":
                        wait_release.set()
                    yield _signed_stage_frame(key, 1, "begin", None)
                    await release_frame.wait()
                    if frame_case in ("end", "success"):
                        end_sent_at = asyncio.get_running_loop().time()
                        yield _signed_stage_frame(key, 2, "end", None)
                        end_fed_at = asyncio.get_running_loop().time()
                        if frame_case == "end":
                            wait_release.set()
                    elif frame_case == "invalid":
                        yield _signed_stage_frame(key, 3, "end", None)
                    frame_sent.set()
                    await output_release.wait()
                    if frame_case == "success":
                        yield "tail output"
                finally:
                    self.output_finished.set()

        class FramedSandbox(_FakeControlledSandbox):
            async def upload_file(self, path: str, _data: bytes) -> None:
                acknowledgements.append(path)

        workload = FramedWorkload(
            wait_release=wait_release, wait_error=None if frame_case == "success" else loss, natural=False
        )
        sandbox = FramedSandbox(workload)

        class ReceiptController(ExternalServiceDeadlineController):
            end_receipt_at: float | None = None

            def end_generation(self, now: float) -> None:
                self.end_receipt_at = now
                super().end_generation(now)

        controller = ReceiptController(base_allowance_seconds=10.0)
        finalized = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                messages.append,
                controller,
                finalized,
                on_accounting_failure=AsyncMock(),
                stage_key=key,
            )
        )
        try:
            async with asyncio.timeout(1) as watchdog:
                if frame_case == "begin":
                    await workload.wait_finished.wait()
                else:
                    while not acknowledgements:
                        await asyncio.sleep(0)
                    assert acknowledgements == ["/run/valkyrie-stage/ack/1"]
                release_frame.set()
                await frame_sent.wait()
                if frame_case == "success":
                    while "/run/valkyrie-stage/ack/2" not in acknowledgements:
                        await asyncio.sleep(0)
                    assert acknowledgements == ["/run/valkyrie-stage/ack/1", "/run/valkyrie-stage/ack/2"]
                    workload.closed.set()
                    wait_release.set()
                    output_release.set()
                    reason, _ = await asyncio.shield(task)
                    assert reason is None
                    assert messages == ["tail output"]
                    assert controller.active_since is None
                    assert end_sent_at is not None and end_fed_at is not None
                    assert controller.end_receipt_at is not None
                    assert end_sent_at <= controller.end_receipt_at <= end_fed_at
                    finalized.assert_awaited_once()
                    assert workload.kill_calls == 0
                elif frame_case == "invalid":
                    output_release.set()
                    with pytest.raises(ControlledGenerationError, match="out of order"):
                        await asyncio.shield(task)
                    assert workload.kill_calls == 1
                    assert messages == []
                else:
                    wait_release.set()
                    with pytest.raises(SandboxNotFoundError) as raised:
                        await asyncio.shield(task)
                    assert raised.value is loss
                    assert workload.kill_calls == 0
                    assert not output_release.is_set()
                    assert messages == []
                    finalized.assert_awaited_once()
                    if frame_case == "end":
                        assert controller.active_since is None
                        assert end_sent_at is not None and end_fed_at is not None
                        assert controller.end_receipt_at is not None
                        assert end_sent_at <= controller.end_receipt_at <= end_fed_at
                    else:
                        assert controller.active_since is None
            assert not watchdog.expired()
        finally:
            release_frame.set()
            output_release.set()
            wait_release.set()
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)

    @pytest.mark.parametrize("late_end", [False, True], ids=["exhausted-end", "late-end"])
    async def test_staged_end_deadline_while_wait_pending(
        self, monkeypatch: pytest.MonkeyPatch, late_end: bool
    ) -> None:
        key = b"stage-test-key"
        base_allowance = 1.0
        real_monotonic = time.monotonic
        offset = 0.0
        held_at: float | None = None
        ack_begin = asyncio.Event()
        release_end = asyncio.Event()
        release_wait = asyncio.Event()
        messages: list[str] = []
        acknowledgements: list[str] = []
        end_fed_at: float | None = None

        def shared_monotonic() -> float:
            return held_at if held_at is not None else real_monotonic() + offset

        class BoundaryWorkload(_FakeControlledWorkload):
            active_at_kill: bool | None = None

            async def kill(self) -> None:
                self.active_at_kill = controller.active_since is not None
                await super().kill()

            async def output(self) -> AsyncIterator[str]:
                nonlocal offset, held_at, end_fed_at
                try:
                    yield _signed_stage_frame(key, 1, "begin", None)
                    await release_end.wait()
                    assert controller.active_since is not None
                    end_at = controller.active_since + base_allowance + (0.03125 if late_end else 0.0)
                    assert end_at > shared_monotonic()
                    held_at = end_at
                    try:
                        yield _signed_stage_frame(key, 2, "end", None)
                    finally:
                        if held_at is not None:
                            offset = end_at - real_monotonic()
                            held_at = None
                    end_fed_at = end_at
                    await self.closed.wait()
                    yield "tail output"
                finally:
                    self.output_finished.set()

        class BoundarySandbox(_FakeControlledSandbox):
            async def upload_file(self, path: str, _data: bytes) -> None:
                acknowledgements.append(path)
                if path.endswith("/ack/1"):
                    ack_begin.set()

        class ReceiptController(ExternalServiceDeadlineController):
            end_receipt_at: float | None = None
            end_receipt_before_kill: float | None = None

            def end_generation(self, now: float) -> None:
                self.end_receipt_at = now
                if workload.kill_calls == 0:
                    self.end_receipt_before_kill = now
                super().end_generation(now)

        workload = BoundaryWorkload(
            wait_release=release_wait, wait_error=SandboxNotFoundError("wait lost"), natural=False
        )
        controller = ReceiptController(base_allowance_seconds=base_allowance)
        finalized = AsyncMock()
        accounting_failed = AsyncMock()
        with monkeypatch.context() as clock_patch:
            clock_patch.setattr(time, "monotonic", shared_monotonic)
            task = asyncio.create_task(
                _stream_controlled_output(
                    cast(Any, BoundarySandbox(workload)),
                    "echo done",
                    "/workspace",
                    messages.append,
                    controller,
                    finalized,
                    on_accounting_failure=accounting_failed,
                    stage_key=key,
                )
            )
            try:
                async with asyncio.timeout(2) as watchdog:
                    await ack_begin.wait()
                    release_end.set()
                    reason, _ = await asyncio.shield(task)
                assert not watchdog.expired()
                assert reason is AgentCausedExitReason.TIMEOUT
                assert workload.wait_finished.is_set()
                assert workload.output_finished.is_set()
                assert end_fed_at is not None
                if late_end:
                    assert controller.end_receipt_before_kill is None
                    assert workload.active_at_kill is True
                    assert controller.end_receipt_at is not None
                    assert controller.end_receipt_at > end_fed_at
                    assert controller.active_elapsed_seconds > base_allowance
                else:
                    assert controller.end_receipt_before_kill == end_fed_at
                    assert controller.active_elapsed_seconds == base_allowance
                    assert workload.active_at_kill is False
                assert workload.kill_calls == 1
                assert acknowledgements == ["/run/valkyrie-stage/ack/1"]
                assert messages == ["tail output"]
                finalized.assert_awaited_once()
                accounting_failed.assert_not_awaited()
            finally:
                if held_at is not None:
                    offset = held_at - real_monotonic()
                    held_at = None
                release_wait.set()
                release_end.set()
                workload.closed.set()
                task.cancel()
                await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)

    async def test_staged_deadline_drains_output_after_confirmed_stop(self) -> None:
        key = b"stage-test-key"
        messages: list[str] = []
        acknowledgements: list[str] = []

        class DeadlineWorkload(_FakeControlledWorkload):
            async def output(self) -> AsyncIterator[str]:
                try:
                    yield _signed_stage_frame(key, 1, "begin", None)
                    await self.closed.wait()
                    yield "deadline tail"
                finally:
                    self.output_finished.set()

        class DeadlineSandbox(_FakeControlledSandbox):
            async def upload_file(self, path: str, _data: bytes) -> None:
                acknowledgements.append(path)

        workload = DeadlineWorkload(wait_release=asyncio.Event(), natural=False)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=0.001)
        finalized = AsyncMock()
        async with asyncio.timeout(1) as watchdog:
            reason, _ = await _stream_controlled_output(
                cast(Any, DeadlineSandbox(workload)),
                "echo done",
                "/workspace",
                messages.append,
                controller,
                finalized,
                on_accounting_failure=AsyncMock(),
                stage_key=key,
            )
        assert not watchdog.expired()
        assert reason is AgentCausedExitReason.TIMEOUT
        assert acknowledgements == ["/run/valkyrie-stage/ack/1"]
        assert messages == ["deadline tail"]
        assert workload.kill_calls == 1
        finalized.assert_awaited_once()

    @pytest.mark.parametrize("failure", ["wait", "output", "kill"])
    async def test_confirmed_sandbox_loss_finalizes_once_and_reaches_task_handler(
        self, failure: Literal["wait", "output", "kill"]
    ) -> None:
        loss = SandboxNotFoundError("sandbox disappeared")
        workload = _FakeControlledWorkload(
            wait_error=(
                ProviderSandboxError("wait failed") if failure == "kill" else loss if failure == "wait" else None
            ),
            natural=False,
        )
        if failure == "output":
            workload.output_error = loss
        if failure == "kill":
            workload.kill_error = loss
        workload.closed.set()
        sandbox = _FakeControlledSandbox(workload)
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted = AsyncMock()

        with pytest.raises(SandboxNotFoundError) as raised:
            await _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                on_accounting_failure=AsyncMock(),
            )

        assert raised.value is loss
        assert sandbox.controlled_calls == [("echo done", "/workspace", None)]
        assert workload.kill_calls == (1 if failure == "kill" else 0)
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        assert client.read_calls >= 1
        assert controller.credit_frozen
        persisted.assert_awaited_once()
        assert persisted.await_args_list[0].args[0].external_service_credit_revision == client.read_calls
        assert persisted.await_args_list[0].args[0].generation_elapsed_seconds > 0
        assert controller.active_since is None

    async def test_staged_generic_wait_error_stops_drains_and_finalizes(self) -> None:
        workload = _FakeControlledWorkload(wait_error=RuntimeError("wait transport failed"), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        messages: list[str] = []
        finalized = AsyncMock()

        async with asyncio.timeout(1) as watchdog:
            with pytest.raises(ControlledGenerationError) as raised:
                await _stream_controlled_output(
                    cast(Any, sandbox),
                    "echo done",
                    "/workspace",
                    messages.append,
                    ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                    finalized,
                    on_accounting_failure=AsyncMock(),
                    stage_key=b"stage-test-key",
                )

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert str(raised.value.__cause__) == "wait transport failed"
        assert not watchdog.expired()
        assert workload.kill_calls == 1
        assert workload.closed.is_set()
        assert workload.output_finished.is_set()
        assert messages == ["agent output"]
        finalized.assert_awaited_once()

    @pytest.mark.parametrize("failure", ["wait", "kill"])
    async def test_confirmed_loss_finalizes_without_waiting_for_stalled_output(
        self, failure: Literal["wait", "kill"]
    ) -> None:
        loss = SandboxNotFoundError("sandbox disappeared")
        workload = _FakeControlledWorkload(
            wait_error=loss if failure == "wait" else ProviderSandboxError("read failed"),
            natural=False,
        )
        if failure == "kill":
            workload.kill_error = loss
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(workload.output_started.wait(), 1)
            with pytest.raises(SandboxNotFoundError) as raised:
                await asyncio.wait_for(asyncio.shield(task), 0.2)
            assert raised.value is loss
            assert not workload.closed.is_set()
            assert workload.output_finished.is_set()
            assert workload.kill_calls == (1 if failure == "kill" else 0)
            assert client.read_calls >= 1
            assert controller.credit_frozen
            persisted.assert_awaited_once()
            assert persisted.await_args_list[0].args[0].external_service_credit_revision == client.read_calls
        finally:
            workload.closed.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_out_of_order_stage_cleanup_loss_finalizes_without_draining_output(self) -> None:
        key = b"stage-key"
        wait_release = asyncio.Event()
        wait_started = asyncio.Event()
        emit_frame = asyncio.Event()
        output_gate = asyncio.Event()
        loss = SandboxNotFoundError("sandbox disappeared during cleanup kill")

        frame = _signed_stage_frame(key, 2, "begin", "nested-1")

        class OutOfOrderWorkload(_FakeControlledWorkload):
            async def output(self) -> AsyncIterator[str]:
                self.output_started.set()
                try:
                    await emit_frame.wait()
                    yield frame
                    await output_gate.wait()
                    self.output_drain_started.set()
                finally:
                    self.output_finished.set()

            async def wait(self) -> _CompletedControlledWorkload:
                wait_started.set()
                return await super().wait()

        workload = OutOfOrderWorkload(wait_release=wait_release)
        workload.kill_error = loss
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted = AsyncMock()
        sandbox = _FakeControlledSandbox(workload)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                stage_key=key,
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(wait_started.wait(), 1)
            await asyncio.wait_for(workload.output_started.wait(), 1)
            assert not workload.wait_finished.is_set()
            emit_frame.set()
            with pytest.raises(SandboxNotFoundError) as raised:
                await asyncio.wait_for(asyncio.shield(task), 1)
            assert raised.value is loss
            assert type(loss.__context__) is ControlledGenerationError
            assert str(loss.__context__) == "Sandbox error: Stage frames arrived out of order"
            assert workload.kill_calls == 1
            assert workload.wait_finished.is_set()
            assert workload.output_finished.is_set()
            assert not workload.output_drain_started.is_set()
            assert not output_gate.is_set()
            assert controller.credit_frozen
            persisted.assert_awaited_once()
            summary = persisted.await_args_list[0].args[0]
            assert summary.external_service_credit_revision == client.read_calls
            assert summary.generation_elapsed_seconds == 0
        finally:
            wait_release.set()
            emit_frame.set()
            output_gate.set()
            joined = asyncio.gather(task, return_exceptions=True)
            try:
                await asyncio.wait_for(asyncio.shield(joined), 1)
            except TimeoutError:
                task.cancel()
                await asyncio.wait_for(joined, 1)

    async def test_staged_confirmed_wait_loss_finalizes_with_pending_output(self) -> None:
        loss = SandboxNotFoundError("sandbox disappeared during wait")
        workload = _FakeControlledWorkload(wait_error=loss, natural=False)
        client = _FakeAccountingClient()
        controller = ExternalServiceDeadlineController(
            client=cast(Any, client),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=10.0,
            credit_cap_seconds=1.0,
        )
        persisted = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                stage_key=b"stage-key",
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(workload.wait_finished.wait(), 1)
            await asyncio.wait_for(workload.output_started.wait(), 1)
            with pytest.raises(SandboxNotFoundError) as raised:
                await asyncio.wait_for(asyncio.shield(task), 0.2)
            assert raised.value is loss
            assert not workload.closed.is_set()
            assert workload.output_finished.is_set()
            assert controller.credit_frozen
            persisted.assert_awaited_once()
        finally:
            workload.closed.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_staged_confirmed_wait_loss_accounts_for_buffered_end(self, monkeypatch: pytest.MonkeyPatch) -> None:
        loss = SandboxNotFoundError("sandbox disappeared during wait")
        key = b"stage-key"
        ack = asyncio.Event()
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        end_receipt_at: float | None = None
        original_frame = sandbox_module._StageFrame

        def capture_frame(seq: int, event: str, container: str | None, wire: str, received_at: float) -> Any:
            nonlocal end_receipt_at
            frame = original_frame(seq, event, container, wire, received_at)
            if event == "end":
                end_receipt_at = frame.received_at
            return frame

        monkeypatch.setattr(sandbox_module, "_StageFrame", capture_frame)
        end_fed = asyncio.Event()

        class AckGatedWorkload(_FakeControlledWorkload):
            async def output(self) -> AsyncIterator[str]:
                self.output_started.set()
                try:
                    yield _signed_stage_frame(key, 1, "begin", "nested-1")
                    await ack.wait()
                    await refresh_started.wait()
                    yield _signed_stage_frame(key, 2, "end", "nested-1")
                    end_fed.set()
                    await self.closed.wait()
                finally:
                    self.output_finished.set()

            async def wait(self) -> _CompletedControlledWorkload:
                try:
                    await end_fed.wait()
                    raise loss
                finally:
                    self.wait_finished.set()

        class AckSandbox(_FakeControlledSandbox):
            async def upload_file(self, path: str, data: bytes) -> None:
                assert path.endswith("/ack/1")
                ack.set()

        class DelayedRefreshClient(_FakeAccountingClient):
            async def read_counter(self, _accounting_id: str) -> AccountingSnapshot:
                self.read_calls += 1
                if self.read_calls >= 2:
                    refresh_started.set()
                    await release_refresh.wait()
                    return _accounting_snapshot(overhead_ms=1000, revision=self.read_calls)
                return _accounting_snapshot(revision=self.read_calls)

        class ReceiptController(ExternalServiceDeadlineController):
            started_at: float | None = None
            ended_at: float | None = None

            def begin_generation(self, now: float | None = None) -> None:
                super().begin_generation(now)
                self.started_at = self.active_since

            def end_generation(self, now: float) -> None:
                self.ended_at = now
                super().end_generation(now)

        workload = AckGatedWorkload(natural=False)
        controller = ReceiptController(
            client=cast(Any, DelayedRefreshClient()),
            snapshot=_accounting_snapshot(),
            base_allowance_seconds=0.03,
            credit_cap_seconds=1.0,
        )
        persisted = AsyncMock()
        failure_upload = AsyncMock()
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, AckSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                stage_key=key,
                on_accounting_failure=failure_upload,
            )
        )
        try:
            await asyncio.wait_for(refresh_started.wait(), 1)
            await asyncio.wait_for(end_fed.wait(), 1)
            await asyncio.wait_for(workload.wait_finished.wait(), 1)
            received_before_refresh_release = asyncio.get_running_loop().time()
            release_refresh.set()
            with pytest.raises(SandboxNotFoundError) as raised:
                await asyncio.wait_for(asyncio.shield(task), 0.2)
            assert raised.value is loss
            assert end_receipt_at is not None and end_receipt_at < received_before_refresh_release
            assert controller.started_at is not None
            assert controller.ended_at == end_receipt_at
            assert workload.output_finished.is_set()
            persisted.assert_awaited_once()
            failure_upload.assert_not_awaited()
            assert persisted.await_args.args[0].generation_elapsed_seconds == end_receipt_at - controller.started_at
        finally:
            release_refresh.set()
            workload.closed.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_deadline_stop_propagates_confirmed_sandbox_loss(self) -> None:
        loss = SandboxNotFoundError("sandbox disappeared during stop")
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.kill_error = loss
        workload.closed.set()
        persisted = AsyncMock()
        controller = ExternalServiceDeadlineController(base_allowance_seconds=0.001)

        with pytest.raises(SandboxNotFoundError) as raised:
            await _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                on_accounting_failure=AsyncMock(),
            )

        assert raised.value is loss
        assert workload.kill_calls == 1
        persisted.assert_awaited_once()
        assert controller.active_since is None

    async def test_unrelated_observation_and_kill_failures_remain_unconfirmed(self) -> None:
        workload = _FakeControlledWorkload(wait_error=ProviderSandboxError("wait failed"), natural=False)
        workload.kill_error = ProviderSandboxError("kill unavailable")
        finalized = AsyncMock()

        with pytest.raises(ControlledGenerationTerminationUnconfirmedError):
            await _stream_controlled_output(
                cast(Any, _FakeControlledSandbox(workload)),
                "echo done",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                finalized,
                on_accounting_failure=AsyncMock(),
            )

        assert workload.kill_calls == 1
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()
        finalized.assert_not_awaited()

    @pytest.mark.parametrize("failure", ["wait", "output"])
    async def test_unconfirmed_failure_bounds_blocked_kill_without_finalizing(
        self, monkeypatch: pytest.MonkeyPatch, failure: Literal["wait", "output"]
    ) -> None:
        monkeypatch.setattr(sandbox_module, "GENERATION_TERMINATION_GRACE_SECONDS", 0.01)
        workload = _FakeControlledWorkload(
            wait_error=ProviderSandboxError("wait failed") if failure == "wait" else None,
            natural=failure == "output",
        )
        if failure == "output":
            workload.output_error = ProviderSandboxError("output failed")
        workload.block_kill = True
        finalized = AsyncMock()

        async with asyncio.timeout(1) as watchdog:
            with pytest.raises(ControlledGenerationTerminationUnconfirmedError):
                await _stream_controlled_output(
                    cast(Any, _FakeControlledSandbox(workload)),
                    "echo done",
                    "/workspace",
                    _ignore_output,
                    ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                    finalized,
                    on_accounting_failure=AsyncMock(),
                )
        assert not watchdog.expired()
        assert workload.kill_calls == 1
        if failure == "wait":
            assert not workload.closed.is_set()
        finalized.assert_not_awaited()

    async def test_controlled_cancellation_kills_before_propagation(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        sandbox = _FakeControlledSandbox(workload)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                on_accounting_failure=AsyncMock(),
            )
        )
        await workload.output_started.wait()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert workload.kill_calls == 1
        assert workload.closed.is_set()
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

    async def test_controlled_cancellation_joins_children_when_kill_fails(self) -> None:
        workload = _FakeControlledWorkload(wait_release=asyncio.Event(), natural=False)
        workload.kill_error = ProviderSandboxError("kill unavailable")
        sandbox = _FakeControlledSandbox(workload)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "echo done",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=10.0),
                on_accounting_failure=AsyncMock(),
            )
        )
        await workload.output_started.wait()

        task.cancel()
        with pytest.raises(ControlledGenerationTerminationUnconfirmedError):
            await task

        assert workload.kill_calls == 1
        assert workload.wait_finished.is_set()
        assert workload.output_finished.is_set()

    async def test_active_deadline_stops_and_finalizes_before_canceled_upload_cleanup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        begin_acked = asyncio.Event()
        notification_started = asyncio.Event()
        canceled = asyncio.Event()
        release_cleanup = asyncio.Event()
        persisted = asyncio.Event()
        uploads: list[str] = []

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/ack/1"):
                begin_acked.set()
            if path.endswith("/exhausted"):
                notification_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    canceled.set()
                    await release_cleanup.wait()
                    raise RuntimeError("late upload cleanup")

        async def persist(_summary: GenerationAccountingSummary) -> None:
            assert workload.wait_finished.is_set() and workload.output_finished.is_set()
            persisted.set()

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=1.0)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                controller,
                persist,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        observed = False
        try:
            await asyncio.wait_for(begin_acked.wait(), 1)
            offset += 2.0
            await asyncio.wait_for(notification_started.wait(), 1)
            offset += sandbox_module.EPISODE_END_GRACE_SECONDS + 1.0
            await asyncio.wait_for(canceled.wait(), 1)
            await asyncio.wait_for(persisted.wait(), 1)
            assert workload.kill_calls == 1
            assert not task.done()
            assert uploads == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
            observed = True
        finally:
            release_cleanup.set()
            workload.closed.set()
            workload.wait_release.set()
            if not observed:
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        reason, _duration = task.result()
        assert reason == AgentCausedExitReason.TIMEOUT

    async def test_active_deadline_upload_error_is_not_deadline_expiry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        begin_acked = asyncio.Event()
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        uploads: list[str] = []
        persisted = AsyncMock()

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/ack/1"):
                begin_acked.set()
            else:
                raise ProviderSandboxError("upload unavailable")

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=1.0),
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(begin_acked.wait(), 1)
            offset += 2.0
            with pytest.raises(ProviderSandboxError, match="upload unavailable"):
                await asyncio.wait_for(task, 1)
        finally:
            workload.closed.set()
            workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert workload.kill_calls == 1
        persisted.assert_awaited_once()
        assert uploads == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]

    @pytest.mark.parametrize("upload_error", [False, True])
    async def test_notification_completion_before_cutoff_survives_delayed_waiter(
        self, monkeypatch: pytest.MonkeyPatch, upload_error: bool
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0

        def controlled_time() -> float:
            return real_time() + offset

        monkeypatch.setattr(loop, "time", controlled_time)
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        begin_acked = asyncio.Event()
        notification_uploaded = asyncio.Event()
        uploaded: list[str] = []
        persisted = AsyncMock()
        original_error = ProviderSandboxError("early upload failure")

        def advance_past_cutoff() -> None:
            nonlocal offset
            offset += sandbox_module.EPISODE_END_GRACE_SECONDS + 1.0

        async def upload(path: str, _content: bytes) -> None:
            uploaded.append(path)
            if path.endswith("/exhausted"):
                notification_uploaded.set()
                loop.call_soon(advance_past_cutoff)
                if upload_error:
                    raise original_error
            else:
                begin_acked.set()

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=1.0),
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(begin_acked.wait(), 1)
            offset += 2.0
            if upload_error:
                with pytest.raises(ProviderSandboxError) as captured:
                    await task
                assert captured.value is original_error
                assert uploaded == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
                assert workload.kill_calls == 1
            else:
                await asyncio.wait_for(notification_uploaded.wait(), sandbox_module.EPISODE_END_GRACE_SECONDS + 2.0)
                assert uploaded == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
                assert workload.kill_calls == 0
                workload.closed.set()
                workload.wait_release.set()
                await task
            persisted.assert_awaited_once()
        finally:
            workload.closed.set()
            workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    @pytest.mark.parametrize("after_cutoff", [0.0, 1.0])
    async def test_exhausted_notification_at_or_after_cutoff_stops_without_further_ack(
        self, monkeypatch: pytest.MonkeyPatch, after_cutoff: float
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        frozen = False
        fixed_time = 0.0
        origin = 0.0
        begin_acked = asyncio.Event()
        upload_started = asyncio.Event()
        clock_resumed = asyncio.Event()
        persisted_event = asyncio.Event()
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        uploads: list[str] = []
        persisted = AsyncMock(side_effect=lambda _summary: persisted_event.set())

        def clock() -> float:
            return fixed_time if frozen else real_time() + offset

        def resume_clock() -> None:
            nonlocal frozen, offset
            offset = fixed_time - real_time()
            frozen = False
            clock_resumed.set()

        async def upload(path: str, _content: bytes) -> None:
            nonlocal fixed_time, frozen
            uploads.append(path)
            if path.endswith("/ack/1"):
                begin_acked.set()
            if path.endswith("/exhausted"):
                fixed_time = origin + sandbox_module.EPISODE_END_GRACE_SECONDS + after_cutoff
                frozen = True
                upload_started.set()
                loop.call_soon(resume_clock)
                raise ProviderSandboxError("late upload failure")

        monkeypatch.setattr(loop, "time", clock)
        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        controller = ExternalServiceDeadlineController(base_allowance_seconds=1.0)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        stopped = False
        try:
            await asyncio.wait_for(begin_acked.wait(), 1)
            assert controller.active_since is not None
            deadline = controller.deadline(loop.time())
            assert deadline is not None
            origin = deadline
            offset += 2.0
            await asyncio.wait_for(upload_started.wait(), sandbox_module.EPISODE_END_GRACE_SECONDS + 2.0)
            await asyncio.wait_for(clock_resumed.wait(), 1)
            await asyncio.wait_for(workload.kill_started.wait(), 1)
            await asyncio.wait_for(persisted_event.wait(), 1)
            assert uploads == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
            assert workload.kill_calls == 1
            stopped = True
        finally:
            workload.closed.set()
            workload.wait_release.set()
            if not stopped:
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        reason, _duration = task.result()
        assert reason == AgentCausedExitReason.TIMEOUT
        persisted.assert_awaited_once()

    async def test_repeated_cancellation_owns_terminal_counter_read_after_stop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        acked = asyncio.Event()
        read_started = asyncio.Event()
        read_release = asyncio.Event()
        persisted = AsyncMock()

        class BlockedCounterClient(_FakeAccountingClient):
            async def read_counter(self, accounting_id: str) -> AccountingSnapshot:
                read_started.set()
                await read_release.wait()
                return await super().read_counter(accounting_id)

        async def upload(path: str, _content: bytes) -> None:
            if path.endswith("/ack/1"):
                acked.set()

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=None, client=cast(Any, BlockedCounterClient()), snapshot=_accounting_snapshot()
        )
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                controller,
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        observed = False
        try:
            await asyncio.wait_for(acked.wait(), 1)
            task.cancel()
            await asyncio.wait_for(workload.kill_started.wait(), 1)
            await asyncio.wait_for(read_started.wait(), 1)
            task.cancel()
            assert not task.done()
            observed = True
        finally:
            read_release.set()
            workload.closed.set()
            workload.wait_release.set()
            if not observed:
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert workload.wait_finished.is_set() and workload.output_finished.is_set()
        persisted.assert_awaited_once()

    async def test_episode_duration_excludes_async_factory_preparation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        loop = asyncio.get_running_loop()
        clock = 100.0
        monkeypatch.setattr(loop, "time", lambda: clock)
        workload = _FakeControlledWorkload(absence_confirmed_at=200.0)
        sandbox = _FakeControlledSandbox(workload)

        async def prepare(
            _sandbox: Any, _command: str, *, cwd: str | None = None, env_vars: Mapping[str, str] | None = None
        ) -> _FakeControlledWorkload:
            nonlocal clock
            await asyncio.sleep(0)
            clock = 200.0
            return workload

        monkeypatch.setattr(sandbox_module, "controlled_episode_workload", prepare)
        reason, duration = await _stream_controlled_output(
            cast(Any, sandbox),
            "agent",
            "/workspace",
            _ignore_output,
            ExternalServiceDeadlineController(base_allowance_seconds=1.0),
            stage_key=b"k" * 32,
            episode=Episode(command="stage", parallel_agents=1),
            on_accounting_failure=AsyncMock(),
        )
        assert reason is None
        assert duration == 0.0

    async def test_episode_accepts_successive_aggregate_intervals(self) -> None:
        key = b"k" * 32
        acknowledgements: list[str] = []
        acked = {seq: asyncio.Event() for seq in range(1, 5)}
        controller = ExternalServiceDeadlineController(base_allowance_seconds=5.0)

        class SlotIntervals(_FakeControlledWorkload):
            def __init__(self) -> None:
                super().__init__(wait_release=asyncio.Event(), natural=False)

            async def output(self) -> AsyncIterator[str]:
                try:
                    for seq, event in enumerate(("begin", "end", "begin", "end"), start=1):
                        yield _signed_stage_frame(key, seq, event, None)
                        await acked[seq].wait()
                        if seq == 2:
                            assert controller.active_since is None
                        if seq == 3:
                            assert controller.active_since is not None
                    yield "orchestrator evaluation output"
                    self.closed.set()
                    assert self.wait_release is not None
                    self.wait_release.set()
                finally:
                    self.output_finished.set()

        class EpisodeSandbox(_FakeControlledSandbox):
            async def upload_file(self, path: str, _content: bytes) -> None:
                acknowledgements.append(path)
                acked[int(path.rsplit("/", 1)[1])].set()

        workload = SlotIntervals()
        messages: list[str] = []
        persisted = AsyncMock()
        async with asyncio.timeout(1):
            reason, _duration = await _stream_controlled_output(
                cast(Any, EpisodeSandbox(workload)),
                "orchestrator",
                "/workspace",
                messages.append,
                controller,
                persisted,
                on_accounting_failure=AsyncMock(),
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=2),
            )
        assert reason is None
        assert controller.active_since is None
        assert controller.active_elapsed_seconds > 0
        assert acknowledgements == [f"{sandbox_module._STAGE_DIR}/ack/{seq}" for seq in range(1, 5)]
        assert messages == ["orchestrator evaluation output"]
        assert workload.kill_calls == 0
        persisted.assert_awaited_once()

    async def test_timely_notice_without_end_expires_second_grace_and_finalizes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        begin_acked = asyncio.Event()
        notice_uploaded = asyncio.Event()
        uploads: list[str] = []
        persisted = AsyncMock()

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/ack/1"):
                begin_acked.set()
            if path.endswith("/exhausted"):
                notice_uploaded.set()

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=1.0),
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(begin_acked.wait(), 1)
            offset += 2.0
            await asyncio.wait_for(notice_uploaded.wait(), 1)
            await asyncio.sleep(0)
            assert uploads == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
            assert workload.kill_calls == 0
            assert not task.done()

            offset += sandbox_module.EPISODE_END_GRACE_SECONDS + 1.0
            reason, _duration = await asyncio.wait_for(task, 1)
            assert reason == AgentCausedExitReason.TIMEOUT
            assert workload.kill_calls == 1
            assert workload.wait_finished.is_set()
            assert workload.output_finished.is_set()
            persisted.assert_awaited_once()
            assert uploads == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
        finally:
            workload.closed.set()
            workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_run_agent_notice_then_nonzero_returns_timeout_and_archives(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        key = b"k" * 32
        notice = asyncio.Event()
        acked = asyncio.Event()
        uploads: list[str] = []
        finalized = AsyncMock()

        class ExitingOrchestrator(_FakeControlledWorkload):
            def __init__(self) -> None:
                super().__init__(exit_code=42, wait_release=asyncio.Event(), natural=False)

            async def output(self) -> AsyncIterator[str]:
                try:
                    yield _signed_stage_frame(key, 1, "begin", None)
                    await notice.wait()
                    await acked.wait()
                    self.closed.set()
                    assert self.wait_release is not None
                    self.wait_release.set()
                finally:
                    self.output_finished.set()

        workload = ExitingOrchestrator()
        sandbox = _FakeControlledSandbox(workload)

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/exhausted"):
                notice.set()
            if path.endswith("/ack/1"):
                acked.set()

        async def archive_after_finalization(*_args: Any, **_kwargs: Any) -> None:
            assert workload.wait_finished.is_set() and workload.output_finished.is_set()
            finalized.assert_awaited_once()

        archive = AsyncMock(side_effect=archive_after_finalization)
        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        monkeypatch.setattr(sandbox_module, "_provision_stage", AsyncMock(return_value=key))
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(return_value=ExecResult(exit_code=0)))
        monkeypatch.setattr(sandbox_module, "archive_and_upload_output", archive)
        messages: list[str] = []
        task = asyncio.create_task(
            run_agent(
                cast(Any, sandbox),
                _controlled_contract(),
                "/tmp/problem.txt",
                "task_0",
                messages.append,
                "/workspace",
                object_store=_mock_object_store(),
                agent_output_s3_key="benchmarks/run/task/output.tar.gz",
                plan=GenerationPlan(
                    allowance_seconds=1.0, staged=True, episode=Episode(command="stage", parallel_agents=1)
                ),
                deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=1.0),
                on_external_service_finalized=finalized,
            )
        )
        try:
            await asyncio.wait_for(acked.wait(), 1)
            offset += 2.0
            await asyncio.wait_for(notice.wait(), 1)
            reason, _duration = await asyncio.wait_for(task, 1)
            assert reason is AgentCausedExitReason.TIMEOUT
            assert uploads == [
                f"{sandbox_module._STAGE_DIR}/agent.json",
                f"{sandbox_module._STAGE_DIR}/ack/1",
                f"{sandbox_module._STAGE_DIR}/exhausted",
            ]
            archive.assert_awaited_once()
            assert archive.await_args.args[1] == "/workspace/.valkyrie/slots"
            assert workload.kill_calls == 0
            assert any("evaluation will proceed" in message for message in messages)
        finally:
            workload.closed.set()
            assert workload.wait_release is not None
            workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_timely_notification_and_end_allow_evaluation_past_window(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        key = b"k" * 32

        emit_end = asyncio.Event()
        continue_evaluation = asyncio.Event()

        class EndThenEvaluate(_StageFrameWorkload):
            async def output(self) -> AsyncIterator[str]:
                try:
                    self.output_started.set()
                    yield _signed_stage_frame(key, 1, "begin", None)
                    await emit_end.wait()
                    yield _signed_stage_frame(key, 2, "end", None)
                    await continue_evaluation.wait()
                    yield "evaluation still running\n"
                    await self.closed.wait()
                    self.output_drain_started.set()
                finally:
                    self.output_finished.set()

        workload = EndThenEvaluate("")
        sandbox = _FakeControlledSandbox(workload)
        ended = asyncio.Event()
        begun = asyncio.Event()
        evaluation_output = asyncio.Event()
        uploads: list[str] = []
        persisted = AsyncMock()

        def observe_output(chunk: str) -> None:
            if "evaluation still running" in chunk:
                evaluation_output.set()

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/ack/1"):
                begun.set()
            if path.endswith("/ack/2"):
                ended.set()

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                observe_output,
                ExternalServiceDeadlineController(base_allowance_seconds=5.0),
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(begun.wait(), 1)
            offset += 6.0
            emit_end.set()
            await asyncio.wait_for(ended.wait(), 1)
            offset += sandbox_module.EPISODE_END_GRACE_SECONDS + 1.0
            continue_evaluation.set()
            await asyncio.wait_for(evaluation_output.wait(), 1)
            assert f"{sandbox_module._STAGE_DIR}/exhausted" in uploads
            assert workload.kill_calls == 0
            assert not task.done()
            workload.closed.set()
            workload.wait_release.set()
            reason, _duration = await asyncio.wait_for(task, 1)
            assert reason == AgentCausedExitReason.TIMEOUT
            persisted.assert_awaited_once()
        finally:
            emit_end.set()
            continue_evaluation.set()
            workload.closed.set()
            workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_exhausted_end_checks_nested_container_before_ack_and_seal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        key = b"k" * 32
        begin_ack = asyncio.Event()
        end_ack = asyncio.Event()
        emit_end = asyncio.Event()
        confirming = asyncio.Event()
        release_confirmation = asyncio.Event()
        deadline_waiting = asyncio.Event()
        release_deadline = asyncio.Event()
        original_sleep = asyncio.sleep

        async def sleep(delay: float) -> None:
            if delay >= 4.0:
                deadline_waiting.set()
                await release_deadline.wait()
            else:
                await original_sleep(delay)

        monkeypatch.setattr(asyncio, "sleep", sleep)

        class EndAfterBegin(_StageFrameWorkload):
            async def output(self) -> AsyncIterator[str]:
                nonlocal offset
                try:
                    self.output_started.set()
                    yield _signed_stage_frame(key, 1, "begin", "nested-live")
                    await emit_end.wait()
                    offset = 6.0
                    yield _signed_stage_frame(key, 2, "end", "nested-live")
                    await self.closed.wait()
                    self.output_drain_started.set()
                finally:
                    self.output_finished.set()

        workload = EndAfterBegin("")
        sandbox = _FakeControlledSandbox(workload)
        persisted = AsyncMock()
        uploads: list[str] = []

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/ack/1"):
                begin_ack.set()
            if path.endswith("/ack/2"):
                end_ack.set()

        async def exec_container_command(command: str) -> ExecResult:
            if command == "cat /proc/$$/task/$$/children >/dev/null":
                return ExecResult(exit_code=0)
            if command == "docker rm -f nested-live":
                confirming.set()
                await release_confirmation.wait()
                return ExecResult(exit_code=0, output="")
            assert command == "docker container ls -a --format '{{.Names}}'"
            return ExecResult(exit_code=0, output="nested-live\n")

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        monkeypatch.setattr(sandbox, "exec", exec_container_command, raising=False)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=5),
                persisted,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        try:
            await asyncio.wait_for(begin_ack.wait(), 1)
            await asyncio.wait_for(deadline_waiting.wait(), 1)
            emit_end.set()
            confirmation_started = asyncio.create_task(confirming.wait())
            end_acknowledged = asyncio.create_task(end_ack.wait())
            done, pending = await asyncio.wait(
                {confirmation_started, end_acknowledged}, timeout=10, return_when=asyncio.FIRST_COMPLETED
            )
            for waiter in pending:
                waiter.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            assert confirmation_started in done, "END acknowledged before nested-container absence confirmation"
            assert not end_ack.is_set()
            persisted.assert_not_awaited()
            release_confirmation.set()
            with pytest.raises(ControlledGenerationTerminationUnconfirmedError):
                await asyncio.wait_for(task, 10)
            assert not end_ack.is_set()
            persisted.assert_not_awaited()
        finally:
            release_deadline.set()
            emit_end.set()
            release_confirmation.set()
            workload.closed.set()
            workload.wait_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_external_cancellation_owns_notification_until_after_stop_and_finalize(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
        key = b"k" * 32
        workload = _StageFrameWorkload(_signed_stage_frame(key, 1, "begin", None))
        sandbox = _FakeControlledSandbox(workload)
        begin_acked = asyncio.Event()
        upload_started = asyncio.Event()
        notification_cancelled = asyncio.Event()
        release_cleanup = asyncio.Event()
        persisted = asyncio.Event()
        uploads: list[str] = []

        async def upload(path: str, _content: bytes) -> None:
            uploads.append(path)
            if path.endswith("/ack/1"):
                begin_acked.set()
                return
            assert path.endswith("/exhausted")
            upload_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                notification_cancelled.set()
                await release_cleanup.wait()

        async def persist(_summary: GenerationAccountingSummary) -> None:
            assert workload.wait_finished.is_set() and workload.output_finished.is_set()
            persisted.set()

        monkeypatch.setattr(sandbox, "upload_file", upload, raising=False)
        task = asyncio.create_task(
            _stream_controlled_output(
                cast(Any, sandbox),
                "agent",
                "/workspace",
                _ignore_output,
                ExternalServiceDeadlineController(base_allowance_seconds=1.0),
                persist,
                stage_key=key,
                episode=Episode(command="stage", parallel_agents=1),
                on_accounting_failure=AsyncMock(),
            )
        )
        observed = False
        try:
            await asyncio.wait_for(begin_acked.wait(), 1)
            offset += 2.0
            await asyncio.wait_for(upload_started.wait(), 1)
            task.cancel()
            task.cancel()
            await asyncio.wait_for(persisted.wait(), 1)
            await asyncio.wait_for(notification_cancelled.wait(), 1)
            assert workload.kill_calls == 1
            assert not task.done()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), 0.05)
            assert uploads == [f"{sandbox_module._STAGE_DIR}/ack/1", f"{sandbox_module._STAGE_DIR}/exhausted"]
            observed = True
        finally:
            release_cleanup.set()
            workload.closed.set()
            workload.wait_release.set()
            if not observed:
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)


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
        setup_results: deque[AgentCausedExitReason | None] = deque([AgentCausedExitReason.TIMEOUT, None])

        async def fake_stream_command_output(
            _sandbox: Any,
            command: str,
            _log_output: Any,
            **_kwargs: Any,
        ) -> AgentCausedExitReason | None:
            observed_commands.append(command)

            return setup_results.popleft()

        def log_output(_message: str) -> None:
            pass

        monkeypatch.setattr(sandbox_module, "stream_command_output", fake_stream_command_output)

        await _install_agent_dependencies(Mock(), contract, log_output)

        expected_command = "cd /bundle/test-agent && exec timeout 600 sh -c 'apt-get update -qq && echo done'"
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
                None,
            ]
        )
        sleep = AsyncMock()
        monkeypatch.setattr(sandbox_module, "stream_command_output", stream_command)
        monkeypatch.setattr(asyncio, "sleep", sleep)

        redact_error = sandbox_module.create_agent_error_redactor(("fake-install-secret",))
        await _install_agent_dependencies(Mock(), contract, _ignore_output, redact_error=redact_error)

        assert stream_command.await_count == 4
        assert [call.args[0] for call in sleep.await_args_list] == [0.0, 10.0, 60.0]
        assert all(call.kwargs["redact_error"] is redact_error for call in stream_command.await_args_list)

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
        redact_error = sandbox_module.create_agent_error_redactor(("fake-install-secret",))

        with pytest.raises(AgentRunFailedError):
            await _install_agent_dependencies(
                Mock(),
                contract,
                _ignore_output,
                mode=mode.FINAL_FRESH_SANDBOX,
                redact_error=redact_error,
                secret_env_names=("PROVIDER_TOKEN",),
            )

        assert stream_command.await_count == 1
        assert stream_command.call_args.kwargs["redact_error"] is redact_error
        assert stream_command.call_args.kwargs["secret_env_names"] == ("PROVIDER_TOKEN",)
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
            creation_state=SandboxCreationState(cleanup_pending=False),
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
        async with create_sandbox(**common_arguments, creation_state=SandboxCreationState(cleanup_pending=False)):
            pass
        async with create_sandbox(
            **common_arguments, creation_state=SandboxCreationState(cleanup_pending=False), unique_name=False
        ):
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
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            assert sandbox is mock_sandbox

        request = provider.create_sandbox.await_args.args[0]
        assert request.source == source
        assert distribution.call_args.kwargs["tags"] == {"image": "snapshot"}
        set_sandbox_context.assert_called_once_with(mock_sandbox, image="snapshot")
        provider.delete_sandbox.assert_awaited_once_with(mock_sandbox.id)

    @pytest.mark.parametrize(
        ("create_error", "expected_error", "metric_class"),
        [
            pytest.param(RuntimeError("create failed"), RuntimeError, "RuntimeError", id="unexpected"),
            pytest.param(ProviderSandboxError("quota"), SandboxSetupError, "SandboxError", id="provider"),
        ],
    )
    async def test_create_sandbox_emits_error_metric(
        self,
        monkeypatch: pytest.MonkeyPatch,
        create_error: Exception,
        expected_error: type[Exception],
        metric_class: str,
    ) -> None:
        """Provider creation failures become a fresh-sandbox retry; the metric still names the provider error."""
        increments: list[tuple[str, dict[str, str]]] = []

        async def mock_create_sandbox(*_args: Any, **_kwargs: Any) -> Never:
            raise create_error

        def fake_incr(name: str, _value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)

        resources = Resources(vcpu=2, memory=4, disk=5)
        with pytest.raises(expected_error):
            async with create_sandbox(
                provider=AsyncMock(),
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=resources,
                creation_semaphore=asyncio.Semaphore(1),
                creation_state=SandboxCreationState(cleanup_pending=False),
            ):
                pass

        assert increments == [("valkyrie.sandbox.create.errors", {"error_class": metric_class})]

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

        async def mock_delete_sandbox(sandbox: AsyncMock, _provider: Any, **kwargs: Any) -> bool:
            active_sandbox_ids.remove(sandbox.id)
            deletion_initiators.append(kwargs.get("initiated_by"))
            return True

        monkeypatch.setattr(sandbox_module, "_create_sandbox", mock_create_sandbox)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", mock_delete_sandbox)

        async def use_sandbox() -> None:
            async with create_sandbox(
                provider=AsyncMock(),
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=Resources(vcpu=2, memory=4, disk=5),
                creation_semaphore=asyncio.Semaphore(1),
                creation_state=SandboxCreationState(cleanup_pending=False),
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
            creation_state=SandboxCreationState(cleanup_pending=False),
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
            creation_state=SandboxCreationState(cleanup_pending=False),
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
                creation_state=SandboxCreationState(cleanup_pending=False),
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
                creation_state=SandboxCreationState(cleanup_pending=False),
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
                creation_state=SandboxCreationState(cleanup_pending=False),
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


class TestSandboxCreationState:
    @pytest.mark.parametrize("cancel_creation", [False, True], ids=["teardown", "cancelled-create"])
    @pytest.mark.parametrize(
        ("delete_error", "pending", "propagates"),
        [
            (None, False, False),
            (SandboxNotFoundError("gone"), False, False),
            (RuntimeError("delete unknown"), True, False),
            (ProviderSandboxError("delete failed"), True, True),
            (asyncio.CancelledError(), True, True),
        ],
        ids=["acknowledged", "already-gone", "unexpected-error", "provider-error", "interrupted-delete"],
    )
    async def test_cleanup_pending_clears_only_after_acknowledged_delete(
        self, cancel_creation: bool, delete_error: BaseException | None, pending: bool, propagates: bool
    ) -> None:
        state = SandboxCreationState(cleanup_pending=False)
        creating = asyncio.Event()
        finish_creation = asyncio.Event()
        deleting = asyncio.Event()
        finish_deletion = asyncio.Event()
        sandbox = Mock(id="sandbox-state", name="sandbox-state", labels={})
        provider = AsyncMock()

        async def create(_request: Any) -> Mock:
            assert state.cleanup_pending
            creating.set()
            await finish_creation.wait()
            return sandbox

        async def delete(_sandbox_id: str) -> None:
            assert state.cleanup_pending
            deleting.set()
            await finish_deletion.wait()
            if delete_error is not None:
                raise delete_error

        provider.create_sandbox.side_effect = create
        provider.delete_sandbox.side_effect = delete

        async def use_sandbox() -> None:
            async with create_sandbox(
                provider=provider,
                sandbox_name="sandbox-state",
                source=ImageSource(image="test-image"),
                resources=Resources(vcpu=1, memory=2, disk=3),
                creation_semaphore=asyncio.Semaphore(1),
                creation_state=state,
            ):
                assert state.cleanup_pending

        entering = asyncio.create_task(use_sandbox())
        await asyncio.wait_for(creating.wait(), timeout=5)
        if cancel_creation:
            entering.cancel()
        finish_creation.set()
        await asyncio.wait_for(deleting.wait(), timeout=5)
        assert state.cleanup_pending
        assert not entering.done()
        finish_deletion.set()
        if isinstance(delete_error, asyncio.CancelledError) or (cancel_creation and not propagates):
            with pytest.raises(asyncio.CancelledError):
                await entering
        elif cancel_creation and propagates:
            with pytest.raises(ProviderSandboxError) as exc_info:
                await entering
            assert exc_info.value is delete_error
        else:
            await entering
        assert state.cleanup_pending is pending
        provider.delete_sandbox.assert_awaited_once_with(sandbox.id)

    async def test_cancel_before_provider_entry_has_no_cleanup_pending(self) -> None:
        waiting = asyncio.Event()

        class ObservedSemaphore(asyncio.Semaphore):
            async def acquire(self) -> Literal[True]:
                waiting.set()
                return await super().acquire()

        state = SandboxCreationState(cleanup_pending=False)
        provider = AsyncMock()

        async def use_sandbox() -> None:
            async with create_sandbox(
                provider=provider,
                sandbox_name="waiting",
                source=ImageSource(image="test-image"),
                resources=Resources(vcpu=1, memory=2, disk=3),
                creation_semaphore=ObservedSemaphore(0),
                creation_state=state,
            ):
                pytest.fail("cancelled before creation")

        entering = asyncio.create_task(use_sandbox())
        await asyncio.wait_for(waiting.wait(), timeout=5)
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering
        assert not state.cleanup_pending
        provider.create_sandbox.assert_not_awaited()
        provider.delete_sandbox.assert_not_awaited()

    async def test_unknown_creation_keeps_cleanup_pending_without_a_handle(self) -> None:
        state = SandboxCreationState(cleanup_pending=False)
        error = ProviderSandboxError("creation unknown")
        provider = AsyncMock()
        provider.create_sandbox.side_effect = error
        with pytest.raises(SandboxSetupError) as exc_info:
            async with create_sandbox(
                provider=provider,
                sandbox_name="unknown",
                source=ImageSource(image="test-image"),
                resources=Resources(vcpu=1, memory=2, disk=3),
                creation_semaphore=asyncio.Semaphore(1),
                creation_state=state,
            ):
                pytest.fail("creation failed")
        assert exc_info.value.__cause__ is error
        assert state.cleanup_pending
        provider.delete_sandbox.assert_not_awaited()


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
            acknowledged = await _delete_sandbox(self._sandbox(), provider, initiated_by="force_stop", org_id="org-1")
            assert acknowledged is (outcome in ("deleted", "already_gone"))

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
        monkeypatch.setattr(upload_agent_artifacts.retry, "wait", wait_none())
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

    async def test_upload_agent_artifacts_keeps_sandbox_not_found_type(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        """A vanished sandbox must reach the sandbox-loss recovery path, not become a generic SandboxError."""
        store = _mock_object_store()
        store.temporary_download_url = AsyncMock(return_value="https://example.com/presigned")
        monkeypatch.setattr(sandbox_module, "_exec", AsyncMock(side_effect=SandboxNotFoundError("gone")))

        with pytest.raises(SandboxNotFoundError):
            await upload_agent_artifacts(AsyncMock(), contract, "bench-123", store)


class TestEgressPolicy:
    """Tracker-side replacement of the sandbox egress policy."""

    @pytest.mark.parametrize(
        ("policy", "expected_event"),
        [
            ("*", "clear"),
            ([], "block"),
            (["https://api.openai.com"], "modify:https://api.openai.com"),
        ],
    )
    async def test_apply_egress_policy_replaces_provider_policy(
        self,
        policy: EgressPolicy,
        expected_event: str,
    ) -> None:
        events: list[str] = []

        async def modify(allowed_addresses: list[str]) -> None:
            events.append(f"modify:{','.join(allowed_addresses)}")

        async def block() -> None:
            events.append("block")

        async def clear() -> None:
            events.append("clear")

        sandbox = Mock()
        sandbox.modify_egress_rules = modify
        sandbox.block_all_egress = block
        sandbox.clear_egress_rules = clear

        await _apply_egress_policy(sandbox, policy)

        assert events == [expected_event]

    @pytest.mark.parametrize(
        ("provider_error", "expected_error", "message", "attempts", "transient"),
        [
            (
                ValueError("bad allowlist"),
                InvalidSandboxConfigurationError,
                "Failed to apply egress policy: bad allowlist",
                1,
                False,
            ),
            (ProviderSandboxError("provider failed"), SandboxError, "provider failed", 3, True),
        ],
    )
    async def test_apply_egress_policy_maps_provider_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
        provider_error: Exception,
        expected_error: type[SandboxError],
        message: str,
        attempts: int,
        transient: bool,
    ) -> None:
        """Provider network failures are retried and stay transient; a rejected allowlist fails the same way anywhere."""
        monkeypatch.setattr(_run_egress_operation.retry, "wait", wait_none())
        sandbox = Mock()
        sandbox.modify_egress_rules = AsyncMock(side_effect=provider_error)

        with pytest.raises(expected_error, match=message) as exc_info:
            await _apply_egress_policy(sandbox, ["https://api.openai.com"])

        assert exc_info.value.transient is transient
        assert sandbox.modify_egress_rules.await_count == attempts


class TestStreamCommandOutputAgentFailure:
    """Agent command output and error classification."""

    async def _reported_failure(self, content: str, secret_values: tuple[str, ...] | None) -> tuple[str, list[str]]:
        commands: list[str] = []

        async def command(_command: str) -> AsyncIterator[str]:
            yield "unrelated raw output"
            raise ProviderSandboxCommandError(1)

        async def execute(command: str) -> ExecResult:
            commands.append(command)
            if command.startswith("head -c "):
                limit = int(command.split()[2])

                return ExecResult(exit_code=0, output=content.encode()[:limit].decode(errors="replace"))

            return ExecResult(exit_code=0, output="1000000000")

        sandbox = Mock(id="privacy-test", command=command, exec=execute)
        sandbox.name = "privacy-test"
        sandbox.state = "started"

        with pytest.raises(AgentRunFailedError) as error:
            await sandbox_module.stream_command_output(
                sandbox,
                "run-agent",
                lambda _: None,
                redact_error=None
                if secret_values is None
                else sandbox_module.create_agent_error_redactor(secret_values),
            )

        return str(error.value), commands

    @pytest.mark.parametrize("content", ["test-secret-value", "ValueError: test-secret-value"])
    async def test_opaque_secrets_disable_optional_error_read(self, content: str) -> None:
        message, commands = await self._reported_failure(content, None)

        assert message == "Sandbox error: Agent command failed with exit code 1"
        assert not any(command.startswith("head -c ") for command in commands)

    async def test_known_secret_values_are_masked_before_error_is_raised(self) -> None:
        message, _ = await self._reported_failure(
            "ValueError: rejected test-secret-long and test-secret",
            ("test-secret", "test-secret-long", ""),
        )

        assert message.endswith("ValueError: rejected [REDACTED] and [REDACTED]")
        assert "test-secret" not in message
        assert "unrelated raw output" not in message

    async def test_multiline_secret_is_masked_before_whitespace_normalization(self) -> None:
        message, _ = await self._reported_failure("ValueError: rejected line-one\nline-two", ("line-one\nline-two",))

        assert message.endswith("ValueError: rejected [REDACTED]")

    @pytest.mark.parametrize(("padding", "retained_characters"), [("x", 2025), ("é", 1012)])
    async def test_masking_expansion_preserves_a_bounded_summary(self, padding: str, retained_characters: int) -> None:
        prefix = "ValueError: API_KEY "
        padding_bytes = 2048 - len(prefix.encode())
        count, remainder = divmod(padding_bytes, len(padding.encode()))
        content = prefix + padding * count + "x" * remainder

        message, _ = await self._reported_failure(content, ("API_KEY",))

        assert "ValueError: [REDACTED] " in message
        assert "API_KEY" not in message

        summary = message.split("exit code 1: ", 1)[1]
        assert summary == "ValueError: [REDACTED] " + padding * retained_characters
        assert len(summary.encode()) <= 2048
        assert "\ufffd" not in summary

    @pytest.mark.parametrize("content", ["raw prompt or model output", "", "ValueError:", "ValueError: bad\x00text"])
    async def test_malformed_report_keeps_only_exit_code(self, content: str) -> None:
        message, _ = await self._reported_failure(content, ())

        assert message == "Sandbox error: Agent command failed with exit code 1"

    async def test_oversized_report_is_not_truncated_through_a_secret(self) -> None:
        content = "ValueError: " + "x" * 2030 + "test-secret-value"

        message, commands = await self._reported_failure(content, ("test-secret-value",))

        assert message == "Sandbox error: Agent command failed with exit code 1"
        assert any(command.startswith("head -c 2049 ") for command in commands)

    async def test_small_original_error_is_preserved_with_no_declared_secrets(self) -> None:
        message, _ = await self._reported_failure("ValueError: no patch\n", ())

        assert message.endswith("ValueError: no patch")

    async def test_stream_command_output_forwards_output_and_returns_reason(self) -> None:
        observed_commands: list[str] = []
        output: list[str] = []

        async def stream_command(command: str) -> AsyncIterator[str]:
            observed_commands.append(command)
            yield "done\n"

        mock_sandbox = Mock()
        mock_sandbox.command = stream_command

        exit_reason = await sandbox_module.stream_command_output(mock_sandbox, "run-agent.sh", on_output=output.append)

        assert exit_reason is None
        assert observed_commands == ["run-agent.sh"]
        assert output == ["done\n"]

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

        def fake_set_tag(key: str, value: object) -> None:
            tagged[key] = str(value)

        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", fake_set_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(
                mock_sandbox,
                "run-agent.sh",
                on_output=lambda _: None,
                redact_error=sandbox_module.create_agent_error_redactor(()),
            )

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

    async def test_agent_reported_error_is_surfaced_and_file_removed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The agent's own error, written to $VALKYRIE_ERROR_PATH, becomes the persisted message."""
        observed_commands: list[str] = []
        exec_commands: list[str] = []
        error_files: dict[str, str] = {}

        async def stream_command(command: str) -> AsyncIterator[str]:
            observed_commands.append(command)
            match = re.search(r"export VALKYRIE_ERROR_PATH=([^;\s]+)", command)
            assert match is not None
            error_files[match.group(1)] = "ValueError: model returned no patch\n  hint: see logs\n"
            yield "raw model output that must not be persisted\n"
            raise ProviderSandboxCommandError(1)

        async def exec_command(command: str) -> ExecResult:
            exec_commands.append(command)
            if command.startswith("head -c "):
                path = command.rsplit(" ", 1)[1]
                if path in error_files:
                    return ExecResult(exit_code=0, output=error_files[path])
                return ExecResult(exit_code=1, output="head: cannot open")
            if command.startswith("rm -f "):
                for path in command.split()[2:]:
                    error_files.pop(path, None)
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command
        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", _ignore_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(
                mock_sandbox,
                "run-agent.sh",
                on_output=lambda _: None,
                redact_error=sandbox_module.create_agent_error_redactor(()),
            )

        assert (
            str(exc_info.value)
            == "Sandbox error: Agent command failed with exit code 1: ValueError: model returned no patch hint: see logs"
        )
        assert "raw model output" not in str(exc_info.value)
        assert error_files == {}
        assert exec_commands[-1].startswith("rm -f ") and ".error" in exec_commands[-1]

    async def test_unreadable_agent_error_file_keeps_exit_code_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield ""
            raise ProviderSandboxCommandError(3)

        async def exec_command(command: str) -> ExecResult:
            if command.startswith("head -c "):
                raise ProviderSandboxError("exec transport failed")
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command
        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", _ignore_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(
                mock_sandbox,
                "run-agent.sh",
                on_output=lambda _: None,
                redact_error=sandbox_module.create_agent_error_redactor(()),
            )

        assert str(exc_info.value) == "Sandbox error: Agent command failed with exit code 3"

    @pytest.mark.parametrize("read_prefix", ["head -c ", "cat "])
    async def test_sandbox_lost_during_post_exit_read_keeps_exit_code_error(
        self, monkeypatch: pytest.MonkeyPatch, read_prefix: str
    ) -> None:
        """Sandbox disappearing after the agent exited must not replace the agent's failure."""

        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield ""
            raise ProviderSandboxCommandError(4)

        async def exec_command(command: str) -> ExecResult:
            if command.startswith(read_prefix):
                raise SandboxNotFoundError("sandbox-123")
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command
        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", _ignore_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(
                mock_sandbox,
                "run-agent.sh",
                on_output=lambda _: None,
                redact_error=sandbox_module.create_agent_error_redactor(()),
            )

        assert str(exc_info.value) == "Sandbox error: Agent command failed with exit code 4"

    async def test_hanging_post_exit_read_times_out_and_keeps_exit_code_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield ""
            raise ProviderSandboxCommandError(5)

        async def exec_command(command: str) -> ExecResult:
            if command.startswith("head -c "):
                await asyncio.Event().wait()
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command
        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", _ignore_tag)
        monkeypatch.setattr("tracker.sandbox._POST_EXIT_READ_TIMEOUT_SECONDS", 0.01)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(
                mock_sandbox,
                "run-agent.sh",
                on_output=lambda _: None,
                redact_error=sandbox_module.create_agent_error_redactor(()),
            )

        assert str(exc_info.value) == "Sandbox error: Agent command failed with exit code 5"

    async def test_hanging_cleanup_preserves_reported_original_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield "raw model output must not enter the task error"
            raise ProviderSandboxCommandError(1)

        async def exec_command(command: str) -> ExecResult:
            if command.startswith("head -c "):
                return ExecResult(exit_code=0, output="TimeoutError: controlled timeout")
            if command.startswith("rm -f "):
                await asyncio.Event().wait()
            return ExecResult(exit_code=0)

        sandbox = Mock(id="cleanup-timeout", name="cleanup-timeout", command=stream_command, exec=exec_command)
        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", _ignore_tag)
        monkeypatch.setattr("tracker.sandbox._POST_EXIT_READ_TIMEOUT_SECONDS", 0.01)
        with pytest.raises(AgentRunFailedError) as error:
            await asyncio.wait_for(
                sandbox_module.stream_command_output(
                    sandbox,
                    "run-agent.sh",
                    on_output=lambda _: None,
                    redact_error=sandbox_module.create_agent_error_redactor(()),
                ),
                timeout=1,
            )
        assert (
            str(error.value) == "Sandbox error: Agent command failed with exit code 1: TimeoutError: controlled timeout"
        )
        assert "raw model output" not in str(error.value)

    async def test_install_failure_names_the_install_phase(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield ""
            raise ProviderSandboxCommandError(2)

        async def exec_command(command: str) -> ExecResult:
            if command.startswith("head -c "):
                return ExecResult(exit_code=0, output="pip: No matching distribution found for torch==99")
            return ExecResult(exit_code=0)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = exec_command
        monkeypatch.setattr("tracker.sandbox.sentry_sdk.set_tag", _ignore_tag)
        contract = AgentContractRequest(name="my-agent", install_cmd="pip install -r requirements.txt", run_cmd="run")

        with pytest.raises(AgentRunFailedError) as exc_info:
            await _install_agent_dependencies_once(
                mock_sandbox,
                contract,
                _ignore_output,
                redact_error=sandbox_module.create_agent_error_redactor(()),
            )

        assert str(exc_info.value) == (
            "Sandbox error: Dependency installation for contract my-agent failed with exit code 2: "
            "pip: No matching distribution found for torch==99"
        )


@pytest.mark.parametrize(
    ("exec_outcome", "expected"),
    [
        pytest.param(ExecResult(exit_code=0, output="93.184.216.34 gateway.example.com"), True, id="resolves"),
        pytest.param(ExecResult(exit_code=2, output=""), False, id="not-found"),
        pytest.param(ExecResult(exit_code=1, output="service is not running"), True, id="other-exit"),
        pytest.param(ProviderSandboxError("exec failed"), True, id="exec-failed"),
        pytest.param(SandboxNotFoundError("gone"), True, id="sandbox-gone"),
        pytest.param(TimeoutError(), True, id="timed-out"),
    ],
)
async def test_sandbox_resolves_reports_only_an_observed_dns_failure(
    exec_outcome: ExecResult | Exception, expected: bool
) -> None:
    sandbox = AsyncMock()
    if isinstance(exec_outcome, Exception):
        sandbox.exec.side_effect = exec_outcome
    else:
        sandbox.exec.return_value = exec_outcome

    assert await sandbox_resolves(sandbox, "gateway.example.com") is expected
    (command,) = sandbox.exec.call_args.args
    assert command == "[ -x /usr/bin/getent ] || exit 0; /usr/bin/getent ahosts gateway.example.com"


async def test_sandbox_resolves_gives_up_on_a_hung_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.sandbox._DNS_CHECK_TIMEOUT_SECONDS", 0.01)
    sandbox = AsyncMock()

    async def _hang(*_args: Any, **_kwargs: Any) -> ExecResult:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    sandbox.exec.side_effect = _hang

    assert await sandbox_resolves(sandbox, "gateway.example.com") is True
