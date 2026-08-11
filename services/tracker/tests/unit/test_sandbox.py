"""Tests for tracker sandbox orchestration.

Run: pytest services/tracker/tests/unit/test_sandbox.py
"""

import asyncio
import shlex
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

from tracker import sandbox as sandbox_module
from tracker.database.models import (
    AgentCausedExitReason,
    AgentContractRequest,
    MAX_OUTPUT_ARTIFACT_BYTES,
    OutputArtifact,
)
from tracker.exceptions import (
    AgentRunFailedError,
    DependencySetupExhaustedError,
    InvalidSandboxConfigurationError,
    OutputArtifactError,
    SSLConnectionError,
    SandboxError,
    SandboxSetupError,
)
from tracker.sandbox import (
    OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES,
    create_sandbox,
    run_agent,
    upload_agent_artifacts,
    upload_output_artifacts,
)
from tracker.types import AWSCredentials


def _ignore_output(_message: str) -> None:
    pass


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

    async def test_upload_output_artifacts_downloads_file_without_exec_output(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        """
        Verify artifact contents use the sandbox file-transfer API instead of command output.

        Test cases:
        - A 288,928-byte SkillsBench sidecar is downloaded without a base64 exec call.
        - The exact bytes are uploaded to the task-scoped S3 key.
        """
        artifact = "artifacts/turns.jsonl"
        artifact_content = b"x" * 288_928
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output=str(len(artifact_content)))
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.download_file = AsyncMock(return_value=artifact_content)

        await upload_output_artifacts(
            mock_sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            harness_config.s3_bucket,
        )

        assert uploaded == [(artifact_content, "benchmarks/benchmark-123/task_0/artifacts/turns.jsonl")]

    async def test_upload_output_artifacts_rechecks_authority_after_download(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        artifact = "artifacts/turns.jsonl"
        authority_checks = iter([True, False])
        uploaded: list[bytes] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecResult(exit_code=0, output="3")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, *_args: Any, **_kwargs: Any) -> None:
            uploaded.append(file_content)

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)
        mock_sandbox = Mock()
        mock_sandbox.download_file = AsyncMock(return_value=b"old")

        await upload_output_artifacts(
            mock_sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            harness_config.s3_bucket,
            execution_is_current=lambda: next(authority_checks),
        )

        mock_sandbox.download_file.assert_awaited_once()
        assert uploaded == []

    async def test_upload_output_artifacts_can_upload_explicit_glob_sources(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.download_file = AsyncMock(side_effect=[b'{"llm":{}}\n', b'{"turns":[]}\n'])

        await upload_output_artifacts(
            mock_sandbox,
            [
                OutputArtifact(path="artifacts/config.json", source="/logs/*/turns/init/config.json"),
                OutputArtifact(path="artifacts/result.json", source="/logs/*/result.json"),
            ],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            harness_config.s3_bucket,
        )

        assert uploaded == [
            (b'{"llm":{}}\n', "benchmarks/benchmark-123/task_0/artifacts/config.json"),
            (b'{"turns":[]}\n', "benchmarks/benchmark-123/task_0/artifacts/result.json"),
        ]

    async def test_upload_output_artifacts_uses_result_paired_with_model_library_config(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /logs/model-library-run/result.json":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /logs/model-library-run/result.json":
                return ExecResult(exit_code=0, output="13")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.download_file = AsyncMock(return_value=b'{"turns":[]}\n')

        await upload_output_artifacts(
            mock_sandbox,
            [OutputArtifact(path="artifacts/result.json", source="/logs/model-library-run/result.json")],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            harness_config.s3_bucket,
        )

        assert uploaded == [(b'{"turns":[]}\n', "benchmarks/benchmark-123/task_0/artifacts/result.json")]

    async def test_upload_output_artifacts_fails_when_declared_file_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        artifact = "artifacts/missing.json"

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            assert command == "test -f /tmp/valkyrie/artifacts/missing.json"
            return ExecResult(exit_code=1, output="")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)

        with pytest.raises(OutputArtifactError, match="Required output artifact missing"):
            await upload_output_artifacts(Mock(), [artifact], "benchmark-123", "task_0", harness_config.aws, "bucket")

    async def test_upload_output_artifacts_skips_missing_optional_model_patch(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        artifact = OutputArtifact(
            path="artifacts/model.patch",
            source="/logs/artifacts/model.patch",
            required=False,
        )

        sandbox = Mock()
        exec_mock = AsyncMock(return_value=ExecResult(exit_code=1, output=""))
        upload_mock = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_exec", exec_mock)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", upload_mock)

        await upload_output_artifacts(
            sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            "bucket",
        )

        exec_mock.assert_awaited_once_with(
            sandbox,
            "test -f /logs/artifacts/model.patch && ! test -L /logs/artifacts/model.patch",
        )
        upload_mock.assert_not_awaited()

    @pytest.mark.parametrize(
        ("required", "expected_uploads"),
        [(True, [b"secret"]), (False, [])],
        ids=["required-collected", "optional-skipped"],
    )
    async def test_upload_output_artifacts_handles_non_glob_symlinks_by_requiredness(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
        required: bool,
        expected_uploads: list[bytes],
    ) -> None:
        source = "/logs/symlink result.json"
        uploaded: list[bytes] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f '/logs/symlink result.json'":
                return ExecResult(exit_code=0, output="")
            if command == "test -f '/logs/symlink result.json' && ! test -L '/logs/symlink result.json'":
                return ExecResult(exit_code=1, output="")
            if command == "stat -c%s '/logs/symlink result.json'":
                return ExecResult(exit_code=0, output="6")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, _s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append(file_content)

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        sandbox = Mock()
        sandbox.id = "sandbox-123"
        sandbox.name = "task-alias"
        sandbox.download_file = AsyncMock(return_value=b"secret")
        artifact = OutputArtifact(path="artifacts/result.json", source=source, required=required)

        await upload_output_artifacts(
            sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            "bucket",
        )

        assert uploaded == expected_uploads

    async def test_upload_output_artifacts_prioritizes_required_artifacts_for_total_size_limit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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

        async def fake_download_file(path: str) -> bytes:
            return path.encode()

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        sandbox = Mock()
        sandbox.id = "sandbox-123"
        sandbox.name = "task-alias"
        sandbox.download_file = fake_download_file

        await upload_output_artifacts(
            sandbox,
            [
                OutputArtifact(path="telemetry/optional.json", source=optional_source, required=False),
                OutputArtifact(path="scoring/required.json", source=required_source),
            ],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            "bucket",
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
        harness_config: Any,
    ) -> None:
        artifact = "artifacts/large.json"

        async def fake_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "test -f /tmp/valkyrie/artifacts/large.json":
                return ExecResult(exit_code=0, output="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/large.json":
                return ExecResult(exit_code=0, output=str(MAX_OUTPUT_ARTIFACT_BYTES + 1))
            raise AssertionError(f"unexpected command: {command}")

        upload_mock = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", upload_mock)

        with pytest.raises(OutputArtifactError, match="too large"):
            await upload_output_artifacts(Mock(), [artifact], "benchmark-123", "task_0", harness_config.aws, "bucket")

        upload_mock.assert_not_awaited()

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
        harness_config: Any,
        stat_result: ExecResult,
        total_bytes: int,
        error: str,
    ) -> None:
        sandbox = Mock()
        sandbox.download_file = AsyncMock()
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
                harness_config.aws,
                "bucket",
                total_bytes,
            )

        sandbox.download_file.assert_not_awaited()

    async def test_upload_output_artifacts_skips_invalid_optional_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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
        upload_mock = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_exec", exec_mock)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", upload_mock)

        await upload_output_artifacts(
            sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            "bucket",
        )

        assert exec_mock.await_args_list == [
            call(
                sandbox,
                "test -f /logs/trajectory_atif.json && ! test -L /logs/trajectory_atif.json",
            ),
            call(sandbox, "stat -c%s /logs/trajectory_atif.json"),
        ]
        upload_mock.assert_not_awaited()


class TestArchiveAndUploadOutput:
    """Streaming of the agent output archive from the sandbox to S3."""

    async def test_archive_and_upload_output_streams_archive_to_s3(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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
            _aws: Any,
            _s3_bucket: str,
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
        monkeypatch.setattr(sandbox_module, "upload_stream_to_s3", fake_upload_stream_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.stream_download = fake_stream_download

        await sandbox_module.archive_and_upload_output(
            mock_sandbox,
            "/logs",
            "benchmarks/benchmark-123/task_0/output.tar.gz",
            harness_config.aws,
            harness_config.s3_bucket,
        )

        assert uploaded == [(b"chunk-1chunk-2", "benchmarks/benchmark-123/task_0/output.tar.gz")]
        assert exec_commands[0].startswith("tar -czf ")
        assert exec_commands[-1].startswith("rm -f ")


class TestRunAgent:
    """Agent execution, output collection, and runtime command construction."""

    async def test_run_agent_uploads_declared_output_artifacts(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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
            _aws: Any,
            _s3_bucket: str,
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
            aws=harness_config.aws,
            s3_bucket=harness_config.s3_bucket,
            benchmark_id="benchmark-123",
        )

        assert artifact_calls == ["benchmark-123:task_0:artifacts/result.json"]

    async def test_run_agent_threads_benchmark_id_to_archive_and_upload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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
            _aws: Any,
            _s3_bucket: str,
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
            aws=harness_config.aws,
            s3_bucket=harness_config.s3_bucket,
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
            aws=harness_config.aws,
            s3_bucket=harness_config.s3_bucket,
            agent_output_s3_key="benchmarks/run/task/agent_output.tar.gz",
            benchmark_id="benchmark-123",
            execution_is_current=lambda: False,
        )
        assert archive_calls == []

    async def test_run_agent_wraps_compose_runtime_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
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
            aws=harness_config.aws,
            s3_bucket=harness_config.s3_bucket,
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
        harness_config: Any,
    ) -> None:
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
            aws=harness_config.aws,
            s3_bucket=harness_config.s3_bucket,
            agent_timeout=2.5,
        )

        assert observed_commands == [f"cd /workspace && PYTHONSAFEPATH=1 timeout 2.5 sh -c {shlex.quote(run_cmd)}"]


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

    async def test_create_sandbox_passes_request_to_provider(self, monkeypatch: pytest.MonkeyPatch) -> None:
        span_calls: list[tuple[str, str, int]] = []

        def fake_create_span_attrs(sandbox_name: str, source: Any, resources: Any) -> None:
            span_calls.append((sandbox_name, source.image, resources.vcpu))

        monkeypatch.setattr(sandbox_module, "_set_sandbox_create_span_attributes", fake_create_span_attrs)

        mock_sandbox = AsyncMock()
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
        aws_credentials: AWSCredentials,
        exit_code: int,
        retryable: bool,
    ) -> None:
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
        monkeypatch.setattr(
            "tracker.sandbox.create_presigned_url",
            AsyncMock(return_value="https://example.com/presigned"),
        )

        expected = SSLConnectionError if retryable else SandboxError
        with pytest.raises(expected) as exc_info:
            await upload_agent_artifacts(
                mock_sandbox,
                contract,
                "bench-123",
                aws_credentials,
                "test-bucket",
            )

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
    async def test_non_zero_exit_raises_agent_run_failed_and_tags_exit_code(
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
        assert f"exit code: {exit_code}" in str(exc_info.value)
        assert "last line" in str(exc_info.value)
        assert tagged == {"agent_exit_code": str(exit_code)}

    async def test_output_tail_is_byte_capped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Test cases:
        - Output retained for the failure message is capped by characters, not chunk count.
        - The newest output survives while old output beyond the cap is dropped.
        """
        tail_cap = getattr(sandbox_module, "_OUTPUT_TAIL_MAX_CHARS")

        async def stream_command(_command: str) -> AsyncIterator[str]:
            yield "old-marker\n"
            yield "x" * (tail_cap + 1) + "\n"
            yield "new-marker\n"
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
            await sandbox_module.stream_command_output(mock_sandbox, "run-agent.sh", on_output=lambda _: None)

        assert "new-marker" in str(exc_info.value)
        assert "old-marker" not in str(exc_info.value)
