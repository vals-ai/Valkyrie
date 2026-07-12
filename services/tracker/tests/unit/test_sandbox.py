"""Tests for tracker sandbox orchestration.

Run: pytest services/tracker/tests/unit/test_sandbox.py
"""

import asyncio
import shlex
from collections import deque
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import ComposeSandbox, ComposeSource, ExecResult, ImageSource, Resources, SnapshotSource
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
    OutputArtifactError,
    SSLConnectionError,
    SandboxError,
    SandboxSetupError,
)
from tracker.sandbox import (
    create_sandbox,
    run_agent,
    upload_agent_artifacts,
    upload_output_artifacts,
)
from tracker.types import AWSCredentials

_create_sandbox = getattr(sandbox_module, "_create_sandbox")
_delete_sandbox = getattr(sandbox_module, "delete_sandbox")
_exec = getattr(sandbox_module, "_exec")
_apply_egress_allowlist = getattr(sandbox_module, "_apply_egress_allowlist")
_install_agent_dependencies = getattr(sandbox_module, "install_agent_dependencies")
_stream_command_output_with_egress_allowlist = getattr(sandbox_module, "_stream_command_output_with_egress_allowlist")
_upload_agent_artifacts = getattr(sandbox_module, "upload_agent_artifacts")


class TestOutputArtifacts:
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


class TestAgentOutputTelemetry:
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

    def test_sandbox_retry_decorators_use_observability_retry_callbacks(self) -> None:
        upload_before_sleep = _upload_agent_artifacts.retry.before_sleep
        deps_before_sleep = _install_agent_dependencies.retry.before_sleep

        assert upload_before_sleep is not None
        assert deps_before_sleep is not None
        assert callable(upload_before_sleep)
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

        class FakeSpan:
            def set_attribute(self, key: str, value: str | int) -> None:
                span_attributes[key] = value

        monkeypatch.setattr("tracker.sandbox.trace.get_current_span", lambda: FakeSpan())

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
        sandbox = await _create_sandbox(
            provider,
            "task-alias",
            ImageSource(image="ghcr.io/vals/swebench:latest"),
            resources,
        )

        assert sandbox is mock_sandbox
        assert span_calls == [("task-alias", "ghcr.io/vals/swebench:latest", 2)]
        request = provider.create_sandbox.await_args.args[0]
        assert request.name == "task-alias"
        assert request.resources == resources
        assert request.auto_stop_interval == sandbox_module.SANDBOX_AUTO_STOP_INTERVAL
        assert request.create_timeout == sandbox_module.SANDBOX_CREATE_TIMEOUT

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

        provider = AsyncMock()
        provider.delete_sandbox = AsyncMock(side_effect=ProviderSandboxError("state change"))

        with pytest.raises(ProviderSandboxError, match="state change"):
            await _delete_sandbox(mock_sandbox, provider)

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

        async def fake_create_sandbox(*_args: Any, **_kwargs: Any) -> Any:
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

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create_sandbox)
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

    async def test_create_sandbox_emits_error_metric(self, monkeypatch: pytest.MonkeyPatch) -> None:
        create_error = RuntimeError("create failed")
        increments: list[tuple[str, dict[str, str]]] = []

        async def fake_create_sandbox(*_args: Any, **_kwargs: Any) -> Any:
            raise create_error

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create_sandbox)
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


class TestUploadAgentArtifacts:
    @pytest.mark.parametrize(
        "exit_code,retryable",
        [
            (35, True),  # curl SSL/TLS error — transient, retry with new sandbox
            (1, False),  # generic failure — deterministic, fail the task
        ],
    )
    async def test_exit_code_maps_to_retryable_exception(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
        exit_code: int,
        retryable: bool,
    ) -> None:
        """
        Exit code 35 (curl SSL/TLS) raises SandboxSetupError so process_task retries
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

        aws = AWSCredentials(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_default_region="us-east-1",
        )

        expected = SSLConnectionError if retryable else SandboxError
        with pytest.raises(expected) as exc_info:
            await upload_agent_artifacts(mock_sandbox, contract, "bench-123", aws, "test-bucket")

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
    async def test_stream_command_output_removes_timing_files(self) -> None:
        async def stream_command(_command: str) -> Any:
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
        assert exec_commands[-1].startswith("rm -f ")
        assert ".start_ns" in exec_commands[-1]
        assert ".end_ns" in exec_commands[-1]

    @pytest.mark.parametrize("exit_code", [1, 2, 127])
    async def test_non_zero_exit_raises_agent_run_failed_and_tags_exit_code(
        self, monkeypatch: pytest.MonkeyPatch, exit_code: int
    ) -> None:
        async def stream_command(_command: str) -> Any:
            yield "last line\n"
            raise ProviderSandboxCommandError(exit_code)

        tagged: dict[str, str] = {}
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "test-sandbox"
        mock_sandbox.command = stream_command
        mock_sandbox.exec = AsyncMock(
            side_effect=[
                ExecResult(exit_code=0, output="1000000000"),
                ExecResult(exit_code=0, output="3000000000"),
            ]
        )

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
