import asyncio
from collections import deque
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import ExecResult, ImageSource, Resources, SnapshotSource
from benchmark_service.sandbox import SandboxCommandError as ProviderSandboxCommandError
from benchmark_service.sandbox import SandboxError as ProviderSandboxError
from daytona.common.errors import DaytonaError, DaytonaRateLimitError
from tenacity import stop_after_attempt, wait_none

import tracker.daytona_retry as daytona_retry_module
import tracker.observability.retry as retry_module
from tracker import sandbox as sandbox_module
from tracker.database.models import AgentContractRequest
from tracker.exceptions import AgentRunFailedError, SSLConnectionError, SandboxError, SandboxSetupError
from tracker.sandbox import create_sandbox, run_agent, upload_agent_artifacts
from tracker.types import AWSCredentials

_create_sandbox = getattr(sandbox_module, "_create_sandbox")
_delete_sandbox = getattr(sandbox_module, "delete_sandbox")
_exec = getattr(sandbox_module, "_exec")
_install_agent_dependencies = getattr(sandbox_module, "install_agent_dependencies")
_upload_agent_artifacts = getattr(sandbox_module, "upload_agent_artifacts")


class TestAgentOutputTelemetry:
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

    def test_sandbox_retry_decorators_use_observability_retry_callbacks(self) -> None:
        exec_before_sleep = _exec.retry.before_sleep
        upload_before_sleep = _upload_agent_artifacts.retry.before_sleep
        deps_before_sleep = _install_agent_dependencies.retry.before_sleep

        assert exec_before_sleep is not None
        assert upload_before_sleep is not None
        assert deps_before_sleep is not None
        assert callable(exec_before_sleep)
        assert callable(upload_before_sleep)
        assert callable(deps_before_sleep)

    def test_daytona_retry_wait_uses_retry_after_header(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"Retry-After-Sandbox-Create": "7"},
        )
        wait_strategy = _exec.retry.wait

        assert wait_strategy(retry_state) == 7

    def test_daytona_retry_wait_uses_generic_retry_after_header(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"Retry-After": "3"},
        )
        wait_strategy = _exec.retry.wait

        assert wait_strategy(retry_state) == 3

    def test_daytona_retry_wait_uses_unknown_throttler_retry_after_header(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"Retry-After-Custom-Throttler": "4"},
        )
        wait_strategy = _exec.retry.wait

        assert wait_strategy(retry_state) == 4

    def test_daytona_retry_wait_uses_retry_after_header_without_local_cap(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"retry-after-sandbox-create": "120"},
        )
        wait_strategy = _exec.retry.wait

        assert wait_strategy(retry_state) == 120

    @pytest.mark.parametrize(
        "headers", [{}, {"Retry-After-Sandbox-Create": "bad"}, {"Retry-After-Sandbox-Create": "-1"}]
    )
    def test_daytona_retry_wait_falls_back_to_exponential_backoff(self, headers: dict[str, str]) -> None:
        retry_state = Mock()
        retry_state.attempt_number = 2
        retry_state.outcome.exception.return_value = DaytonaRateLimitError("rate limited", headers=headers)
        wait_strategy = _exec.retry.wait

        assert wait_strategy(retry_state) == 2

    def test_daytona_retry_wait_preserves_non_rate_limit_waits(self) -> None:
        retry_state = Mock()
        retry_state.attempt_number = 4
        retry_state.outcome.exception.return_value = DaytonaError("transient")

        assert _exec.retry.wait(retry_state) == 2

    def test_daytona_retry_callback_emits_rate_limit_metrics(self, monkeypatch: pytest.MonkeyPatch) -> None:
        increments: list[tuple[str, dict[str, str]]] = []
        distributions: list[tuple[str, float, dict[str, str]]] = []
        log_records: list[dict[str, Any]] = []

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_warning(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(sandbox_module, "distribution", fake_distribution, raising=False)

        monkeypatch.setattr(retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(daytona_retry_module.logger, "warning", fake_warning)

        state = Mock()
        state.attempt_number = 1
        state.fn.__name__ = "_create_sandbox"
        state.idle_for = 0
        state.next_action.sleep = 7
        state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={
                "Retry-After-Sandbox-Create": "7",
                "X-RateLimit-Remaining-Sandbox-Create": "0",
                "X-RateLimit-Reset-Sandbox-Create": "7",
            },
        )
        callback = daytona_retry_module.daytona_retry_callback("valkyrie.sandbox.create", op="sandbox.create")

        callback(state)

        assert ("valkyrie.sandbox.create.retry", {"error_class": "DaytonaRateLimitError"}) in increments
        assert (
            "valkyrie.daytona.rate_limit.retry",
            {"op": "sandbox.create", "throttler": "sandbox-create"},
        ) in increments
        assert distributions == [
            (
                "valkyrie.daytona.rate_limit.retry_sleep",
                7,
                {"op": "sandbox.create", "throttler": "sandbox-create"},
            )
        ]
        assert log_records == [
            {
                "message": "daytona.rate_limit_retry",
                "op": "sandbox.create",
                "throttler": "sandbox-create",
                "attempt": 1,
                "sleep_seconds": 7,
                "rate_limit_remaining": "0",
                "rate_limit_reset": "7",
            }
        ]

    def test_daytona_retry_callback_uses_remaining_header_for_throttler(self, monkeypatch: pytest.MonkeyPatch) -> None:
        increments: list[tuple[str, dict[str, str]]] = []
        distributions: list[tuple[str, float, dict[str, str]]] = []
        log_records: list[dict[str, Any]] = []

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_warning(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        monkeypatch.setattr(retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(daytona_retry_module.logger, "warning", fake_warning)

        state = Mock()
        state.attempt_number = 1
        state.fn.__name__ = "_create_sandbox"
        state.idle_for = 0
        state.next_action = None
        state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"X-RateLimit-Remaining-Sandbox-Lifecycle": "0"},
        )
        callback = daytona_retry_module.daytona_retry_callback("valkyrie.sandbox.create", op="sandbox.create")

        callback(state)

        assert (
            "valkyrie.daytona.rate_limit.retry",
            {"op": "sandbox.create", "throttler": "sandbox-lifecycle"},
        ) in increments
        assert distributions == []
        assert log_records[0]["throttler"] == "sandbox-lifecycle"
        assert log_records[0]["rate_limit_remaining"] == "0"
        assert log_records[0]["rate_limit_reset"] is None

    def test_daytona_retry_callback_collapses_unknown_throttler_metric_tag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        increments: list[tuple[str, dict[str, str]]] = []
        distributions: list[tuple[str, float, dict[str, str]]] = []
        log_records: list[dict[str, Any]] = []

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_warning(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        monkeypatch.setattr(retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(daytona_retry_module.logger, "warning", fake_warning)

        state = Mock()
        state.attempt_number = 1
        state.fn.__name__ = "_create_sandbox"
        state.idle_for = 0
        state.next_action.sleep = 4
        state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={
                "Retry-After-Custom-Tenant": "4",
                "X-RateLimit-Remaining-Custom-Tenant": "0",
            },
        )
        callback = daytona_retry_module.daytona_retry_callback("valkyrie.sandbox.create", op="sandbox.create")

        callback(state)

        assert (
            "valkyrie.daytona.rate_limit.retry",
            {"op": "sandbox.create", "throttler": "unknown"},
        ) in increments
        assert distributions == [
            (
                "valkyrie.daytona.rate_limit.retry_sleep",
                4,
                {"op": "sandbox.create", "throttler": "unknown"},
            )
        ]
        assert log_records[0]["throttler"] == "unknown"
        assert log_records[0]["rate_limit_remaining"] is None
        assert log_records[0]["rate_limit_reset"] is None

    def test_daytona_retry_callback_ignores_non_rate_limit_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        increments: list[tuple[str, dict[str, str]]] = []
        distributions: list[tuple[str, float, dict[str, str]]] = []
        log_records: list[dict[str, Any]] = []

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_warning(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        monkeypatch.setattr(retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(daytona_retry_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(daytona_retry_module.logger, "warning", fake_warning)

        state = Mock()
        state.attempt_number = 1
        state.fn.__name__ = "_create_sandbox"
        state.idle_for = 0
        state.next_action.sleep = 2
        state.outcome.exception.return_value = DaytonaError("transient")
        callback = daytona_retry_module.daytona_retry_callback("valkyrie.sandbox.create", op="sandbox.create")

        callback(state)

        assert increments == [("valkyrie.sandbox.create.retry", {"error_class": "DaytonaError"})]
        assert distributions == []
        assert log_records == []

    def test_metric_source_name_drops_high_cardinality_tag_and_digest(self) -> None:
        metric_source_name = getattr(sandbox_module, "_metric_source_name")

        assert metric_source_name(ImageSource(image="ghcr.io/vals/swebench:latest")) == "ghcr.io/vals/swebench"
        assert (
            metric_source_name(ImageSource(image="registry.local:5000/vals/swebench@sha256:abcdef"))
            == "registry.local:5000/vals/swebench"
        )
        assert metric_source_name(SnapshotSource(snapshot="base-python")) == "snapshot"

    def test_sandbox_span_helpers_set_safe_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        span_attributes: dict[str, str | int] = {}

        class FakeSpan:
            def set_attribute(self, key: str, value: str | int) -> None:
                span_attributes[key] = value

        monkeypatch.setattr(sandbox_module.trace, "get_current_span", lambda: FakeSpan())

        create_span_attrs = getattr(sandbox_module, "_set_sandbox_create_span_attributes")
        sandbox_span_attrs = getattr(sandbox_module, "_set_sandbox_span_attributes")
        resources = Resources(vcpu=2, memory=4, disk=5)

        create_span_attrs("task-alias", ImageSource(image="ghcr.io/vals/swebench:latest"), resources)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        sandbox_span_attrs(mock_sandbox)

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

    async def test_delete_sandbox_raises_provider_errors(self) -> None:
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        provider = AsyncMock()
        provider.delete_sandbox = AsyncMock(side_effect=ProviderSandboxError("state change"))

        with pytest.raises(ProviderSandboxError, match="state change"):
            await _delete_sandbox(mock_sandbox, provider)

        provider.delete_sandbox.assert_awaited_once_with("sandbox-123")

    async def test_exec_tags_daytona_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        daytona_error = DaytonaError("exec failed")
        daytona_errors: list[tuple[Exception, str]] = []

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)
        monkeypatch.setattr(sandbox_module, "_set_sandbox_span_attributes", Mock(), raising=False)

        mock_sandbox = AsyncMock()
        mock_inner = AsyncMock()
        mock_inner.process.exec = AsyncMock(side_effect=daytona_error)
        monkeypatch.setattr(sandbox_module, "_daytona_inner", lambda _sandbox: mock_inner)

        with pytest.raises(DaytonaError):
            await _exec.retry_with(stop=stop_after_attempt(1), wait=wait_none())(mock_sandbox, "echo hi")

        assert daytona_errors == [(daytona_error, "sandbox.exec")]

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
        monkeypatch.setattr(sandbox_module.time, "monotonic", fake_monotonic)

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

    async def test_create_sandbox_emits_error_metric_and_daytona_tag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        daytona_error = DaytonaError("create failed")
        increments: list[tuple[str, dict[str, str]]] = []
        daytona_errors: list[tuple[Exception, str]] = []

        async def fake_create_sandbox(*_args: Any, **_kwargs: Any) -> Any:
            raise daytona_error

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create_sandbox)
        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)

        resources = Resources(vcpu=2, memory=4, disk=5)
        with pytest.raises(DaytonaError):
            async with create_sandbox(
                provider=AsyncMock(),
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=resources,
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        assert increments == [("valkyrie.sandbox.create.errors", {"error_class": "DaytonaError"})]
        assert daytona_errors == [(daytona_error, "sandbox.create")]

    async def test_create_sandbox_does_not_tag_non_daytona_create_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        increments: list[tuple[str, dict[str, str]]] = []
        daytona_errors: list[tuple[Exception, str]] = []

        async def fake_create_sandbox(*_args: Any, **_kwargs: Any) -> Any:
            raise ValueError("bad config")

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create_sandbox)
        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)

        resources = Resources(vcpu=2, memory=4, disk=5)
        with pytest.raises(ValueError, match="bad config"):
            async with create_sandbox(
                provider=AsyncMock(),
                sandbox_name="task-alias",
                source=ImageSource(image="ghcr.io/vals/swebench:latest"),
                resources=resources,
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        assert increments == [("valkyrie.sandbox.create.errors", {"error_class": "ValueError"})]
        assert daytona_errors == []


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
            Mock(return_value="https://example.com/presigned"),
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


class TestStreamCommandOutputAgentFailure:
    @pytest.mark.parametrize("exit_code", [1, 2, 127])
    async def test_non_zero_exit_raises_agent_run_failed_and_tags_exit_code(
        self, monkeypatch: pytest.MonkeyPatch, exit_code: int
    ) -> None:
        async def command(_command: str) -> Any:
            yield "last line\n"
            raise ProviderSandboxCommandError(exit_code)

        tagged: dict[str, str] = {}
        mock_sandbox = Mock()
        mock_sandbox.command = command

        def fake_set_tag(key: str, value: object) -> None:
            tagged[key] = str(value)

        monkeypatch.setattr(sandbox_module.sentry_sdk, "set_tag", fake_set_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await sandbox_module.stream_command_output(mock_sandbox, "run-agent.sh", on_output=lambda _: None)

        assert isinstance(exc_info.value, SandboxError)
        assert not isinstance(exc_info.value, SandboxSetupError)
        assert f"exit code: {exit_code}" in str(exc_info.value)
        assert "last line" in str(exc_info.value)
        assert tagged == {"agent_exit_code": str(exit_code)}
