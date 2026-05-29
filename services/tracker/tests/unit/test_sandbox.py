import asyncio
from collections import deque
from collections.abc import AsyncGenerator, Mapping
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from daytona import ExecuteResponse, SandboxState
from daytona.common.errors import DaytonaConnectionError, DaytonaError, DaytonaNotFoundError, DaytonaRateLimitError
from tenacity import stop_after_attempt, wait_none

import tracker.daytona_retry as daytona_retry_module
import tracker.observability.retry as retry_module
import tracker.utils as utils_module
from tracker import sandbox as sandbox_module
from tracker.database.models import AgentContractRequest, OutputArtifact
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
    stream_command_output,
    upload_agent_artifacts,
    upload_output_artifacts,
)
from tracker.types import AWSCredentials

_create_sandbox = getattr(sandbox_module, "_create_sandbox")
_create_pty_session = getattr(sandbox_module, "_create_pty_session")
_check_sandbox_health = getattr(sandbox_module, "_check_sandbox_health")
_delete_sandbox = getattr(sandbox_module, "delete_sandbox")
_exec = getattr(sandbox_module, "_exec")
_install_agent_dependencies = getattr(sandbox_module, "install_agent_dependencies")
_reconnect_and_wait_pty = getattr(sandbox_module, "_reconnect_and_wait_pty")
_upload_agent_artifacts = getattr(sandbox_module, "upload_agent_artifacts")
_wait_for_pty = getattr(sandbox_module, "_wait_for_pty")
_stop_active_sandboxes = getattr(utils_module, "_stop_active_sandboxes")  # pyright: ignore[reportPrivateUsage]


def _ignore_pty_data(_data: bytes) -> None:
    pass


async def _async_iter(items: list[Any]) -> AsyncGenerator[Any, None]:
    for item in items:
        yield item


class TestPtyHandshakeSemaphore:
    async def test_create_pty_session_emits_handshake_metrics_structured_log_and_span_attrs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cap = 5
        monkeypatch.setattr(sandbox_module, "_PTY_HANDSHAKE_CAP", cap)
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(cap))
        monkeypatch.setattr(sandbox_module, "_pty_handshake_in_flight_count", 0, raising=False)
        monkeypatch.setattr(sandbox_module.uuid, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))

        distributions: list[tuple[str, float, dict[str, str]]] = []
        gauges: list[tuple[str, float, dict[str, str]]] = []
        log_records: list[dict[str, Any]] = []
        span_calls: list[tuple[str, str, str]] = []
        pty_context_calls: list[str] = []

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_gauge(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            gauges.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_info(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        def fake_set_span_attrs(sandbox: Any, session_id: str) -> None:
            span_calls.append((sandbox.id, sandbox.name, session_id))

        def fake_set_pty_context(*, session_id: str, attempt: int | None = None) -> None:
            pty_context_calls.append(session_id)

        monkeypatch.setattr(sandbox_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(sandbox_module, "gauge", fake_gauge, raising=False)
        monkeypatch.setattr(sandbox_module.logger, "info", fake_info)
        monkeypatch.setattr(sandbox_module, "_set_pty_span_attributes", fake_set_span_attrs, raising=False)
        monkeypatch.setattr(sandbox_module, "set_pty_context", fake_set_pty_context, raising=False)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_handle = Mock()
        mock_sandbox.process.create_pty_session = AsyncMock(return_value=mock_handle)
        debug_output: list[bytes] = []

        handle, salted_id = await _create_pty_session(mock_sandbox, "session-1", debug_output.append)

        assert handle is mock_handle
        assert salted_id == "session-1-abcdef12"
        assert debug_output == [b"[Debug]: Creating PTY session with the following id session-1-abcdef12\n"]
        wait_duration_call = next(call for call in distributions if call[0] == "valkyrie.pty.handshake.wait_duration")
        assert 0 <= wait_duration_call[1] < 1
        assert wait_duration_call[2] == {"operation": "create"}
        assert [call for call in gauges if call[0] == "valkyrie.pty.handshake.in_flight"] == [
            ("valkyrie.pty.handshake.in_flight", 1, {"operation": "create"}),
            ("valkyrie.pty.handshake.in_flight", 0, {"operation": "create"}),
        ]
        assert log_records == [
            {
                "message": "pty.create",
                "pty_event": "create",
                "session_id": "session-1-abcdef12",
                "sandbox_id": "sandbox-123",
            }
        ]
        assert span_calls == [("sandbox-123", "task-alias", "session-1-abcdef12")]
        assert pty_context_calls == ["session-1-abcdef12"]

    async def test_reconnect_emits_counter_metrics_structured_log_and_span_attrs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(5))
        monkeypatch.setattr(sandbox_module, "_pty_handshake_in_flight_count", 0, raising=False)

        increments: list[tuple[str, dict[str, str]]] = []
        distributions: list[tuple[str, float, dict[str, str]]] = []
        gauges: list[tuple[str, float, dict[str, str]]] = []
        log_records: list[dict[str, Any]] = []
        span_calls: list[tuple[str, str, str]] = []
        pty_context_calls: list[str] = []

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_gauge(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            gauges.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_info(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        def fake_set_span_attrs(sandbox: Any, session_id: str) -> None:
            span_calls.append((sandbox.id, sandbox.name, session_id))

        def fake_set_pty_context(*, session_id: str, attempt: int | None = None) -> None:
            pty_context_calls.append(session_id)

        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(sandbox_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(sandbox_module, "gauge", fake_gauge, raising=False)
        monkeypatch.setattr(sandbox_module.logger, "info", fake_info)
        monkeypatch.setattr(sandbox_module, "_set_pty_span_attributes", fake_set_span_attrs, raising=False)
        monkeypatch.setattr(sandbox_module, "set_pty_context", fake_set_pty_context, raising=False)
        monkeypatch.setattr(sandbox_module, "_check_sandbox_health", AsyncMock())

        mock_handle = AsyncMock()
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.process.get_pty_session_info = AsyncMock()
        mock_sandbox.process.connect_pty_session = AsyncMock(return_value=mock_handle)
        outputs: list[str] = []

        await _reconnect_and_wait_pty(mock_sandbox, "session-1", _ignore_pty_data, outputs.append)

        assert increments == [
            ("valkyrie.pty.reconnect.count", {"operation": "reconnect"}),
            ("valkyrie.pty.reconnect.success", {}),
        ]
        assert outputs == ["[Debug]: Disconnected from websocket, creating a new reader and reconnecting\n"]
        assert log_records == [
            {
                "message": "pty.reconnect_start",
                "pty_event": "reconnect_start",
                "session_id": "session-1",
                "sandbox_id": "sandbox-123",
            },
            {
                "message": "pty.reconnect_success",
                "pty_event": "reconnect_success",
                "session_id": "session-1",
                "sandbox_id": "sandbox-123",
            },
        ]
        handshake_duration_call = next(call for call in distributions if call[0] == "valkyrie.pty.handshake.duration")
        assert 0 <= handshake_duration_call[1] < 1
        assert handshake_duration_call[2] == {"operation": "reconnect"}
        assert [call for call in gauges if call[0] == "valkyrie.pty.handshake.in_flight"] == [
            ("valkyrie.pty.handshake.in_flight", 1, {"operation": "reconnect"}),
            ("valkyrie.pty.handshake.in_flight", 0, {"operation": "reconnect"}),
        ]
        assert span_calls == [("sandbox-123", "task-alias", "session-1")]
        assert pty_context_calls == ["session-1"]

    async def test_handshake_slot_warns_when_handshake_is_slow(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(5))
        monkeypatch.setattr(sandbox_module, "_pty_handshake_in_flight_count", 0, raising=False)
        monkeypatch.setattr(sandbox_module, "_PTY_HANDSHAKE_SLOW_LOG_THRESHOLD", -1)
        monkeypatch.setattr(sandbox_module, "distribution", Mock(), raising=False)
        monkeypatch.setattr(sandbox_module, "gauge", Mock(), raising=False)

        warnings: list[str] = []

        def fake_warning(message: str) -> None:
            warnings.append(message)

        monkeypatch.setattr(sandbox_module.logger, "warning", fake_warning)

        pty_handshake_slot = getattr(sandbox_module, "_pty_handshake_slot")
        async with pty_handshake_slot("create", "session-1"):
            pass

        assert warnings
        assert warnings[0].startswith("PTY handshake slow: create session=session-1 duration=")

    def test_set_pty_span_attributes_sets_safe_span_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        span_attributes: dict[str, str] = {}

        class FakeSpan:
            def set_attribute(self, key: str, value: str) -> None:
                span_attributes[key] = value

        monkeypatch.setattr(sandbox_module.trace, "get_current_span", lambda: FakeSpan())

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        set_pty_span_attributes = getattr(sandbox_module, "_set_pty_span_attributes")
        set_pty_span_attributes(mock_sandbox, "session-1")

        assert span_attributes == {
            "valkyrie.sandbox_id": "sandbox-123",
            "valkyrie.sandbox_name": "task-alias",
            "valkyrie.pty_session_id": "session-1",
        }


class TestOutputArtifacts:
    async def test_upload_output_artifacts_uploads_declared_file_to_task_prefix(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        artifact = "artifacts/turns.jsonl"
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            if command == "test -f /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecuteResponse(exit_code=0, result="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecuteResponse(exit_code=0, result="12")
            if command == "base64 /tmp/valkyrie/artifacts/turns.jsonl":
                return ExecuteResponse(exit_code=0, result="eyJ0dXJuIjoxfQo=")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

        await upload_output_artifacts(
            mock_sandbox,
            [artifact],
            "benchmark-123",
            "task_0",
            harness_config.aws,
            harness_config.s3_bucket,
        )

        assert uploaded == [(b'{"turn":1}\n', "benchmarks/benchmark-123/task_0/artifacts/turns.jsonl")]

    async def test_upload_output_artifacts_can_upload_explicit_glob_sources(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        uploaded: list[tuple[bytes, str]] = []

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            if command == "find /logs -type f -path '/logs/*/turns/init/config.json' | sort | head -n 1":
                return ExecuteResponse(exit_code=0, result="/logs/task/turns/init/config.json\n")
            if command == "stat -c%s /logs/task/turns/init/config.json":
                return ExecuteResponse(exit_code=0, result="11")
            if command == "base64 /logs/task/turns/init/config.json":
                return ExecuteResponse(exit_code=0, result="eyJsbG0iOnt9fQo=")
            if command == "find /logs -type f -path '/logs/*/result.json' | sort | head -n 1":
                return ExecuteResponse(exit_code=0, result="/logs/task/result.json\n")
            if command == "stat -c%s /logs/task/result.json":
                return ExecuteResponse(exit_code=0, result="13")
            if command == "base64 /logs/task/result.json":
                return ExecuteResponse(exit_code=0, result="eyJ0dXJucyI6W119Cg==")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

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

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            if command == "test -f /logs/model-library-run/result.json":
                return ExecuteResponse(exit_code=0, result="")
            if command == "stat -c%s /logs/model-library-run/result.json":
                return ExecuteResponse(exit_code=0, result="13")
            if command == "base64 /logs/model-library-run/result.json":
                return ExecuteResponse(exit_code=0, result="eyJ0dXJucyI6W119Cg==")
            raise AssertionError(f"unexpected command: {command}")

        async def fake_upload_to_s3(file_content: bytes, s3_key: str, _aws: Any, _s3_bucket: str) -> None:
            uploaded.append((file_content, s3_key))

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)
        monkeypatch.setattr(sandbox_module, "upload_to_s3", fake_upload_to_s3)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"

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

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            assert command == "test -f /tmp/valkyrie/artifacts/missing.json"
            return ExecuteResponse(exit_code=1, result="")

        monkeypatch.setattr(sandbox_module, "_exec", fake_exec)

        with pytest.raises(OutputArtifactError, match="Required output artifact missing"):
            await upload_output_artifacts(Mock(), [artifact], "benchmark-123", "task_0", harness_config.aws, "bucket")

    async def test_upload_output_artifacts_fails_when_file_exceeds_tracker_limit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: Any,
    ) -> None:
        artifact = "artifacts/large.json"

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            if command == "test -f /tmp/valkyrie/artifacts/large.json":
                return ExecuteResponse(exit_code=0, result="")
            if command == "stat -c%s /tmp/valkyrie/artifacts/large.json":
                return ExecuteResponse(exit_code=0, result=str(sandbox_module.MAX_OUTPUT_ARTIFACT_BYTES + 1))
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

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            if command.startswith("mkdir -p") or command == "test -e /logs":
                return ExecuteResponse(exit_code=0, result="")
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

        async def fake_exec(_sandbox: Any, command: str) -> ExecuteResponse:
            if command.startswith("mkdir -p") or command.startswith("test -e"):
                return ExecuteResponse(exit_code=0, result="")
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

    async def test_wait_for_pty_emits_structured_logs_for_clean_disconnect_and_missing_status(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        log_records: list[dict[str, Any]] = []

        def fake_info(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        monkeypatch.setattr(sandbox_module.logger, "info", fake_info)
        monkeypatch.setattr(sandbox_module, "_check_sandbox_health", AsyncMock())
        monkeypatch.setattr(
            sandbox_module,
            "_exec",
            AsyncMock(
                side_effect=[
                    ExecuteResponse(exit_code=1, result=""),
                    ExecuteResponse(exit_code=0, result=""),
                ]
            ),
        )
        reconnect = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_reconnect_and_wait_pty", reconnect)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        handle = AsyncMock()
        outputs: list[str] = []

        await _wait_for_pty(mock_sandbox, "session-1", handle, _ignore_pty_data, outputs.append, "/tmp/status")

        assert outputs == [
            "[Debug]: PTY has been disconnected, handler has stopped polling\n",
            "[Debug]: PTY closed but status file not written yet, reconnecting\n",
        ]
        assert [record["pty_event"] for record in log_records] == [
            "stream_disconnect",
            "reconnect_status_missing",
        ]
        assert reconnect.await_count == 1

    async def test_wait_for_pty_emits_structured_log_for_disconnect_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        log_records: list[dict[str, Any]] = []

        def fake_info(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        monkeypatch.setattr(sandbox_module.logger, "info", fake_info)
        monkeypatch.setattr(sandbox_module, "_check_sandbox_health", AsyncMock())
        monkeypatch.setattr(
            sandbox_module,
            "_exec",
            AsyncMock(return_value=ExecuteResponse(exit_code=0, result="")),
        )
        reconnect = AsyncMock()
        monkeypatch.setattr(sandbox_module, "_reconnect_and_wait_pty", reconnect)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        handle = AsyncMock()
        handle.wait = AsyncMock(side_effect=RuntimeError("websocket closed"))
        outputs: list[str] = []

        await _wait_for_pty(mock_sandbox, "session-1", handle, _ignore_pty_data, outputs.append, "/tmp/status")

        assert outputs == ["[Debug]: PTY stream has been disconnected (Attempting reconnection): websocket closed\n"]
        assert [record["pty_event"] for record in log_records] == ["stream_disconnect_with_error"]
        assert reconnect.await_count == 1

    def test_pty_retry_decorators_use_observability_retry_callbacks(self) -> None:
        create_before_sleep = _create_pty_session.retry.before_sleep
        reconnect_before_sleep = _reconnect_and_wait_pty.retry.before_sleep

        assert create_before_sleep is not None
        assert reconnect_before_sleep is not None
        assert callable(create_before_sleep)
        assert callable(reconnect_before_sleep)

    def test_sandbox_retry_decorators_use_observability_retry_callbacks(self) -> None:
        create_before_sleep = _create_sandbox.retry.before_sleep
        exec_before_sleep = _exec.retry.before_sleep
        delete_before_sleep = _delete_sandbox.retry.before_sleep
        upload_before_sleep = _upload_agent_artifacts.retry.before_sleep
        deps_before_sleep = _install_agent_dependencies.retry.before_sleep

        assert create_before_sleep is not None
        assert exec_before_sleep is not None
        assert delete_before_sleep is not None
        assert upload_before_sleep is not None
        assert deps_before_sleep is not None
        assert callable(create_before_sleep)
        assert callable(exec_before_sleep)
        assert callable(delete_before_sleep)
        assert callable(upload_before_sleep)
        assert callable(deps_before_sleep)

    def test_daytona_retry_wait_uses_retry_after_header(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"Retry-After-Sandbox-Create": "7"},
        )
        wait_strategy = _create_sandbox.retry.wait

        assert wait_strategy(retry_state) == 7

    def test_daytona_retry_wait_uses_generic_retry_after_header(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"Retry-After": "3"},
        )
        wait_strategy = _create_sandbox.retry.wait

        assert wait_strategy(retry_state) == 3

    def test_daytona_retry_wait_uses_unknown_throttler_retry_after_header(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"Retry-After-Custom-Throttler": "4"},
        )
        wait_strategy = _create_sandbox.retry.wait

        assert wait_strategy(retry_state) == 4

    def test_daytona_retry_wait_uses_retry_after_header_without_local_cap(self) -> None:
        retry_state = Mock()
        retry_state.outcome.exception.return_value = DaytonaRateLimitError(
            "rate limited",
            headers={"retry-after-sandbox-create": "120"},
        )
        wait_strategy = _create_sandbox.retry.wait

        assert wait_strategy(retry_state) == 120

    @pytest.mark.parametrize(
        "headers", [{}, {"Retry-After-Sandbox-Create": "bad"}, {"Retry-After-Sandbox-Create": "-1"}]
    )
    def test_daytona_retry_wait_falls_back_to_exponential_backoff(self, headers: dict[str, str]) -> None:
        retry_state = Mock()
        retry_state.attempt_number = 2
        retry_state.outcome.exception.return_value = DaytonaRateLimitError("rate limited", headers=headers)
        wait_strategy = _create_sandbox.retry.wait

        assert wait_strategy(retry_state) == 2

    def test_daytona_retry_wait_preserves_non_rate_limit_waits(self) -> None:
        cases = [
            (_delete_sandbox.retry.wait, 4, 2),
            (_create_sandbox.retry.wait, 2, 5),
            (_exec.retry.wait, 4, 2),
            (_create_pty_session.retry.wait, 4, 2),
            (_reconnect_and_wait_pty.retry.wait, 4, 1),
        ]

        for wait_strategy, attempt_number, expected_seconds in cases:
            retry_state = Mock()
            retry_state.attempt_number = attempt_number
            retry_state.outcome.exception.return_value = DaytonaError("transient")

            assert wait_strategy(retry_state) == expected_seconds

    @pytest.mark.parametrize("headers", [{}, {"Retry-After-Sandbox-Lifecycle": "bad"}])
    def test_pty_reconnect_retry_wait_uses_exponential_fallback_for_rate_limits(self, headers: dict[str, str]) -> None:
        retry_state = Mock()
        retry_state.attempt_number = 4
        retry_state.outcome.exception.return_value = DaytonaRateLimitError("rate limited", headers=headers)
        wait_strategy = _reconnect_and_wait_pty.retry.wait

        assert wait_strategy(retry_state) == 8

    async def test_stop_active_sandboxes_does_not_retry_non_rate_limit_daytona_errors(self) -> None:
        benchmark = Mock()
        benchmark.name = "benchmark"
        benchmark.id = "benchmark-id"
        daytona_client = Mock()
        daytona_client.list.side_effect = DaytonaError("transient")

        with pytest.raises(DaytonaError):
            await _stop_active_sandboxes.retry_with(stop=stop_after_attempt(3), wait=wait_none())(
                benchmark, daytona_client, {}, set()
            )

        assert daytona_client.list.call_count == 1

    async def test_stop_active_sandboxes_retries_rate_limit_daytona_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        benchmark = Mock()
        benchmark.name = "benchmark"
        benchmark.id = "benchmark-id"
        active = Mock(id="s1", state=SandboxState.STARTED)
        active.name = "sandbox-1"
        deleted = Mock(id="s2", state=SandboxState.DESTROYED)
        deleted.name = "sandbox-2"
        daytona_client = Mock()
        daytona_client.list.side_effect = [
            DaytonaRateLimitError("rate limited"),
            _async_iter([active, deleted]),
        ]

        stopped: list[Any] = []

        async def fake_stop_sandbox(sandbox: Any, _client: Any) -> None:
            stopped.append(sandbox)
            return None

        monkeypatch.setattr(utils_module, "stop_sandbox", fake_stop_sandbox)

        results: dict[str, str | None] = {}
        attempted: set[str] = set()
        await _stop_active_sandboxes.retry_with(stop=stop_after_attempt(3), wait=wait_none())(
            benchmark, daytona_client, results, attempted
        )

        # The rate-limited first attempt is retried; the second pass stops only the active sandbox.
        assert daytona_client.list.call_count == 2
        assert stopped == [active]
        assert results == {"sandbox-1": None}

    async def test_stop_active_sandboxes_skips_already_attempted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        benchmark = Mock()
        benchmark.name = "benchmark"
        benchmark.id = "benchmark-id"
        first = Mock(id="s1", state=SandboxState.STARTED)
        first.name = "sandbox-1"
        second = Mock(id="s2", state=SandboxState.STARTED)
        second.name = "sandbox-2"
        daytona_client = Mock()
        daytona_client.list.return_value = _async_iter([first, second])

        stopped: list[Any] = []

        async def fake_stop_sandbox(sandbox: Any, _client: Any) -> None:
            stopped.append(sandbox)
            return None

        monkeypatch.setattr(utils_module, "stop_sandbox", fake_stop_sandbox)

        attempted: set[str] = {"s1"}  # s1 already handled (e.g. before a rate-limit restart)
        results: dict[str, str | None] = {}
        await _stop_active_sandboxes(benchmark, daytona_client, results, attempted)

        # s1 is skipped because it is already in `attempted`; only s2 is stopped.
        assert stopped == [second]
        assert attempted == {"s1", "s2"}
        assert results == {"sandbox-2": None}

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
        callback = _create_sandbox.retry.before_sleep
        assert callback is not None

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
        callback = _create_sandbox.retry.before_sleep
        assert callback is not None

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
        callback = _create_sandbox.retry.before_sleep
        assert callback is not None

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
        callback = _create_sandbox.retry.before_sleep
        assert callback is not None

        callback(state)

        assert increments == [("valkyrie.sandbox.create.retry", {"error_class": "DaytonaError"})]
        assert distributions == []
        assert log_records == []

    def test_metric_image_name_drops_high_cardinality_tag_and_digest(self) -> None:
        metric_image_name = getattr(sandbox_module, "_metric_image_name")

        assert metric_image_name("ghcr.io/vals/swebench:latest") == "ghcr.io/vals/swebench"
        assert (
            metric_image_name("registry.local:5000/vals/swebench@sha256:abcdef") == "registry.local:5000/vals/swebench"
        )
        assert metric_image_name("snapshot:base-python") == "snapshot"

    def test_sandbox_span_helpers_set_safe_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        span_attributes: dict[str, str | int] = {}

        class FakeSpan:
            def set_attribute(self, key: str, value: str | int) -> None:
                span_attributes[key] = value

        monkeypatch.setattr(sandbox_module.trace, "get_current_span", lambda: FakeSpan())

        create_span_attrs = getattr(sandbox_module, "_set_sandbox_create_span_attributes")
        sandbox_span_attrs = getattr(sandbox_module, "_set_sandbox_span_attributes")
        resources = sandbox_module.TrackerResources(vcpu=2, memory=4, disk=5)

        create_span_attrs("task-alias", "ghcr.io/vals/swebench:latest", resources)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        sandbox_span_attrs(mock_sandbox)

        mock_sandbox.state = SandboxState.STARTED
        sandbox_span_attrs(mock_sandbox)

        assert span_attributes == {
            "valkyrie.sandbox_name": "task-alias",
            "valkyrie.image": "ghcr.io/vals/swebench:latest",
            "valkyrie.resources.vcpu": 2,
            "valkyrie.resources.memory": 4,
            "valkyrie.resources.disk": 5,
            "valkyrie.sandbox_id": "sandbox-123",
            "valkyrie.sandbox_state": str(SandboxState.STARTED),
        }

    async def test_create_sandbox_sets_create_span_attributes_for_existing_sandbox(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        span_calls: list[tuple[str, str, int]] = []

        def fake_create_span_attrs(sandbox_name: str, image: str, resources: Any) -> None:
            span_calls.append((sandbox_name, image, resources.vcpu))

        monkeypatch.setattr(sandbox_module, "_set_sandbox_create_span_attributes", fake_create_span_attrs)

        mock_sandbox = AsyncMock()
        mock_sandbox.wait_for_sandbox_start = AsyncMock()
        daytona = AsyncMock()
        daytona.get = AsyncMock(return_value=mock_sandbox)

        resources = sandbox_module.TrackerResources(vcpu=2, memory=4, disk=5)
        sandbox = await _create_sandbox.retry_with(stop=stop_after_attempt(1), wait=wait_none())(
            daytona,
            "task-alias",
            "ghcr.io/vals/swebench:latest",
            resources,
        )

        assert sandbox is mock_sandbox
        assert span_calls == [("task-alias", "ghcr.io/vals/swebench:latest", 2)]

    async def test_delete_sandbox_tags_daytona_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        daytona_error = DaytonaError("delete failed")
        daytona_errors: list[tuple[Exception, str]] = []

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.name = "task-alias"
        mock_sandbox.refresh_data = AsyncMock(side_effect=daytona_error)

        with pytest.raises(DaytonaError):
            await _delete_sandbox.retry_with(stop=stop_after_attempt(1), wait=wait_none())(mock_sandbox, AsyncMock())

        assert daytona_errors == [(daytona_error, "sandbox.delete")]

    async def test_exec_tags_daytona_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        daytona_error = DaytonaError("exec failed")
        daytona_errors: list[tuple[Exception, str]] = []

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)
        monkeypatch.setattr(sandbox_module, "_set_sandbox_span_attributes", Mock(), raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.process.exec = AsyncMock(side_effect=daytona_error)

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

        resources = sandbox_module.TrackerResources(vcpu=2, memory=4, disk=5)
        async with create_sandbox(
            daytona=AsyncMock(),
            sandbox_name="task-alias",
            image="ghcr.io/vals/swebench:latest",
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

        resources = sandbox_module.TrackerResources(vcpu=2, memory=4, disk=5)
        with pytest.raises(DaytonaError):
            async with create_sandbox(
                daytona=AsyncMock(),
                sandbox_name="task-alias",
                image="ghcr.io/vals/swebench:latest",
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

        resources = sandbox_module.TrackerResources(vcpu=2, memory=4, disk=5)
        with pytest.raises(ValueError, match="bad config"):
            async with create_sandbox(
                daytona=AsyncMock(),
                sandbox_name="task-alias",
                image="ghcr.io/vals/swebench:latest",
                resources=resources,
                creation_semaphore=asyncio.Semaphore(1),
            ):
                pass

        assert increments == [("valkyrie.sandbox.create.errors", {"error_class": "ValueError"})]
        assert daytona_errors == []

    async def test_check_sandbox_health_allows_running_sandboxes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        increments: list[tuple[str, dict[str, str]]] = []

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.state = SandboxState.STARTED

        await _check_sandbox_health(mock_sandbox)

        assert increments == []

    @pytest.mark.parametrize("dead_state", [SandboxState.DESTROYED, SandboxState.ERROR])
    async def test_check_sandbox_health_emits_unhealthy_state_telemetry(
        self, monkeypatch: pytest.MonkeyPatch, dead_state: SandboxState
    ) -> None:
        tags: dict[str, str] = {}
        increments: list[tuple[str, dict[str, str]]] = []

        def fake_set_tag(key: str, value: str) -> None:
            tags[key] = value

        def fake_incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
            increments.append((name, {str(k): str(v) for k, v in (tags or {}).items()}))

        monkeypatch.setattr(sandbox_module, "sentry_sdk", SimpleNamespace(set_tag=fake_set_tag), raising=False)
        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.state = dead_state

        with pytest.raises(SandboxError, match="crashed during command execution"):
            await _check_sandbox_health(mock_sandbox)

        assert tags == {"sandbox_state": str(dead_state)}
        assert increments == [("valkyrie.sandbox.unhealthy", {"state": str(dead_state)})]

    async def test_check_sandbox_health_tags_daytona_refresh_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        daytona_error = DaytonaError("refresh failed")
        daytona_errors: list[tuple[Exception, str]] = []

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.name = "task-alias"
        mock_sandbox.refresh_data = AsyncMock(side_effect=daytona_error)

        with pytest.raises(DaytonaError):
            await _check_sandbox_health(mock_sandbox)

        assert all(exc is daytona_error and op == "sandbox.health_check" for exc, op in daytona_errors)
        assert len(daytona_errors) >= 1

    async def test_check_sandbox_health_does_not_tag_non_daytona_refresh_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        daytona_errors: list[tuple[Exception, str]] = []

        def fake_tag_daytona_error(exc: Exception, *, op: str) -> None:
            daytona_errors.append((exc, op))

        monkeypatch.setattr(sandbox_module, "tag_daytona_error", fake_tag_daytona_error, raising=False)

        mock_sandbox = AsyncMock()
        mock_sandbox.name = "task-alias"
        mock_sandbox.refresh_data = AsyncMock(side_effect=RuntimeError("refresh failed"))

        with pytest.raises(SandboxError, match="Failed to check sandbox"):
            await _check_sandbox_health(mock_sandbox)

        assert daytona_errors == []

    async def test_semaphore_caps_concurrent_handshakes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Concurrent _create_pty_session calls never exceed the cap on in-flight
        sandbox.process.create_pty_session calls.

        Regression guard: if someone removes the `async with _pty_handshake_slot(...)`
        wrapper around the handshake call, concurrency becomes unbounded.
        """
        cap = 5
        total = 25
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(cap))

        concurrent = 0
        max_concurrent = 0

        async def fake_create_pty_session(*_args: Any, **_kwargs: Any) -> Mock:
            nonlocal concurrent, max_concurrent

            concurrent += 1
            max_concurrent = max(max_concurrent, concurrent)

            # Hold long enough for other tasks to pile up behind the gate
            await asyncio.sleep(0.05)

            concurrent -= 1
            return Mock()

        mock_sandbox = Mock()
        mock_sandbox.process.create_pty_session = fake_create_pty_session

        results = await asyncio.gather(
            *[_create_pty_session(mock_sandbox, f"session-{i}", _ignore_pty_data) for i in range(total)]
        )

        assert len(results) == total
        assert max_concurrent <= cap
        # Sanity: without the cap we'd see total concurrency; confirm contention actually happened
        assert max_concurrent > 1

    @pytest.mark.parametrize(
        "pty_info_side_effect, sandbox_state, connect_side_effect, stop_after_one, expected_exc_type, expected_match, expected_tags",
        [
            pytest.param(
                DaytonaNotFoundError("gone"),
                SandboxState.STARTED,
                None,
                False,
                SandboxError,
                "no longer exists",
                {"pty.disconnect_reason": "pty_session_killed", "sandbox_state": str(SandboxState.STARTED)},
                id="pty_not_found",
            ),
            pytest.param(
                DaytonaError("toolbox unreachable"),
                SandboxState.DESTROYING,
                None,
                False,
                SandboxError,
                "destroyed during PTY reconnect",
                {"pty.disconnect_reason": "sandbox_killed", "sandbox_state": str(SandboxState.DESTROYING)},
                id="sandbox_dead_after_toolbox_error",
            ),
            pytest.param(
                DaytonaError("502 bad gateway"),
                SandboxState.STARTED,
                None,
                True,
                DaytonaError,
                "502 bad gateway",
                {},
                id="transient_error_alive_sandbox",
            ),
            pytest.param(
                None,
                SandboxState.STARTED,
                DaytonaConnectionError("PTY session not found"),
                False,
                SandboxError,
                "PTY session not found",
                {"pty.disconnect_reason": "pty_session_killed"},
                id="toctou_connect_not_found",
            ),
        ],
    )
    async def test_reconnect_fast_fail_paths(
        self,
        monkeypatch: pytest.MonkeyPatch,
        pty_info_side_effect: Exception | None,
        sandbox_state: SandboxState,
        connect_side_effect: Exception | None,
        stop_after_one: bool,
        expected_exc_type: type[Exception],
        expected_match: str,
        expected_tags: dict[str, str],
    ) -> None:
        tags: dict[str, str] = {}
        monkeypatch.setattr(
            sandbox_module, "sentry_sdk", SimpleNamespace(set_tag=lambda k, v: tags.update({k: v})), raising=False
        )
        monkeypatch.setattr(sandbox_module, "_check_sandbox_health", AsyncMock())
        monkeypatch.setattr(sandbox_module, "incr", Mock(), raising=False)
        monkeypatch.setattr(sandbox_module.logger, "info", Mock())
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(5))
        monkeypatch.setattr(sandbox_module, "_pty_handshake_in_flight_count", 0, raising=False)
        monkeypatch.setattr(sandbox_module, "distribution", Mock(), raising=False)
        monkeypatch.setattr(sandbox_module, "gauge", Mock(), raising=False)
        monkeypatch.setattr(sandbox_module, "_set_pty_span_attributes", Mock(), raising=False)
        monkeypatch.setattr(sandbox_module, "set_pty_context", Mock(), raising=False)

        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.state = sandbox_state
        mock_sandbox.refresh_data = AsyncMock()
        mock_sandbox.process.get_pty_session_info = AsyncMock(side_effect=pty_info_side_effect)
        if connect_side_effect is not None:
            mock_sandbox.process.connect_pty_session = AsyncMock(side_effect=connect_side_effect)

        fn = (
            _reconnect_and_wait_pty.retry_with(stop=stop_after_attempt(1), wait=wait_none())
            if stop_after_one
            else _reconnect_and_wait_pty
        )

        with pytest.raises(expected_exc_type, match=expected_match):
            await fn(mock_sandbox, "session-1", _ignore_pty_data, lambda _: None)

        for key, value in expected_tags.items():
            assert tags[key] == value

    async def test_semaphore_released_after_handshake_returns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        The semaphore slot is released as soon as the handshake call returns, before the
        caller does anything else with the handle (handle.wait, send_input, etc.).

        Regression guard: if someone widens the `async with _pty_handshake_slot(...)` scope
        — e.g. wraps code after create_pty_session returns, or wraps the whole caller flow
        around handle.wait() — a second concurrent handshake would block until the first
        task finishes its session lifetime. With cap=1 and a simulated long-running
        post-handshake step, that regression turns this test into a timeout.
        """
        cap = 1
        monkeypatch.setattr(sandbox_module, "_pty_handshake_semaphore", asyncio.Semaphore(cap))

        async def fake_create_pty_session(*_args: Any, **_kwargs: Any) -> Mock:
            return Mock()

        mock_sandbox = Mock()
        mock_sandbox.process.create_pty_session = fake_create_pty_session

        events: list[str] = []

        async def task_holding_handle() -> None:
            await _create_pty_session(mock_sandbox, "s1", _ignore_pty_data)
            events.append("a:handshake_done")

            # Simulate a long-running handle.wait() AFTER the handshake.
            # The slot must already be released, otherwise the other task can't acquire.
            await asyncio.sleep(0.2)
            events.append("a:post_handshake_done")

        async def task_needing_slot() -> None:
            # Tiny delay so task A acquires the slot first
            await asyncio.sleep(0.01)
            await _create_pty_session(mock_sandbox, "s2", _ignore_pty_data)
            events.append("b:handshake_done")

        await asyncio.wait_for(asyncio.gather(task_holding_handle(), task_needing_slot()), timeout=1.0)

        # Task B's handshake must complete BEFORE task A's post-handshake work finishes.
        # That ordering is only reachable if the slot was released at handshake exit.
        assert events.index("b:handshake_done") < events.index("a:post_handshake_done")


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
            AsyncMock(return_value=ExecuteResponse(exit_code=exit_code, result="error output")),
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
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "sb-1"
        mock_sandbox.name = "sb-1"
        mock_handle = AsyncMock()

        async def _mock_create_pty(*_args: Any, **_kwargs: Any) -> tuple[AsyncMock, str]:
            return mock_handle, "sb-1:pty-abc"

        async def _noop(*_args: Any, **_kwargs: Any) -> None:
            return None

        async def _mock_read_exit_code(*_args: Any, **_kwargs: Any) -> int:
            return exit_code

        monkeypatch.setattr(sandbox_module, "_create_pty_session", _mock_create_pty)
        monkeypatch.setattr(sandbox_module, "_wait_for_pty", _noop)
        monkeypatch.setattr(sandbox_module, "_check_sandbox_health", _noop)
        monkeypatch.setattr(sandbox_module, "_read_exit_code", _mock_read_exit_code)
        monkeypatch.setattr(
            sandbox_module,
            "_exec",
            AsyncMock(return_value=ExecuteResponse(exit_code=0, result="1000000000")),
        )

        tagged: dict[str, str] = {}

        def fake_set_tag(key: str, value: object) -> None:
            tagged[key] = str(value)

        monkeypatch.setattr(sandbox_module.sentry_sdk, "set_tag", fake_set_tag)

        with pytest.raises(AgentRunFailedError) as exc_info:
            await stream_command_output(mock_sandbox, "run-agent.sh", on_output=lambda _: None)

        assert isinstance(exc_info.value, SandboxError)
        assert not isinstance(exc_info.value, SandboxSetupError)
        assert f"exit code: {exit_code}" in str(exc_info.value)
        assert tagged == {"agent_exit_code": str(exit_code)}
