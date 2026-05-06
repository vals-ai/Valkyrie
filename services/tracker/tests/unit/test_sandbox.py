import asyncio
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from daytona import ExecuteResponse

from tracker import sandbox as sandbox_module
from tracker.database.models import AgentContractRequest
from tracker.exceptions import SSLConnectionError, SandboxError, SandboxSetupError
from tracker.sandbox import upload_agent_artifacts
from tracker.types import AWSCredentials

_create_pty_session = getattr(sandbox_module, "_create_pty_session")
_reconnect_and_wait_pty = getattr(sandbox_module, "_reconnect_and_wait_pty")
_wait_for_pty = getattr(sandbox_module, "_wait_for_pty")


def _ignore_pty_data(_data: bytes) -> None:
    pass


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

        def fake_distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            distributions.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_gauge(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
            gauges.append((name, value, {str(k): str(v) for k, v in (tags or {}).items()}))

        def fake_info(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        def fake_set_span_attrs(sandbox: Any, session_id: str) -> None:
            span_calls.append((sandbox.id, sandbox.name, session_id))

        monkeypatch.setattr(sandbox_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(sandbox_module, "gauge", fake_gauge, raising=False)
        monkeypatch.setattr(sandbox_module.logger, "info", fake_info)
        monkeypatch.setattr(sandbox_module, "_set_pty_span_attributes", fake_set_span_attrs, raising=False)

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

        monkeypatch.setattr(sandbox_module, "incr", fake_incr, raising=False)
        monkeypatch.setattr(sandbox_module, "distribution", fake_distribution, raising=False)
        monkeypatch.setattr(sandbox_module, "gauge", fake_gauge, raising=False)
        monkeypatch.setattr(sandbox_module.logger, "info", fake_info)
        monkeypatch.setattr(sandbox_module, "_set_pty_span_attributes", fake_set_span_attrs, raising=False)
        monkeypatch.setattr(sandbox_module, "_check_sandbox_health", AsyncMock())

        mock_handle = AsyncMock()
        mock_sandbox = Mock()
        mock_sandbox.id = "sandbox-123"
        mock_sandbox.name = "task-alias"
        mock_sandbox.process.connect_pty_session = AsyncMock(return_value=mock_handle)
        outputs: list[str] = []

        await _reconnect_and_wait_pty(mock_sandbox, "session-1", _ignore_pty_data, outputs.append)

        assert increments == [("valkyrie.pty.reconnect.count", {"operation": "reconnect"})]
        assert outputs == ["[Debug]: Disconnected from websocket, creating a new reader and reconnecting\n"]
        assert log_records == [
            {
                "message": "pty.reconnect_start",
                "pty_event": "reconnect_start",
                "session_id": "session-1",
                "sandbox_id": "sandbox-123",
            }
        ]
        handshake_duration_call = next(call for call in distributions if call[0] == "valkyrie.pty.handshake.duration")
        assert 0 <= handshake_duration_call[1] < 1
        assert handshake_duration_call[2] == {"operation": "reconnect"}
        assert [call for call in gauges if call[0] == "valkyrie.pty.handshake.in_flight"] == [
            ("valkyrie.pty.handshake.in_flight", 1, {"operation": "reconnect"}),
            ("valkyrie.pty.handshake.in_flight", 0, {"operation": "reconnect"}),
        ]
        assert span_calls == [("sandbox-123", "task-alias", "session-1")]

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
        assert create_before_sleep.__module__ == "tracker.observability"
        assert reconnect_before_sleep.__module__ == "tracker.observability"

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
