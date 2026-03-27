"""Test WebSocket disconnection handling in stream_command_output."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from daytona.common.errors import DaytonaError

from tracker import sandbox as sandbox_module
from tracker.sandbox import is_websocket_stream_error, stream_command_output, stream_session_command_logs


@dataclass
class FakeCommand:
    exit_code: int | None = 0


@dataclass
class FakeSessionExecResponse:
    cmd_id: str = "cmd-123"


@dataclass
class FakeProcess:
    log_error: Exception | None = None
    log_error_count: int = 0
    exit_code: int = 0
    call_count: int = field(default=0, init=False)
    status_poll_count: int = field(default=0, init=False)

    async def create_session(self, session_id: str) -> None:
        pass

    async def execute_session_command(self, session_id: str, request: Any) -> FakeSessionExecResponse:
        return FakeSessionExecResponse()

    async def get_session_command_logs_async(
        self,
        session_id: str,
        command_id: str,
        on_stdout: Callable[[str], None],
        on_stderr: Callable[[str], None],
    ) -> None:
        self.call_count += 1
        on_stdout(f"output from attempt {self.call_count}\n")
        if self.call_count <= self.log_error_count and self.log_error is not None:
            raise self.log_error

    async def get_session_command(self, session_id: str, command_id: str) -> FakeCommand:
        await asyncio.sleep(0)
        self.status_poll_count += 1
        return FakeCommand(exit_code=self.exit_code)

    async def delete_session(self, session_id: str) -> None:
        pass


@dataclass
class FakeSandbox:
    id: str = "sandbox-123"
    process: FakeProcess = field(default_factory=FakeProcess)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("WebSocket error: no close frame received or sent", True),
        ("WebSocket error: sent 1011 (internal error) keepalive ping timeout; no close frame received", True),
        ("Failed to get session command logs: timed out during opening handshake", True),
        ("Failed to get session command logs: server rejected WebSocket connection: HTTP 502", True),
        ("some other error", False),
    ],
)
def test_is_websocket_stream_error(message: str, expected: bool) -> None:
    assert is_websocket_stream_error(DaytonaError(message)) == expected


async def test_stream_command_output_succeeds_without_disconnect() -> None:
    collected: list[str] = []
    sandbox = FakeSandbox()

    await stream_command_output(sandbox, "echo hello", collected.append)  # type: ignore[reportArgumentType]

    assert collected == ["output from attempt 1\n"]
    assert sandbox.process.status_poll_count >= 1


async def test_stream_session_command_logs_retries_websocket_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox_module, "LOG_STREAM_RETRY_DELAY_SECONDS", 0.0)
    collected: list[str] = []
    sandbox = FakeSandbox(
        process=FakeProcess(
            log_error=DaytonaError(
                "Failed to get session command logs: server rejected WebSocket connection: HTTP 502"
            ),
            log_error_count=2,
        )
    )

    await stream_session_command_logs(
        sandbox,
        session_id="session-123",
        command_id="cmd-123",
        on_output=collected.append,
    )

    assert sandbox.process.call_count == 3
    assert "output from attempt 3\n" in collected


async def test_stream_session_command_logs_warns_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox_module, "LOG_STREAM_RETRY_DELAY_SECONDS", 0.0)
    collected: list[str] = []
    sandbox = FakeSandbox(
        process=FakeProcess(
            log_error=DaytonaError(
                "Failed to get session command logs: server rejected WebSocket connection: HTTP 502"
            ),
            log_error_count=10,
        )
    )

    await stream_session_command_logs(
        sandbox,
        session_id="session-123",
        command_id="cmd-123",
        on_output=collected.append,
    )

    assert sandbox.process.call_count == 10
    assert any("continuing without live logs" in line.lower() for line in collected)


async def test_stream_session_command_logs_warns_on_non_websocket_error() -> None:
    collected: list[str] = []
    sandbox = FakeSandbox(process=FakeProcess(log_error=DaytonaError("some other error"), log_error_count=1))

    await stream_session_command_logs(
        sandbox,
        session_id="session-123",
        command_id="cmd-123",
        on_output=collected.append,
    )

    assert sandbox.process.call_count == 1
    assert any("continuing without live logs" in line.lower() for line in collected)


async def test_stream_command_output_raises_on_nonzero_exit_code() -> None:
    sandbox = FakeSandbox(process=FakeProcess(exit_code=2))

    with pytest.raises(Exception, match="exit code: 2"):
        await stream_command_output(sandbox, "echo hello", lambda text: None)  # type: ignore[reportArgumentType]
