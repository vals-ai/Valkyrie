"""Test WebSocket disconnection handling in stream_command_output."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from daytona.common.errors import DaytonaError

from tracker import sandbox as sandbox_module
from tracker.sandbox import stream_command_output, stream_session_command_logs

MAX_LOG_STREAM_RETRIES = 10


@dataclass
class FakeCommand:
    exit_code: int | None = 0


@dataclass
class FakeSessionExecResponse:
    cmd_id: str = "cmd-123"


@dataclass
class FakeProcess:
    """Fake sandbox process for command streaming and completion."""

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


@pytest.mark.asyncio
async def test_stream_command_output_succeeds_without_disconnect() -> None:
    collected: list[str] = []
    sandbox = FakeSandbox()

    await stream_command_output(sandbox, "echo hello", collected.append)  # type: ignore[reportArgumentType]

    assert collected == ["output from attempt 1\n"]
    assert sandbox.process.status_poll_count >= 1


@pytest.mark.asyncio
async def test_stream_session_command_logs_retries_websocket_errors(monkeypatch) -> None:
    monkeypatch.setattr(sandbox_module, "LOG_STREAM_RETRY_DELAY_SECONDS", 0.0)
    collected: list[str] = []
    sandbox_obj = FakeSandbox(
        process=FakeProcess(
            log_error=DaytonaError(
                "Failed to get session command logs: server rejected WebSocket connection: HTTP 502"
            ),
            log_error_count=2,
        )
    )

    await stream_session_command_logs(
        sandbox_obj,
        session_id="session-123",
        command_id="cmd-123",
        on_output=collected.append,
    )

    assert sandbox_obj.process.call_count == 3
    assert "output from attempt 3\n" in collected


@pytest.mark.asyncio
async def test_stream_session_command_logs_warns_after_retries(monkeypatch) -> None:
    monkeypatch.setattr(sandbox_module, "LOG_STREAM_RETRY_DELAY_SECONDS", 0.0)
    sandbox_obj = FakeSandbox(
        process=FakeProcess(
            log_error=DaytonaError(
                "Failed to get session command logs: server rejected WebSocket connection: HTTP 502"
            ),
            log_error_count=MAX_LOG_STREAM_RETRIES,
        )
    )

    collected: list[str] = []
    await stream_session_command_logs(
        sandbox_obj,
        session_id="session-123",
        command_id="cmd-123",
        on_output=collected.append,
    )

    assert sandbox_obj.process.call_count == MAX_LOG_STREAM_RETRIES
    assert any("continuing without live logs" in line.lower() for line in collected)


@pytest.mark.asyncio
async def test_stream_session_command_logs_continues_on_non_websocket_log_error() -> None:
    collected: list[str] = []
    sandbox_obj = FakeSandbox(process=FakeProcess(log_error=DaytonaError("some other error"), log_error_count=1))

    await stream_session_command_logs(
        sandbox_obj,
        session_id="session-123",
        command_id="cmd-123",
        on_output=collected.append,
    )

    assert sandbox_obj.process.call_count == 1
    assert any("continuing without live logs" in line.lower() for line in collected)


@pytest.mark.asyncio
async def test_stream_command_output_raises_on_nonzero_exit_code() -> None:
    sandbox = FakeSandbox(process=FakeProcess(exit_code=2))

    with pytest.raises(Exception, match="exit code: 2"):
        await stream_command_output(sandbox, "echo hello", lambda text: None)  # type: ignore[reportArgumentType]
