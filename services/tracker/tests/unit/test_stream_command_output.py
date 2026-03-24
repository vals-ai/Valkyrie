"""Test WebSocket disconnection handling in stream_command_output."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from daytona.common.errors import DaytonaError

from tracker.sandbox import is_retriable_websocket_error, stream_command_output


@dataclass
class FakeCommand:
    exit_code: int | None = 0


@dataclass
class FakeSessionExecResponse:
    cmd_id: str = "cmd-123"


@dataclass
class FakeProcess:
    """Fake sandbox process that can simulate WebSocket disconnections."""

    disconnect_count: int = 0
    exit_code: int = 0
    call_count: int = field(default=0, init=False)

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

        if self.call_count <= self.disconnect_count:
            raise DaytonaError(
                "Failed to get session command logs: WebSocket error: "
                "sent 1011 (internal error) keepalive ping timeout; no close frame received"
            )

    async def get_session_command(self, session_id: str, command_id: str) -> FakeCommand:
        return FakeCommand(exit_code=self.exit_code)

    async def delete_session(self, session_id: str) -> None:
        pass


@dataclass
class FakeSandbox:
    id: str = "sandbox-123"
    process: FakeProcess = field(default_factory=FakeProcess)


@pytest.mark.parametrize(
    "message, expected",
    [
        ("WebSocket error: no close frame received or sent", True),
        ("WebSocket error: sent 1011 (internal error) keepalive ping timeout; no close frame received", True),
        ("Failed to get session command logs: timed out during opening handshake", True),
        ("some other error", False),
    ],
)
def test_is_retriable_websocket_error(message: str, expected: bool) -> None:
    assert is_retriable_websocket_error(DaytonaError(message)) == expected


async def test_stream_command_output_succeeds_without_disconnect() -> None:
    collected: list[str] = []
    sandbox = FakeSandbox()

    await stream_command_output(sandbox, "echo hello", collected.append)  # type: ignore[reportArgumentType]

    assert collected == ["output from attempt 1\n"]


async def test_stream_command_output_retries_on_websocket_disconnect() -> None:
    collected: list[str] = []
    sandbox = FakeSandbox(process=FakeProcess(disconnect_count=2))

    await stream_command_output(sandbox, "echo hello", collected.append)  # type: ignore[reportArgumentType]

    assert sandbox.process.call_count == 3


async def test_stream_command_output_raises_after_max_retries() -> None:
    sandbox = FakeSandbox(process=FakeProcess(disconnect_count=20))

    with pytest.raises(DaytonaError, match="1011"):
        await stream_command_output(sandbox, "echo hello", lambda text: None)  # type: ignore[reportArgumentType]


async def test_stream_command_output_raises_non_websocket_error() -> None:
    @dataclass
    class FailingProcess(FakeProcess):
        async def get_session_command_logs_async(  # type: ignore[reportIncompatibleMethodOverride]
            self, **kwargs: Any
        ) -> None:
            raise DaytonaError("some other error")

    sandbox = FakeSandbox(process=FailingProcess())

    with pytest.raises(DaytonaError, match="some other error"):
        await stream_command_output(sandbox, "echo hello", lambda text: None)  # type: ignore[reportArgumentType]
