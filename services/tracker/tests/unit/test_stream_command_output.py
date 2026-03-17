"""Test WebSocket disconnection handling in run_agent."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from daytona.common.errors import DaytonaError

from tracker.sandbox import is_websocket_disconnect, run_agent
from tracker.database.models import AgentContractRequest


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


CONTRACT = AgentContractRequest(
    name="test-agent",
    run_cmd="echo {problem_statement}",
    install_cmd="echo install",
)


@pytest.mark.parametrize(
    "message, expected",
    [
        ("WebSocket error: no close frame received or sent", True),
        ("WebSocket error: sent 1011 (internal error) keepalive ping timeout; no close frame received", True),
        ("some other error", False),
    ],
)
def test_is_websocket_disconnect(message: str, expected: bool) -> None:
    assert is_websocket_disconnect(DaytonaError(message)) == expected


@pytest.mark.asyncio
async def test_run_agent_succeeds_without_disconnect() -> None:
    sandbox = FakeSandbox()

    await run_agent(sandbox, CONTRACT, "test problem", "task-1")  # type: ignore[reportArgumentType]


@pytest.mark.asyncio
async def test_run_agent_retries_on_websocket_disconnect() -> None:
    sandbox = FakeSandbox(process=FakeProcess(disconnect_count=2))

    await run_agent(sandbox, CONTRACT, "test problem", "task-1")  # type: ignore[reportArgumentType]

    assert sandbox.process.call_count == 3


@pytest.mark.asyncio
async def test_run_agent_raises_after_max_retries() -> None:
    sandbox = FakeSandbox(process=FakeProcess(disconnect_count=20))

    with pytest.raises(DaytonaError, match="1011"):
        await run_agent(sandbox, CONTRACT, "test problem", "task-1")  # type: ignore[reportArgumentType]


@pytest.mark.asyncio
async def test_run_agent_raises_non_websocket_error() -> None:
    @dataclass
    class FailingProcess(FakeProcess):
        async def get_session_command_logs_async(  # type: ignore[reportIncompatibleMethodOverride]
            self, **kwargs: Any
        ) -> None:
            raise DaytonaError("some other error")

    sandbox = FakeSandbox(process=FailingProcess())

    with pytest.raises(DaytonaError, match="some other error"):
        await run_agent(sandbox, CONTRACT, "test problem", "task-1")  # type: ignore[reportArgumentType]
