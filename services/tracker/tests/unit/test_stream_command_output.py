from collections.abc import Mapping

import pytest
from benchmark_service.sandbox import ExecResult, Sandbox, SandboxFile

from tracker.database.models import AgentContractRequest
from tracker.exceptions import SandboxError
from tracker.sandbox import install_agent_dependencies, stream_command_output


class FakeSandbox(Sandbox):
    def __init__(self, result: ExecResult) -> None:
        super().__init__(provider=object(), id="sandbox-123", name="sandbox-123")  # type: ignore[arg-type]
        self._result = result
        self.last_command: str | None = None
        self.last_cwd: str | None = None

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout=None,
        on_stderr=None,
    ) -> ExecResult:
        self.last_command = command
        self.last_cwd = cwd
        if on_stdout:
            on_stdout("stdout\n")
        if on_stderr:
            on_stderr("stderr\n")
        return self._result

    async def upload_file(self, file: SandboxFile) -> None:
        raise NotImplementedError

    async def upload_files(self, files: list[SandboxFile]) -> None:
        raise NotImplementedError

    async def download_file(self, remote_path: str) -> bytes:
        raise NotImplementedError

    async def create_folder(self, remote_path: str) -> None:
        raise NotImplementedError

    async def wait_until_ready(self) -> None:
        raise NotImplementedError


@pytest.mark.asyncio
async def test_stream_command_output_allows_unknown_exit_code() -> None:
    collected: list[str] = []
    sandbox = FakeSandbox(ExecResult(exit_code=None))

    await stream_command_output(sandbox, "echo hello", collected.append)

    assert sandbox.last_command == "echo hello"
    assert collected == ["stdout\n", "stderr\n"]


@pytest.mark.asyncio
async def test_stream_command_output_raises_on_explicit_nonzero_exit_code() -> None:
    sandbox = FakeSandbox(ExecResult(exit_code=2))

    with pytest.raises(SandboxError, match="exit code: 2"):
        await stream_command_output(sandbox, "echo hello", lambda text: None)


@pytest.mark.asyncio
async def test_install_agent_dependencies_allows_unknown_exit_code() -> None:
    contract = AgentContractRequest(
        name="agent",
        install_cmd="uv sync",
        run_cmd="python agent.py",
    )
    sandbox = FakeSandbox(ExecResult(exit_code=None))
    collected: list[str] = []

    await install_agent_dependencies(sandbox, contract, collected.append)

    assert sandbox.last_command == "uv sync"
    assert sandbox.last_cwd == "/bundle/agent"
    assert collected[0] == "Installing dependencies for contract: agent"
    assert collected[-1] == "Finished installing dependencies for contract: agent"


@pytest.mark.asyncio
async def test_install_agent_dependencies_raises_on_explicit_nonzero_exit_code() -> None:
    contract = AgentContractRequest(
        name="agent",
        install_cmd="uv sync",
        run_cmd="python agent.py",
    )
    sandbox = FakeSandbox(ExecResult(exit_code=1))

    with pytest.raises(SandboxError, match="exit code: 1"):
        await install_agent_dependencies(sandbox, contract, lambda text: None)
