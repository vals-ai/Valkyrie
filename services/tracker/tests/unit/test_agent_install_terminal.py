"""Exercise the real install wrapper, output streaming, and exit handling in a terminal."""

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from pathlib import Path
from unittest.mock import Mock

import pytest
from benchmark_service.sandbox import SandboxCommandError

from tracker import sandbox as sandbox_module
from tracker.database.models import AgentContractRequest
from tracker.exceptions import AgentRunFailedError, SandboxError
from tracker.sandbox import DependencySetupMode, install_agent_dependencies


@pytest.fixture
def terminal_sandbox(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Mock]:
    def contract_path(_name: str) -> Path:
        return tmp_path

    monkeypatch.setattr(sandbox_module, "get_contract_path", contract_path)
    monkeypatch.setattr(sandbox_module, "AGENT_INSTALL_TIMEOUT_SECONDS", 1)
    terminal_helper = Path(__file__).parents[1] / "fixtures" / "terminal_command.py"

    async def command(command: str) -> AsyncIterator[str]:
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(terminal_helper), command],
            capture_output=True,
            text=True,
            timeout=10,
        )
        yield result.stdout
        if result.returncode:
            raise SandboxCommandError(result.returncode)

    try:
        yield Mock(command=command)
    finally:
        child_file = tmp_path / "child.pid"
        if child_file.exists():
            with suppress(ProcessLookupError):
                child_group = os.getpgid(int(child_file.read_text()))
                assert child_group != os.getpgrp()
                os.killpg(child_group, signal.SIGKILL)


@pytest.mark.skipif(shutil.which("timeout") is None, reason="GNU timeout is required")
@pytest.mark.parametrize(
    "install_command, expected_error",
    [
        ("stty -echo && printf installed > installed.txt", None),
        ("exit 7", AgentRunFailedError),
        ("exec sleep 30", SandboxError),
        ("sh -c 'trap \"\" HUP; echo $$ > child.pid; sleep 30; touch leaked.txt' & wait", SandboxError),
    ],
    ids=["terminal", "failed", "deadline", "child-deadline"],
)
async def test_installer_terminal_and_deadline(
    terminal_sandbox: Mock,
    tmp_path: Path,
    install_command: str,
    expected_error: type[SandboxError] | None,
) -> None:
    contract = AgentContractRequest(name="terminal-test", install_cmd=install_command, run_cmd="true")
    started = time.monotonic()
    if expected_error:
        message = "exit code 7" if expected_error is AgentRunFailedError else "timed out after 1 seconds"
        with pytest.raises(expected_error, match=message):
            await install_agent_dependencies(
                terminal_sandbox, contract, lambda _message: None, DependencySetupMode.FINAL_FRESH_SANDBOX
            )
    else:
        await install_agent_dependencies(
            terminal_sandbox, contract, lambda _message: None, DependencySetupMode.FINAL_FRESH_SANDBOX
        )
        assert (tmp_path / "installed.txt").read_text() == "installed"

    assert time.monotonic() - started < 5
    child_file = tmp_path / "child.pid"
    if child_file.exists():
        child_id = int(child_file.read_text())
        result = subprocess.run(["ps", "-o", "stat=", "-p", str(child_id)], capture_output=True, text=True, check=False)
        assert not result.stdout.strip() or result.stdout.strip().startswith("Z")
        assert not (tmp_path / "leaked.txt").exists()
