"""Execute the sandbox report reader with synthetic provider environment values."""

import asyncio
import shlex
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from benchmark_service import ExecResult

from tracker import sandbox as sandbox_module


@pytest.mark.parametrize(
    ("report", "environment", "expected"),
    [
        (
            b"AgentError: rejected provider-placeholder",
            {"PROVIDER_TOKEN": "provider-placeholder"},
            "AgentError: rejected [REDACTED]",
        ),
        (b"AgentError: rejected a.+[b]", {"PROVIDER_TOKEN": "a.+[b]"}, "AgentError: rejected [REDACTED]"),
        (b"AgentError: rejected first\nsecond", {"PROVIDER_TOKEN": "first\nsecond"}, "AgentError: rejected [REDACTED]"),
        (
            b"AgentError: rejected prefix-suffix prefix",
            {"PROVIDER_TOKEN": "prefix", "OTHER_TOKEN": "prefix-suffix"},
            "AgentError: rejected [REDACTED] [REDACTED]",
        ),
        (
            b"AgentError: rejected provider-placeholder and host-placeholder",
            {"PROVIDER_TOKEN": "provider-placeholder"},
            "AgentError: rejected [REDACTED] and [REDACTED]",
        ),
        (b"AgentError: rejected provider-placeholder", {}, ""),
        (b"AgentError: rejected provider-placeholder", {"PROVIDER_TOKEN": ""}, ""),
        (b"AgentError: " + b"x" * 2048, {"PROVIDER_TOKEN": "provider-placeholder"}, ""),
        (b"AgentError: " + b"p" * 2100, {"PROVIDER_TOKEN": "p" * 1500}, ""),
        (b"AgentError: " + b"x" * 300, {"PROVIDER_TOKEN": "x"}, ("AgentError: " + "[REDACTED]" * 300)[:2048]),
        (
            ("AgentError: xxxx" + "界" * 670).encode(),
            {"PROVIDER_TOKEN": "x"},
            "AgentError: " + "[REDACTED]" * 4 + "界" * 665,
        ),
        (b"AgentError: invalid \xff", {"PROVIDER_TOKEN": "provider-placeholder"}, ""),
    ],
    ids=[
        "literal",
        "regex-characters",
        "multiline",
        "overlap",
        "mixed-sources",
        "missing",
        "empty",
        "oversized-input",
        "oversized-maskable-input",
        "masking-expansion",
        "utf8-expansion",
        "invalid-utf8",
    ],
)
async def test_provider_values_are_masked_before_returning_from_sandbox(
    tmp_path: Path, report: bytes, environment: dict[str, str], expected: str
) -> None:
    report_path = tmp_path / "agent error.txt"
    report_path.write_bytes(report)
    commands: list[str] = []
    outputs: list[str] = []

    async def execute(command: str) -> ExecResult:
        commands.append(command)
        argv = shlex.split(command)
        assert argv[:4] == ["python3", "-I", "-S", "-c"]
        # Use the test interpreter, but run the exact generated script and arguments.
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            *argv[1:],
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert stderr == b""
        outputs.append(stdout.decode())
        return ExecResult(exit_code=process.returncode or 0, output=stdout.decode())

    sandbox = Mock()
    sandbox.exec = execute
    names = ("PROVIDER_TOKEN", "OTHER_TOKEN") if "OTHER_TOKEN" in environment else ("PROVIDER_TOKEN",)
    result = await sandbox_module._read_agent_error(
        sandbox,
        str(report_path),
        sandbox_module.create_agent_error_redactor(["host-placeholder"]),
        secret_env_names=names,
    )
    assert result == expected
    assert len(result.encode()) <= 2048
    assert commands
    for value in environment.values():
        if value:
            assert value not in "".join(outputs)
            # Short values may occur in Python source; realistic tokens must not be arguments.
            if len(value) > 8:
                assert value not in "".join(commands)


async def test_provider_reader_without_python_retains_fallback(tmp_path: Path) -> None:
    async def execute(command: str) -> ExecResult:
        process = await asyncio.create_subprocess_exec(
            "/bin/sh",
            "-c",
            command,
            env={"PATH": str(tmp_path)},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await process.communicate()
        assert process.returncode == 127
        assert stdout == b""
        return ExecResult(exit_code=127)

    sandbox = Mock()
    sandbox.exec = execute
    assert (
        await sandbox_module._read_agent_error(
            sandbox,
            str(tmp_path / "report"),
            sandbox_module.create_agent_error_redactor([]),
            secret_env_names=("PROVIDER_TOKEN",),
        )
        == ""
    )
