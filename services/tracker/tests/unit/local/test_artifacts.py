"""Local bundle transfer preserves file bytes and executable permissions.

Run: pytest tests/unit/local/test_artifacts.py
"""

import io
import zipfile
from typing import cast
from unittest.mock import AsyncMock, Mock, call

import pytest
from benchmark_service import ExecResult, Sandbox

from tracker.exceptions import SandboxError
from tracker.local.artifacts import upload_local_agent_artifacts


_BINARY_CONTENT = b"\x00binary\xff"


def _bundle() -> bytes:
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as archive:
        archive.writestr("local agent/empty/", "")
        archive.writestr("local agent/input.bin", _BINARY_CONTENT)
        executable = zipfile.ZipInfo("local agent/run.sh")
        executable.external_attr = 0o100755 << 16
        archive.writestr(executable, "#!/bin/sh\nexit 0\n")

    return content.getvalue()


class TestLocalBundleTransfer:
    """Exercise the real ZIP transfer helper with sandbox I/O replaced."""

    async def test_binary_files_directories_and_executable_modes(self) -> None:
        sandbox = Mock(spec=Sandbox)
        sandbox.upload_file = AsyncMock()
        sandbox.exec = AsyncMock(return_value=ExecResult(exit_code=0, output=""))

        await upload_local_agent_artifacts(cast(Sandbox, sandbox), _bundle())

        assert sandbox.upload_file.await_args_list == [
            call("/bundle/local agent/input.bin", _BINARY_CONTENT),
            call("/bundle/local agent/run.sh", b"#!/bin/sh\nexit 0\n"),
        ]
        assert sandbox.exec.await_args_list == [
            call("mkdir -p '/bundle/local agent/empty'"),
            call("chmod 755 '/bundle/local agent/run.sh'"),
        ]

    @pytest.mark.parametrize("failed_operation", ["mkdir", "chmod"])
    async def test_failed_file_preparation_rejects_bundle(self, failed_operation: str) -> None:
        async def execute(command: str) -> ExecResult:
            return ExecResult(exit_code=int(command.startswith(failed_operation)), output="")

        sandbox = Mock(spec=Sandbox)
        sandbox.upload_file = AsyncMock()
        sandbox.exec = AsyncMock(side_effect=execute)

        with pytest.raises(SandboxError, match="Failed to prepare local agent file"):
            await upload_local_agent_artifacts(cast(Sandbox, sandbox), _bundle())

        if failed_operation == "mkdir":
            sandbox.upload_file.assert_not_awaited()
