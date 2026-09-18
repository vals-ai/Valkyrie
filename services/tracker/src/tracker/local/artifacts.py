"""Transfer validated local agent bundles through the sandbox provider."""

import asyncio
import io
import shlex
import zipfile

from benchmark_service import Sandbox

from tracker.exceptions import SandboxError


async def upload_local_agent_artifacts(sandbox: Sandbox, content: bytes) -> None:
    """Transfer a frozen bundle already validated when admitted to the agent library."""
    archive = await asyncio.to_thread(zipfile.ZipFile, io.BytesIO(content))
    for member in archive.infolist():
        path = f"/bundle/{member.filename.rstrip('/')}"
        quoted_path = shlex.quote(path)
        if member.is_dir():
            command = f"mkdir -p {quoted_path}"
        else:
            data = await asyncio.to_thread(archive.read, member)
            await sandbox.upload_file(path, data)
            if not member.external_attr >> 16 & 0o111:
                continue
            command = f"chmod 755 {quoted_path}"
        result = await sandbox.exec(command)
        if result.exit_code:
            raise SandboxError(f"Failed to prepare local agent file {member.filename!r}")
