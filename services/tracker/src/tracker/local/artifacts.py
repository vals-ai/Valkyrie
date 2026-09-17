"""Transfer validated local agent bundles through the sandbox provider."""

import asyncio
import io
import shlex
import zipfile

from benchmark_service import Sandbox

from tracker.agent.archive import validate_agent_archive
from tracker.exceptions import SandboxError
from tracker.runtime.lifecycle import finish_cleanup


async def upload_local_agent_artifacts(sandbox: Sandbox, name: str, content: bytes) -> None:
    """Upload files without a signed URL, shared host mount, or package installation."""
    stream = io.BytesIO(content)
    await asyncio.to_thread(validate_agent_archive, stream, name)
    archive = await asyncio.to_thread(zipfile.ZipFile, stream)
    try:
        for member in archive.infolist():
            path = f"/bundle/{member.filename.rstrip('/')}"
            quoted_path = shlex.quote(path)
            if member.is_dir():
                result = await sandbox.exec(f"mkdir -p {quoted_path}")
            else:
                data = await finish_cleanup(asyncio.create_task(asyncio.to_thread(archive.read, member)))
                await sandbox.upload_file(path, data)
                # Preserve executability while discarding special permission bits.
                mode = 0o755 if member.external_attr >> 16 & 0o111 else 0o644
                result = await sandbox.exec(f"chmod {mode:o} {quoted_path}")
            if result.exit_code:
                raise SandboxError(f"Failed to prepare local agent file {member.filename!r}")
    finally:
        archive.close()
