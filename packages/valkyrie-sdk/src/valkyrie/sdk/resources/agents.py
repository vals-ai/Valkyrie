"""Tracker-backed agent library management."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, BinaryIO

import httpx

from valkyrie.sdk.agent_bundle import extract_agent_archive, get_agent_zip_stream, read_agent_name, validate_agent_name
from valkyrie.sdk.agent_install import checkout_agent
from valkyrie.sdk.errors import ValkyrieTransportError
from valkyrie.sdk.models import AgentDownloadURLResponse, AgentEntry, AgentsResponse

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


async def _file_chunks(stream: BinaryIO) -> AsyncIterator[bytes]:
    while chunk := await asyncio.to_thread(stream.read, 1024 * 1024):
        yield chunk


class AgentsResource:
    """Async operations for the configured deployment's shared agent library."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def list(self) -> AgentsResponse:
        """List uploaded agents in the configured shared library."""
        return await self._sdk.request_model("GET", "/agents", AgentsResponse)

    async def download_url(self, name: str) -> AgentDownloadURLResponse:
        """Create a temporary download URL for an uploaded agent."""
        validate_agent_name(name)

        return await self._sdk.request_model(
            "GET",
            f"/agents/{name}/download-url",
            AgentDownloadURLResponse,
        )

    async def push(self, agent_path: str | Path, *, name: str | None = None) -> AgentEntry:
        """Bundle a directory containing contract.yaml or contract.yml and upload it through Tracker.

        name defaults to the contract name and replaces that library alias if it exists.
        """
        path = Path(agent_path)
        contract_name = await asyncio.to_thread(read_agent_name, path)
        agent_name = validate_agent_name(name) if name is not None else contract_name
        bundle = get_agent_zip_stream(agent_name, path)
        stream = await asyncio.to_thread(bundle.__enter__)
        try:
            size = await asyncio.to_thread(stream.seek, 0, 2)
            await asyncio.to_thread(stream.seek, 0)

            return await self._sdk.request_model(
                "PUT",
                f"/agents/{agent_name}",
                AgentEntry,
                content=_file_chunks(stream),
                headers={"Content-Type": "application/zip", "Content-Length": str(size)},
            )
        finally:
            await asyncio.to_thread(bundle.__exit__, None, None, None)

    async def download(
        self,
        name: str,
        output_dir: str | Path | None = None,
        *,
        overwrite: bool = False,
    ) -> Path:
        """Download and safely extract an agent into output_dir/name.

        output_dir defaults to the current directory; overwrite replaces an existing agent directory
        only after archive validation succeeds.
        """
        validate_agent_name(name)
        directory = Path(output_dir) if output_dir is not None else Path.cwd()
        target = directory / name
        if target.is_symlink() or (target.exists() and (not overwrite or not target.is_dir())):
            raise FileExistsError(f"Target already exists: {target}; use overwrite for an existing directory")
        response = await self.download_url(name)
        try:
            # A separate client must never inherit Tracker credentials for presigned transfers.
            async with httpx.AsyncClient(timeout=120) as client:
                with tempfile.TemporaryFile() as stream:
                    async with client.stream("GET", response.download_url) as download:
                        download.raise_for_status()
                        async for chunk in download.aiter_bytes():
                            await asyncio.to_thread(stream.write, chunk)
                    await asyncio.to_thread(stream.seek, 0)

                    return await asyncio.to_thread(
                        extract_agent_archive,
                        stream,
                        name,
                        directory,
                        overwrite=overwrite,
                    )
        except httpx.HTTPError as error:
            raise ValkyrieTransportError("Agent archive download failed") from error

    async def remove(self, name: str) -> AgentEntry:
        """Remove an uploaded alias; missing agents return a 404 API error."""
        validate_agent_name(name)

        return await self._sdk.request_model("DELETE", f"/agents/{name}", AgentEntry)

    async def install(self, github_url: str, *, name: str | None = None) -> AgentEntry:
        """Use local Git to clone an HTTPS GitHub repository or /tree/branch/subfolder URL and push it.

        name defaults to the contract name and replaces that library alias if it exists.
        """
        if name is not None:
            validate_agent_name(name)
        async with checkout_agent(github_url) as agent_path:
            return await self.push(agent_path, name=name)
