"""Access run artifacts without direct storage credentials."""

import asyncio
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import httpx

from valkyrie.sdk.errors import ValkyrieStreamError, ValkyrieTransportError
from valkyrie.sdk.models.artifacts import RunArtifactsResponse, RunArtifactDownloadResponse

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


def _path(value: str, *, allow_empty: bool = False) -> str:
    if not value and allow_empty:
        return value
    if "\\" in value or ":" in value or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("Artifact path must be a relative file or directory path")
    return value


class ArtifactsResource:
    """List and download artifacts scoped to one run."""

    def __init__(self, client: "ValkyrieClient") -> None:
        self._sdk = client

    async def list(
        self, run_id: UUID, *, prefix: str = "", cursor: str | None = None, limit: int = 100
    ) -> RunArtifactsResponse:
        """List one page matching an exact artifact path or directory prefix. limit must be between 1 and 1000."""
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        params: dict[str, str | int] = {"prefix": _path(prefix.rstrip("/"), allow_empty=True), "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        return await self._sdk.request_model(
            "GET", f"/benchmarks/{run_id}/artifacts", RunArtifactsResponse, params=params
        )

    async def download_url(self, run_id: UUID, path: str) -> RunArtifactDownloadResponse:
        """Create a temporary URL for one exact artifact path."""
        return await self._sdk.request_model(
            "GET",
            f"/benchmarks/{run_id}/artifacts/download-url",
            RunArtifactDownloadResponse,
            params={"path": _path(path)},
        )

    async def download(
        self,
        run_id: UUID,
        output_dir: str | Path,
        *,
        path: str = "",
        max_bytes: int = 5 * 1024**3,
        max_entries: int = 100_000,
    ) -> Path:
        """Download a file or directory prefix into a new local directory.

        Paths remain relative to the run. Existing output directories are refused.
        Positive max_bytes and max_entries limits bound the entire transfer.
        """
        path = _path(path.rstrip("/"), allow_empty=True)
        if min(max_bytes, max_entries) <= 0:
            raise ValueError("Artifact download limits must be positive")
        output_dir = Path(output_dir)
        if await asyncio.to_thread(lambda: output_dir.exists() or output_dir.is_symlink()):
            raise FileExistsError(f"Output directory already exists: {output_dir}")
        await asyncio.to_thread(output_dir.parent.mkdir, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=output_dir.parent) as temporary:
            staging = Path(temporary) / "artifacts"
            await asyncio.to_thread(staging.mkdir)
            cursor = None
            cursors: set[str] = set()
            paths: set[str] = set()
            size = 0
            try:
                async with httpx.AsyncClient(timeout=120) as download_client:
                    while True:
                        page = await self.list(run_id, prefix=path, cursor=cursor, limit=1000)
                        for entry in page.artifacts:
                            relative = _path(entry.path)
                            if path and relative != path and not relative.startswith(path + "/"):
                                raise ValueError("Tracker returned an artifact outside the requested path")
                            if relative.casefold() in paths:
                                raise ValueError("Tracker returned a duplicate artifact path")
                            paths.add(relative.casefold())
                            if len(paths) > max_entries or entry.size < 0 or entry.size > max_bytes - size:
                                raise ValueError("Artifacts exceed download limits")
                            url = await self.download_url(run_id, relative)
                            destination = staging / relative
                            await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
                            with destination.open("xb") as output:
                                async with download_client.stream("GET", url.download_url) as response:
                                    response.raise_for_status()
                                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                                        size += len(chunk)
                                        if size > max_bytes:
                                            raise ValueError("Artifacts exceed download limits")
                                        await asyncio.to_thread(output.write, chunk)
                        if page.next_cursor is None:
                            break
                        if page.next_cursor in cursors:
                            raise ValkyrieStreamError("Tracker returned a repeated artifact cursor")
                        cursors.add(page.next_cursor)
                        cursor = page.next_cursor
            except httpx.HTTPError as error:
                raise ValkyrieTransportError("Artifact download failed") from error
            if not paths:
                raise FileNotFoundError("No artifacts found at the requested path")
            if await asyncio.to_thread(lambda: output_dir.exists() or output_dir.is_symlink()):
                raise FileExistsError(f"Output directory already exists: {output_dir}")
            await asyncio.to_thread(staging.rename, output_dir)
        return output_dir
