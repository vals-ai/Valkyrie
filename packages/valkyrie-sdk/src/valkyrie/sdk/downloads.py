"""Bounded transfers from signed HTTP URLs or explicitly configured local storage."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import BinaryIO
from urllib.parse import unquote, urlsplit

import httpx

from valkyrie.sdk.config import ValkyrieConfig


async def download_chunks(client: httpx.AsyncClient, url: str, config: ValkyrieConfig) -> AsyncIterator[bytes]:
    """Read local files only within the explicitly configured artifact directory."""
    parsed = urlsplit(url)
    if parsed.scheme != "file":
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Unsupported artifact download URL scheme")
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                yield chunk
        return

    if config.execution_environment != "local" or config.local_data_root is None:
        raise ValueError("File downloads require explicitly configured local storage")
    if parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("Local artifact URL must contain only an absolute file path")

    configured_root = config.local_data_root

    def open_file() -> BinaryIO:
        root = configured_root.resolve()
        path = Path(unquote(parsed.path))
        if not path.is_absolute() or not path.resolve().is_relative_to(root):
            raise ValueError("Local artifact URL escapes the configured artifact directory")
        return path.resolve().open("rb")

    stream = await asyncio.to_thread(open_file)
    try:
        while chunk := await asyncio.to_thread(stream.read, 1024 * 1024):
            yield chunk
    finally:
        await asyncio.to_thread(stream.close)
