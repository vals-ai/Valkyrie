"""Bounded transfers from signed HTTP URLs or explicitly configured local storage."""

import asyncio
from collections.abc import AsyncIterator, Coroutine
from pathlib import Path
from contextlib import ExitStack
from typing import Any, BinaryIO, TypeVar
from urllib.parse import unquote, urlsplit

import httpx

from valkyrie.sdk.config import ValkyrieConfig


T = TypeVar("T")


async def _io(operation: Coroutine[Any, Any, T]) -> T:
    task = asyncio.create_task(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


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
    stack = ExitStack()

    def open_file() -> BinaryIO:
        root = configured_root.resolve()
        path = Path(unquote(parsed.path))
        if not path.is_absolute() or not path.resolve().is_relative_to(root):
            raise ValueError("Local artifact URL escapes the configured artifact directory")
        return stack.enter_context(path.resolve().open("rb"))

    try:
        stream = await _io(asyncio.to_thread(open_file))
        while chunk := await _io(asyncio.to_thread(stream.read, 1024 * 1024)):
            yield chunk
    finally:
        await _io(asyncio.to_thread(stack.close))
