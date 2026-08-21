"""Shared helpers for downloading CLI artifacts."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import TypeVar

from tracker.exceptions import S3Error

DOWNLOAD_CONCURRENCY = 8

Item = TypeVar("Item")


async def gather_in_batches(items: Sequence[Item], worker: Callable[[Item], Awaitable[None]]) -> None:
    """Run a download worker in bounded batches and drain siblings after failure."""

    async def run_worker(item: Item) -> None:
        await worker(item)

    for start in range(0, len(items), DOWNLOAD_CONCURRENCY):
        batch = items[start : start + DOWNLOAD_CONCURRENCY]
        tasks = [asyncio.create_task(run_worker(item)) for item in batch]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise


def resolve_download_destination(key: str, prefix: str, output_dir: Path) -> Path:
    """Map a storage key under `prefix` to a safe local destination."""
    relative = key.removeprefix(prefix).lstrip("/")
    destination = (output_dir / relative if relative else output_dir / Path(key).name).resolve()
    if not destination.is_relative_to(output_dir):
        raise S3Error(f"Requested path is not relative to the output directory '{key}'")
    destination.parent.mkdir(parents=True, exist_ok=True)

    return destination
