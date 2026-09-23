"""Persist synchronous benchmark-service checkpoint callbacks through an async boundary."""

import asyncio
from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any, TypeVar

Result = TypeVar("Result")
CheckpointCallback = Callable[[dict[str, Any]], None]


async def run_with_checkpoints(
    operation: Callable[[CheckpointCallback], Awaitable[Result]],
    persist: Callable[[dict[str, Any]], Awaitable[None]],
) -> Result:
    """Preserve checkpoint order and settle writes before returning a result or cancellation."""
    checkpoints: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

    def received(state: dict[str, Any]) -> None:
        checkpoints.put_nowait(deepcopy(state))

    async def execute() -> Result:
        try:
            return await operation(received)
        finally:
            checkpoints.put_nowait(None)

    async def write() -> None:
        while (state := await checkpoints.get()) is not None:
            await persist(state)

    execution = asyncio.create_task(execute())
    writer = asyncio.create_task(write())

    async def settle() -> None:
        await asyncio.gather(execution, writer, return_exceptions=True)

    try:
        await asyncio.wait((execution, writer), return_when=asyncio.FIRST_COMPLETED)
        await asyncio.shield(writer)

        return await execution
    finally:
        if not execution.done():
            execution.cancel()
        cleanup = asyncio.create_task(settle())
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                continue
        await cleanup
