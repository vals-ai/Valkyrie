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
    """Persist retained snapshots in order and settle writes before returning or propagating cancellation."""
    checkpoint_available = asyncio.Event()
    operation_finished = asyncio.Event()
    pending_checkpoint: dict[str, Any] | None = None

    def received(state: dict[str, Any]) -> None:
        nonlocal pending_checkpoint
        pending_checkpoint = deepcopy(state)
        checkpoint_available.set()

    async def execute() -> Result:
        try:
            return await operation(received)
        finally:
            operation_finished.set()
            checkpoint_available.set()

    async def write() -> None:
        nonlocal pending_checkpoint
        while True:
            await checkpoint_available.wait()
            checkpoint_available.clear()
            state = pending_checkpoint
            pending_checkpoint = None
            if state is None:
                if operation_finished.is_set():
                    return
                continue
            await persist(state)
            if operation_finished.is_set() and pending_checkpoint is None:
                return

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
