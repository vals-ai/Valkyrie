"""Drain asynchronous cleanup before propagating caller cancellation."""

from asyncio import CancelledError, Task, shield
from typing import TypeVar

T = TypeVar("T")


async def finish_cleanup(task: Task[T]) -> T:
    """Wait for cleanup, including repeated cancellation of the waiting task."""
    cancelled = False
    while not task.done():
        try:
            await shield(task)
        except CancelledError:
            cancelled = True

    result = task.result()
    if cancelled:
        raise CancelledError
    return result
