"""Drain asynchronous cleanup before propagating caller cancellation."""

from asyncio import CancelledError, Task, shield


async def finish_cleanup(task: Task[None]) -> None:
    """Wait for cleanup, including repeated cancellation of the waiting task."""
    cancelled = False
    while not task.done():
        try:
            await shield(task)
        except CancelledError:
            cancelled = True

    task.result()
    if cancelled:
        raise CancelledError
