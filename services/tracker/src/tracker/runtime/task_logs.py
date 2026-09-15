"""Buffer task output and finish pending writes before task cleanup."""

import asyncio
import time
from contextlib import suppress

from tracker.logging import get_logger
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.logs import BenchmarkLogSink

logger = get_logger(__name__)


class TaskLogBuffer:
    """Batch output, flush idle streams, and drain writes on close."""

    def __init__(self, sink: BenchmarkLogSink, stream_key: str) -> None:
        self._sink = sink
        self._stream_key = stream_key
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=20)
        self._pending_writes: set[asyncio.Future[None]] = set()
        self._flush_task = asyncio.create_task(self._auto_flush())
        self._close_task: asyncio.Task[None] | None = None
        self.last_log_time = time.monotonic()

    def write(self, data: str) -> None:
        self.last_log_time = time.monotonic()
        self._queue.put_nowait(data)
        self.buffer_logs()

    def buffer_logs(self, *, force_flush: bool = False) -> None:
        if not self._queue.full() and not force_flush:
            return

        messages: list[str] = []
        while not self._queue.empty():
            messages.append(self._queue.get_nowait())
        message = "".join(messages)
        if not message:
            return

        future = asyncio.get_running_loop().run_in_executor(None, self._sink.write, self._stream_key, message)
        self._pending_writes.add(future)
        future.add_done_callback(self._write_finished)

    def _write_finished(self, completed: asyncio.Future[None]) -> None:
        self._pending_writes.discard(completed)
        if not completed.cancelled() and (error := completed.exception()) is not None:
            logger.error("Task log write failed", exc_info=(type(error), error, error.__traceback__))

    async def _auto_flush(self) -> None:
        while True:
            await asyncio.sleep(1)
            if time.monotonic() - self.last_log_time >= 10:
                self.buffer_logs(force_flush=True)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await finish_cleanup(self._close_task)

    async def _close(self) -> None:
        self._flush_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._flush_task

        self.buffer_logs(force_flush=True)
        if self._pending_writes:
            await asyncio.gather(*self._pending_writes, return_exceptions=True)
