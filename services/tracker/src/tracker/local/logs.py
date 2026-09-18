"""Persistent local task logs stored as JSON lines per run."""

import asyncio
import fcntl
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ValidationError

from tracker.local.storage import local_path
from tracker.runtime.logs import (
    LogEvent,
    LogPage,
    LogProviderError,
    RunLogReference,
    TaskLogReference,
    task_log_stream_name,
)


def _timestamp(value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


class _LogRecord(BaseModel):
    stream: str
    timestamp: float
    message: str


class FilesystemLogs:
    """Store each run's append-only logs in a local JSONL file."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("Local log root must be absolute")
        self.root = root.resolve()

    def _path(self, benchmark_id: str) -> Path:
        return local_path(self.root, f"{benchmark_id}/logs.jsonl")

    def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        path = self._path(benchmark_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)

    def write(self, stream_key: str, message: str) -> None:
        benchmark_id, separator, stream = stream_key.partition(":")
        if not separator or not benchmark_id or not stream:
            raise LogProviderError("Invalid local log stream key")
        record = _LogRecord(stream=stream, timestamp=datetime.now(UTC).timestamp(), message=message)
        with self._path(benchmark_id).open("ab") as output:
            # Separate executor processes can append to the same run.
            fcntl.flock(output, fcntl.LOCK_EX)
            output.write(record.model_dump_json().encode() + b"\n")
            output.flush()

    def benchmark_location(self, benchmark_id: str) -> str:
        return str(self._path(benchmark_id))

    def task_location(self, benchmark_id: str, task_stream_id: str) -> str:
        return self.benchmark_location(benchmark_id)

    async def fetch(
        self,
        reference: RunLogReference | TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        if not 1 <= limit <= 10_000:
            raise LogProviderError("Local log page limit must be between 1 and 10000")
        try:
            offset = int(cursor) if cursor is not None else 0
            if offset < 0:
                raise ValueError
        except ValueError:
            raise LogProviderError("Invalid local log cursor") from None

        tasks = (reference,) if isinstance(reference, TaskLogReference) else reference.tasks
        task_names = {
            task_log_stream_name(task.task_id, task.started_at).rsplit("_", 1)[0]: task.task_id for task in tasks
        }

        def read() -> LogPage:
            path = self._path(str(reference.run_id))
            if not path.exists():
                return LogPage(events=[])
            events: list[LogEvent] = []
            with path.open("rb") as source:
                source.seek(offset)
                while line := source.readline():
                    # A concurrent writer may not have appended the newline yet.
                    if not line.endswith(b"\n"):
                        break
                    record = _LogRecord.model_validate_json(line)
                    task = record.stream.rsplit("_", 1)[0]
                    if isinstance(reference, TaskLogReference) and task not in task_names:
                        continue
                    if query and query not in record.message:
                        continue
                    if start_time is not None and record.timestamp < _timestamp(start_time):
                        continue
                    if end_time is not None and record.timestamp > _timestamp(end_time):
                        continue
                    if len(events) == limit:
                        return LogPage(events=events, next_cursor=events[-1].event_id)
                    events.append(
                        LogEvent(
                            event_id=str(source.tell()),
                            task_id=task_names.get(task),
                            timestamp=datetime.fromtimestamp(record.timestamp, UTC),
                            message=record.message,
                        )
                    )
            return LogPage(events=events)

        try:
            return await asyncio.to_thread(read)
        except (OSError, ValidationError) as error:
            raise LogProviderError("Failed to read local task logs") from error

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncGenerator[LogEvent]:
        cursor: str | None = None
        while True:
            page = await self.fetch(reference, query=query, start_time=start_time, end_time=end_time, cursor=cursor)
            for event in page.events:
                cursor = event.event_id
                yield event
            if page.next_cursor is not None:
                continue
            if end_time is not None and datetime.now(UTC).timestamp() >= _timestamp(end_time):
                return
            await asyncio.sleep(poll_interval)
