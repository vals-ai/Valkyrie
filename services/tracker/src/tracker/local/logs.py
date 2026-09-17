"""Persistent local task logs, indexed by run, task, and attempt."""

import asyncio
import sqlite3
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

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


class FilesystemLogs:
    """Store each run's append-only logs in a local SQLite database."""

    def __init__(self, root: Path, host_root: Path) -> None:
        if not root.is_absolute() or not host_root.is_absolute():
            raise ValueError("Local log roots must be absolute")
        self.root = root.resolve()
        self.host_root = host_root

    def _path(self, benchmark_id: str) -> Path:
        return local_path(self.root, f"{benchmark_id}/logs.sqlite3")

    @contextmanager
    def _connection(self, path: Path) -> Generator[sqlite3.Connection]:
        connection = sqlite3.connect(path, timeout=30)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        path = self._path(benchmark_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS events ("
                "id INTEGER PRIMARY KEY, stream TEXT NOT NULL, task TEXT NOT NULL, "
                "timestamp REAL NOT NULL, message TEXT NOT NULL)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS events_task ON events(task, id)")

    def write(self, stream_key: str, message: str) -> None:
        benchmark_id, separator, stream = stream_key.partition(":")
        if not separator or not benchmark_id or not stream:
            raise LogProviderError("Invalid local log stream key")
        with self._connection(self._path(benchmark_id)) as connection:
            connection.execute(
                "INSERT INTO events(stream, task, timestamp, message) VALUES (?, ?, ?, ?)",
                (stream, stream.rsplit("_", 1)[0], datetime.now(UTC).timestamp(), message),
            )

    def benchmark_location(self, benchmark_id: str) -> str:
        return str(self.host_root / self._path(benchmark_id).relative_to(self.root))

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
            clauses = ["id > ?"]
            parameters: list[str | int | float] = [offset]
            if isinstance(reference, TaskLogReference):
                clauses.append("task = ?")
                parameters.append(next(iter(task_names)))
            if query:
                clauses.append("instr(message, ?) > 0")
                parameters.append(query)
            if start_time is not None:
                clauses.append("timestamp >= ?")
                parameters.append(_timestamp(start_time))
            if end_time is not None:
                clauses.append("timestamp <= ?")
                parameters.append(_timestamp(end_time))
            parameters.append(limit + 1)
            with self._connection(path) as connection:
                rows = connection.execute(
                    "SELECT id, task, timestamp, message FROM events WHERE "
                    + " AND ".join(clauses)
                    + " ORDER BY id LIMIT ?",
                    parameters,
                ).fetchall()
            events = [
                LogEvent(
                    event_id=str(row[0]),
                    task_id=task_names.get(row[1]),
                    timestamp=datetime.fromtimestamp(row[2], UTC),
                    message=row[3],
                )
                for row in rows[:limit]
            ]
            return LogPage(events=events, next_cursor=str(rows[limit - 1][0]) if len(rows) > limit else None)

        try:
            return await asyncio.to_thread(read)
        except sqlite3.Error as error:
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
