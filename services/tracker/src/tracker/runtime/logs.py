"""Provider-neutral benchmark log capabilities."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
import hashlib
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID


class BenchmarkLogSink(Protocol):
    """Create benchmark log destinations and write task log messages."""

    async def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        """Ensure a benchmark log destination exists."""
        raise NotImplementedError

    def write(self, stream_key: str, message: str) -> None:
        """Write one message to the task stream identified by ``stream_key``."""
        raise NotImplementedError


class BenchmarkLogLocations(Protocol):
    """Provider-native locations for benchmark and task logs."""

    def benchmark_location(self, benchmark_id: str) -> str:
        raise NotImplementedError

    def task_location(self, benchmark_id: str, task_stream_id: str) -> str:
        raise NotImplementedError


class LogProviderError(Exception):
    """A log provider could not complete an operation."""


@dataclass(frozen=True)
class LogEvent:
    """One log message returned by a provider."""

    timestamp: datetime
    message: str
    task_id: str | None = None
    ingestion_time: datetime | None = None
    event_id: str | None = None


@dataclass(frozen=True)
class LogPage:
    """A bounded page of log messages and its continuation cursor."""

    events: list[LogEvent]
    next_cursor: str | None = None


@dataclass(frozen=True)
class RunTaskLogReference:
    """Provider-neutral identity for one task in an aggregate log read."""

    task_id: str
    started_at: datetime


@dataclass(frozen=True)
class TaskLogReference:
    """Provider-neutral identity for one task's current log stream."""

    run_id: UUID
    task_id: str
    started_at: datetime
    siblings: tuple[RunTaskLogReference, ...] | None = None


@dataclass(frozen=True)
class RunLogReference:
    """Provider-neutral identity for a run and its current task streams."""

    run_id: UUID
    tasks: tuple[RunTaskLogReference, ...] = ()


class LogProvider(Protocol):
    """Read and search logs without exposing the backing service to callers."""

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
        """Return one page of logs for a run or one task."""
        raise NotImplementedError

    def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        """Yield existing and newly-arriving task logs."""
        raise NotImplementedError


def sanitize_log_stream_name(task_id: str) -> str:
    """Escape rejected characters and distinguish the result from legacy names."""
    escaped = task_id.replace("%", "%25").replace(":", "%3A").replace("*", "%2A")
    if escaped == task_id:
        return task_id
    digest = hashlib.sha256(task_id.encode()).hexdigest()[:16]
    return f"{escaped}-{digest}"


def task_log_stream_name(task_id: str, started_at: datetime) -> str:
    """Return the canonical versioned log stream for a task attempt."""
    if started_at.utcoffset() is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    suffix = f"{int(started_at.timestamp() * 1_000_000):x}"
    return f"{sanitize_log_stream_name(task_id)}_{suffix}"
