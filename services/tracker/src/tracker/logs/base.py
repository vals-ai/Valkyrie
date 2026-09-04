"""Provider-neutral interfaces for benchmark log access."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


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
class TaskLogReference:
    """Provider-neutral identity for one task's current log stream."""

    run_id: UUID
    task_id: str
    started_at: datetime


@dataclass(frozen=True)
class RunTaskLogReference:
    """Provider-neutral identity for one task in an aggregate log read."""

    task_id: str
    started_at: datetime


@dataclass(frozen=True)
class RunLogReference:
    """Provider-neutral identity for a run and its current task streams."""

    run_id: UUID
    tasks: tuple[RunTaskLogReference, ...] = ()


class LogProvider(ABC):
    """Read and search logs without exposing the backing service to callers."""

    @abstractmethod
    async def fetch_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        """Return one page of a task's logs."""
        raise NotImplementedError

    @abstractmethod
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

    @abstractmethod
    async def fetch_run(
        self,
        reference: RunLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        """Return one page of logs across a run."""
        raise NotImplementedError
