"""Defines `RunSource`, the interface the UI reads runs, tasks and logs through, and `TrackerSource`, which implements it over the async SDK. The fake is in fake.py."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse
from uuid import UUID

from valkyrie.sdk import ValkyrieClient, ValkyrieConfig, ValkyrieSDKError
from valkyrie.sdk.login import LoginSession
from valkyrie.sdk.models import (
    BenchmarkStatus,
    BenchmarkStatusEntry,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchTasksRequest,
    LogEvent,
    SingleTaskResponse,
    TasksResponse,
    TaskStatus,
)

TASK_PAGE_LIMIT = 500
# Errors, then what's running, then what's waiting or done: the order tasks are paged and listed in.
URGENT_TASKS = [TaskStatus.ERROR, TaskStatus.EVALUATING, TaskStatus.IN_PROGRESS, TaskStatus.BUILDING]
OTHER_TASKS = [TaskStatus.PENDING, TaskStatus.STOPPED, TaskStatus.FINISHED]
TRIAGE_ORDER = URGENT_TASKS + OTHER_TASKS
FINAL_STATUSES = frozenset({BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED})


@dataclass(frozen=True)
class Environment:
    """Names the Tracker the UI is pointed at, by its host or as fake data, for the top bar's badge."""

    name: str
    url: str

    @classmethod
    def of(cls, url: str) -> Environment:
        return cls(urlparse(url).netloc or url, url)


class RunSource(Protocol):
    environment: Environment

    async def runs(self, limit: int) -> list[BenchmarkTableRow]: ...

    async def statuses(self, run_ids: Sequence[UUID]) -> list[BenchmarkStatusEntry]: ...

    def watch_run(self, run_id: UUID) -> AsyncIterator[FetchBenchmarkResponse]:
        """Yield the run's state on every change. Ends when the run does, or never."""
        ...

    async def tasks(self, run_id: UUID) -> TasksResponse:
        """Return up to TASK_PAGE_LIMIT tasks, with every error and running task that fits."""
        ...

    async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse: ...

    async def fetch_logs(self, run_id: UUID, task_id: str) -> list[LogEvent]:
        """Return the task's whole log as of now, for tasks that have stopped writing to it."""
        ...

    def stream_logs(self, run_id: UUID, task_id: str) -> AsyncIterator[LogEvent]:
        """Yield the task's log so far, then follow it. May never end."""
        ...

    async def close(self) -> None: ...


class TrackerSource:
    """Reads from a tracker through the async SDK. `from_cli_config()` uses the tracker the CLI is configured for."""

    def __init__(self, client: ValkyrieClient, environment: Environment) -> None:
        self._client = client
        self.environment = environment

    @classmethod
    def from_cli_config(cls) -> TrackerSource:
        return cls.from_config(ValkyrieConfig())

    @classmethod
    def from_config(cls, config: ValkyrieConfig) -> TrackerSource:
        return cls(ValkyrieClient(config), Environment.of(config.url))

    async def runs(self, limit: int) -> list[BenchmarkTableRow]:
        page = await self._client.runs.list(FetchBenchmarksRequest(limit=limit))
        return page.benchmarks

    async def statuses(self, run_ids: Sequence[UUID]) -> list[BenchmarkStatusEntry]:
        response = await self._client.benchmarks.statuses(run_ids)
        return response.entries

    def watch_run(self, run_id: UUID) -> AsyncIterator[FetchBenchmarkResponse]:
        return self._client.runs.stream(run_id)

    async def tasks(self, run_id: UUID) -> TasksResponse:
        # The tracker sorts finished tasks above running ones, so one page of a big run can miss every running task.
        # Sorting the urgent page by status keeps errors first when more than a page of tasks is urgent.
        urgent, rest = await asyncio.gather(
            self._client.benchmarks.tasks(
                run_id, FetchTasksRequest(status=URGENT_TASKS, sort="status", limit=TASK_PAGE_LIMIT)
            ),
            self._client.benchmarks.tasks(run_id, FetchTasksRequest(status=OTHER_TASKS, limit=TASK_PAGE_LIMIT)),
        )
        room = max(TASK_PAGE_LIMIT - len(urgent.tasks), 0)
        return TasksResponse(tasks=urgent.tasks + rest.tasks[:room], total_count=urgent.total_count + rest.total_count)

    async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse:
        try:
            return await self._client.benchmarks.task(run_id, task_id)
        except ValueError as error:
            # The SDK refuses IDs the Tracker's task route can't take, such as `jobshoph/rep1`.
            raise ValkyrieSDKError(f"No detail for this task: {error}") from error

    async def fetch_logs(self, run_id: UUID, task_id: str) -> list[LogEvent]:
        return await self._client.logs.fetch_task(run_id, task_id)

    def stream_logs(self, run_id: UUID, task_id: str) -> AsyncIterator[LogEvent]:
        return self._client.logs.stream_task(run_id, task_id)

    async def close(self) -> None:
        await self._client.close()


class Switcher(Protocol):
    def choices(self) -> list[Environment]: ...

    def open(self, url: str) -> RunSource: ...


class TrackerSwitcher:
    """Offers the Trackers with a saved `valk login`, by URL, so a self-hosted Tracker shows up like a hosted one."""

    def choices(self) -> list[Environment]:
        return [Environment.of(login.tracker_url) for login in LoginSession.saved()]

    def open(self, url: str) -> RunSource:
        # As in the CLI, VALKYRIE_API_KEY, when set, is sent to whichever Tracker this is.
        return TrackerSource.from_config(ValkyrieConfig(url=url))
