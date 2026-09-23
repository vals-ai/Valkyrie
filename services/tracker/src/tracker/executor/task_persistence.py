"""Task execution state and ordered writes without database dependencies."""

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from collections.abc import Coroutine
from typing import Any, Protocol, TypeVar
from uuid import uuid4

import httpx

from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.schemas import RunStatus, TaskState, TaskStatus
from tracker.executor_api.v1.task_schemas import Mutation, PendingTask

Result = TypeVar("Result")


async def settle_task_io(operation: Coroutine[Any, Any, Result]) -> Result:
    """Do not release persistence locks while a cancelled caller's write is still running."""
    task = asyncio.create_task(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()
        raise


def attempt_time(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass(frozen=True)
class TaskSnapshot:
    task: TaskState
    run_status: RunStatus
    identity: dict[str, str]


class TaskPersistence(Protocol):
    async def load(self) -> TaskSnapshot | None:
        """Load only this dispatch's original task attempt."""
        raise NotImplementedError

    async def current(self) -> bool:
        raise NotImplementedError

    async def write(self, mutation: Mutation) -> bool:
        """Persist a replay-safe mutation, or return false after task authority is lost."""
        raise NotImplementedError

    async def resume(self) -> dict[str, Any] | None:
        """Acquire evaluation exclusivity and return the durable checkpoint."""
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class ApiTaskPersistence:
    """Serialize one attempt's mutations and retain its revision across sandbox retries."""

    def __init__(self, api: ExecutorClient, task: TaskState) -> None:
        self._api = api
        self._task = task
        self._revision: int | None = None
        self._revoked = False
        self._lock = asyncio.Lock()

    async def _snapshot(self) -> TaskSnapshot | None:
        if self._revoked:
            return None
        try:
            state = await self._api.run_state([self._task.task_id], include_eval_resume_state=True)
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 409:
                raise
            self._revoked = True
            return None
        task = state.tasks[0] if len(state.tasks) == 1 else None
        if (
            not state.current
            or task is None
            or task.id != self._task.id
            or attempt_time(task.started_at) != attempt_time(self._task.started_at)
            or task.status == TaskStatus.STOPPED
        ):
            self._revoked = True
            return None
        identity = {"benchmark_name": state.run.benchmark_name, "agent_name": state.run.agent_name}
        if state.run.started_by_email:
            identity["email"] = state.run.started_by_email

        return TaskSnapshot(task, state.run.status, identity)

    async def load(self) -> TaskSnapshot | None:
        async with self._lock:
            return await settle_task_io(self._load())

    async def _load(self) -> TaskSnapshot | None:
        snapshot = await self._snapshot()
        if snapshot is None:
            return None
        if self._revision is None:
            if snapshot.run_status != RunStatus.IN_PROGRESS:
                return None
            try:
                claimed = await self._api.claim_task(self._task.id, self._task.started_at, command_id=uuid4())
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 409:
                    raise
                self._revoked = True
                return None
            self._revision = claimed.revision
        elif snapshot.task.status in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS) or (
            snapshot.task.status == TaskStatus.EVALUATING and snapshot.task.eval_resume_state is None
        ):
            # A fresh sandbox retry must return to pending before requesting a new build.
            if not await self._write(PendingTask()):
                return None
            snapshot = replace(snapshot, task=snapshot.task.model_copy(update={"status": TaskStatus.PENDING}))

        return snapshot

    async def current(self) -> bool:
        async with self._lock:
            return await self._snapshot() is not None

    async def write(self, mutation: Mutation) -> bool:
        async with self._lock:
            return await settle_task_io(self._write(mutation))

    async def _write(self, mutation: Mutation) -> bool:
        if self._revoked or self._revision is None:
            return False
        try:
            written = await self._api.write_task(
                self._task.id,
                self._task.started_at,
                mutation,
                command_id=uuid4(),
                expected_revision=self._revision,
            )
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 409:
                raise
            self._revoked = True
            return False
        self._revision = written.revision

        return True

    async def resume(self) -> dict[str, Any] | None:
        snapshot = await self.load()
        if snapshot is None or snapshot.task.status != TaskStatus.EVALUATING:
            return None

        return snapshot.task.eval_resume_state

    async def close(self) -> None:
        """The dispatch claim protects API evaluation; no database lock is retained."""
