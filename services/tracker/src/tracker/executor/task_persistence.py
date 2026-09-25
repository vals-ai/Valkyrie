"""Task execution state and ordered writes without database dependencies."""

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from collections.abc import Coroutine
from typing import Any, TypeVar
from uuid import UUID, uuid4

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


class ApiTaskPersistence:
    """Serialize one attempt's mutations and retain its revision across sandbox retries."""

    def __init__(self, api: ExecutorClient, task: TaskState) -> None:
        self._api = api
        self._task = task
        self._revision: int | None = None
        self._revoked = False
        self._lock = asyncio.Lock()
        self._reservation_id: UUID | None = None
        self._task_authority_supported = True

    async def _snapshot(self, *, include_eval_resume_state: bool = True) -> TaskSnapshot | None:
        if self._revoked:
            return None
        try:
            state = await self._api.run_state([self._task.task_id], include_eval_resume_state=include_eval_resume_state)
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
            if self._revoked:
                return False
            if self._task_authority_supported:
                try:
                    authority = await self._api.task_authority(self._task.id, self._task.started_at)
                except httpx.HTTPStatusError as error:
                    if error.response.status_code == 404:
                        # A rolling Tracker update may still serve the earlier v1 API.
                        self._task_authority_supported = False
                    elif error.response.status_code == 409:
                        self._revoked = True
                        return False
                    else:
                        raise
                else:
                    self._revoked = not authority.current
                    return authority.current

            return await self._snapshot(include_eval_resume_state=False) is not None

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

    async def reserve_pool(self) -> bool:
        """Reserve creation without losing a committed revision when the caller is cancelled."""
        async with self._lock:
            return await settle_task_io(self._reserve_pool())

    async def _reserve_pool(self) -> bool:
        if self._revoked or self._revision is None:
            return False
        reserved = await self._api.reserve_pool(
            self._task.id,
            self._task.started_at,
            command_id=uuid4(),
            expected_revision=self._revision,
        )
        if not reserved.reserved:
            return False
        assert reserved.reservation_id is not None and reserved.revision is not None
        self._reservation_id = reserved.reservation_id
        self._revision = reserved.revision

        return True

    async def release_pool(self) -> None:
        async with self._lock:
            await settle_task_io(self._release_pool())

    async def _release_pool(self) -> None:
        if self._reservation_id is None:
            return
        await self._api.release_pool(
            self._task.id,
            self._task.started_at,
            self._reservation_id,
            command_id=uuid4(),
        )
        self._reservation_id = None
