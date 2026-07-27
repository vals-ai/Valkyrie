"""PostgreSQL-backed provider admission for queued sandbox creation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from types import TracebackType
from typing import cast
from uuid import UUID

from benchmark_service import Resources, Sandbox, SandboxProvider, SandboxSource
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session, col, select, update

from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.scheduler.store import next_eligible_task, queue_pool_id, reset_abandoned_builds

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]


class PostgresPoolLock:
    """A nonblocking session advisory lock held on one dedicated connection."""

    def __init__(self, engine: Engine, pool_id: str) -> None:
        self._engine = engine
        self._lock_key = int.from_bytes(sha256(pool_id.encode()).digest()[:8], byteorder="big", signed=True)
        self._connection: Connection | None = None

    async def __aenter__(self) -> bool:
        acquire_task = asyncio.create_task(asyncio.to_thread(self._try_acquire))
        try:
            return await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            acquired = await acquire_task
            if acquired:
                await asyncio.to_thread(self._release)
            raise

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._connection is None:
            return

        release_task = asyncio.create_task(asyncio.to_thread(self._release))
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            await release_task
            raise

    def _try_acquire(self) -> bool:
        connection = self._engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        try:
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": self._lock_key},
                ).scalar_one()
            )
            if acquired:
                self._connection = connection

                return True
        except BaseException:
            connection.close()
            raise

        connection.close()

        return False

    def _release(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return

        try:
            unlocked = bool(
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": self._lock_key},
                ).scalar_one()
            )
            if not unlocked:
                raise RuntimeError("PostgreSQL advisory lock ownership was lost before release")
        except BaseException:
            connection.invalidate()
            raise
        finally:
            connection.close()


@dataclass(frozen=True, slots=True)
class SandboxQueueContext:
    provider: SandboxProvider = field(repr=False)
    pool_id: str
    engine: Engine = field(repr=False)
    poll_interval_seconds: float = 1.0


def create_queue_context(
    *,
    engine: Engine,
    provider: SandboxProvider,
    poll_interval_seconds: float = 1.0,
) -> SandboxQueueContext:
    provider_pool_id = provider.admission_pool_id
    if provider_pool_id is None:
        raise ValueError("Sandbox provider does not support queued admission")

    return SandboxQueueContext(
        provider=provider,
        pool_id=queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=float(poll_interval_seconds),
    )


def _queued_task_state(
    session: Session,
    task_row_id: UUID,
) -> tuple[TaskStatus, BenchmarkStatus] | None:
    raw_state = cast(
        tuple[str, str] | None,
        session.exec(
            select(col(Task.status), col(Benchmark.status))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.id) == task_row_id)
        ).first(),
    )
    if raw_state is None:
        return None

    task_status, benchmark_status = raw_state

    return TaskStatus(task_status), BenchmarkStatus(benchmark_status)


def _transition_queued_task(
    session: Session,
    *,
    task_row_id: UUID,
    started_at: datetime,
    from_status: TaskStatus,
    to_status: TaskStatus,
) -> bool:
    active_benchmarks = select(col(Benchmark.id)).where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
    result = session.exec(
        update(Task)
        .where(col(Task.id) == task_row_id)
        .where(col(Task.status) == from_status)
        .where(col(Task.started_at) == started_at)
        .where(col(Task.benchmark).in_(active_benchmarks))
        .values(status=to_status)
    )
    session.commit()

    return result.rowcount == 1


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    source: SandboxSource,
    resources: Resources,
    create: SandboxFactory,
) -> Sandbox | None:
    """Wait for this task's global turn and enter its sandbox cleanup context."""
    while True:
        async with PostgresPoolLock(context.engine, context.pool_id) as acquired:
            if acquired:
                with Session(context.engine) as session:
                    reset_abandoned_builds(session, context.pool_id, datetime.now(UTC))
                    session.commit()
                    scheduled = next_eligible_task(session, context.pool_id)
                    current_state = _queued_task_state(session, task_row_id)

                if current_state != (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS):
                    return None

                if scheduled is not None and scheduled.task_row_id == task_row_id:
                    has_capacity = await context.provider.check_admission(source, resources)
                    if has_capacity:
                        with Session(context.engine) as session:
                            scheduled = next_eligible_task(session, context.pool_id)
                            current_state = _queued_task_state(session, task_row_id)
                            claimed = (
                                current_state == (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS)
                                and scheduled is not None
                                and scheduled.task_row_id == task_row_id
                                and _transition_queued_task(
                                    session,
                                    task_row_id=task_row_id,
                                    started_at=scheduled.started_at,
                                    from_status=TaskStatus.PENDING,
                                    to_status=TaskStatus.BUILDING,
                                )
                            )

                        if current_state != (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS):
                            return None

                        if claimed and scheduled is not None:
                            sandbox = await stack.enter_async_context(create())
                            with Session(context.engine) as session:
                                started = _transition_queued_task(
                                    session,
                                    task_row_id=task_row_id,
                                    started_at=scheduled.started_at,
                                    from_status=TaskStatus.BUILDING,
                                    to_status=TaskStatus.IN_PROGRESS,
                                )
                            if not started:
                                return None

                            return sandbox

        await asyncio.sleep(context.poll_interval_seconds)
