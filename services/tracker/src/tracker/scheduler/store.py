"""PostgreSQL-backed sandbox queue policy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from types import TracebackType
from typing import cast
from uuid import UUID

from sqlalchemy import JSON, case, func, text, type_coerce
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, select, update

from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus

_ACTIVE_TASK_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)


@dataclass(frozen=True, slots=True)
class ScheduledTask:
    task_row_id: UUID
    task_id: str
    benchmark_id: UUID
    started_at: datetime
    priority: int


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


def queue_pool_id(provider_pool_id: str) -> str:
    """Return the stable non-secret identifier persisted for a provider pool."""
    return f"pool_{sha256(provider_pool_id.encode()).hexdigest()[:24]}"


def next_eligible_task(session: Session, pool_id: str) -> ScheduledTask | None:
    """Select the global queue head whose run has current concurrency capacity."""
    active_task = aliased(Task)
    active_count = (
        select(func.count(col(active_task.id)))
        .where(col(active_task.benchmark) == col(Benchmark.id))
        .where(col(active_task.status).in_(_ACTIVE_TASK_STATUSES))
        .correlate(Benchmark)
        .scalar_subquery()
    )
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    concurrency = arguments["concurrency"].as_integer()
    queued_pool = arguments["queue_pool_id"].as_string()

    row = cast(
        tuple[Task, int] | None,
        session.exec(
            select(Task, priority.label("priority"))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.status) == TaskStatus.PENDING)
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
            .where(queued_pool == pool_id)
            .where(priority.between(0, 4))
            .where(active_count < concurrency)
            .order_by(priority, col(Task.started_at), col(Task.id))
            .limit(1)
        ).first(),
    )
    if row is None:
        return None

    task, selected_priority = row

    return ScheduledTask(
        task_row_id=task.id,
        task_id=task.task_id,
        benchmark_id=task.benchmark,
        started_at=task.started_at,
        priority=selected_priority,
    )


def _eligible_task_id(pool_id: str):
    active_task = aliased(Task)
    active_count = (
        select(func.count(col(active_task.id)))
        .where(col(active_task.benchmark) == col(Benchmark.id))
        .where(col(active_task.status).in_(_ACTIVE_TASK_STATUSES))
        .correlate(Benchmark)
        .scalar_subquery()
    )
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    concurrency = arguments["concurrency"].as_integer()

    return (
        select(col(Task.id))
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        .where(arguments["queue_pool_id"].as_string() == pool_id)
        .where(priority.between(0, 4))
        .where(active_count < concurrency)
        .order_by(priority, col(Task.started_at), col(Task.id))
        .limit(1)
        .scalar_subquery()
    )


def eligible_task_is(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    """Return whether this exact attempt is the current global eligible head."""
    return (
        session.exec(
            select(col(Task.id))
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.status) == TaskStatus.PENDING)
            .where(col(Task.id) == _eligible_task_id(pool_id))
        ).first()
        is not None
    )


def claim_eligible_task(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    """Atomically claim this attempt only when it is the global eligible head."""
    result = session.exec(
        update(Task)
        .where(col(Task.id) == task_row_id)
        .where(col(Task.started_at) == expected_started_at)
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(col(Task.id) == _eligible_task_id(pool_id))
        .values(status=TaskStatus.BUILDING)
    )
    session.commit()

    return result.rowcount == 1


def reset_abandoned_builds(session: Session, pool_id: str, now: datetime) -> int:
    """Return abandoned sandbox builds in one provider pool to the queue."""
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    queued_benchmarks = select(col(Benchmark.id)).where(
        col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
        arguments["queue_pool_id"].as_string() == pool_id,
    )
    result = session.exec(
        update(Task)
        .where(col(Task.status) == TaskStatus.BUILDING)
        .where(col(Task.benchmark).in_(queued_benchmarks))
        .values(
            status=TaskStatus.PENDING,
            started_at=case(
                (col(Task.started_at) >= now, col(Task.started_at) + timedelta(microseconds=1)),
                else_=now,
            ),
        )
    )

    return result.rowcount
