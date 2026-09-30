"""PostgreSQL-backed sandbox queue policy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from types import TracebackType
from uuid import UUID

from sqlalchemy import JSON, case, func, text, type_coerce
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, delete, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)

_ACTIVE_TASK_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
_TASK_EVALUATION_LOCK_SCOPE = "task-evaluation"


@dataclass(frozen=True)
class ReservationResourceTotals:
    vcpu: int
    memory: int
    disk: int
    gpu: int


def _advisory_lock_key(resource_id: str) -> int:
    return int.from_bytes(
        sha256(resource_id.encode()).digest()[:8],
        byteorder="big",
        signed=True,
    )


def _task_evaluation_lock_resource_id(task_row_id: UUID) -> str:
    return f"{_TASK_EVALUATION_LOCK_SCOPE}:{task_row_id}"


class PostgresAdvisoryLock:
    """A nonblocking session advisory lock held on one dedicated connection."""

    def __init__(self, engine: Engine, *, resource_id: str) -> None:
        self._engine = engine
        self._lock_key = _advisory_lock_key(resource_id)
        self._connection: Connection | None = None

    @property
    def connection(self) -> Connection:
        assert self._connection is not None
        return self._connection

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
        connection = self._engine.connect()
        try:
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": self._lock_key},
                ).scalar_one()
            )
            connection.commit()
            if acquired:
                self._connection = connection

                return True
        except BaseException:
            connection.close()
            raise

        connection.close()

        return False

    def _release(self) -> None:
        connection = self.connection
        self._connection = None
        try:
            unlocked = bool(
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": self._lock_key},
                ).scalar_one()
            )
            if not unlocked:
                raise RuntimeError("PostgreSQL advisory lock ownership was lost before release")
            connection.commit()
        except BaseException:
            connection.invalidate()
            raise
        finally:
            connection.close()


def queue_pool_lock(engine: Engine, pool_id: str) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=pool_id)


def task_evaluation_lock(engine: Engine, task_row_id: UUID) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=_task_evaluation_lock_resource_id(task_row_id))


def try_task_evaluation_transaction_lock(session: Session, task_row_id: UUID) -> bool:
    """Fence a task evaluation until the caller's current transaction ends."""
    return bool(
        session.connection()
        .execute(
            text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
            {"lock_key": _advisory_lock_key(_task_evaluation_lock_resource_id(task_row_id))},
        )
        .scalar_one()
    )


def queue_pool_id(provider_pool_id: str) -> str:
    """Return the stable non-secret identifier persisted for a provider pool."""
    return f"pool_{sha256(provider_pool_id.encode()).hexdigest()[:24]}"


def _active_task_count():
    active_task = aliased(Task)
    return (
        select(func.count(col(active_task.id)))
        .where(col(active_task.benchmark) == col(Benchmark.id))
        .where(col(active_task.status).in_(_ACTIVE_TASK_STATUSES))
        .correlate(Benchmark)
        .scalar_subquery()
    )


def _eligible_task_id(pool_id: str):
    active_count = _active_task_count()
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    concurrency = arguments["concurrency"].as_integer()
    reserved_task = (
        select(col(SandboxBuildReservation.task_row_id))
        .where(col(SandboxBuildReservation.task_row_id) == col(Task.id))
        .exists()
    )

    return (
        select(col(Task.id))
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(~reserved_task)
        .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        .where(arguments["queue_pool_id"].as_string() == pool_id)
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

    return result.rowcount == 1


def building_task_count(session: Session, pool_id: str) -> int:
    """Return the hard count of BUILDING tasks assigned to one provider pool."""
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    return int(
        session.exec(
            select(func.count(col(Task.id)))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(arguments["queue_pool_id"].as_string() == pool_id)
        ).one()
    )


def active_reservation_resources(session: Session, pool_id: str) -> ReservationResourceTotals:
    """Sum all resources reserved in one provider pool."""
    row = session.exec(
        select(
            func.coalesce(func.sum(col(SandboxBuildReservation.requested_vcpu)), 0),
            func.coalesce(func.sum(col(SandboxBuildReservation.requested_memory)), 0),
            func.coalesce(func.sum(col(SandboxBuildReservation.requested_disk)), 0),
            func.coalesce(func.sum(col(SandboxBuildReservation.requested_gpu)), 0),
        ).where(col(SandboxBuildReservation.pool_id) == pool_id)
    ).one()
    return ReservationResourceTotals(
        vcpu=int(row[0]),
        memory=int(row[1]),
        disk=int(row[2]),
        gpu=int(row[3]),
    )


def claim_eligible_task_with_reservation(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
    *,
    requested_vcpu: int,
    requested_memory: int,
    requested_disk: int,
    requested_gpu: int,
) -> bool:
    """Claim the exact FIFO head and insert its reservation in the caller transaction."""
    if not claim_eligible_task(session, pool_id, task_row_id, expected_started_at):
        return False

    session.add(
        SandboxBuildReservation(
            task_row_id=task_row_id,
            attempt_started_at=expected_started_at,
            pool_id=pool_id,
            requested_vcpu=requested_vcpu,
            requested_memory=requested_memory,
            requested_disk=requested_disk,
            requested_gpu=requested_gpu,
        )
    )
    session.flush()
    return True


def delete_build_reservation(
    session: Session,
    *,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    """Delete one exact reservation after its sandbox cleanup is confirmed."""
    result = session.exec(
        delete(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
        .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
    )
    return result.rowcount == 1


def promote_reserved_task(
    session: Session,
    *,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    """Promote one exact reserved build and release its capacity."""
    active_benchmarks = select(col(Benchmark.id)).where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
    exact_reservation = (
        select(col(SandboxBuildReservation.task_row_id))
        .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
        .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
        .exists()
    )
    promoted = session.exec(
        update(Task)
        .where(col(Task.id) == task_row_id)
        .where(col(Task.started_at) == expected_started_at)
        .where(col(Task.status) == TaskStatus.BUILDING)
        .where(col(Task.benchmark).in_(active_benchmarks))
        .where(exact_reservation)
        .values(status=TaskStatus.IN_PROGRESS)
    ).rowcount
    if promoted != 1:
        return False

    if not delete_build_reservation(
        session,
        task_row_id=task_row_id,
        expected_started_at=expected_started_at,
    ):
        raise RuntimeError("Reserved build disappeared during promotion")
    return True


def reset_abandoned_builds(session: Session, pool_id: str, now: datetime) -> None:
    """Return unreserved abandoned sandbox builds in one provider pool to the queue."""
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    queued_benchmarks = select(col(Benchmark.id)).where(
        col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
        arguments["queue_pool_id"].as_string() == pool_id,
    )
    active_reservation = (
        select(col(SandboxBuildReservation.task_row_id))
        .where(col(SandboxBuildReservation.task_row_id) == col(Task.id))
        .exists()
    )
    session.exec(
        update(Task)
        .where(col(Task.status) == TaskStatus.BUILDING)
        .where(col(Task.benchmark).in_(queued_benchmarks))
        .where(~active_reservation)
        .values(
            status=TaskStatus.PENDING,
            started_at=case(
                (col(Task.started_at) >= now, col(Task.started_at) + timedelta(microseconds=1)),
                else_=now,
            ),
        )
    )
