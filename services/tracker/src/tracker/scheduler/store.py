"""PostgreSQL-backed sandbox queue policy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import TracebackType
from uuid import UUID

from benchmark_service import Resources
from sqlalchemy import JSON, case, func, text, type_coerce
from sqlalchemy import select as sa_select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, delete, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)

_ACTIVE_TASK_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
_TASK_EVALUATION_LOCK_SCOPE = "task-evaluation"
_TASK_BUILD_LOCK_SCOPE = "task-build"
_RESERVED_QUEUE_POOL_SUFFIX = ".reserved"


@dataclass(frozen=True)
class ActiveReservations:
    """Capacity held by every in-flight reserved build in one provider pool."""

    count: int
    vcpu: int
    memory: int
    disk: int


def _advisory_lock_key(resource_id: str) -> int:
    return int.from_bytes(
        sha256(resource_id.encode()).digest()[:8],
        byteorder="big",
        signed=True,
    )


def _task_evaluation_lock_resource_id(task_row_id: UUID) -> str:
    return f"{_TASK_EVALUATION_LOCK_SCOPE}:{task_row_id}"


def _task_build_lock_resource_id(task_row_id: UUID) -> str:
    return f"{_TASK_BUILD_LOCK_SCOPE}:{task_row_id}"


class PostgresAdvisoryLock:
    """A nonblocking session advisory lock held on one dedicated connection."""

    def __init__(self, engine: Engine, *, resource_id: str) -> None:
        self._engine = engine
        self._lock_key = _advisory_lock_key(resource_id)
        self._connection: Connection | None = None

    @property
    def held(self) -> bool:
        return self._connection is not None

    @property
    def connection(self) -> Connection:
        assert self._connection is not None
        return self._connection

    async def __aenter__(self) -> bool:
        return await self.acquire()

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.release()

    async def acquire(self) -> bool:
        """Try to take the lock without blocking; the caller must release it when True."""
        acquire_task = asyncio.create_task(asyncio.to_thread(self._try_acquire))
        try:
            return await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            acquired = await acquire_task
            if acquired:
                await asyncio.to_thread(self._release)
            raise

    async def release(self) -> None:
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
    """Lock one provider capacity pool; both queue protocol ids share the lock."""
    return PostgresAdvisoryLock(engine, resource_id=queue_pool_lock_id(pool_id))


def task_evaluation_lock(engine: Engine, task_row_id: UUID) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=_task_evaluation_lock_resource_id(task_row_id))


def task_build_lock(engine: Engine, task_row_id: UUID) -> PostgresAdvisoryLock:
    """Prove a reserved build's creator is alive for as long as the lock is held."""
    return PostgresAdvisoryLock(engine, resource_id=_task_build_lock_resource_id(task_row_id))


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


def reserved_queue_pool_id(pool_id: str) -> str:
    """Return the queue id whose builds hold reservations instead of the pool lock."""
    return f"{queue_pool_lock_id(pool_id)}{_RESERVED_QUEUE_POOL_SUFFIX}"


def queue_pool_lock_id(pool_id: str) -> str:
    """Return the provider capacity pool shared by a legacy or reserved queue id."""
    return pool_id.removesuffix(_RESERVED_QUEUE_POOL_SUFFIX)


def is_reserved_queue_pool_id(pool_id: str) -> bool:
    return pool_id.endswith(_RESERVED_QUEUE_POOL_SUFFIX)


def _active_task_count():
    active_task = aliased(Task)
    return (
        select(func.count(col(active_task.id)))
        .where(col(active_task.benchmark) == col(Benchmark.id))
        .where(col(active_task.status).in_(_ACTIVE_TASK_STATUSES))
        .correlate(Benchmark)
        .scalar_subquery()
    )


def _eligible_tasks(pool_id: str):
    """PENDING tasks a claimed executor may admit right now, before any ordering."""
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    concurrency = arguments["concurrency"].as_integer()
    return (
        select(col(Task.id))
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        .where(arguments["queue_pool_id"].as_string() == pool_id)
        .where(_active_task_count() < concurrency)
        .where(
            col(Benchmark.id).in_(
                select(col(ExecutorDispatch.benchmark_id)).where(
                    col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING
                )
            )
        )
        .where(col(Task.id).not_in(select(col(SandboxBuildReservation.task_row_id))))
    )


def _admissible_ids(pool_id: str, *, head_only: bool):
    """Eligible tasks, or only the pool head: highest priority, then oldest attempt."""
    eligible = _eligible_tasks(pool_id)
    if not head_only:
        return eligible
    priority = type_coerce(col(Benchmark.arguments), JSON)["priority"].as_integer()
    return eligible.order_by(priority, col(Task.started_at), col(Task.id)).limit(1)


def eligible_task_is(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
    *,
    head_only: bool = True,
) -> bool:
    """Return whether this exact attempt may be admitted, as the pool head unless ``head_only`` is off."""
    return (
        session.exec(
            select(col(Task.id))
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.id).in_(_admissible_ids(pool_id, head_only=head_only)))
        ).first()
        is not None
    )


def claim_eligible_task(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
    *,
    reservation: Resources | None = None,
) -> bool:
    """Atomically claim this attempt, reserving ``reservation`` capacity in the caller transaction.

    Reserving claims may take any eligible task; others only the pool head.
    """
    result = session.exec(
        update(Task)
        .where(col(Task.id) == task_row_id)
        .where(col(Task.started_at) == expected_started_at)
        .where(col(Task.id).in_(_admissible_ids(pool_id, head_only=reservation is None)))
        .values(status=TaskStatus.BUILDING)
    )
    if result.rowcount != 1:
        return False

    if reservation is not None:
        session.add(
            SandboxBuildReservation(
                task_row_id=task_row_id,
                attempt_started_at=expected_started_at,
                pool_id=queue_pool_lock_id(pool_id),
                requested_vcpu=reservation.vcpu,
                requested_memory=reservation.memory,
                requested_disk=reservation.disk,
            )
        )
        session.flush()
    return True


def reserved_builds(session: Session, pool_id: str) -> dict[UUID, Resources]:
    """Every reservation held in one provider capacity pool, keyed by task attempt row."""
    rows = session.exec(
        select(SandboxBuildReservation).where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
    ).all()
    return {
        row.task_row_id: Resources(vcpu=row.requested_vcpu, memory=row.requested_memory, disk=row.requested_disk)
        for row in rows
    }


def sum_reservations(builds: dict[UUID, Resources]) -> ActiveReservations:
    return ActiveReservations(
        count=len(builds),
        vcpu=sum(r.vcpu for r in builds.values()),
        memory=sum(r.memory for r in builds.values()),
        disk=sum(r.disk for r in builds.values()),
    )


def active_reservations(session: Session, pool_id: str) -> ActiveReservations:
    """Count and sum every reservation held in one provider capacity pool."""
    count, vcpu, memory, disk = (
        session.connection()
        .execute(
            sa_select(
                func.count(col(SandboxBuildReservation.task_row_id)),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_vcpu)), 0),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_memory)), 0),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_disk)), 0),
            ).where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
        )
        .one()
    )
    return ActiveReservations(count=count, vcpu=vcpu, memory=memory, disk=disk)


def release_reservation(session: Session, task_row_id: UUID, expected_started_at: datetime) -> None:
    session.exec(
        delete(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
        .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
    )


def queued_sandbox_name(task_row_id: UUID, started_at: datetime) -> str:
    attempt = started_at.replace(tzinfo=UTC) if started_at.tzinfo is None else started_at.astimezone(UTC)
    return f"queued-{task_row_id.hex}-{int(attempt.timestamp() * 1_000_000):x}"


def lock_abandoned_reservations(session: Session, pool_id: str) -> list[SandboxBuildReservation]:
    """Hold each absent creator's lock until its provider cleanup transaction ends."""
    candidates = session.exec(
        select(SandboxBuildReservation.task_row_id).where(
            col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id)
        )
    ).all()
    abandoned: list[SandboxBuildReservation] = []
    for task_row_id in candidates:
        if not _try_task_build_transaction_lock(session, task_row_id):
            continue

        reservation = session.get(SandboxBuildReservation, task_row_id)
        if reservation is not None:
            abandoned.append(reservation)

    return abandoned


def _try_task_build_transaction_lock(session: Session, task_row_id: UUID) -> bool:
    return bool(
        session.connection()
        .execute(
            text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
            {"lock_key": _advisory_lock_key(_task_build_lock_resource_id(task_row_id))},
        )
        .scalar_one()
    )


def reset_abandoned_builds(session: Session, pool_id: str, now: datetime) -> None:
    """Requeue builds whose creator is gone.

    A creator holds its task build lock from the claim until the sandbox is started
    or deleted. A free lock only proves the creator is gone, not that its provider
    sandbox is, so the reservation is left in place and keeps the task ineligible
    until it is reconciled. Legacy builds hold the pool lock instead, and this only
    runs under it.
    """
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    queued_benchmarks = select(col(Benchmark.id)).where(
        col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
        arguments["queue_pool_id"].as_string() == pool_id,
    )
    building = (
        select(col(Task.id))
        .where(col(Task.status) == TaskStatus.BUILDING)
        .where(col(Task.benchmark).in_(queued_benchmarks))
    )
    abandoned = [
        task_row_id
        for task_row_id in session.exec(building).all()
        if _try_task_build_transaction_lock(session, task_row_id)
    ]
    if not abandoned:
        return

    session.exec(
        update(Task)
        .where(col(Task.id).in_(abandoned))
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
