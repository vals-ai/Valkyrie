"""PostgreSQL-backed sandbox queue policy."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import TracebackType
from typing import cast
from uuid import UUID

from benchmark_service import SandboxCapacity, SandboxReservationDemand
from sqlalchemy import JSON, ColumnElement, and_, case, func, text, type_coerce
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, delete, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    SandboxAdmissionCandidate,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)

_ACTIVE_TASK_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
_TASK_EVALUATION_LOCK_SCOPE = "task-evaluation"
_TASK_BUILD_LOCK_SCOPE = "task-build"
_SANDBOX_COST_SWEEP_LOCK_RESOURCE_ID = "sandbox-cost-sweep"
_RESERVED_QUEUE_POOL_SUFFIX = ".reserved"


@dataclass(frozen=True)
class ActiveReservations:
    """Quota charged by in-flight builds or by a hypothetical priority packing."""

    count: int
    vcpu: float
    memory: float
    disk: float
    gpu: float


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


def sandbox_cost_sweep_lock(engine: Engine) -> PostgresAdvisoryLock:
    """Let one Tracker worker at a time run the Daytona cost sweep."""
    return PostgresAdvisoryLock(engine, resource_id=_SANDBOX_COST_SWEEP_LOCK_RESOURCE_ID)


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
        .where(~_blocking_reservation_exists())
    )


def _blocking_reservation_exists():
    # A same-attempt retry must wait for retention purge, not reuse its charged row.
    return (
        select(col(SandboxBuildReservation.task_row_id))
        .where(col(SandboxBuildReservation.task_row_id) == col(Task.id))
        .where(
            col(SandboxBuildReservation.released_at).is_(None)
            | (col(SandboxBuildReservation.attempt_started_at) == col(Task.started_at))
        )
        .correlate(Task)
        .exists()
    )


def _admissible_ids(pool_id: str, *, limit: int | None):
    """The first ``limit`` eligible tasks by priority, then oldest attempt; every eligible task when None."""
    eligible = _eligible_tasks(pool_id)
    if limit is None:
        return eligible
    priority = type_coerce(col(Benchmark.arguments), JSON)["priority"].as_integer()
    return eligible.order_by(priority, col(Task.started_at), col(Task.id)).limit(limit)


def eligible_task_ids(session: Session, pool_id: str, task_row_ids: Sequence[UUID]) -> set[UUID]:
    """Batch the run-slot eligibility precheck for a context's waiting contenders."""
    return set(session.exec(_eligible_tasks(pool_id).where(col(Task.id).in_(task_row_ids))).all())


def eligible_task_is(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
    *,
    limit: int | None = 1,
) -> bool:
    """Return whether this exact attempt is among the first ``limit`` admissible tasks (the head by default)."""
    return (
        session.exec(
            select(col(Task.id))
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.id).in_(_admissible_ids(pool_id, limit=limit)))
        ).first()
        is not None
    )


def claim_eligible_task(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    """Atomically claim the legacy queue head in the caller transaction."""
    result = session.exec(
        update(Task)
        .where(col(Task.id) == task_row_id)
        .where(col(Task.started_at) == expected_started_at)
        .where(col(Task.id).in_(_admissible_ids(pool_id, limit=1)))
        .values(status=TaskStatus.BUILDING)
    )
    if result.rowcount != 1:
        return False

    return True


def prepare_admission_candidate(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
    dispatch_id: UUID,
    demand: SandboxReservationDemand,
    *,
    admission_ready: bool,
) -> None:
    """Publish resolved demand and readiness under the caller's execution authority."""
    values = {
        "task_row_id": task_row_id,
        "attempt_started_at": expected_started_at,
        "executor_dispatch_id": dispatch_id,
        "admission_ready": admission_ready,
        "pool_id": queue_pool_lock_id(pool_id),
        "target_id": demand.target_id,
        "sandbox_class": demand.sandbox_class,
        "requested_vcpu": demand.vcpu,
        "requested_memory": demand.memory,
        "requested_disk": demand.disk,
        "requested_gpu": demand.gpu,
    }
    session.exec(
        insert(SandboxAdmissionCandidate)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["task_row_id", "attempt_started_at"],
            set_=values,
        )
    )


def clear_admission_candidate(
    session: Session, task_row_id: UUID, expected_started_at: datetime, dispatch_id: UUID
) -> None:
    session.exec(
        delete(SandboxAdmissionCandidate)
        .where(col(SandboxAdmissionCandidate.task_row_id) == task_row_id)
        .where(col(SandboxAdmissionCandidate.attempt_started_at) == expected_started_at)
        .where(col(SandboxAdmissionCandidate.executor_dispatch_id) == dispatch_id)
    )


def _current_candidate_attempt():
    return (
        select(col(Task.id))
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .join(ExecutorDispatch, col(ExecutorDispatch.benchmark_id) == col(Benchmark.id))
        .where(col(Task.id) == col(SandboxAdmissionCandidate.task_row_id))
        .where(col(Task.started_at) == col(SandboxAdmissionCandidate.attempt_started_at))
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        .where(col(ExecutorDispatch.id) == col(SandboxAdmissionCandidate.executor_dispatch_id))
        .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
    )


def purge_stale_admission_candidates(session: Session, pool_id: str) -> None:
    """Retire prepared rows whose attempt or executor has ended."""
    session.exec(
        delete(SandboxAdmissionCandidate)
        .where(col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id))
        .where(~_current_candidate_attempt().exists())
    )


def fits_in_capacity(demand: SandboxReservationDemand, capacity: SandboxCapacity, reserved: ActiveReservations) -> bool:
    """Charge the snapshot-unseen ledger and retain the existing ten-percent headroom.

    Zero-charge dimensions are independent: GPU-attached compute does not consume
    ordinary compute quota, and ordinary compute does not consume GPU quota.
    """
    return (
        (demand.vcpu == 0 or demand.vcpu <= capacity.cpu.available - reserved.vcpu - capacity.cpu.total * 0.10)
        and (
            demand.memory == 0
            or demand.memory <= capacity.memory.available - reserved.memory - capacity.memory.total * 0.10
        )
        and (demand.disk == 0 or demand.disk <= capacity.disk.available - reserved.disk - capacity.disk.total * 0.10)
        and (demand.gpu == 0 or (capacity.gpu is not None and demand.gpu <= capacity.gpu.available - reserved.gpu))
    )


def packed_admission_candidates(
    session: Session,
    pool_id: str,
    capacities: Mapping[tuple[str, str], SandboxCapacity],
    capacity_read_at: datetime,
    limit: int | None,
) -> Iterator[SandboxAdmissionCandidate]:
    """Yield fitting candidates in priority order, charging the hypothetical prefix.

    Unknown demand with spare run slots blocks lower priorities, but not ready
    peers at the same priority. Selection is a hint; claims still require locks.
    """
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    candidates = session.exec(
        select(Task, SandboxAdmissionCandidate, priority, arguments["concurrency"].as_integer() - _active_task_count())
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .outerjoin(
            SandboxAdmissionCandidate,
            and_(
                col(SandboxAdmissionCandidate.task_row_id) == col(Task.id),
                col(SandboxAdmissionCandidate.attempt_started_at) == col(Task.started_at),
                col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id),
                _current_candidate_attempt().correlate(SandboxAdmissionCandidate).exists(),
            ),
        )
        .where(col(Task.id).in_(_eligible_tasks(pool_id).correlate(None)))
        .order_by(priority, col(Task.started_at), col(Task.id))
    )
    charged = {key: ActiveReservations(count=0, vcpu=0, memory=0, disk=0, gpu=0) for key in capacities}
    charged.update(charged_reservations_by_domain(session, pool_id, capacity_read_at))
    slots: dict[UUID, int] = {}
    selected = 0
    unknown_priority: int | None = None
    for task, joined_candidate, rank, available_slots in candidates:
        if limit is not None and selected >= limit:
            return
        if unknown_priority is not None and rank > unknown_priority:
            return
        # SQLModel's select typing does not reflect nullable outer-joined entities.
        candidate = cast(SandboxAdmissionCandidate | None, joined_candidate)
        benchmark_id = task.benchmark
        if benchmark_id not in slots:
            slots[benchmark_id] = available_slots
        if slots[benchmark_id] <= 0:
            continue
        if candidate is None:
            # Keep this barrier even if a later ready peer fills the run's slots.
            unknown_priority = rank
            continue
        if not candidate.admission_ready:
            continue
        domain = candidate.target_id, candidate.sandbox_class
        # A newer candidate's domain may not exist in this older capacity snapshot.
        if domain not in capacities:
            continue
        demand = SandboxReservationDemand(
            target_id=candidate.target_id,
            sandbox_class=candidate.sandbox_class,
            vcpu=candidate.requested_vcpu,
            memory=candidate.requested_memory,
            disk=candidate.requested_disk,
            gpu=candidate.requested_gpu,
        )
        if not fits_in_capacity(demand, capacities[domain], charged[domain]):
            continue
        slots[benchmark_id] -= 1
        selected += 1
        prior = charged[domain]
        charged[domain] = replace(
            prior,
            count=prior.count + 1,
            vcpu=prior.vcpu + demand.vcpu,
            memory=prior.memory + demand.memory,
            disk=prior.disk + demand.disk,
            gpu=prior.gpu + demand.gpu,
        )
        yield candidate


def claim_prepared_task(
    session: Session,
    pool_id: str,
    task_row_id: UUID,
    expected_started_at: datetime,
    dispatch_id: UUID,
    capacities: Mapping[tuple[str, str], SandboxCapacity],
    capacity_read_at: datetime,
    limit: int | None,
) -> bool:
    """Claim an exact prepared attempt under the caller's pool and task build locks."""
    for candidate in packed_admission_candidates(session, pool_id, capacities, capacity_read_at, limit):
        if candidate.task_row_id != task_row_id or candidate.executor_dispatch_id != dispatch_id:
            continue
        result = session.exec(
            update(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.status) == TaskStatus.PENDING)
            .values(status=TaskStatus.BUILDING)
        )
        if result.rowcount != 1:
            return False
        session.add(
            SandboxBuildReservation(
                task_row_id=task_row_id,
                attempt_started_at=expected_started_at,
                reserved_at=database_now(session),
                pool_id=queue_pool_lock_id(pool_id),
                target_id=candidate.target_id,
                sandbox_class=candidate.sandbox_class,
                requested_vcpu=candidate.requested_vcpu,
                requested_memory=candidate.requested_memory,
                requested_disk=candidate.requested_disk,
                requested_gpu=candidate.requested_gpu,
            )
        )
        clear_admission_candidate(session, task_row_id, expected_started_at, dispatch_id)
        session.flush()
        return True
    return False


def database_now(session: Session) -> datetime:
    """The database wall clock (not the transaction start), so stamps from different executors compare."""
    return session.connection().execute(sa_select(func.clock_timestamp())).scalar_one()


def reserved_builds(session: Session, pool_id: str) -> dict[UUID, SandboxReservationDemand]:
    """Every reservation held in one provider capacity pool, keyed by task attempt row."""
    rows = session.exec(
        select(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
        .where(col(SandboxBuildReservation.released_at).is_(None))
    ).all()
    return {
        row.task_row_id: SandboxReservationDemand(
            target_id=row.target_id,
            sandbox_class=row.sandbox_class,
            vcpu=row.requested_vcpu,
            memory=row.requested_memory,
            disk=row.requested_disk,
            gpu=row.requested_gpu,
        )
        for row in rows
    }


def sum_reservations(builds: dict[UUID, SandboxReservationDemand]) -> ActiveReservations:
    return ActiveReservations(
        count=len(builds),
        vcpu=sum(r.vcpu for r in builds.values()),
        memory=sum(r.memory for r in builds.values()),
        disk=sum(r.disk for r in builds.values()),
        gpu=sum(r.gpu for r in builds.values()),
    )


def active_reservations(session: Session, pool_id: str) -> ActiveReservations:
    """Count and sum every reservation held in one provider capacity pool."""
    return _sum_reservation_rows(session, pool_id, col(SandboxBuildReservation.released_at).is_(None))


def charged_reservations(session: Session, pool_id: str, capacity_read_at: datetime) -> ActiveReservations:
    """Reservations a capacity snapshot taken at ``capacity_read_at`` cannot account for.

    Held reservations are sandboxes still being created. Reservations released after the
    snapshot are sandboxes promoted since it; the provider now bills them, the snapshot does
    not. Either way the claimer must charge them on top of the snapshot.
    """
    return _sum_reservation_rows(
        session,
        pool_id,
        (col(SandboxBuildReservation.released_at).is_(None))
        | (col(SandboxBuildReservation.released_at) > capacity_read_at),
    )


def charged_reservations_by_domain(
    session: Session, pool_id: str, capacity_read_at: datetime
) -> dict[tuple[str, str], ActiveReservations]:
    """Snapshot-unseen charges, separated by the provider's quota owner."""
    rows = session.connection().execute(
        sa_select(
            col(SandboxBuildReservation.target_id),
            col(SandboxBuildReservation.sandbox_class),
            func.count(col(SandboxBuildReservation.task_row_id)),
            func.sum(col(SandboxBuildReservation.requested_vcpu)),
            func.sum(col(SandboxBuildReservation.requested_memory)),
            func.sum(col(SandboxBuildReservation.requested_disk)),
            func.sum(col(SandboxBuildReservation.requested_gpu)),
        )
        .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
        .where(
            col(SandboxBuildReservation.released_at).is_(None)
            | (col(SandboxBuildReservation.released_at) > capacity_read_at)
        )
        .group_by(col(SandboxBuildReservation.target_id), col(SandboxBuildReservation.sandbox_class))
    )
    return {
        (target_id, sandbox_class): ActiveReservations(count=count, vcpu=vcpu, memory=memory, disk=disk, gpu=gpu)
        for target_id, sandbox_class, count, vcpu, memory, disk, gpu in rows
    }


def _sum_reservation_rows(session: Session, pool_id: str, condition: ColumnElement[bool]) -> ActiveReservations:
    count, vcpu, memory, disk, gpu = (
        session.connection()
        .execute(
            sa_select(
                func.count(col(SandboxBuildReservation.task_row_id)),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_vcpu)), 0),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_memory)), 0),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_disk)), 0),
                func.coalesce(func.sum(col(SandboxBuildReservation.requested_gpu)), 0),
            )
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(condition)
        )
        .one()
    )
    return ActiveReservations(count=count, vcpu=vcpu, memory=memory, disk=disk, gpu=gpu)


def admissions_since(session: Session, pool_id: str, since: datetime) -> int:
    """How many reserved builds one provider pool admitted at or after ``since``."""
    return (
        session.connection()
        .execute(
            sa_select(func.count(col(SandboxBuildReservation.task_row_id)))
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxBuildReservation.reserved_at) >= since)
        )
        .scalar_one()
    )


def release_reservation(session: Session, task_row_id: UUID, expected_started_at: datetime) -> None:
    """Stop charging the reservation; the row stays until ``purge_released_reservations`` for later claimers."""
    session.exec(
        update(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
        .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
        .where(col(SandboxBuildReservation.released_at).is_(None))
        .values(released_at=database_now(session))
    )


def purge_released_reservations(session: Session, pool_id: str, released_before: datetime) -> None:
    session.exec(
        delete(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
        .where(col(SandboxBuildReservation.released_at) < released_before)
    )


def queued_sandbox_name(task_row_id: UUID, started_at: datetime) -> str:
    attempt = started_at.replace(tzinfo=UTC) if started_at.tzinfo is None else started_at.astimezone(UTC)
    return f"queued-{task_row_id.hex}-{int(attempt.timestamp() * 1_000_000):x}"


def lock_abandoned_reservations(session: Session, pool_id: str) -> list[SandboxBuildReservation]:
    """Hold each absent creator's lock until its provider cleanup transaction ends."""
    candidates = session.exec(
        select(SandboxBuildReservation.task_row_id)
        .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
        .where(col(SandboxBuildReservation.released_at).is_(None))
    ).all()
    abandoned = try_task_build_transaction_locks(session, candidates)
    if not abandoned:
        return []

    return list(
        session.exec(
            select(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.task_row_id).in_(abandoned))
            .where(col(SandboxBuildReservation.released_at).is_(None))
        ).all()
    )


def try_task_build_transaction_locks(session: Session, task_row_ids: Sequence[UUID]) -> set[UUID]:
    """Take every free task build lock among ``task_row_ids`` in one round trip; held until the transaction ends."""
    if not task_row_ids:
        return set()
    keys = {_advisory_lock_key(_task_build_lock_resource_id(task_row_id)): task_row_id for task_row_id in task_row_ids}
    locked = session.connection().execute(
        text("SELECT key FROM unnest(CAST(:keys AS bigint[])) AS key WHERE pg_try_advisory_xact_lock(key)"),
        {"keys": list(keys)},
    )
    return {keys[key] for (key,) in locked}


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
    abandoned = try_task_build_transaction_locks(session, session.exec(building).all())
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
