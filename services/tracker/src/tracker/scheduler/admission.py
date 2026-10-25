"""PostgreSQL-backed provider admission for queued sandbox creation."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

from benchmark_service import (
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxReservationDemand,
    SandboxSource,
)
from sqlalchemy import tuple_
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session, col, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    TaskStatus,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.sandbox import SandboxCreationState, delete_sandbox
from tracker.scheduler.store import (
    admissions_since,
    claim_eligible_task,
    claim_prepared_task,
    clear_admission_candidate,
    database_now,
    eligible_task_is,
    eligible_task_ids,
    is_reserved_queue_pool_id,
    lock_abandoned_reservations,
    prepare_admission_candidate,
    packed_admission_candidates,
    purge_stale_admission_candidates,
    queue_pool_id,
    queue_pool_lock,
    queue_pool_lock_id,
    purge_released_reservations,
    queued_sandbox_name,
    release_reservation,
    reserved_builds,
    reset_abandoned_builds,
    task_build_lock,
)

logger = get_logger(__name__)

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]

_CAPACITY_CACHE_SECONDS = 2.0
_RATE_WINDOW = timedelta(minutes=1)
# Released reservations outlive the rate window and any capacity snapshot still in use.
_RELEASED_RESERVATION_RETENTION = 2 * _RATE_WINDOW
_ORPHAN_CLEANUP_GRACE = timedelta(minutes=5)


@dataclass(slots=True)
class CapacitySnapshot:
    capacities: dict[tuple[str, str], SandboxCapacity]
    read_at: datetime
    """Database clock just before the provider read; reservation stamps compare against it."""
    fetched_at: float


class CapacityCache:
    """One provider capacity read shared by every contender of an executor for a short while."""

    def __init__(self, provider: SandboxProvider, engine: Engine, ttl_seconds: float = _CAPACITY_CACHE_SECONDS) -> None:
        self._provider = provider
        self._engine = engine
        self._ttl = ttl_seconds
        self._snapshot: CapacitySnapshot | None = None
        self._refresh: asyncio.Task[CapacitySnapshot | None] | None = None

    async def get(self) -> CapacitySnapshot | None:
        """Return a snapshot at most ``ttl`` old; None when the provider cannot state capacity."""
        snapshot = self._snapshot
        if snapshot is not None and asyncio.get_running_loop().time() - snapshot.fetched_at < self._ttl:
            return snapshot
        if self._refresh is None or self._refresh.done():
            self._refresh = asyncio.create_task(self._read())
        return await asyncio.shield(self._refresh)

    def _database_now(self) -> datetime:
        with Session(self._engine) as session:
            return database_now(session)

    async def _read(self) -> CapacitySnapshot | None:
        read_at = await asyncio.to_thread(self._database_now)
        fetched_at = asyncio.get_running_loop().time()
        try:
            domains = await self._provider.get_capacity_domains()
        except SandboxError:
            logger.warning("sandbox.admission.capacity_failed", exc_info=True)
            return None
        if domains is None:
            return None
        self._snapshot = CapacitySnapshot(
            capacities={(domain.target_id, domain.sandbox_class): domain.capacity for domain in domains},
            read_at=read_at,
            fetched_at=fetched_at,
        )
        return self._snapshot


@dataclass(frozen=True, slots=True)
class SandboxQueueContext:
    provider: SandboxProvider = field(repr=False)
    pool_id: str
    engine: Engine = field(repr=False)
    capacity_cache: CapacityCache = field(repr=False)
    preparation_slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(10), repr=False)
    admission_turns: _AdmissionTurns = field(default_factory=lambda: _AdmissionTurns(), repr=False)
    poll_interval_seconds: float = 1.0
    admission_rate_per_minute: int = 0
    """Reserved admissions per pool per minute; 0 leaves provider capacity as the only limit."""

    def serves_queue(self, pool_id: str) -> bool:
        """Return whether a run's persisted queue id belongs to this provider pool."""
        return queue_pool_lock_id(pool_id) == queue_pool_lock_id(self.pool_id)

    def for_queue(self, pool_id: str) -> SandboxQueueContext:
        """Adopt a run's persisted queue id, which selects its admission protocol."""
        return replace(self, pool_id=pool_id)


def create_queue_context(
    *,
    engine: Engine,
    provider: SandboxProvider,
    poll_interval_seconds: float = 1.0,
    admission_rate_per_minute: int = 0,
) -> SandboxQueueContext:
    provider_pool_id = provider.admission_pool_id
    if provider_pool_id is None:
        raise ValueError("Sandbox provider does not support queued admission")

    return SandboxQueueContext(
        provider=provider,
        pool_id=queue_pool_id(provider_pool_id),
        engine=engine,
        capacity_cache=CapacityCache(provider, engine),
        poll_interval_seconds=poll_interval_seconds,
        admission_rate_per_minute=admission_rate_per_minute,
    )


def _is_waiting(session: Session, task_row_id: UUID, expected_started_at: datetime) -> bool:
    return (
        session.exec(
            select(col(Task.id))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.status) == TaskStatus.PENDING)
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        ).first()
        is not None
    )


def _start_claimed_task(
    session: Session,
    *,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    active_benchmarks = select(col(Benchmark.id)).where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
    result = session.exec(
        update(Task)
        .where(col(Task.id) == task_row_id)
        .where(col(Task.started_at) == expected_started_at)
        .where(col(Task.status) == TaskStatus.BUILDING)
        .where(col(Task.benchmark).in_(active_benchmarks))
        .values(status=TaskStatus.IN_PROGRESS)
    )
    if result.rowcount != 1:
        return False

    release_reservation(session, task_row_id, expected_started_at)
    return True


async def _reset_abandoned_pool_builds(connection: Connection, context: SandboxQueueContext) -> None:
    with Session(connection) as session:
        reset_abandoned_builds(session, context.pool_id, database_now(session))
        purge_stale_admission_candidates(session, context.pool_id)
        session.commit()


async def _reconcile_abandoned_reservations(context: SandboxQueueContext) -> None:
    """Release reservations whose creator is gone once their sandbox is; needs no pool lock.

    Each reservation is guarded by its task build lock alone, and claimers charge a
    reservation whether it is held or released after their capacity snapshot, so the
    provider calls here never delay a claim.
    """
    with Session(context.engine) as session:
        leak_cutoff = database_now(session) - _ORPHAN_CLEANUP_GRACE
        for reservation in lock_abandoned_reservations(session, context.pool_id):
            name = queued_sandbox_name(reservation.task_row_id, reservation.attempt_started_at)
            if not await _cleanup_orphan_sandbox(context, name):
                if reservation.reserved_at.replace(tzinfo=UTC) > leak_cutoff:
                    continue

                logger.error(
                    "sandbox.admission.reservation_leaked",
                    extra={"sandbox_name": name, "reserved_at": reservation.reserved_at.isoformat()},
                )

            release_reservation(session, reservation.task_row_id, reservation.attempt_started_at)

        purge_released_reservations(session, context.pool_id, database_now(session) - _RELEASED_RESERVATION_RETENTION)
        session.commit()


async def _cleanup_orphan_sandbox(context: SandboxQueueContext, name: str) -> bool:
    """Whether the provider no longer holds a sandbox under ``name``."""
    try:
        sandbox = await context.provider.get_sandbox(name)
        if sandbox.name != name:
            logger.error("sandbox.admission.recovery_identity_mismatch", extra={"sandbox_name": name})
            return False

        return await delete_sandbox(sandbox, context.provider, initiated_by="orphan_cleanup")
    except SandboxNotFoundError:
        return True
    except SandboxError:
        logger.warning("sandbox.admission.recovery_failed", extra={"sandbox_name": name}, exc_info=True)
        return False


async def recover_queued_pool(context: SandboxQueueContext) -> None:
    """Reconcile abandoned reservations, then reset abandoned builds if the pool lock is free."""
    await _reconcile_abandoned_reservations(context)
    lock = queue_pool_lock(context.engine, context.pool_id)
    async with lock as acquired:
        if acquired:
            await _reset_abandoned_pool_builds(lock.connection, context)


async def _close_stack_before_cancellation(stack: AsyncExitStack) -> None:
    close_task = asyncio.create_task(stack.aclose())
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError:
        await close_task
        raise


def _remaining_budget(session: Session, context: SandboxQueueContext) -> int | None:
    if not context.admission_rate_per_minute:
        return None
    since = database_now(session) - _RATE_WINDOW
    return context.admission_rate_per_minute - admissions_since(session, context.pool_id, since)


def _budget_is_spent(context: SandboxQueueContext) -> bool:
    """Whether the rate window is used up, checked before any provider read so waiting runs cost the DB only."""
    with Session(context.engine) as session:
        budget = _remaining_budget(session, context)
    return budget is not None and budget <= 0


def _is_eligible_without_lock(
    engine: Engine, pool_id: str, task_row_id: UUID, expected_started_at: datetime, *, by_capacity: bool
) -> bool | None:
    """Cheap pre-check so only admissible tasks contend for the pool lock; None means gone.

    Reserved builds are eligible whenever their run has room; full-lock builds only as
    the pool head.
    """
    with Session(engine) as session:
        if eligible_task_is(session, pool_id, task_row_id, expected_started_at, limit=None if by_capacity else 1):
            return True
        return False if _is_waiting(session, task_row_id, expected_started_at) else None


def _is_waiting_without_lock(engine: Engine, task_row_id: UUID, expected_started_at: datetime) -> bool:
    with Session(engine) as session:
        return _is_waiting(session, task_row_id, expected_started_at)


async def _poll_sleep(context: SandboxQueueContext) -> None:
    """Sleep one poll interval with jitter so contenders do not retry the pool lock in lockstep."""
    await asyncio.sleep(context.poll_interval_seconds * (0.5 + random.random()))


@dataclass(slots=True)
class _AdmissionTurn:
    snapshot: CapacitySnapshot | None


@dataclass(slots=True)
class _AdmissionWaiter:
    attempt_started_at: datetime
    dispatch_id: UUID
    ready: bool
    future: asyncio.Future[_AdmissionTurn | None]
    granted: bool = False


class _AdmissionTurns:
    """Park prepared tasks behind one polling loop, with bounded outstanding turns."""

    def __init__(self) -> None:
        self.claim_lock = asyncio.Lock()
        self._waiters: dict[UUID, _AdmissionWaiter] = {}
        self._active = {True: 0, False: 0}
        self._driver: asyncio.Task[None] | None = None
        self._changed = asyncio.Event()
        self._round_ran = False

    @asynccontextmanager
    async def turn(
        self,
        context: SandboxQueueContext,
        task_row_id: UUID,
        attempt_started_at: datetime,
        dispatch_id: UUID,
        *,
        ready: bool,
    ) -> AsyncGenerator[_AdmissionTurn | None]:
        waiter = _AdmissionWaiter(attempt_started_at, dispatch_id, ready, asyncio.get_running_loop().create_future())
        self._waiters[task_row_id] = waiter
        self._changed.set()
        if self._driver is None:
            self._driver = asyncio.create_task(self._run(context))
        try:
            yield await waiter.future
        finally:
            del self._waiters[task_row_id]
            self._changed.set()
            if waiter.granted:
                self._active[ready] -= 1
            if not self._waiters:
                await self._stop()

    async def _stop(self) -> None:
        driver, self._driver = self._driver, None
        if driver is not None:
            driver.cancel()
            with suppress(asyncio.CancelledError):
                await driver

    async def _run(self, context: SandboxQueueContext) -> None:
        try:
            while self._waiters:
                retry_rejected = self._round_ran
                if self._round_ran:
                    await _poll_sleep(context)
                self._round_ran = True
                await self._round(context, retry_rejected=retry_rejected)
                if not any(not waiter.future.done() for waiter in self._waiters.values()):
                    # All outstanding turns are already running. Sleep until a
                    # task returns its lease or a new waiter joins, not on a timer.
                    self._changed.clear()
                    await self._changed.wait()
        except Exception as error:
            for waiter in self._waiters.values():
                if not waiter.future.done():
                    waiter.future.set_exception(error)
        finally:
            if self._driver is asyncio.current_task():
                self._driver = None

    def _waiting(
        self,
        context: SandboxQueueContext,
        waiters: dict[UUID, _AdmissionWaiter],
    ) -> tuple[set[UUID], set[UUID], int | None]:
        keys = [(task_id, waiter.attempt_started_at, waiter.dispatch_id) for task_id, waiter in waiters.items()]
        with Session(context.engine) as session:
            waiting = set(
                session.exec(
                    select(col(Task.id))
                    .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
                    .join(ExecutorDispatch, col(ExecutorDispatch.benchmark_id) == col(Benchmark.id))
                    .where(tuple_(col(Task.id), col(Task.started_at), col(ExecutorDispatch.id)).in_(keys))
                    .where(col(Task.status) == TaskStatus.PENDING)
                    .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
                    .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
                ).all()
            )
            eligible: set[UUID] = eligible_task_ids(session, context.pool_id, list(waiting)) if waiting else set()
            return waiting, eligible, _remaining_budget(session, context)

    def _ready(
        self,
        context: SandboxQueueContext,
        waiters: dict[UUID, _AdmissionWaiter],
        snapshot: CapacitySnapshot,
        budget: int | None,
        slots: int,
    ) -> list[UUID]:
        selected: list[UUID] = []
        with Session(context.engine) as session:
            for candidate in packed_admission_candidates(
                session,
                context.pool_id,
                snapshot.capacities,
                snapshot.read_at,
                budget,
            ):
                waiter = waiters.get(candidate.task_row_id)
                if waiter is not None and waiter.ready and waiter.dispatch_id == candidate.executor_dispatch_id:
                    selected.append(candidate.task_row_id)
                    if len(selected) == slots:
                        break
        return selected

    async def _round(self, context: SandboxQueueContext, *, retry_rejected: bool) -> None:
        waiters = {task_id: waiter for task_id, waiter in self._waiters.items() if not waiter.future.done()}
        if not waiters:
            return
        waiting, eligible, budget = await asyncio.to_thread(self._waiting, context, waiters)
        for task_id, waiter in waiters.items():
            if task_id not in waiting and not waiter.future.done():
                waiter.future.set_result(None)
        live = {
            task_id: waiter for task_id, waiter in waiters.items() if task_id in eligible and not waiter.future.done()
        }
        ready_slots = 10 - self._active[True]
        ready_ids: list[UUID] = []
        snapshot: CapacitySnapshot | None = None
        if ready_slots and budget != 0 and any(waiter.ready for waiter in live.values()):
            snapshot = await context.capacity_cache.get()
            if snapshot is not None:
                ready_ids = await asyncio.to_thread(self._ready, context, live, snapshot, budget, ready_slots)
        # Rejected candidates rotate separately from ready work. Neither group may
        # occupy every turn of the other, and rechecks still use preparation_slots.
        retry_ids = (
            [task_id for task_id, waiter in live.items() if not waiter.ready][: 10 - self._active[False]]
            if retry_rejected
            else []
        )
        for task_id in [*ready_ids, *retry_ids]:
            waiter = live[task_id]
            if not waiter.future.done():
                self._active[waiter.ready] += 1
                waiter.granted = True
                waiter.future.set_result(_AdmissionTurn(snapshot if waiter.ready else None))


async def _start_sandbox(
    *,
    stack: AsyncExitStack,
    bind: Connection,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    create: SandboxFactory,
) -> Sandbox | None:
    """Create the claimed sandbox, then move the exact attempt to IN_PROGRESS."""
    sandbox = await stack.enter_async_context(create())
    try:
        with Session(bind) as session:
            try:
                lock_execution_authority(session, authority)
            except ExecutionAuthorityRevoked:
                session.rollback()
                started = False
            else:
                started = _start_claimed_task(
                    session,
                    task_row_id=task_row_id,
                    expected_started_at=expected_started_at,
                )
                if started:
                    session.commit()
                else:
                    session.rollback()
    except Exception:
        await _close_stack_before_cancellation(stack)
        raise
    if not started:
        await _close_stack_before_cancellation(stack)

        return None

    return sandbox


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    source: SandboxSource,
    resources: Resources,
    creation_state: SandboxCreationState,
    create: SandboxFactory,
    preparation_stack: AsyncExitStack,
) -> Sandbox | None:
    """Resolve the backlog outside the pool lock, then claim in priority order.

    Reserved builds publish exact provider demand and hold only their task build lock
    while creating. The pool lock covers DB-only packing, rate budget and own claim.
    Legacy queues retain their full-lock creation protocol.
    """
    reserves = is_reserved_queue_pool_id(context.pool_id)
    demand: SandboxReservationDemand | None = None
    prepared = False
    admission_ready = False
    build_lock = task_build_lock(context.engine, task_row_id)
    try:
        while True:
            async with AsyncExitStack() as turn_stack:
                snapshot: CapacitySnapshot | None = None
                if reserves and prepared:
                    turn = await turn_stack.enter_async_context(
                        context.admission_turns.turn(
                            context,
                            task_row_id,
                            expected_started_at,
                            authority.dispatch_id,
                            ready=admission_ready,
                        )
                    )
                    if turn is None:
                        return None
                    snapshot = turn.snapshot
                if reserves and not prepared:
                    if not await asyncio.to_thread(
                        _is_waiting_without_lock, context.engine, task_row_id, expected_started_at
                    ):
                        return None
                elif not reserves or not admission_ready:
                    eligible_without_lock = await asyncio.to_thread(
                        _is_eligible_without_lock,
                        context.engine,
                        context.pool_id,
                        task_row_id,
                        expected_started_at,
                        by_capacity=reserves,
                    )
                    if eligible_without_lock is None:
                        return None
                    if not eligible_without_lock:
                        if not reserves:
                            await _poll_sleep(context)
                        continue

                if reserves:
                    if not admission_ready:
                        if demand is None:
                            demand = await context.provider.reservable_demand(source, resources)
                        async with AsyncExitStack() as check_stack:
                            if prepared:
                                await check_stack.enter_async_context(context.preparation_slots)
                            admission_ready = await context.provider.check_admission(source, resources)
                        with Session(context.engine) as session:
                            lock_execution_authority(session, authority)
                            if not _is_waiting(session, task_row_id, expected_started_at):
                                return None
                            prepare_admission_candidate(
                                session,
                                context.pool_id,
                                task_row_id,
                                expected_started_at,
                                authority.dispatch_id,
                                demand,
                                admission_ready=admission_ready,
                            )
                            session.commit()
                        prepared = True
                        # Resolved waiters release preparation slots and park without polling.
                        await preparation_stack.aclose()
                        continue
                    if context.admission_rate_per_minute and await asyncio.to_thread(_budget_is_spent, context):
                        continue
                    # A selected local batch must not collide with itself on the
                    # nonblocking pool lock. Only the short claim section is serial.
                    await turn_stack.enter_async_context(context.admission_turns.claim_lock)

                lock = queue_pool_lock(context.engine, context.pool_id)
                async with lock as acquired:
                    if acquired:
                        try:
                            if reserves:
                                admit = await build_lock.acquire()
                            else:
                                await _reset_abandoned_pool_builds(lock.connection, context)
                                with Session(lock.connection) as session:
                                    reserved_now = reserved_builds(session, context.pool_id)
                                    session.rollback()
                                admit = not reserved_now and await context.provider.check_admission(source, resources)

                            claimed = False
                            if admit:
                                with Session(lock.connection) as session:
                                    lock_execution_authority(session, authority)
                                    if snapshot is not None:
                                        claimed = claim_prepared_task(
                                            session,
                                            context.pool_id,
                                            task_row_id,
                                            expected_started_at,
                                            authority.dispatch_id,
                                            snapshot.capacities,
                                            snapshot.read_at,
                                            _remaining_budget(session, context),
                                        )
                                    else:
                                        claimed = claim_eligible_task(
                                            session, context.pool_id, task_row_id, expected_started_at
                                        )
                                    if claimed:
                                        session.commit()
                                        prepared = False
                                    elif not _is_waiting(session, task_row_id, expected_started_at):
                                        return None
                            if not claimed:
                                await build_lock.release()
                            elif not reserves:
                                return await _start_sandbox(
                                    stack=stack,
                                    bind=lock.connection,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                    authority=authority,
                                    create=create,
                                )
                        except ExecutionAuthorityRevoked:
                            return None

            # The turn ends before provider creation; long builds cannot pin dispatch.
            if build_lock.held:
                return await _start_sandbox(
                    stack=stack,
                    bind=build_lock.connection,
                    task_row_id=task_row_id,
                    expected_started_at=expected_started_at,
                    authority=authority,
                    create=create,
                )
            if not reserves:
                await _poll_sleep(context)
    except ExecutionAuthorityRevoked:
        return None
    finally:
        try:
            if prepared:
                with Session(context.engine) as session:
                    clear_admission_candidate(session, task_row_id, expected_started_at, authority.dispatch_id)
                    session.commit()
            if build_lock.held and not creation_state.cleanup_pending:
                with Session(build_lock.connection) as session:
                    release_reservation(session, task_row_id, expected_started_at)
                    session.commit()
        finally:
            await build_lock.release()
