"""PostgreSQL-backed provider admission for queued sandbox creation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from uuid import UUID

from benchmark_service import (
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxSource,
)
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session, col, select, update

from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.sandbox import SandboxCreationState, delete_sandbox
from tracker.scheduler.store import (
    ActiveReservations,
    reserved_builds,
    sum_reservations,
    claim_eligible_task,
    eligible_task_is,
    is_reserved_queue_pool_id,
    lock_abandoned_reservations,
    queue_pool_id,
    queue_pool_lock,
    queue_pool_lock_id,
    queued_sandbox_name,
    release_reservation,
    reset_abandoned_builds,
    task_build_lock,
)

logger = get_logger(__name__)

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]


@dataclass(frozen=True, slots=True)
class SandboxQueueContext:
    provider: SandboxProvider = field(repr=False)
    pool_id: str
    engine: Engine = field(repr=False)
    poll_interval_seconds: float = 1.0

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
) -> SandboxQueueContext:
    provider_pool_id = provider.admission_pool_id
    if provider_pool_id is None:
        raise ValueError("Sandbox provider does not support queued admission")

    return SandboxQueueContext(
        provider=provider,
        pool_id=queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=poll_interval_seconds,
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
        for reservation in lock_abandoned_reservations(session, context.pool_id):
            name = queued_sandbox_name(reservation.task_row_id, reservation.attempt_started_at)
            try:
                sandbox = await context.provider.get_sandbox(name)
                if sandbox.name != name:
                    logger.error("sandbox.admission.recovery_identity_mismatch", extra={"sandbox_name": name})
                    continue

                if not await delete_sandbox(sandbox, context.provider, initiated_by="orphan_cleanup"):
                    continue
            except SandboxNotFoundError:
                pass
            except SandboxError:
                logger.warning("sandbox.admission.recovery_failed", extra={"sandbox_name": name}, exc_info=True)
                continue

            release_reservation(session, reservation.task_row_id, reservation.attempt_started_at)

        reset_abandoned_builds(session, context.pool_id, datetime.now(UTC))
        session.commit()


async def recover_queued_pool(context: SandboxQueueContext) -> None:
    """Reset abandoned builds when the pool lock is free; retry on the next coordinator pass."""
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


_CAPACITY_HEADROOM = 0.10


async def _reservable_demand(
    context: SandboxQueueContext, source: SandboxSource, resources: Resources
) -> Resources | None:
    """Ask the provider what this build will consume; None keeps the task on the full-lock path."""
    try:
        return await context.provider.reservable_demand(source, resources)
    except SandboxError:
        logger.warning("sandbox.admission.demand_failed", extra={"pool_id": context.pool_id}, exc_info=True)
        return None


def _is_eligible_without_lock(
    engine: Engine, pool_id: str, task_row_id: UUID, expected_started_at: datetime, *, by_capacity: bool
) -> bool | None:
    """Cheap pre-check so only admissible tasks contend for the pool lock; None means gone.

    Reserved builds are eligible whenever their run has room; full-lock builds only as
    the pool head.
    """
    with Session(engine) as session:
        if eligible_task_is(session, pool_id, task_row_id, expected_started_at, head_only=not by_capacity):
            return True
        return False if _is_waiting(session, task_row_id, expected_started_at) else None


async def _reservable_capacity(context: SandboxQueueContext) -> SandboxCapacity | None:
    """Read provider capacity; None keeps the task on the full-lock path."""
    try:
        return await context.provider.get_capacity()
    except SandboxError:
        logger.warning("sandbox.admission.capacity_failed", extra={"pool_id": context.pool_id}, exc_info=True)
        return None


def _fits(resources: Resources, capacity: SandboxCapacity, reserved: ActiveReservations) -> bool:
    """Live provider usage misses reserved builds still being created.

    ``reserved`` must be read before ``capacity``: a build promoted in between then
    counts in both, never in neither. The pool keeps ``_CAPACITY_HEADROOM`` of its total
    free for concurrent claimers whose reservations are not visible to each other yet.
    """
    return (
        resources.vcpu <= capacity.cpu.available - reserved.vcpu - capacity.cpu.total * _CAPACITY_HEADROOM
        and resources.memory <= capacity.memory.available - reserved.memory - capacity.memory.total * _CAPACITY_HEADROOM
        and resources.disk <= capacity.disk.available - reserved.disk - capacity.disk.total * _CAPACITY_HEADROOM
    )


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
) -> Sandbox | None:
    """Wait for this exact attempt's global turn and enter its sandbox context.

    On a reserved queue, builds whose demand the provider can state up front hold a
    capacity reservation and their task build lock instead of the pool lock while the
    sandbox is created; the build lock keeps recovery away from the attempt until it
    is started or torn down. Every other build waits for all capacity reservations to
    drain, then keeps the pool lock for the whole creation.
    """
    demand = (
        await _reservable_demand(context, source, resources) if is_reserved_queue_pool_id(context.pool_id) else None
    )
    reserves = demand is not None
    build_lock = task_build_lock(context.engine, task_row_id)
    try:
        while True:
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
                await asyncio.sleep(context.poll_interval_seconds)
                continue

            # Provider reads happen before the pool lock so reserved builds claim in parallel.
            capacity = None
            if reserves:
                assert demand is not None
                with Session(context.engine) as session:
                    reserved = reserved_builds(session, context.pool_id)
                capacity = await _reservable_capacity(context)
                if capacity and (
                    not _fits(demand, capacity, sum_reservations(reserved))
                    or not await context.provider.check_admission(source, resources)
                ):
                    await asyncio.sleep(context.poll_interval_seconds)
                    continue
            reservation = demand if capacity else None

            lock = queue_pool_lock(context.engine, context.pool_id)
            async with lock as acquired:
                if acquired:
                    await _reset_abandoned_pool_builds(lock.connection, context)
                    try:
                        with Session(lock.connection) as session:
                            reserved_now = reserved_builds(session, context.pool_id)
                            session.rollback()

                        if reservation and capacity:
                            # Re-read capacity under the lock, after the reservations: a build that claimed and
                            # promoted since the first read is otherwise in neither count.
                            capacity = await _reservable_capacity(context)
                            admit = (
                                capacity is not None
                                and _fits(reservation, capacity, sum_reservations(reserved_now))
                                and await build_lock.acquire()
                            )
                        else:
                            admit = not reserved_now and await context.provider.check_admission(source, resources)

                        claimed = False
                        if admit:
                            with Session(lock.connection) as session:
                                lock_execution_authority(session, authority)
                                claimed = claim_eligible_task(
                                    session, context.pool_id, task_row_id, expected_started_at, reservation=reservation
                                )
                                if claimed:
                                    session.commit()
                                elif not _is_waiting(session, task_row_id, expected_started_at):
                                    return None
                        if not claimed:
                            await build_lock.release()
                        elif not reservation:
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

            if build_lock.held:
                return await _start_sandbox(
                    stack=stack,
                    bind=build_lock.connection,
                    task_row_id=task_row_id,
                    expected_started_at=expected_started_at,
                    authority=authority,
                    create=create,
                )

            await asyncio.sleep(context.poll_interval_seconds)
    finally:
        try:
            if build_lock.held and not creation_state.cleanup_pending:
                with Session(build_lock.connection) as session:
                    release_reservation(session, task_row_id, expected_started_at)
                    session.commit()
        finally:
            await build_lock.release()
