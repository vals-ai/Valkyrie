"""PostgreSQL-backed provider admission for queued sandbox creation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from benchmark_service import (
    ComposeSource,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxProvider,
    SandboxSource,
)
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session, col, select, update

from tracker.config import SANDBOX_QUEUE_BUILDING_CAP
from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.scheduler.store import (
    ActiveReservations,
    PostgresAdvisoryLock,
    active_reservations,
    claim_eligible_task,
    claim_eligible_task_with_reservation,
    eligible_task_is,
    is_reserved_queue_pool_id,
    promote_reserved_task,
    queue_pool_id,
    queue_pool_lock,
    queue_pool_lock_id,
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

    @property
    def reserves_capacity(self) -> bool:
        return is_reserved_queue_pool_id(self.pool_id)

    def serves_queue(self, pool_id: str) -> bool:
        """Return whether a run's persisted queue id belongs to this provider pool."""
        return queue_pool_lock_id(pool_id) == queue_pool_lock_id(self.pool_id)

    def for_queue(self, pool_id: str) -> SandboxQueueContext:
        """Adopt a run's persisted queue id, which selects its admission protocol."""
        if not self.serves_queue(pool_id):
            raise ValueError("Queue id belongs to another provider pool")
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


def _queued_task_state(
    session: Session,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> tuple[TaskStatus, BenchmarkStatus] | None:
    raw_state = cast(
        tuple[str, str] | None,
        session.exec(
            select(col(Task.status), col(Benchmark.status))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
        ).first(),
    )
    if raw_state is None:
        return None

    task_status, benchmark_status = raw_state

    return TaskStatus(task_status), BenchmarkStatus(benchmark_status)


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

    return result.rowcount == 1


def _reset_abandoned_pool_builds(connection: Connection, pool_id: str) -> None:
    with Session(connection) as session:
        reset_abandoned_builds(session, pool_id, datetime.now(UTC))
        session.commit()


async def recover_queued_pool(context: SandboxQueueContext) -> None:
    """Reset abandoned builds once while holding this provider pool's lock."""
    while True:
        lock = queue_pool_lock(context.engine, context.pool_id)
        async with lock as acquired:
            if acquired:
                _reset_abandoned_pool_builds(lock.connection, context.pool_id)

                return

        await asyncio.sleep(context.poll_interval_seconds)


async def _close_stack_before_cancellation(stack: AsyncExitStack) -> None:
    close_task = asyncio.create_task(stack.aclose())
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError:
        await close_task
        raise


def _has_exact_demand(source: SandboxSource, resources: Resources) -> bool:
    """Image sandboxes without GPUs are the only sources whose provider usage is known up front."""
    return resources.gpu == 0 and (
        isinstance(source, ImageSource) or (isinstance(source, ComposeSource) and isinstance(source.outer, ImageSource))
    )


def _fits_unreserved_capacity(
    capacity: SandboxCapacity,
    resources: Resources,
    reserved: ActiveReservations,
) -> bool:
    return (
        resources.vcpu <= capacity.cpu.available - reserved.vcpu
        and resources.memory <= capacity.memory.available - reserved.memory
        and resources.disk <= capacity.disk.available - reserved.disk
    )


async def _reservable_capacity(context: SandboxQueueContext) -> SandboxCapacity | None:
    """Read provider capacity; None keeps the task on the full-lock path."""
    try:
        return await context.provider.get_capacity()
    except Exception:
        logger.warning(
            "sandbox.admission.capacity_failed",
            extra={"pool_id": context.pool_id},
            exc_info=True,
        )
        return None


async def _claim_reserved_build(
    *,
    session: Session,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    capacity: SandboxCapacity,
    reserved: ActiveReservations,
    resources: Resources,
) -> PostgresAdvisoryLock | None:
    """Claim the head with a reservation and return the build lock proving its creator is alive.

    ``reserved`` must be read before ``capacity``: a build promoted in between then
    counts in both, never in neither.
    """
    if not _fits_unreserved_capacity(capacity, resources, reserved):
        return None

    build_lock = task_build_lock(context.engine, task_row_id)
    if not await build_lock.acquire():
        return None
    try:
        claimed = claim_eligible_task_with_reservation(
            session,
            context.pool_id,
            task_row_id=task_row_id,
            expected_started_at=expected_started_at,
            requested_vcpu=resources.vcpu,
            requested_memory=resources.memory,
            requested_disk=resources.disk,
            requested_gpu=resources.gpu,
        )
        if claimed:
            session.commit()
            return build_lock
    except BaseException:
        await build_lock.release()
        raise

    await build_lock.release()
    return None


async def _finish_reserved_build(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    create: SandboxFactory,
    build_lock: PostgresAdvisoryLock,
) -> Sandbox | None:
    """Create outside the pool lock, then promote the exact reserved attempt.

    Promotion needs no pool lock: the build lock keeps recovery away from this
    attempt, and the reservation is released in the same transaction that moves
    the task to IN_PROGRESS. The build lock is released last so recovery only
    requeues this attempt, and only drops its reservation, once creation and any
    cleanup have finished.
    """
    try:
        sandbox = await stack.enter_async_context(create())
        with Session(bind=context.engine) as session:
            try:
                lock_execution_authority(session, authority)
            except ExecutionAuthorityRevoked:
                session.rollback()
                promoted = False
            else:
                promoted = promote_reserved_task(
                    session,
                    task_row_id=task_row_id,
                    expected_started_at=expected_started_at,
                )
                if promoted:
                    session.commit()
                else:
                    session.rollback()

        if not promoted:
            await _close_stack_before_cancellation(stack)

            return None

        return sandbox
    finally:
        await build_lock.release()


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    source: SandboxSource,
    resources: Resources,
    create: SandboxFactory,
) -> Sandbox | None:
    """Wait for this exact attempt's global turn and enter its sandbox context.

    Reserved queues let image builds with known demand hold a capacity reservation
    instead of the pool lock while their sandbox is created. Every other build, and
    every build whose capacity read fails, keeps the lock for the whole creation.
    """
    reserves = context.reserves_capacity and _has_exact_demand(source, resources)
    while True:
        build_lock: PostgresAdvisoryLock | None = None
        lock = queue_pool_lock(context.engine, context.pool_id)
        try:
            async with lock as acquired:
                if acquired:
                    _reset_abandoned_pool_builds(lock.connection, context.pool_id)
                    with Session(lock.connection) as session:
                        try:
                            lock_execution_authority(session, authority)
                        except ExecutionAuthorityRevoked:
                            session.rollback()
                            return None
                        eligible = eligible_task_is(
                            session,
                            context.pool_id,
                            task_row_id,
                            expected_started_at,
                        )
                        waiting = eligible or _queued_task_state(
                            session,
                            task_row_id,
                            expected_started_at,
                        ) == (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS)
                        reserved = active_reservations(session, context.pool_id)
                        session.rollback()

                    if not waiting:
                        return None

                    below_cap = not reserves or reserved.count < SANDBOX_QUEUE_BUILDING_CAP
                    if eligible and below_cap and await context.provider.check_admission(source, resources):
                        capacity = await _reservable_capacity(context) if reserves else None
                        with Session(lock.connection) as session:
                            try:
                                lock_execution_authority(session, authority)
                            except ExecutionAuthorityRevoked:
                                session.rollback()
                                return None
                            if capacity is not None:
                                build_lock = await _claim_reserved_build(
                                    session=session,
                                    context=context,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                    capacity=capacity,
                                    reserved=reserved,
                                    resources=resources,
                                )
                                claimed = build_lock is not None
                            else:
                                claimed = claim_eligible_task(
                                    session,
                                    context.pool_id,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                )
                                if claimed:
                                    session.commit()
                            if not claimed:
                                waiting = _queued_task_state(
                                    session,
                                    task_row_id,
                                    expected_started_at,
                                ) == (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS)
                                session.rollback()
                                if not waiting:
                                    return None

                        if claimed and build_lock is None:
                            sandbox = await stack.enter_async_context(create())
                            with Session(lock.connection) as session:
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
                            if not started:
                                await _close_stack_before_cancellation(stack)

                                return None

                            return sandbox
        except BaseException:
            if build_lock is not None:
                await build_lock.release()
            raise

        if build_lock is not None:
            return await _finish_reserved_build(
                stack=stack,
                context=context,
                task_row_id=task_row_id,
                expected_started_at=expected_started_at,
                authority=authority,
                create=create,
                build_lock=build_lock,
            )

        await asyncio.sleep(context.poll_interval_seconds)
