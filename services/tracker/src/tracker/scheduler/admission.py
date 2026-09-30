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
    SandboxError,
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
    queue_pool_id,
    queue_pool_lock,
    queue_pool_lock_id,
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
    if result.rowcount != 1:
        return False

    release_reservation(session, task_row_id, expected_started_at)
    return True


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


async def _reservable_capacity(context: SandboxQueueContext) -> SandboxCapacity | None:
    """Read provider capacity; None keeps the task on the full-lock path."""
    try:
        return await context.provider.get_capacity()
    except SandboxError:
        logger.warning("sandbox.admission.capacity_failed", extra={"pool_id": context.pool_id}, exc_info=True)
        return None


async def _reserve(
    session: Session,
    context: SandboxQueueContext,
    build_lock: PostgresAdvisoryLock,
    *,
    task_row_id: UUID,
    expected_started_at: datetime,
    resources: Resources,
    capacity: SandboxCapacity,
    reserved: ActiveReservations,
) -> bool:
    """Claim the head with a reservation, holding its build lock on success.

    ``reserved`` must be read before ``capacity``: a build promoted in between then
    counts in both, never in neither.
    """
    fits = (
        resources.vcpu <= capacity.cpu.available - reserved.vcpu
        and resources.memory <= capacity.memory.available - reserved.memory
        and resources.disk <= capacity.disk.available - reserved.disk
    )
    if not fits or not await build_lock.acquire():
        return False
    if claim_eligible_task_with_reservation(session, context.pool_id, task_row_id, expected_started_at, resources):
        return True

    await build_lock.release()
    return False


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
    create: SandboxFactory,
) -> Sandbox | None:
    """Wait for this exact attempt's global turn and enter its sandbox context.

    On a reserved queue, image builds with known demand hold a capacity reservation
    and their task build lock instead of the pool lock while the sandbox is created;
    the build lock keeps recovery away from the attempt until it is started or torn
    down. Every other build keeps the pool lock for the whole creation.
    """
    reserves = is_reserved_queue_pool_id(context.pool_id) and _has_exact_demand(source, resources)
    build_lock = task_build_lock(context.engine, task_row_id)
    try:
        while True:
            lock = queue_pool_lock(context.engine, context.pool_id)
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

                    admissible = eligible and (not reserves or reserved.count < SANDBOX_QUEUE_BUILDING_CAP)
                    if admissible and await context.provider.check_admission(source, resources):
                        capacity = await _reservable_capacity(context) if reserves else None
                        with Session(lock.connection) as session:
                            try:
                                lock_execution_authority(session, authority)
                            except ExecutionAuthorityRevoked:
                                session.rollback()
                                return None
                            if capacity is None:
                                claimed = claim_eligible_task(
                                    session,
                                    context.pool_id,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                )
                            else:
                                claimed = await _reserve(
                                    session,
                                    context,
                                    build_lock,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                    resources=resources,
                                    capacity=capacity,
                                    reserved=reserved,
                                )
                            if claimed:
                                session.commit()
                            else:
                                waiting = _queued_task_state(
                                    session,
                                    task_row_id,
                                    expected_started_at,
                                ) == (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS)
                                session.rollback()
                                if not waiting:
                                    return None

                        if claimed and capacity is None:
                            return await _start_sandbox(
                                stack=stack,
                                bind=lock.connection,
                                task_row_id=task_row_id,
                                expected_started_at=expected_started_at,
                                authority=authority,
                                create=create,
                            )

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
        await build_lock.release()
