"""PostgreSQL-backed provider admission for queued sandbox creation."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
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
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, col, select, update

from tracker.config import SANDBOX_QUEUE_BUILDING_CAP
from tracker.database.models import Benchmark, BenchmarkStatus, SandboxBuildReservation, Task, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.scheduler.store import (
    ReservationResourceTotals,
    active_reservation_resources,
    building_task_count,
    claim_eligible_task,
    claim_eligible_task_with_reservation,
    eligible_task_is,
    promote_reserved_task,
    queue_pool_id,
    queue_pool_lock,
    reset_abandoned_builds,
)

logger = get_logger(__name__)

SandboxFactory = Callable[[Callable[[], None] | None], AbstractAsyncContextManager[Sandbox]]


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


def _supports_reserved_admission(source: SandboxSource) -> bool:
    return isinstance(source, ImageSource) or (
        isinstance(source, ComposeSource) and isinstance(source.outer, ImageSource)
    )


def _has_reserved_capacity(
    capacity: SandboxCapacity,
    resources: Resources,
    reserved: ReservationResourceTotals,
) -> bool:
    if resources.gpu > 0:
        if capacity.gpu is None:
            return False
        if capacity.allowed_gpu_types is not None and (
            not capacity.allowed_gpu_types
            or (resources.gpu_type is not None and resources.gpu_type not in capacity.allowed_gpu_types)
        ):
            return False
        gpu_available = capacity.gpu.available - reserved.gpu
    else:
        gpu_available = 0

    return (
        resources.vcpu <= capacity.cpu.available - reserved.vcpu
        and resources.memory <= capacity.memory.available - reserved.memory
        and resources.disk <= capacity.disk.available - reserved.disk
        and resources.gpu <= gpu_available
    )


async def _capacity_for_reserved_admission(
    context: SandboxQueueContext,
    source: SandboxSource,
) -> SandboxCapacity | None:
    if not _supports_reserved_admission(source):
        return None

    try:
        return await context.provider.get_capacity()
    except Exception:
        logger.warning(
            "sandbox.admission.capacity_failed",
            extra={"pool_id": context.pool_id},
            exc_info=True,
        )
        return None


async def _finish_reserved_build(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    create: SandboxFactory,
) -> Sandbox | None:
    cleanup_confirmed = False
    promoted = False

    def confirm_cleanup() -> None:
        nonlocal cleanup_confirmed
        cleanup_confirmed = True

    try:
        sandbox = await stack.enter_async_context(create(confirm_cleanup))
        while True:
            lock = queue_pool_lock(context.engine, context.pool_id)
            async with lock as acquired:
                if acquired:
                    # Keep the hold visible until an in-flight capacity read and
                    # claim completes. Otherwise its snapshot could miss this build.
                    with Session(lock.connection) as session:
                        try:
                            lock_execution_authority(session, authority)
                        except ExecutionAuthorityRevoked:
                            session.rollback()
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
                    break
            await asyncio.sleep(context.poll_interval_seconds)

        return sandbox if promoted else None
    finally:
        if not promoted:
            try:
                await _close_stack_before_cancellation(stack)
            finally:
                # A failed/unknown create or delete needs manual reconciliation.
                # Task state and dispatch revocation do not prove capacity is free.
                active_error = sys.exception()
                try:
                    with Session(context.engine) as session:
                        benchmark = session.exec(
                            select(Benchmark)
                            .join(Task, col(Task.benchmark) == col(Benchmark.id))
                            .where(col(Task.id) == task_row_id)
                            .with_for_update(of=Benchmark)
                        ).one()
                        task = session.exec(select(Task).where(col(Task.id) == task_row_id).with_for_update()).one()
                        reservation = session.exec(
                            select(SandboxBuildReservation)
                            .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
                            .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
                        ).one_or_none()
                        if reservation is not None:
                            if cleanup_confirmed:
                                session.delete(reservation)
                                session.flush()
                            if task.started_at == expected_started_at and task.status == TaskStatus.BUILDING:
                                if benchmark.status == BenchmarkStatus.IN_PROGRESS:
                                    if cleanup_confirmed and active_error is None:
                                        task.status = TaskStatus.PENDING
                                        task.started_at = datetime.now(UTC)
                                elif benchmark.status in (BenchmarkStatus.STOPPING, BenchmarkStatus.STOPPED):
                                    task.status = TaskStatus.STOPPED
                                else:
                                    task.status = TaskStatus.ERROR
                                session.add(task)
                            session.commit()
                        else:
                            session.rollback()
                except SQLAlchemyError:
                    logger.warning(
                        "sandbox.admission.finalization_failed",
                        extra={"pool_id": context.pool_id, "task_row_id": str(task_row_id)},
                        exc_info=True,
                    )
                    if active_error is None:
                        raise
                if not cleanup_confirmed:
                    logger.warning(
                        "sandbox.admission.cleanup_unconfirmed",
                        extra={"pool_id": context.pool_id, "task_row_id": str(task_row_id)},
                    )


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
    """Wait for this exact attempt's global turn and enter its sandbox context."""
    while True:
        reserved_build = False
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
                    session.rollback()

                if not waiting:
                    return None

                if eligible and await context.provider.check_admission(source, resources):
                    capacity = await _capacity_for_reserved_admission(context, source)
                    with Session(lock.connection) as session:
                        try:
                            lock_execution_authority(session, authority)
                        except ExecutionAuthorityRevoked:
                            session.rollback()
                            return None

                        under_building_cap = building_task_count(session, context.pool_id) < SANDBOX_QUEUE_BUILDING_CAP
                        if capacity is None:
                            has_reservations = (
                                session.exec(
                                    select(SandboxBuildReservation.task_row_id)
                                    .where(SandboxBuildReservation.pool_id == context.pool_id)
                                    .limit(1)
                                ).first()
                                is not None
                            )
                            claimed = (
                                under_building_cap
                                and not has_reservations
                                and claim_eligible_task(
                                    session,
                                    context.pool_id,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                )
                            )
                        else:
                            reserved = active_reservation_resources(session, context.pool_id)
                            claimed = (
                                under_building_cap
                                and _has_reserved_capacity(capacity, resources, reserved)
                                and claim_eligible_task_with_reservation(
                                    session,
                                    context.pool_id,
                                    task_row_id=task_row_id,
                                    expected_started_at=expected_started_at,
                                    requested_vcpu=resources.vcpu,
                                    requested_memory=resources.memory,
                                    requested_disk=resources.disk,
                                    requested_gpu=resources.gpu,
                                )
                            )

                        if claimed:
                            session.commit()
                            reserved_build = capacity is not None
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
                        sandbox = await stack.enter_async_context(create(None))
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

        if reserved_build:
            return await _finish_reserved_build(
                stack=stack,
                context=context,
                task_row_id=task_row_id,
                expected_started_at=expected_started_at,
                authority=authority,
                create=create,
            )

        await asyncio.sleep(context.poll_interval_seconds)
