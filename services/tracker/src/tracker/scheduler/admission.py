"""PostgreSQL-backed provider admission for queued sandbox creation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from benchmark_service import Resources, Sandbox, SandboxProvider, SandboxSource, TargetedSnapshotSource
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session, col, select, update

from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.exceptions import SandboxError
from tracker.scheduler.store import (
    PostgresPoolLock,
    claim_eligible_task,
    eligible_task_is,
    queue_pool_id,
    reset_abandoned_builds,
)

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]


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
    session.commit()

    return result.rowcount == 1


def _reset_abandoned_pool_builds(connection: Connection, pool_id: str) -> None:
    with Session(connection) as session:
        reset_abandoned_builds(session, pool_id, datetime.now(UTC))
        session.commit()


async def recover_queued_pool(context: SandboxQueueContext) -> None:
    """Reset abandoned builds once while holding this provider pool's lock."""
    while True:
        lock = PostgresPoolLock(context.engine, context.pool_id)
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


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    source: SandboxSource,
    resources: Resources,
    create: SandboxFactory,
) -> Sandbox | None:
    """Wait for this exact attempt's global turn and enter its sandbox context."""
    if isinstance(source, TargetedSnapshotSource):
        raise SandboxError("Queued admission does not support targeted snapshots")

    while True:
        lock = PostgresPoolLock(context.engine, context.pool_id)
        async with lock as acquired:
            if acquired:
                _reset_abandoned_pool_builds(lock.connection, context.pool_id)
                with Session(lock.connection) as session:
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

                if not waiting:
                    return None

                if eligible and await context.provider.check_admission(source, resources):
                    with Session(lock.connection) as session:
                        claimed = claim_eligible_task(
                            session,
                            context.pool_id,
                            task_row_id=task_row_id,
                            expected_started_at=expected_started_at,
                        )
                        if not claimed and _queued_task_state(
                            session,
                            task_row_id,
                            expected_started_at,
                        ) != (TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS):
                            return None

                    if claimed:
                        sandbox = await stack.enter_async_context(create())
                        with Session(lock.connection) as session:
                            started = _start_claimed_task(
                                session,
                                task_row_id=task_row_id,
                                expected_started_at=expected_started_at,
                            )
                        if not started:
                            await _close_stack_before_cancellation(stack)

                            return None

                        return sandbox

        await asyncio.sleep(context.poll_interval_seconds)
