"""Read-only snapshot of queued and active sandbox work."""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncGenerator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis
from sqlalchemy import JSON, type_coerce
from sqlmodel import Session, col, func, select

from tracker.auth import get_current_org
from tracker.config import REDIS_URL
from tracker.database.models import Benchmark, Org, Task, TaskStatus
from tracker.database.session import get_session
from tracker.scheduler.gate import QueueSnapshotEntry, RedisQueueGate
from tracker.types import (
    SchedulerActiveEntryResponse,
    SchedulerActiveStatus,
    SchedulerOverviewResponse,
    SchedulerPoolResponse,
    SchedulerSummaryResponse,
    SchedulerWaitingEntryResponse,
)

router = APIRouter(prefix="/scheduler")

_ACTIVE_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)


@dataclass(frozen=True, slots=True)
class _QueuedTicket:
    pool_id: str
    benchmark_id: UUID
    task_id: UUID
    priority: int
    task_key: str
    enqueued_at: datetime


def _parse_ticket(ticket: QueueSnapshotEntry) -> _QueuedTicket | None:
    try:
        benchmark_id, task_id, _attempt = ticket.task_key.split(":", 2)
        return _QueuedTicket(
            pool_id=ticket.pool_id,
            benchmark_id=UUID(benchmark_id),
            task_id=UUID(task_id),
            priority=ticket.priority,
            task_key=ticket.task_key,
            enqueued_at=ticket.enqueued_at,
        )
    except ValueError:
        return None


def _queued_benchmarks_expression():
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    return arguments["priority"].as_integer().is_not(None)


def _resolve_waiting_rows(
    *,
    session: Session,
    org_id: UUID,
    tickets: Sequence[_QueuedTicket],
) -> list[tuple[_QueuedTicket, Task, Benchmark]]:
    if not tickets:
        return []

    task_ids = {ticket.task_id for ticket in tickets}
    rows = session.exec(
        select(Task, Benchmark)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(
            col(Task.id).in_(task_ids),
            Task.org_id == org_id,
            Benchmark.org_id == org_id,
            Task.status == TaskStatus.PENDING,
            _queued_benchmarks_expression(),
        )
    ).all()
    by_identity = {(benchmark.id, task.id): (task, benchmark) for task, benchmark in rows}
    return [
        (ticket, *row)
        for ticket in tickets
        if (row := by_identity.get((ticket.benchmark_id, ticket.task_id))) is not None
    ]


def _read_active_rows(
    *,
    session: Session,
    org_id: UUID,
    limit: int,
) -> tuple[list[tuple[Task, Benchmark]], dict[TaskStatus, int]]:
    scope = (
        col(Task.org_id) == org_id,
        col(Benchmark.org_id) == org_id,
        col(Task.status).in_(_ACTIVE_STATUSES),
        _queued_benchmarks_expression(),
    )
    counts = dict(
        session.exec(
            select(col(Task.status), func.count(col(Task.id)))
            .select_from(Task)
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(*scope)
            .group_by(col(Task.status))
        ).all()
    )
    rows = session.exec(
        select(Task, Benchmark)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(*scope)
        .order_by(col(Task.started_at).asc(), col(Task.id).asc())
        .limit(limit + 1)
    ).all()
    return list(rows), counts


async def read_scheduler_overview(
    *,
    gate: RedisQueueGate,
    session: Session,
    org_id: UUID,
    now: datetime,
    waiting_limit: int,
    active_limit: int,
) -> SchedulerOverviewResponse:
    tickets = [parsed for ticket in await gate.snapshot() if (parsed := _parse_ticket(ticket)) is not None]
    positions_by_pool: Counter[str] = Counter()
    global_positions: dict[tuple[str, str], int] = {}
    for ticket in tickets:
        positions_by_pool[ticket.pool_id] += 1
        global_positions[(ticket.pool_id, ticket.task_key)] = positions_by_pool[ticket.pool_id]

    waiting_rows = _resolve_waiting_rows(
        session=session,
        org_id=org_id,
        tickets=tickets,
    )
    active_rows, active_counts = _read_active_rows(session=session, org_id=org_id, limit=active_limit)

    pool_counts = Counter(ticket.pool_id for ticket, _, _ in waiting_rows)
    waiting_entries: list[SchedulerWaitingEntryResponse] = []
    for ticket, task, benchmark in waiting_rows[:waiting_limit]:
        waiting_entries.append(
            SchedulerWaitingEntryResponse(
                benchmark_uuid=benchmark.id,
                task_uuid=task.id,
                benchmark_name=benchmark.name,
                external_task_id=task.task_id,
                started_by_email=benchmark.started_by_email,
                pool_id=ticket.pool_id,
                position=global_positions[(ticket.pool_id, ticket.task_key)],
                priority=ticket.priority,
                enqueued_at=ticket.enqueued_at,
            )
        )

    active_entries = [
        SchedulerActiveEntryResponse(
            benchmark_uuid=benchmark.id,
            task_uuid=task.id,
            benchmark_name=benchmark.name,
            external_task_id=task.task_id,
            started_by_email=benchmark.started_by_email,
            status=SchedulerActiveStatus(task.status.value),
            started_at=task.started_at,
        )
        for task, benchmark in active_rows[:active_limit]
    ]

    return SchedulerOverviewResponse(
        observed_at=now,
        summary=SchedulerSummaryResponse(
            waiting=len(waiting_rows),
            building=active_counts.get(TaskStatus.BUILDING, 0),
            in_progress=active_counts.get(TaskStatus.IN_PROGRESS, 0),
            evaluating=active_counts.get(TaskStatus.EVALUATING, 0),
        ),
        pools=[
            SchedulerPoolResponse(pool_id=pool_id, waiting=waiting) for pool_id, waiting in sorted(pool_counts.items())
        ],
        waiting_entries=waiting_entries,
        active_entries=active_entries,
        waiting_capped=len(waiting_rows) > waiting_limit,
        active_capped=len(active_rows) > active_limit,
    )


async def _get_scheduler_redis() -> AsyncGenerator[Redis]:
    redis = Redis.from_url(REDIS_URL)
    try:
        yield redis
    finally:
        await redis.aclose()


@router.get("/overview", response_model=SchedulerOverviewResponse)
async def get_scheduler_overview(
    waiting_limit: int = Query(default=100, ge=1, le=200),
    active_limit: int = Query(default=100, ge=1, le=200),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
    redis: Redis = Depends(_get_scheduler_redis),
) -> SchedulerOverviewResponse:
    return await read_scheduler_overview(
        gate=RedisQueueGate(redis),
        session=session,
        org_id=org.id,
        now=datetime.now(UTC),
        waiting_limit=waiting_limit,
        active_limit=active_limit,
    )
