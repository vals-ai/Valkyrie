"""Read-only snapshot of queued and active sandbox work."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import JSON, select as sa_select, type_coerce
from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import Session, col, func, select
from sqlmodel.sql.expression import Select

from tracker.auth import get_current_org
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.database.session import get_session
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


def _queued_benchmarks_expression():
    arguments = type_coerce(col(Benchmark.arguments), JSON)

    return arguments["queue_pool_id"].as_string().is_not(None)


def _read_waiting_rows(
    *,
    session: Session,
    org_id: UUID,
    limit: int,
) -> tuple[list[tuple[Task, Benchmark, int, str, int]], dict[str, int]]:
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    pool_id = arguments["queue_pool_id"].as_string()
    queued_tasks = (
        sa_select(
            cast(ColumnElement[UUID], col(Task.id).label("task_id")),
            priority.label("priority"),
            pool_id.label("pool_id"),
            func.row_number()
            .over(
                partition_by=pool_id,
                order_by=(priority.asc(), col(Task.started_at).asc(), col(Task.id).asc()),
            )
            .label("position"),
        )
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(
            col(Task.status) == TaskStatus.PENDING,
            col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
            _queued_benchmarks_expression(),
        )
        .subquery()
    )
    scope = (col(Task.org_id) == org_id, col(Benchmark.org_id) == org_id)
    pool_counts_statement = cast(
        Select[tuple[str, int]],
        sa_select(queued_tasks.c.pool_id, func.count(queued_tasks.c.task_id))
        .join(Task, col(Task.id) == queued_tasks.c.task_id)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(*scope)
        .group_by(queued_tasks.c.pool_id),
    )
    pool_counts = dict(session.exec(pool_counts_statement).all())
    rows_statement = cast(
        Select[tuple[Task, Benchmark, int, str, int]],
        sa_select(
            Task,
            Benchmark,
            queued_tasks.c.priority,
            queued_tasks.c.pool_id,
            queued_tasks.c.position,
        )
        .join(queued_tasks, col(Task.id) == queued_tasks.c.task_id)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(*scope)
        .order_by(queued_tasks.c.priority.asc(), col(Task.started_at).asc(), col(Task.id).asc())
        .limit(limit + 1),
    )
    rows = list(session.exec(rows_statement).all())

    return rows, pool_counts


def _read_active_rows(
    *,
    session: Session,
    org_id: UUID,
    limit: int,
) -> tuple[list[tuple[Task, Benchmark]], dict[TaskStatus, int]]:
    scope = (
        col(Task.org_id) == org_id,
        col(Benchmark.org_id) == org_id,
        col(Benchmark.status).in_((BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)),
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
        .execution_options(populate_existing=True)
    ).all()

    return list(rows), counts


def read_scheduler_overview(
    *,
    session: Session,
    org_id: UUID,
    now: datetime,
    waiting_limit: int,
    active_limit: int,
) -> SchedulerOverviewResponse:
    waiting_rows, pool_counts = _read_waiting_rows(session=session, org_id=org_id, limit=waiting_limit)
    active_rows, active_counts = _read_active_rows(session=session, org_id=org_id, limit=active_limit)
    waiting_entries = [
        SchedulerWaitingEntryResponse(
            benchmark_uuid=benchmark.id,
            task_uuid=task.id,
            benchmark_name=benchmark.name,
            external_task_id=task.task_id,
            started_by_email=benchmark.started_by_email,
            pool_id=pool_id,
            position=position,
            priority=priority,
            enqueued_at=task.started_at,
        )
        for task, benchmark, priority, pool_id, position in waiting_rows[:waiting_limit]
    ]
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
            waiting=sum(pool_counts.values()),
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


@router.get("/overview", response_model=SchedulerOverviewResponse)
def get_scheduler_overview(
    waiting_limit: int = Query(default=100, ge=1, le=200),
    active_limit: int = Query(default=100, ge=1, le=200),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> SchedulerOverviewResponse:
    return read_scheduler_overview(
        session=session,
        org_id=org.id,
        now=datetime.now(UTC),
        waiting_limit=waiting_limit,
        active_limit=active_limit,
    )
