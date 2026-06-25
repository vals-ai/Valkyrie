"""Single-run detail endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, col, func, select

from tracker.api.parsing import parse_csv
from tracker.auth import get_current_org
from tracker.database.models import Benchmark, Org, Task, TaskStatus
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.types import (
    SingleBenchmarkResponse,
    TaskSummary,
    TasksResponse,
)

router = APIRouter(prefix="/benchmarks")


def _escape_sql_like_pattern(value: str) -> str:
    """Escape \\, %, and _ so user input is treated as a literal in a LIKE clause."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@router.get("/{benchmark_id}", response_model=SingleBenchmarkResponse)
def get_single_benchmark(
    benchmark_id: UUID,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> SingleBenchmarkResponse:
    """Fetch a single benchmark with task counts + final score for the SingleRun page."""
    benchmark = get_scoped(Benchmark, benchmark_id, session, org)

    task_state_counts = benchmark.fetch_task_state_counts(session)
    total = sum(task_state_counts.values())
    finished = (
        task_state_counts.get(TaskStatus.FINISHED, 0)
        + task_state_counts.get(TaskStatus.ERROR, 0)
        + task_state_counts.get(TaskStatus.STOPPED, 0)
    )

    return SingleBenchmarkResponse(
        id=benchmark.id,
        name=benchmark.name,
        agent_name=benchmark.arguments.contract.name,
        model=benchmark.arguments.contract.model,
        started_at=benchmark.started_at,
        finished_at=benchmark.finished_at,
        status=benchmark.status,
        total_tasks=total,
        finished_tasks=finished,
        task_state_counts={status.value: count for status, count in task_state_counts.items()},
        started_by_email=benchmark.started_by_email,
        final_score=benchmark.fetch_final_score(session),
        error_message=benchmark.error_message,
    )


@router.get("/{benchmark_id}/tasks", response_model=TasksResponse)
def get_benchmark_tasks(
    benchmark_id: UUID,
    status: str = Query(default=""),
    task_id_search: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TasksResponse:
    """Paginated tasks for a benchmark, with optional status filter + task-id search."""
    get_scoped(Benchmark, benchmark_id, session, org)

    statuses = parse_csv(status, TaskStatus)
    base_filters = [
        col(Task.benchmark) == benchmark_id,
        col(Task.org_id) == org.id,
    ]
    if statuses:
        base_filters.append(col(Task.status).in_(statuses))

    if task_id_search:
        escaped_search = _escape_sql_like_pattern(task_id_search)
        base_filters.append(col(Task.task_id).ilike(f"%{escaped_search}%", escape="\\"))

    rows = session.exec(
        select(Task).where(*base_filters).order_by(col(Task.started_at).desc()).limit(limit).offset(offset)
    ).all()
    total = session.exec(select(func.count(col(Task.id))).where(*base_filters)).one()

    return TasksResponse(
        tasks=[
            TaskSummary(
                id=task.id,
                task_id=task.task_id,
                status=task.status,
                started_at=task.started_at,
                finished_at=task.finished_at,
                error_message=task.error_message,
            )
            for task in rows
        ],
        total_count=total,
    )
