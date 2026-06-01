"""Single-run detail endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, col, func, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Benchmark, Org, Task, TaskStatus, User
from tracker.database.session import get_session
from tracker.types import (
    SingleBenchmarkResponse,
    TaskSummary,
    TasksResponse,
)

router = APIRouter()


def _escape_sql_like_pattern(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _load_benchmark_or_404(benchmark_id: UUID, org: Org, session: Session) -> Benchmark:
    bench = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()
    if bench is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")
    return bench


@router.get("/benchmarks/{benchmark_id}", response_model=SingleBenchmarkResponse)
def get_single_benchmark(
    benchmark_id: UUID,
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> SingleBenchmarkResponse:
    _, org = user_and_org
    bench = _load_benchmark_or_404(benchmark_id, org, session)

    task_state_counts = bench.fetch_task_state_counts(session)
    total = sum(task_state_counts.values())
    finished = task_state_counts.get(TaskStatus.FINISHED, 0) + task_state_counts.get(TaskStatus.ERROR, 0)

    run_by_email: str | None = None
    if bench.run_by_id is not None:
        user_obj = session.get(User, bench.run_by_id)
        run_by_email = user_obj.email if user_obj else None

    return SingleBenchmarkResponse(
        id=bench.id,
        name=bench.name,
        agent_name=bench.arguments.contract.name,
        model=bench.arguments.contract.model,
        started_at=bench.started_at,
        finished_at=bench.finished_at,
        status=bench.status,
        total_tasks=total,
        finished_tasks=finished,
        task_state_counts={k.value: v for k, v in task_state_counts.items()},
        run_by_email=run_by_email,
        final_score=bench.fetch_final_score(session),
    )


def _parse_task_statuses(status_csv: str | None) -> list[TaskStatus]:
    """Parse a CSV string of TaskStatus values into a list."""
    if not status_csv:
        return []
    result: list[TaskStatus] = []
    for token in status_csv.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            result.append(TaskStatus(token))
        except ValueError:
            continue
    return result


@router.get("/benchmarks/{benchmark_id}/tasks", response_model=TasksResponse)
def get_benchmark_tasks(
    benchmark_id: UUID,
    status: str | None = Query(default=None, description="Comma-separated TaskStatus values"),
    task_id_search: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> TasksResponse:
    _, org = user_and_org
    _load_benchmark_or_404(benchmark_id, org, session)

    base_filters = [
        col(Task.benchmark) == benchmark_id,
        col(Task.org_id) == org.id,
    ]
    statuses = _parse_task_statuses(status)
    if len(statuses) == 1:
        base_filters.append(col(Task.status) == statuses[0])
    elif len(statuses) > 1:
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
                id=t.id,
                task_id=t.task_id,
                status=t.status,
                started_at=t.started_at,
                finished_at=t.finished_at,
                error_message=t.error_message,
            )
            for t in rows
        ],
        total_count=total,
    )
