"""Single-run detail endpoints."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import case
from sqlmodel import Session, col, desc, func, select

from tracker.api.parsing import parse_csv
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.resolver import resolve_run_metadata_aws_runtime
from tracker.aws.s3 import create_benchmark_url
from tracker.database.models import Benchmark, ErrorResult, Org, Task, TaskStatus
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.types import SingleBenchmarkResponse, TasksResponse, TaskSummary

router = APIRouter(prefix="/benchmarks")


def _escape_sql_like_pattern(value: str) -> str:
    """Escape \\, %, and _ so user input is treated as a literal in a LIKE clause."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Attention priority — higher is more urgent, so ORDER BY ... DESC surfaces
# errors first, then abnormal/successful terminal, then active, then queued.
_STATUS_SORT_PRIORITY = case(
    {
        TaskStatus.ERROR: 6,
        TaskStatus.STOPPED: 5,
        TaskStatus.FINISHED: 4,
        TaskStatus.EVALUATING: 3,
        TaskStatus.IN_PROGRESS: 2,
        TaskStatus.BUILDING: 1,
        TaskStatus.PENDING: 0,
    },
    value=col(Task.status),
    else_=-1,
)


@router.get("/{benchmark_id}", response_model=SingleBenchmarkResponse)
def get_single_benchmark(
    benchmark_id: UUID,
    request: Request,
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

    cloudwatch_url: str | None = None
    s3_bucket_url: str | None = None
    aws_runtime = resolve_run_metadata_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        org_id=org.id,
    )
    if aws_runtime:
        aws_resources = aws_runtime.resources
        s3_bucket_url = create_benchmark_url(str(benchmark.id), aws_resources)
        if aws_resources.log_group:
            cloudwatch_url = get_benchmark_log_url(
                benchmark_id=str(benchmark.id),
                resources=aws_resources,
            )

    return SingleBenchmarkResponse(
        id=benchmark.id,
        name=benchmark.name,
        agent_name=benchmark.arguments.contract.name,
        model=benchmark.arguments.contract.model,
        executor_release_id=benchmark.executor_release_id,
        current_execution_release_id=benchmark.current_execution_release_id,
        executor_artifact_digest=benchmark.executor_artifact_digest,
        executor_protocol_version=benchmark.executor_protocol_version,
        started_at=benchmark.started_at,
        finished_at=benchmark.finished_at,
        status=benchmark.status,
        total_tasks=total,
        finished_tasks=finished,
        task_state_counts={status.value: count for status, count in task_state_counts.items()},
        started_by_email=benchmark.started_by_email,
        final_score=benchmark.fetch_final_score(session),
        error_message=benchmark.error_message,
        cloudwatch_url=cloudwatch_url,
        s3_bucket_url=s3_bucket_url,
    )


@router.get("/{benchmark_id}/tasks", response_model=TasksResponse)
def get_benchmark_tasks(
    benchmark_id: UUID,
    status: str = Query(default=""),
    task_id_search: str | None = None,
    sort: Literal["task_id", "started_at", "duration", "status"] = Query(default="started_at"),
    sort_dir: Literal["asc", "desc"] = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TasksResponse:
    """Paginated tasks for a benchmark, with optional status filter + task-id search.

    sort=status desc surfaces errors first (attention priority). Default: started_at desc."""
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

    latest_error_message = (
        select(ErrorResult.error_message)
        .where(ErrorResult.task == Task.id)
        .where(ErrorResult.org_id == org.id)
        .where(col(ErrorResult.retry_scheduled).is_(False))
        .order_by(desc(ErrorResult.created_at))
        .limit(1)
        .scalar_subquery()
    )
    sort_expr = {
        "task_id": col(Task.task_id),
        "started_at": col(Task.started_at),
        "duration": func.coalesce(col(Task.finished_at), func.now()) - col(Task.started_at),
        "status": _STATUS_SORT_PRIORITY,
    }[sort]
    primary = sort_expr.asc() if sort_dir == "asc" else sort_expr.desc()
    # Tie-break newest-first for stable ordering within equal keys.
    order_by = [primary, col(Task.started_at).desc()]

    rows = session.exec(
        select(Task, latest_error_message).where(*base_filters).order_by(*order_by).limit(limit).offset(offset)
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
                error_message=error_message if task.status == TaskStatus.ERROR else None,
            )
            for task, error_message in rows
        ],
        total_count=total,
    )
