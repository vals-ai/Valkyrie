"""Per-task drill-in endpoints."""

from __future__ import annotations

from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from sqlalchemy.orm import defer
from sqlmodel import Session, col, desc, func, select

from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import (
    get_benchmark_log_url,
    get_task_log_events,
    list_task_log_attempts,
    task_log_attempt_id,
)
from tracker.aws.resolver import resolve_run_aws_runtime
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX, create_presigned_url, s3_object_exists
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskAttempt as TaskAttemptRow,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.types import (
    ERROR_EXCERPT_MAX_LENGTH,
    ErrorTaskAttempt,
    ExecutionTaskAttempt,
    EvaluationTaskAttempt,
    HarnessConfig,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskAttempt,
    TaskAttemptsResponse,
    TaskLogAttempt,
    TaskLogAttemptsRequest,
    TaskLogAttemptsResponse,
    TaskLogEvent,
    TaskLogEventsRequest,
    TaskLogEventsResponse,
    summarize_attempt_error,
)
from tracker.utils.harness_config import try_fetch_harness_config

router = APIRouter(prefix="/benchmarks")
TaskLogAttemptId = Annotated[str, Path(min_length=1, max_length=32, pattern=r"^[0-9a-f]+$")]
_ACTIVE_TASK_STATUSES = {
    TaskStatus.PENDING,
    TaskStatus.BUILDING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.EVALUATING,
}


def load_task_or_404(benchmark_id: UUID, task_id: str, org: Org, session: Session) -> tuple[Benchmark, Task]:
    """Return (benchmark, task) scoped to org, 404 if either is missing."""
    benchmark = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()

    if benchmark is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    task = session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.org_id == org.id).where(Task.task_id == task_id)
    ).first()

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return benchmark, task


def _task_prefix(benchmark_id: UUID, task_id: str) -> str:
    """S3 prefix for a task's artifacts (presigned URLs + run outputs)."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/"


def _fetch_result_objects(session: Session, task: Task, org: Org) -> tuple[EvaluationResult | None, str | None]:
    """Fetches a task's evaluation result or error message depending on its status."""
    if task.status not in (TaskStatus.FINISHED, TaskStatus.ERROR):
        return None, None

    result_model = EvaluationResult if task.status == TaskStatus.FINISHED else ErrorResult
    result_filters = (
        result_model.task == task.id,
        result_model.org_id == org.id,
    )
    result_order = desc(result_model.created_at)

    if task.status == TaskStatus.FINISHED:
        result_select = select(EvaluationResult)
    else:
        result_select = select(ErrorResult.error_message)

    result = session.exec(result_select.where(*result_filters).order_by(result_order)).first()

    if task.status == TaskStatus.FINISHED:
        return cast(EvaluationResult | None, result), None

    return None, cast(str | None, result)


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/attempts",
    response_model=TaskAttemptsResponse,
)
def get_task_attempts(
    benchmark_id: UUID,
    task_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskAttemptsResponse:
    """Return every persisted task outcome, newest first."""
    _, task = load_task_or_404(benchmark_id, task_id, org, session)
    page_end = offset + limit

    evaluation_filters = (
        col(EvaluationResult.task) == task.id,
        col(EvaluationResult.org_id) == org.id,
    )
    error_filters = (
        col(ErrorResult.task) == task.id,
        col(ErrorResult.org_id) == org.id,
    )
    evaluation_rows = session.exec(
        select(EvaluationResult)
        .where(*evaluation_filters)
        .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id))
        .options(defer(EvaluationResult.result))  # pyright: ignore[reportArgumentType]
        .limit(page_end)
    ).all()
    error_rows = session.exec(
        select(ErrorResult)
        .where(*error_filters)
        .order_by(desc(ErrorResult.created_at), desc(ErrorResult.id))
        .limit(page_end)
    ).all()
    evaluation_exists = (
        select(EvaluationResult.id)
        .where(col(EvaluationResult.task) == col(TaskAttemptRow.task))
        .where(col(EvaluationResult.attempt_id) == col(TaskAttemptRow.attempt_id))
        .where(col(EvaluationResult.org_id) == org.id)
        .exists()
    )
    error_exists = (
        select(ErrorResult.id)
        .where(col(ErrorResult.task) == col(TaskAttemptRow.task))
        .where(col(ErrorResult.attempt_id) == col(TaskAttemptRow.attempt_id))
        .where(col(ErrorResult.org_id) == org.id)
        .exists()
    )
    execution_filters = (
        col(TaskAttemptRow.task) == task.id,
        col(TaskAttemptRow.org_id) == org.id,
        ~evaluation_exists,
        ~error_exists,
    )
    execution_rows = session.exec(
        select(TaskAttemptRow)
        .where(*execution_filters)
        .order_by(desc(TaskAttemptRow.started_at), desc(TaskAttemptRow.id))
        .limit(page_end)
    ).all()

    attempts: list[TaskAttempt] = [
        EvaluationTaskAttempt(
            id=row.id,
            attempt_id=row.attempt_id,
            created_at=row.created_at,
            instance_id=row.instance_id,
            agent_caused_exit_reason=row.agent_caused_exit_reason,
        )
        for row in evaluation_rows
    ]
    for row in error_rows:
        message, truncated, fingerprint = summarize_attempt_error(row.error_message)
        attempts.append(
            ErrorTaskAttempt(
                id=row.id,
                attempt_id=row.attempt_id,
                created_at=row.created_at,
                error_message=message,
                error_message_truncated=truncated,
                error_fingerprint=fingerprint,
            )
        )
    current_attempt_id = task_log_attempt_id(task.started_at)
    attempts.extend(
        ExecutionTaskAttempt(
            id=row.id,
            attempt_id=row.attempt_id,
            created_at=row.started_at,
            status=task.status if row.attempt_id == current_attempt_id else TaskStatus.STOPPED,
            instance_id=row.sandbox_instance_id,
        )
        for row in execution_rows
    )
    attempts.sort(
        key=lambda attempt: (
            attempt.created_at,
            attempt.id.int,
            attempt.kind == "evaluation",
        ),
        reverse=True,
    )

    evaluation_count = session.exec(select(func.count(col(EvaluationResult.id))).where(*evaluation_filters)).one()
    error_count = session.exec(select(func.count(col(ErrorResult.id))).where(*error_filters)).one()
    execution_count = session.exec(select(func.count(col(TaskAttemptRow.id))).where(*execution_filters)).one()
    return TaskAttemptsResponse(
        attempts=attempts[offset:page_end],
        total_count=evaluation_count + error_count + execution_count,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/log-attempts",
    response_model=TaskLogAttemptsResponse,
)
def get_task_log_attempts(
    benchmark_id: UUID,
    task_id: str,
    request: Request,
    query: Annotated[TaskLogAttemptsRequest, Query()],
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskLogAttemptsResponse:
    """Page exact CloudWatch streams for every retry of a task."""
    benchmark, task = load_task_or_404(benchmark_id, task_id, org, session)
    aws_runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        tenant_id=org.name,
        legacy_harness_config=legacy_harness_config,
    ).runtime
    if not aws_runtime.resources.log_group:
        raise HTTPException(status_code=404, detail="Task logs are unavailable for this run")

    current_attempt_id = task_log_attempt_id(task.started_at)
    page = list_task_log_attempts(
        str(benchmark.id),
        task.task_id,
        aws_runtime,
        limit=query.limit,
        cursor=query.cursor,
    )
    return TaskLogAttemptsResponse(
        attempts=[
            TaskLogAttempt(
                id=attempt.attempt_id,
                started_at=attempt.started_at,
                is_current=attempt.attempt_id == current_attempt_id,
                creation_time_ms=attempt.creation_time_ms,
                first_event_time_ms=attempt.first_event_time_ms,
                last_event_time_ms=attempt.last_event_time_ms,
                last_ingestion_time_ms=attempt.last_ingestion_time_ms,
            )
            for attempt in page.attempts
        ],
        current_attempt_id=current_attempt_id,
        next_cursor=page.next_cursor,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/log-attempts/{attempt_id}/events",
    response_model=TaskLogEventsResponse,
)
def get_task_log_attempt_events(
    benchmark_id: UUID,
    task_id: str,
    attempt_id: TaskLogAttemptId,
    request: Request,
    query: Annotated[TaskLogEventsRequest, Query()],
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskLogEventsResponse:
    """Page one attempt's events; forward cursors efficiently tail active work."""
    benchmark, task = load_task_or_404(benchmark_id, task_id, org, session)
    aws_runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        tenant_id=org.name,
        legacy_harness_config=legacy_harness_config,
    ).runtime
    if not aws_runtime.resources.log_group:
        raise HTTPException(status_code=404, detail="Task logs are unavailable for this run")

    is_current = attempt_id == task_log_attempt_id(task.started_at)
    is_active = is_current and task.status in _ACTIVE_TASK_STATUSES
    page = get_task_log_events(
        str(benchmark.id),
        task.task_id,
        attempt_id,
        aws_runtime,
        direction=query.direction,
        limit=query.limit,
        cursor=query.cursor,
    )
    if page is None and not is_active:
        raise HTTPException(status_code=404, detail="Task log attempt not found")
    events = (
        []
        if page is None
        else [
            TaskLogEvent(
                timestamp_ms=event.timestamp_ms,
                ingestion_time_ms=event.ingestion_time_ms,
                message=event.message,
            )
            for event in page.events
        ]
    )

    return TaskLogEventsResponse(
        attempt_id=attempt_id,
        is_current=is_current,
        is_active=is_active,
        task_status=task.status,
        direction=query.direction,
        events=events,
        older_cursor=page.older_cursor if page is not None else None,
        newer_cursor=page.newer_cursor if page is not None else None,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/artifacts",
    response_model=TaskArtifactsResponse,
)
async def get_task_artifacts(
    benchmark_id: UUID,
    task_id: str,
    request: Request,
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    """CloudWatch URL + presigned URL for the agent's output tarball, for the SingleTask page."""
    benchmark, task = load_task_or_404(benchmark_id, task_id, org, session)
    aws_runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        tenant_id=org.name,
        legacy_harness_config=legacy_harness_config,
    ).runtime

    cloudwatch_url: str | None = None
    if aws_runtime.resources.log_group and aws_runtime.resources.region:
        log_stream_suffix = f"{int(task.started_at.timestamp() * 1_000_000):x}"
        cloudwatch_url = get_benchmark_log_url(
            benchmark_id=str(benchmark_id),
            resources=aws_runtime.resources,
            task_id=f"{task.task_id}_{log_stream_suffix}",
        )

    agent_output_url: str | None = None
    ttl_seconds: int | None = None
    key = f"{_task_prefix(benchmark_id, task_id)}agent_output.tar.gz"
    if await s3_object_exists(key, aws_runtime):
        ttl_seconds = aws_runtime.clients.maximum_presign_ttl(300)
        agent_output_url = await create_presigned_url(
            s3_key=key,
            runtime=aws_runtime,
            expiration=ttl_seconds,
        )

    return TaskArtifactsResponse(
        cloudwatch_url=cloudwatch_url,
        agent_output_url=agent_output_url,
        agent_output_expires_in=ttl_seconds,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}",
    response_model=SingleTaskResponse,
)
def get_single_task(
    benchmark_id: UUID,
    task_id: str,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> SingleTaskResponse:
    """Fetch a single task's status + evaluation result for the SingleTask page."""
    _, task = load_task_or_404(benchmark_id, task_id, org, session)

    eval_row, error_message = _fetch_result_objects(session, task, org)

    return SingleTaskResponse(
        id=task.id,
        task_id=task.task_id,
        status=task.status,
        started_at=task.started_at,
        finished_at=task.finished_at,
        error_message=error_message[:ERROR_EXCERPT_MAX_LENGTH] if error_message else None,
        evaluation_result=eval_row.result if eval_row else None,
        agent_caused_exit_reason=(
            eval_row.agent_caused_exit_reason.value if eval_row and eval_row.agent_caused_exit_reason else None
        ),
    )
