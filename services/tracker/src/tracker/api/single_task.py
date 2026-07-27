"""Per-task drill-in endpoints."""

from __future__ import annotations

import hashlib
from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import defer
from sqlmodel import Session, col, desc, func, select

from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX, create_presigned_url, s3_object_exists
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.types import (
    ErrorTaskAttempt,
    EvaluationTaskAttempt,
    HarnessConfig,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskAttempt,
    TaskAttemptsResponse,
    TASK_ATTEMPT_ERROR_MAX_LENGTH,
)
from tracker.utils import fetch_harness_config

router = APIRouter(prefix="/benchmarks")


def _summarize_attempt_error(message: str) -> tuple[str, bool, str]:
    return (
        message[:TASK_ATTEMPT_ERROR_MAX_LENGTH],
        len(message) > TASK_ATTEMPT_ERROR_MAX_LENGTH,
        hashlib.sha256(message.encode()).hexdigest(),
    )


def _load_task_or_404(benchmark_id: UUID, task_id: str, org: Org, session: Session) -> tuple[Benchmark, Task]:
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
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskAttemptsResponse:
    """Return persisted task outcomes, newest first."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)
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
    attempts: list[TaskAttempt] = [
        EvaluationTaskAttempt(
            id=row.id,
            created_at=row.created_at,
            instance_id=row.instance_id,
            agent_caused_exit_reason=row.agent_caused_exit_reason,
        )
        for row in evaluation_rows
    ]
    for row in error_rows:
        message, truncated, fingerprint = _summarize_attempt_error(row.error_message)
        attempts.append(
            ErrorTaskAttempt(
                id=row.id,
                created_at=row.created_at,
                error_message=message,
                error_message_truncated=truncated,
                error_fingerprint=fingerprint,
            )
        )
    attempts.sort(key=lambda attempt: (attempt.created_at, attempt.id.int, attempt.kind), reverse=True)

    evaluation_count = session.exec(select(func.count(col(EvaluationResult.id))).where(*evaluation_filters)).one()
    error_count = session.exec(select(func.count(col(ErrorResult.id))).where(*error_filters)).one()
    return TaskAttemptsResponse(
        attempts=attempts[offset:page_end],
        total_count=evaluation_count + error_count,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/artifacts",
    response_model=TaskArtifactsResponse,
)
async def get_task_artifacts(
    benchmark_id: UUID,
    task_id: str,
    org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    """CloudWatch URL + presigned URL for the agent's output tarball, for the SingleTask page."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)

    cloudwatch_url: str | None = None
    if harness_config.log_group and harness_config.aws.aws_default_region:
        log_stream_suffix = f"{int(task.started_at.timestamp() * 1_000_000):x}"
        cloudwatch_url = get_benchmark_log_url(
            benchmark_id=str(benchmark_id),
            region=harness_config.aws.aws_default_region,
            log_group=harness_config.log_group,
            task_id=f"{task.task_id}_{log_stream_suffix}",
        )

    agent_output_url: str | None = None
    ttl_seconds: int | None = None
    key = f"{_task_prefix(benchmark_id, task_id)}agent_output.tar.gz"
    if await s3_object_exists(key, aws=harness_config.aws, s3_bucket=harness_config.s3_bucket):
        ttl_seconds = 300
        agent_output_url = await create_presigned_url(
            s3_key=key,
            aws=harness_config.aws,
            s3_bucket=harness_config.s3_bucket,
            expiration=ttl_seconds,
        )

    return TaskArtifactsResponse(
        cloudwatch_url=cloudwatch_url,
        agent_output_url=agent_output_url,
        agent_output_expires_in=ttl_seconds,
    )


@router.get(
    "/{benchmark_id}/task",
    operation_id="get_single_task_by_query",
    response_model=SingleTaskResponse,
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
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)

    eval_row, error_message = _fetch_result_objects(session, task, org)

    return SingleTaskResponse(
        id=task.id,
        task_id=task.task_id,
        status=task.status,
        started_at=task.started_at,
        finished_at=task.finished_at,
        error_message=error_message,
        evaluation_result=eval_row.result if eval_row else None,
        agent_caused_exit_reason=(
            eval_row.agent_caused_exit_reason.value if eval_row and eval_row.agent_caused_exit_reason else None
        ),
    )
