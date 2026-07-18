"""Per-task drill-in endpoints."""

from __future__ import annotations

from datetime import timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, desc, select

from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX, create_presigned_url, s3_object_exists
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.types import (
    HarnessConfig,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskAttempt,
    TaskErrorAttempt,
    TaskEvaluationAttempt,
    TaskTiming,
)
from tracker.utils import fetch_harness_config

router = APIRouter(prefix="/benchmarks")


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


def _fetch_attempt_history(session: Session, task: Task, org: Org) -> list[TaskAttempt]:
    evaluations = session.exec(
        select(EvaluationResult)
        .where(EvaluationResult.task == task.id)
        .where(EvaluationResult.org_id == org.id)
        .order_by(desc(EvaluationResult.created_at))
    ).all()
    errors = session.exec(
        select(ErrorResult)
        .where(ErrorResult.task == task.id)
        .where(ErrorResult.org_id == org.id)
        .order_by(desc(ErrorResult.created_at))
    ).all()

    attempts: list[TaskAttempt] = [
        TaskEvaluationAttempt(
            type="evaluation",
            created_at=result.created_at,
            evaluation_result=result.result,
            agent_caused_exit_reason=result.agent_caused_exit_reason,
        )
        for result in evaluations
    ]
    attempts.extend(
        TaskErrorAttempt(type="error", created_at=result.created_at, error_message=result.error_message)
        for result in errors
    )
    return sorted(
        attempts,
        key=lambda attempt: attempt.created_at.replace(tzinfo=attempt.created_at.tzinfo or timezone.utc).timestamp(),
        reverse=True,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}",
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

    attempt_history = _fetch_attempt_history(session, task, org)
    latest_evaluation = next(
        (attempt for attempt in attempt_history if isinstance(attempt, TaskEvaluationAttempt)),
        None,
    )
    latest_error = next(
        (attempt for attempt in attempt_history if isinstance(attempt, TaskErrorAttempt)),
        None,
    )
    timing_row = session.get(TaskBreakdown, task.task_breakdown) if task.task_breakdown else None
    timing = TaskTiming(**timing_row.model_dump()) if timing_row else None

    evaluation_result = (
        latest_evaluation.evaluation_result if task.status == TaskStatus.FINISHED and latest_evaluation else None
    )
    exit_reason = (
        latest_evaluation.agent_caused_exit_reason if task.status == TaskStatus.FINISHED and latest_evaluation else None
    )
    error_message = latest_error.error_message if task.status == TaskStatus.ERROR and latest_error else None

    return SingleTaskResponse(
        id=task.id,
        task_id=task.task_id,
        status=task.status,
        started_at=task.started_at,
        finished_at=task.finished_at,
        error_message=error_message,
        evaluation_result=evaluation_result,
        agent_caused_exit_reason=exit_reason,
        attempt_history=attempt_history,
        timing=timing,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}/artifacts",
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
