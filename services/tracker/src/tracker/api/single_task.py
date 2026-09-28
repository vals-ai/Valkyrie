"""Per-task drill-in endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlmodel import Session, col, desc, select

from tracker.api.dependencies import RunAWSDependency, TrackedBenchmarkId, load_task_for_benchmark_or_404
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import CloudWatchBenchmarkLogLocations, task_log_stream_name
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    create_presigned_url,
    restore_prefix_versions_before,
    s3_object_exists,
)
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.exceptions import S3Error
from tracker.types import (
    RollbackTaskRequest,
    RollbackTaskResponse,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskResultEntry,
    TaskResultsResponse,
)
from tracker.utils.resources import fetch_benchmark_row

router = APIRouter(prefix="/benchmarks")


def _load_task_or_404(benchmark_id: UUID, task_id: str, org: Org, session: Session) -> tuple[Benchmark, Task]:
    """Return (benchmark, task) scoped to org, 404 if either is missing."""
    benchmark = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()

    if benchmark is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    return benchmark, load_task_for_benchmark_or_404(benchmark, task_id, org, session)


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
        result_select = select(ErrorResult.error_message).where(col(ErrorResult.retry_scheduled).is_(False))

    result = session.exec(result_select.where(*result_filters).order_by(result_order)).first()

    if task.status == TaskStatus.FINISHED:
        return cast(EvaluationResult | None, result), None

    return None, cast(str | None, result)


@router.get(
    "/{benchmark_id}/tasks/{task_id}",
    response_model=SingleTaskResponse,
)
def get_single_task(
    benchmark_id: TrackedBenchmarkId,
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


def _evaluation_history(session: Session, task: Task, org: Org) -> list[EvaluationResult]:
    """Every evaluation attempt for a task, newest first."""
    return list(
        session.exec(
            select(EvaluationResult)
            .where(EvaluationResult.task == task.id)
            .where(EvaluationResult.org_id == org.id)
            .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id))
        ).all()
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}/results",
    response_model=TaskResultsResponse,
)
def get_task_results(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskResultsResponse:
    """List every evaluation attempt kept for a task so an operator can pick one to roll back to."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)
    history = _evaluation_history(session, task, org)
    return TaskResultsResponse(
        task_id=task.task_id,
        status=task.status,
        results=[
            TaskResultEntry(
                id=row.id,
                created_at=row.created_at,
                current=index == 0 and task.status == TaskStatus.FINISHED,
                agent_caused_exit_reason=row.agent_caused_exit_reason.value if row.agent_caused_exit_reason else None,
                result=row.result,
            )
            for index, row in enumerate(history)
        ],
    )


@router.post(
    "/{benchmark_id}/tasks/{task_id}/rollback",
    response_model=RollbackTaskResponse,
)
async def rollback_task(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    run_context: RunAWSDependency,
    request: RollbackTaskRequest = Body(default_factory=RollbackTaskRequest),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> RollbackTaskResponse:
    """Make an earlier evaluation attempt the task's current result and restore its S3 artifacts.

    The chosen attempt is re-recorded as the newest evaluation row (history is never deleted), the task's artifact
    prefix is reverted to the object versions that existed when that attempt was evaluated, and the run's final score
    is discarded so `resume` recomputes it.
    """
    benchmark = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
    if benchmark.status in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING):
        raise HTTPException(
            status_code=409,
            detail=f"Run {benchmark_id} is {benchmark.status.value}; stop it or wait for it to finish before rolling back.",
        )
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, session)
    if task.status not in (TaskStatus.FINISHED, TaskStatus.ERROR):
        raise HTTPException(
            status_code=409, detail=f"Task {task_id} is {task.status.value}; only settled tasks roll back."
        )

    history = _evaluation_history(session, task, org)
    current = history[0] if history and task.status == TaskStatus.FINISHED else None
    if request.result_id is None:
        candidates = [row for row in history if current is None or row.id != current.id]
        if not candidates:
            raise HTTPException(status_code=404, detail=f"Task {task_id} has no earlier evaluation to roll back to.")
        target = candidates[0]
    else:
        target = next((row for row in history if row.id == request.result_id), None)
        if target is None:
            raise HTTPException(status_code=404, detail=f"Evaluation {request.result_id} not found for task {task_id}.")
        if current is not None and target.id == current.id:
            raise HTTPException(status_code=409, detail=f"Evaluation {target.id} is already the current result.")

    try:
        artifacts = await restore_prefix_versions_before(
            _task_prefix(benchmark_id, task.task_id), target.created_at, run_context.aws_runtime
        )
    except S3Error as exc:
        session.rollback()
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    now = datetime.now(ZoneInfo("UTC"))
    restored = EvaluationResult(
        org_id=org.id,
        task=task.id,
        created_at=now,
        agent_caused_exit_reason=target.agent_caused_exit_reason,
        result=target.result,
    )
    session.add(restored)
    task.status = TaskStatus.FINISHED
    task.finished_at = now
    session.add(task)
    if benchmark.final_evaluation is not None:
        session.delete(benchmark.final_evaluation)
        benchmark.final_evaluation = None
        session.add(benchmark)
    session.commit()

    return RollbackTaskResponse(
        task_id=task.task_id,
        status=task.status,
        restored_from_result_id=target.id,
        result_id=restored.id,
        artifacts_versioned=artifacts is not None,
        restored_artifacts=artifacts.restored if artifacts else [],
        removed_artifacts=artifacts.removed if artifacts else [],
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}/artifacts",
    response_model=TaskArtifactsResponse,
)
async def get_task_artifacts(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    run_context: RunAWSDependency,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    """CloudWatch URL + presigned URL for the agent's output tarball, for the SingleTask page."""
    task = load_task_for_benchmark_or_404(run_context.benchmark, task_id, org, session)
    aws_runtime = run_context.aws_runtime

    cloudwatch_url: str | None = None
    if aws_runtime.resources.log_group and aws_runtime.resources.region:
        log_locations = CloudWatchBenchmarkLogLocations(aws_runtime.resources)
        if any(character in task.task_id for character in ":*%"):
            # Renamed streams may use either encoding; link to the run without guessing.
            cloudwatch_url = log_locations.benchmark_location(str(benchmark_id))
        else:
            cloudwatch_url = log_locations.task_location(
                str(benchmark_id),
                task_log_stream_name(task.task_id, task.started_at),
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
