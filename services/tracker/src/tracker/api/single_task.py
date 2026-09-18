"""Per-task drill-in endpoints."""

from __future__ import annotations

from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlmodel import Session, col, desc, select

from tracker.api.dependencies import (
    RunBenchmarkDependency,
    RunRuntimeDependency,
    TrackedBenchmarkId,
    load_task_for_benchmark_or_404,
)
from tracker.auth import get_current_org
from tracker.runtime.logs import task_log_stream_name
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX
from tracker.aws.services import CloudRuntimeServices
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.types import SingleTaskResponse, TaskArtifactsResponse

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


@router.get(
    "/{benchmark_id}/tasks/{task_id}/artifacts",
    response_model=TaskArtifactsResponse,
)
async def get_task_artifacts(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    benchmark: RunBenchmarkDependency,
    runtime: RunRuntimeDependency,
    request: Request,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    """Return log and agent output locations for the task detail page."""
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, session)
    cloudwatch_url: str | None
    if benchmark.arguments.environment == "local":
        cloudwatch_url = str(
            request.url_for("get_logs", benchmark_id=benchmark_id).include_query_params(task_id=task_id)
        )
    elif isinstance(runtime, CloudRuntimeServices) and not (
        runtime.aws_runtime.resources.log_group and runtime.aws_runtime.resources.region
    ):
        cloudwatch_url = None
    elif any(character in task.task_id for character in ":*%"):
        # Renamed streams may use either encoding; link to the run without guessing.
        cloudwatch_url = runtime.log_locations.benchmark_location(str(benchmark_id))
    else:
        cloudwatch_url = runtime.log_locations.task_location(
            str(benchmark_id), task_log_stream_name(task.task_id, task.started_at)
        )

    agent_output_url: str | None = None
    ttl_seconds: int | None = None
    key = f"{_task_prefix(benchmark_id, task_id)}agent_output.tar.gz"
    if await runtime.objects.exists(key):
        ttl_seconds = runtime.objects.maximum_download_ttl(300)
        agent_output_url = await runtime.objects.temporary_download_url(key, expires_in=ttl_seconds)
        if agent_output_url is None:
            agent_output_url = str(
                request.url_for("get_run_artifact_url", benchmark_id=benchmark_id).include_query_params(
                    path=f"{task_id}/agent_output.tar.gz", download="true"
                )
            )

    return TaskArtifactsResponse(
        cloudwatch_url=cloudwatch_url,
        agent_output_url=agent_output_url,
        agent_output_expires_in=ttl_seconds,
    )
