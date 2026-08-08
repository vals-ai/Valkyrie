"""Per-task drill-in endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX, create_presigned_url, s3_object_exists
from tracker.database.dependencies import BenchmarkRepositoryDep, TaskRepositoryDep
from tracker.database.models import Benchmark, EvaluationResult, Org, Task
from tracker.database.repositories import BenchmarkRepository, TaskRepository
from tracker.types import HarnessConfig, SingleTaskResponse, TaskArtifactsResponse
from tracker.utils import fetch_harness_config

router = APIRouter(prefix="/benchmarks")


def _load_task_or_404(
    benchmark_id: UUID,
    task_id: str,
    org: Org,
    benchmark_repository: BenchmarkRepository,
    task_repository: TaskRepository,
) -> tuple[Benchmark, Task]:
    """Return (benchmark, task) scoped to org, 404 if either is missing."""
    benchmark = benchmark_repository.get_for_org(benchmark_id, org.id)
    if benchmark is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    task = task_repository.get_for_benchmark(benchmark.id, task_id, org.id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return benchmark, task


def _task_prefix(benchmark_id: UUID, task_id: str) -> str:
    """S3 prefix for a task's artifacts (presigned URLs + run outputs)."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/"


def _fetch_result_objects(
    task_repository: TaskRepository,
    task: Task,
    org: Org,
) -> tuple[EvaluationResult | None, str | None]:
    """Fetch a task's evaluation result or error message depending on its status."""
    return task_repository.get_terminal_result(task, org.id)


@router.get(
    "/{benchmark_id}/tasks/{task_id}",
    response_model=SingleTaskResponse,
)
def get_single_task(
    benchmark_repository: BenchmarkRepositoryDep,
    task_repository: TaskRepositoryDep,
    benchmark_id: UUID,
    task_id: str,
    org: Org = Depends(get_current_org),
) -> SingleTaskResponse:
    """Fetch a single task's status + evaluation result for the SingleTask page."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, benchmark_repository, task_repository)

    eval_row, error_message = _fetch_result_objects(task_repository, task, org)

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
    benchmark_repository: BenchmarkRepositoryDep,
    task_repository: TaskRepositoryDep,
    benchmark_id: UUID,
    task_id: str,
    org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> TaskArtifactsResponse:
    """CloudWatch URL + presigned URL for the agent's output tarball, for the SingleTask page."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, benchmark_repository, task_repository)

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
