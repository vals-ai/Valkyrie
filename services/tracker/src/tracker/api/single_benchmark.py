"""Single-run detail endpoints."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from tracker.api.parsing import parse_csv
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.s3 import create_benchmark_url
from tracker.database.dependencies import BenchmarkRepositoryDep
from tracker.database.models import Org, TaskStatus
from tracker.types import (
    HarnessConfig,
    SingleBenchmarkResponse,
    TasksResponse,
    TaskSummary,
)
from tracker.utils import try_fetch_harness_config

router = APIRouter(prefix="/benchmarks")


@router.get("/{benchmark_id}", response_model=SingleBenchmarkResponse)
def get_single_benchmark(
    benchmark_repository: BenchmarkRepositoryDep,
    benchmark_id: UUID,
    org: Org = Depends(get_current_org),
    harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
) -> SingleBenchmarkResponse:
    """Fetch a single benchmark with task counts + final score for the SingleRun page."""
    benchmark = benchmark_repository.get_for_org(benchmark_id, org.id)
    if benchmark is None:
        raise HTTPException(status_code=404, detail="Not found")

    task_state_counts = benchmark_repository.get_task_state_counts(benchmark.id, org.id)
    total = sum(task_state_counts.values())
    finished = (
        task_state_counts.get(TaskStatus.FINISHED, 0)
        + task_state_counts.get(TaskStatus.ERROR, 0)
        + task_state_counts.get(TaskStatus.STOPPED, 0)
    )

    # region + s3_bucket are required harness headers, so they're present whenever
    # harness_config is; log_group is optional (no log group -> no CloudWatch link).
    cloudwatch_url: str | None = None
    s3_bucket_url: str | None = None
    if harness_config:
        region = harness_config.aws.aws_default_region
        s3_bucket_url = create_benchmark_url(str(benchmark.id), region, harness_config.s3_bucket)
        if harness_config.log_group:
            cloudwatch_url = get_benchmark_log_url(
                benchmark_id=str(benchmark.id),
                region=region,
                log_group=harness_config.log_group,
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
        final_score=benchmark_repository.get_final_score(benchmark.id, org.id),
        error_message=benchmark.error_message,
        cloudwatch_url=cloudwatch_url,
        s3_bucket_url=s3_bucket_url,
    )


@router.get("/{benchmark_id}/tasks", response_model=TasksResponse)
def get_benchmark_tasks(
    benchmark_repository: BenchmarkRepositoryDep,
    benchmark_id: UUID,
    status: str = Query(default=""),
    task_id_search: str | None = None,
    sort: Literal["task_id", "started_at", "duration", "status"] = Query(default="started_at"),
    sort_dir: Literal["asc", "desc"] = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
) -> TasksResponse:
    """Paginated tasks for a benchmark, with optional status filter + task-id search.

    sort=status desc surfaces errors first (attention priority). Default: started_at desc."""
    if benchmark_repository.get_for_org(benchmark_id, org.id) is None:
        raise HTTPException(status_code=404, detail="Not found")

    statuses = parse_csv(status, TaskStatus)
    page = benchmark_repository.list_tasks(
        benchmark_id,
        org.id,
        statuses=statuses,
        task_id_search=task_id_search,
        sort=sort,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )

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
            for task, error_message in page.rows
        ],
        total_count=page.total_count,
    )
