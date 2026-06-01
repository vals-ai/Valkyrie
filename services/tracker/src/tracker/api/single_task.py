"""Per-task drill-in endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.cloudwatch import get_cloudwatch_url
from tracker.database.models import (
    Benchmark,
    EvaluationResult,
    Org,
    OrgConfig,
    Task,
    User,
)
from tracker.database.session import get_session
from tracker.s3 import (
    S3_BENCHMARKS_PREFIX,
    generate_presigned_get_url,
    list_s3_objects_detailed,
    s3_object_exists,
)
from tracker.types import (
    AWSCredentials,
    FileEntry,
    FilesResponse,
    PresignedUrlResponse,
    SingleTaskResponse,
    TaskArtifactsResponse,
)

router = APIRouter()


def _load_task_or_404(benchmark_id: UUID, task_id: str, org: Org, session: Session) -> tuple[Benchmark, Task]:
    bench = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()

    if bench is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    task = session.exec(
        select(Task).where(Task.benchmark == bench.id).where(Task.org_id == org.id).where(Task.task_id == task_id)
    ).first()

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return bench, task


def _task_prefix(benchmark_id: UUID, task_id: str) -> str:
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/"


def _load_aws_or_none(org: Org, session: Session) -> tuple[AWSCredentials | None, str | None, OrgConfig | None]:
    config = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()

    if config is None:
        return None, None, None

    aws = AWSCredentials.from_org_config(config)

    return aws, config.s3_bucket, config


@router.get(
    "/benchmarks/{benchmark_id}/tasks/{task_id}",
    response_model=SingleTaskResponse,
)
def get_single_task(
    benchmark_id: UUID,
    task_id: str,
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> SingleTaskResponse:
    _, org = user_and_org
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)

    eval_row = session.exec(
        select(EvaluationResult).where(EvaluationResult.task == task.id).where(EvaluationResult.org_id == org.id)
    ).first()

    return SingleTaskResponse(
        id=task.id,
        task_id=task.task_id,
        status=task.status,
        started_at=task.started_at,
        finished_at=task.finished_at,
        error_message=task.error_message,
        evaluation_result=eval_row.result if eval_row else None,
        agent_caused_exit_reason=(
            eval_row.agent_caused_exit_reason.value if eval_row and eval_row.agent_caused_exit_reason else None
        ),
    )


@router.get(
    "/benchmarks/{benchmark_id}/tasks/{task_id}/files",
    response_model=FilesResponse,
)
def list_task_files(
    benchmark_id: UUID,
    task_id: str,
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> FilesResponse:
    _, org = user_and_org
    _load_task_or_404(benchmark_id, task_id, org, session)

    aws, bucket, _ = _load_aws_or_none(org, session)

    if aws is None or bucket is None:
        return FilesResponse(files=[])

    prefix = _task_prefix(benchmark_id, task_id)
    rows = list_s3_objects_detailed(prefix=prefix, aws=aws, s3_bucket=bucket)

    return FilesResponse(
        files=[
            FileEntry(
                key=str(r["key"]),
                size=int(r["size"] or 0),
                last_modified=(str(r["last_modified"]) if r.get("last_modified") else None),
            )
            for r in rows
        ]
    )


@router.get(
    "/benchmarks/{benchmark_id}/tasks/{task_id}/files/url",
    response_model=PresignedUrlResponse,
)
def get_file_presigned_url(
    benchmark_id: UUID,
    task_id: str,
    key: str = Query(..., description="S3 key under this task's prefix"),
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> PresignedUrlResponse:
    _, org = user_and_org
    _load_task_or_404(benchmark_id, task_id, org, session)

    prefix = _task_prefix(benchmark_id, task_id)

    if ".." in key or not key.startswith(prefix):
        raise HTTPException(status_code=400, detail="Key is outside task's prefix")

    aws, bucket, _ = _load_aws_or_none(org, session)

    if aws is None or bucket is None:
        raise HTTPException(status_code=400, detail="Org has no S3 configuration")

    ttl_seconds = 300
    url = generate_presigned_get_url(key=key, aws=aws, s3_bucket=bucket, ttl_seconds=ttl_seconds)

    return PresignedUrlResponse(url=url, expires_in=ttl_seconds)


@router.get(
    "/benchmarks/{benchmark_id}/tasks/{task_id}/artifacts",
    response_model=TaskArtifactsResponse,
)
def get_task_artifacts(
    benchmark_id: UUID,
    task_id: str,
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    _, org = user_and_org
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)

    aws, bucket, config = _load_aws_or_none(org, session)

    cloudwatch_url: str | None = None
    if config is not None and config.log_group and config.aws_default_region:
        cloudwatch_url = get_cloudwatch_url(
            benchmark_id=str(benchmark_id),
            region=config.aws_default_region,
            log_group=config.log_group,
            task_id=task.alias,
        )

    agent_output_url: str | None = None
    ttl_seconds: int | None = None
    if aws is not None and bucket is not None:
        key = f"{_task_prefix(benchmark_id, task_id)}agent_output.tar.gz"
        if s3_object_exists(key, aws=aws, s3_bucket=bucket):
            ttl_seconds = 300
            agent_output_url = generate_presigned_get_url(key=key, aws=aws, s3_bucket=bucket, ttl_seconds=ttl_seconds)

    return TaskArtifactsResponse(
        cloudwatch_url=cloudwatch_url,
        agent_output_url=agent_output_url,
        agent_output_expires_in=ttl_seconds,
    )
