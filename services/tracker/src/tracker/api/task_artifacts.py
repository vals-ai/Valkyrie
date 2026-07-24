"""Indexed task artifact browsing endpoints."""

from __future__ import annotations

import base64
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from sqlmodel import Session

from tracker.api.single_task import load_task_or_404
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import task_log_attempt_id
from tracker.aws.resolver import resolve_run_aws_runtime
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import create_presigned_url
from tracker.database.models import Org, Task
from tracker.database.session import get_session
from tracker.task_artifacts import (
    ArtifactIndex,
    ArtifactIndexNotFoundError,
    InvalidArtifactIndexError,
    artifact_candidates,
    find_artifact_file,
    list_artifact_files,
    load_artifact_index,
    read_artifact_content,
    task_artifact_generation_key,
)
from tracker.types import (
    HarnessConfig,
    TaskArtifactContentResponse,
    TaskArtifactDirectory,
    TaskArtifactFile,
    TaskArtifactFilesResponse,
    TaskArtifactIndexResponse,
)
from tracker.utils.harness_config import try_fetch_harness_config

router = APIRouter(prefix="/benchmarks/{benchmark_id}/tasks/{task_id:path}/artifacts")
ArtifactAttemptId = Annotated[
    str | None,
    Query(min_length=1, max_length=32, pattern=r"^[0-9a-f]+$"),
]


def _resolve_context(
    benchmark_id: UUID,
    task_id: str,
    attempt_id: str | None,
    request: Request,
    org: Org,
    legacy_harness_config: HarnessConfig | None,
    session: Session,
) -> tuple[Task, AWSRuntime, str, bool]:
    benchmark, task = load_task_or_404(benchmark_id, task_id, org, session)
    runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        tenant_id=org.name,
        legacy_harness_config=legacy_harness_config,
    ).runtime
    current_attempt_id = task_log_attempt_id(task.started_at)
    resolved_attempt_id = attempt_id or current_attempt_id
    return task, runtime, resolved_attempt_id, resolved_attempt_id == current_attempt_id


async def _load_index(
    benchmark_id: UUID,
    task_id: str,
    attempt_id: str,
    runtime: AWSRuntime,
) -> ArtifactIndex:
    try:
        return await load_artifact_index(
            str(benchmark_id),
            task_id,
            attempt_id,
            runtime,
        )
    except ArtifactIndexNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Artifact index not found for this attempt") from exc
    except InvalidArtifactIndexError as exc:
        raise HTTPException(status_code=502, detail="Artifact index is invalid") from exc


@router.get("/index", response_model=TaskArtifactIndexResponse)
async def get_task_artifact_index(
    benchmark_id: UUID,
    task_id: str,
    request: Request,
    attempt_id: ArtifactAttemptId = None,
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskArtifactIndexResponse:
    """Return one immutable attempt's artifact capabilities."""
    _, runtime, resolved_attempt_id, is_current = _resolve_context(
        benchmark_id,
        task_id,
        attempt_id,
        request,
        org,
        legacy_harness_config,
        session,
    )
    index = await _load_index(benchmark_id, task_id, resolved_attempt_id, runtime)
    trajectory_path, diff_path = artifact_candidates(index)
    return TaskArtifactIndexResponse(
        attempt_id=resolved_attempt_id,
        is_current=is_current,
        archive_available=index.archive_available,
        file_count=len(index.files),
        pack_size_bytes=index.pack_size_bytes,
        trajectory_path=trajectory_path,
        diff_path=diff_path,
    )


@router.get("/files", response_model=TaskArtifactFilesResponse)
async def get_task_artifact_files(
    benchmark_id: UUID,
    task_id: str,
    request: Request,
    attempt_id: ArtifactAttemptId = None,
    prefix: str = Query(default="", max_length=4_000),
    cursor: str | None = Query(default=None, min_length=1, max_length=128),
    limit: int = Query(default=200, ge=1, le=500),
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskArtifactFilesResponse:
    """Page direct children of one artifact directory."""
    _, runtime, resolved_attempt_id, _ = _resolve_context(
        benchmark_id,
        task_id,
        attempt_id,
        request,
        org,
        legacy_harness_config,
        session,
    )
    index = await _load_index(benchmark_id, task_id, resolved_attempt_id, runtime)
    try:
        items, next_cursor = list_artifact_files(index, prefix, cursor, limit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    response_items: list[TaskArtifactDirectory | TaskArtifactFile] = []
    for item in items:
        if item.kind == "directory":
            response_items.append(TaskArtifactDirectory(path=item.path))
            continue
        assert item.size_bytes is not None
        response_items.append(TaskArtifactFile(path=item.path, size_bytes=item.size_bytes))

    return TaskArtifactFilesResponse(
        attempt_id=resolved_attempt_id,
        prefix=prefix,
        items=response_items,
        next_cursor=next_cursor,
    )


@router.get("/content", response_model=TaskArtifactContentResponse)
async def get_task_artifact_content(
    benchmark_id: UUID,
    task_id: str,
    request: Request,
    path: str = Query(min_length=1, max_length=4_000),
    attempt_id: ArtifactAttemptId = None,
    cursor: str | None = Query(default=None, min_length=1, max_length=128),
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> TaskArtifactContentResponse:
    """Return one bounded text or binary page using one S3 range read."""
    _, runtime, resolved_attempt_id, _ = _resolve_context(
        benchmark_id,
        task_id,
        attempt_id,
        request,
        org,
        legacy_harness_config,
        session,
    )
    index = await _load_index(benchmark_id, task_id, resolved_attempt_id, runtime)
    try:
        file = find_artifact_file(index, path)
        content = await read_artifact_content(
            str(benchmark_id),
            task_id,
            resolved_attempt_id,
            index.generation,
            file,
            cursor,
            runtime,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Artifact file not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return TaskArtifactContentResponse(
        attempt_id=resolved_attempt_id,
        path=file.path,
        size_bytes=file.size_bytes,
        next_cursor=content.next_cursor,
        content_base64=base64.b64encode(content.data).decode(),
    )


@router.get(
    "/archive",
    response_class=RedirectResponse,
    status_code=303,
)
async def download_task_artifact_archive(
    benchmark_id: UUID,
    task_id: str,
    request: Request,
    attempt_id: ArtifactAttemptId = None,
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> RedirectResponse:
    """Redirect to one exact attempt's immutable output archive."""
    _, runtime, resolved_attempt_id, _ = _resolve_context(
        benchmark_id,
        task_id,
        attempt_id,
        request,
        org,
        legacy_harness_config,
        session,
    )
    index = await _load_index(benchmark_id, task_id, resolved_attempt_id, runtime)
    if not index.archive_available:
        raise HTTPException(status_code=404, detail="Artifact archive not found for this attempt")
    key = task_artifact_generation_key(
        str(benchmark_id),
        task_id,
        resolved_attempt_id,
        index.generation,
        "agent_output.tar.gz",
    )
    ttl_seconds = runtime.clients.maximum_presign_ttl(300)
    url = await create_presigned_url(key, runtime, expiration=ttl_seconds)
    return RedirectResponse(url, status_code=303)
