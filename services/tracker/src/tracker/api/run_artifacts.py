"""Organization-scoped run artifact listing and download links."""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from tracker.api.dependencies import RunAWSDependency, TrackedBenchmarkId
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/benchmarks")


class RunArtifactEntry(BaseModel):
    """An object stored beneath one run's artifact directory."""

    path: str
    size: int
    last_modified: datetime | None = None


class RunArtifactsResponse(BaseModel):
    """One page of run artifacts."""

    artifacts: list[RunArtifactEntry]
    next_cursor: str | None = None


class RunArtifactDownloadResponse(BaseModel):
    """A temporary URL for a single run artifact."""

    path: str
    download_url: str
    expires_in: int
    size: int


def _path(value: str, *, allow_empty: bool = False) -> str:
    if (
        (not value and not allow_empty)
        or "\\" in value
        or ":" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        if value or not allow_empty:
            raise HTTPException(status_code=400, detail="Artifact path must be a relative file or directory path")
    return value


@contextmanager
def _storage_errors() -> Iterator[None]:
    try:
        yield
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            raise HTTPException(status_code=404, detail="Artifact not found") from error
        if code in {"403", "AccessDenied", "Forbidden"}:
            raise HTTPException(status_code=403, detail="Artifact storage permission denied") from error
        logger.exception("Artifact storage operation failed")
        raise HTTPException(status_code=502, detail="Artifact storage operation failed") from error
    except BotoCoreError as error:
        logger.exception("Artifact storage operation failed")
        raise HTTPException(status_code=502, detail="Artifact storage operation failed") from error


@router.get("/{benchmark_id}/artifacts", response_model=RunArtifactsResponse)
async def list_run_artifacts(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunAWSDependency,
    prefix: str = "",
    cursor: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> RunArtifactsResponse:
    """List an exact artifact path or its descendants within the authorized run."""
    prefix = _path(prefix.rstrip("/"), allow_empty=True)
    root = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"
    runtime = run_context.aws_runtime
    arguments: dict[str, Any] = {"Bucket": runtime.resources.s3_bucket, "Prefix": root + prefix, "MaxKeys": limit}
    if cursor is not None:
        arguments["ContinuationToken"] = cursor
    with _storage_errors():
        async with runtime.clients.s3_client() as client:
            response = await client.list_objects_v2(**arguments)
    entries = []
    for item in response.get("Contents", []):
        key = item["Key"]
        if not key.startswith(root) or key.endswith("/"):
            continue
        path = key[len(root) :]
        if prefix and path != prefix and not path.startswith(prefix + "/"):
            continue
        entries.append(RunArtifactEntry(path=path, size=item["Size"], last_modified=item.get("LastModified")))
    return RunArtifactsResponse(artifacts=entries, next_cursor=response.get("NextContinuationToken"))


@router.get("/{benchmark_id}/artifacts/download-url", response_model=RunArtifactDownloadResponse)
async def get_run_artifact_url(
    benchmark_id: TrackedBenchmarkId, run_context: RunAWSDependency, path: str = Query(min_length=1)
) -> RunArtifactDownloadResponse:
    """Return a temporary download URL for an exact artifact in the authorized run."""
    path = _path(path)
    runtime = run_context.aws_runtime
    key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{path}"
    ttl = runtime.clients.maximum_presign_ttl(300)
    with _storage_errors():
        async with runtime.clients.s3_client() as client:
            metadata = await client.head_object(Bucket=runtime.resources.s3_bucket, Key=key)
            url = await client.generate_presigned_url(
                "get_object", Params={"Bucket": runtime.resources.s3_bucket, "Key": key}, ExpiresIn=ttl
            )
    return RunArtifactDownloadResponse(path=path, download_url=url, expires_in=ttl, size=metadata["ContentLength"])
