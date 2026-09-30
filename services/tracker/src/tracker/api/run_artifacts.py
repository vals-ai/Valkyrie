"""Organization-scoped run artifact listing and download links."""

import logging
from contextlib import contextmanager
from datetime import datetime
from collections.abc import Generator
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from tracker.api.dependencies import RunRuntimeDependency, TrackedBenchmarkId
from tracker.api.download import local_file_response, resolve_download_url
from tracker.runtime.artifacts import benchmark_prefix
from tracker.exceptions import S3Error

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
    if not value and allow_empty:
        return value
    if "\\" in value or any(part in {"", ".", ".."} for part in value.split("/")):
        raise HTTPException(status_code=400, detail="Artifact path must be a relative file or directory path")
    return value


@contextmanager
def _storage_errors() -> Generator[None]:
    try:
        yield
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail="Artifact not found") from error
    except PermissionError as error:
        raise HTTPException(status_code=403, detail="Artifact storage permission denied") from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            raise HTTPException(status_code=404, detail="Artifact not found") from error
        if code in {"403", "AccessDenied", "Forbidden"}:
            raise HTTPException(status_code=403, detail="Artifact storage permission denied") from error
        logger.exception("Artifact storage operation failed")
        raise HTTPException(status_code=502, detail="Artifact storage operation failed") from error
    except (BotoCoreError, S3Error) as error:
        logger.exception("Artifact storage operation failed")
        raise HTTPException(status_code=502, detail="Artifact storage operation failed") from error


@router.get("/{benchmark_id}/artifacts", response_model=RunArtifactsResponse)
async def list_run_artifacts(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunRuntimeDependency,
    prefix: str = "",
    cursor: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> RunArtifactsResponse:
    """List an exact artifact path or its descendants within the authorized run."""
    prefix = _path(prefix.rstrip("/"), allow_empty=True)
    root = benchmark_prefix(str(benchmark_id))
    with _storage_errors():
        objects, next_cursor = await run_context.objects.list_objects_page(root + prefix, cursor=cursor, limit=limit)
    entries: list[RunArtifactEntry] = []
    for item in objects:
        key = item.key
        if not key.startswith(root) or key.endswith("/"):
            continue
        path = key[len(root) :]
        if prefix and path != prefix and not path.startswith(prefix + "/"):
            continue
        entries.append(RunArtifactEntry(path=path, size=item.size, last_modified=item.last_modified))
    return RunArtifactsResponse(artifacts=entries, next_cursor=next_cursor)


@router.get("/{benchmark_id}/artifacts/download-url", response_model=RunArtifactDownloadResponse)
async def get_run_artifact_url(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunRuntimeDependency,
    request: Request,
    path: str = Query(min_length=1),
    download: bool = False,
) -> RunArtifactDownloadResponse | FileResponse:
    """Return a temporary download URL for an exact artifact in the authorized run."""
    path = _path(path)
    key = benchmark_prefix(str(benchmark_id)) + path
    with _storage_errors():
        metadata = await run_context.objects.stat(key)
        if download:
            response = local_file_response(run_context.objects, key, filename=Path(path).name)
            if response is not None:
                return response
        url, ttl = await resolve_download_url(
            run_context.objects,
            key,
            request=request,
            route_name="get_run_artifact_url",
            route_params={"benchmark_id": benchmark_id},
            query_params={"path": path},
            expires_in=300,
        )
    return RunArtifactDownloadResponse(path=path, download_url=url, expires_in=ttl, size=metadata.size)
