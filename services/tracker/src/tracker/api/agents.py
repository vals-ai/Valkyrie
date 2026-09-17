"""Manage the shared agents/ library through the selected runtime."""

from __future__ import annotations

import asyncio
import logging
import tempfile
import zipfile
import zlib
from collections.abc import AsyncIterator, Generator
from contextlib import contextmanager
from typing import BinaryIO

import yaml
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Request, Response

from tracker import config
from tracker.agent.archive import ArchiveLimitError, validate_agent_archive
from tracker.agent.schemas import validate_agent_name
from tracker.api.dependencies import AgentLibraryRuntimeDependency
from tracker.runtime.artifacts import list_agents
from tracker.exceptions import S3Error
from tracker.types import AgentDownloadURLResponse, AgentEntry, AgentsResponse

PRESIGNED_URL_EXPIRES_SECONDS = 300
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/agents")


def _agent_key(name: str) -> str:
    try:
        validate_agent_name(name)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    return f"agents/{name}.zip"


@contextmanager
def _storage_errors() -> Generator[None, None, None]:
    try:
        yield
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail="Agent archive not found") from error
    except PermissionError as error:
        raise HTTPException(status_code=403, detail="Agent library storage permission denied") from error
    except S3Error as error:
        cause = error.__cause__
        if isinstance(cause, ClientError) and (
            cause.response.get("Error", {}).get("Code") in {"AccessDenied", "Forbidden", "403"}
            or cause.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 403
        ):
            raise HTTPException(
                status_code=403,
                detail="Agent library storage permission denied; check the deployment's S3 permissions",
            ) from error
        logger.exception("Agent library storage operation failed")
        raise HTTPException(status_code=502, detail="Agent library storage operation failed") from error


async def _file_chunks(stream: BinaryIO) -> AsyncIterator[bytes]:
    while chunk := await asyncio.to_thread(stream.read, 1024 * 1024):
        yield chunk


@router.get("", response_model=AgentsResponse)
async def list_agents_endpoint(
    runtime: AgentLibraryRuntimeDependency,
) -> AgentsResponse:
    """List agent zips in the configured shared library."""
    with _storage_errors():
        agents = await list_agents(runtime.objects)

    return AgentsResponse(
        agents=[
            AgentEntry(name=name, last_modified=str(last_modified) if last_modified else None)
            for name, last_modified in agents
        ]
    )


@router.get("/{name}/download-url", response_model=AgentDownloadURLResponse)
async def get_agent_download_url(
    name: str,
    runtime: AgentLibraryRuntimeDependency,
) -> AgentDownloadURLResponse:
    """Return a signed URL or a relative route requiring Tracker authorization."""
    key = _agent_key(name)
    with _storage_errors():
        if not await runtime.objects.exists(key):
            raise HTTPException(status_code=404, detail=f"Agent '{name}' not found")
        url = await runtime.objects.temporary_download_url(key, expires_in=PRESIGNED_URL_EXPIRES_SECONDS)
        if url is None:
            return AgentDownloadURLResponse(name=name, download_url=f"/agents/{name}/download", expires_in=0)

    return AgentDownloadURLResponse(name=name, download_url=url, expires_in=PRESIGNED_URL_EXPIRES_SECONDS)


@router.get(
    "/{name}/download",
    response_class=Response,
    responses={200: {"content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}}}},
)
async def download_agent_archive(name: str, runtime: AgentLibraryRuntimeDependency) -> Response:
    """Download the library archive using the caller's existing authorization."""
    key = _agent_key(name)
    with _storage_errors():
        content = await runtime.objects.get_bytes(key)
    return Response(
        content=content,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{name}.zip"', "Cache-Control": "no-store"},
    )


@router.put(
    "/{name}",
    response_model=AgentEntry,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/zip": {
                    "schema": {"type": "string", "format": "binary"},
                }
            },
        }
    },
    responses={
        400: {"description": "Invalid agent archive"},
        403: {"description": "Storage permission denied"},
        413: {"description": "Configured archive limit exceeded"},
    },
)
async def push_agent_endpoint(
    name: str,
    request: Request,
    runtime: AgentLibraryRuntimeDependency,
) -> AgentEntry:
    """Validate and upload a ZIP, replacing agents/<name>.zip if it exists."""
    key = _agent_key(name)
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != "application/zip":
        raise HTTPException(status_code=415, detail="Expected application/zip")
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_bytes = int(content_length)
            if declared_bytes < 0:
                raise ValueError("negative content length")
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from error
        if declared_bytes > config.AGENT_UPLOAD_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Agent upload exceeds AGENT_UPLOAD_MAX_BYTES")

    with tempfile.TemporaryFile() as stream:
        uploaded_bytes = 0
        async for chunk in request.stream():
            uploaded_bytes += len(chunk)
            if uploaded_bytes > config.AGENT_UPLOAD_MAX_BYTES:
                raise HTTPException(status_code=413, detail="Agent upload exceeds AGENT_UPLOAD_MAX_BYTES")
            await asyncio.to_thread(stream.write, chunk)
        await asyncio.to_thread(stream.seek, 0)
        try:
            await asyncio.to_thread(validate_agent_archive, stream, name)
        except ArchiveLimitError as error:
            raise HTTPException(status_code=413, detail=str(error)) from error
        except (
            ValueError,
            zipfile.BadZipFile,
            RuntimeError,
            yaml.YAMLError,
            zlib.error,
        ) as error:
            raise HTTPException(status_code=400, detail="Invalid agent archive or contract") from error
        with _storage_errors():
            await runtime.objects.put_stream(key, _file_chunks(stream))

    return AgentEntry(name=name)


@router.delete(
    "/{name}",
    response_model=AgentEntry,
    responses={403: {"description": "Storage permission denied"}, 404: {"description": "Agent not found"}},
)
async def remove_agent_endpoint(
    name: str,
    runtime: AgentLibraryRuntimeDependency,
) -> AgentEntry:
    """Remove an existing ZIP from the configured shared agent library."""
    key = _agent_key(name)
    with _storage_errors():
        if not await runtime.objects.exists(key):
            raise HTTPException(status_code=404, detail=f"Agent '{name}' not found")
        await runtime.objects.delete(key)

    return AgentEntry(name=name)
