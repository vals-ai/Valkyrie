"""Manage organization-shared agents."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from tracker.agent.contract import MAX_AGENT_ZIP_BYTES
from tracker.agent.schemas import validate_agent_name
from tracker.auth import get_current_org
from tracker.aws.s3 import (
    create_presigned_upload,
    create_presigned_url,
    delete_from_s3,
    get_contract_s3_key,
    get_s3_object_size,
    list_agents,
    s3_object_exists,
)
from tracker.database.models import Org
from tracker.types import (
    AgentDownloadURLResponse,
    AgentEntry,
    AgentsResponse,
    AgentUploadURLResponse,
    HarnessConfig,
    StatusResponse,
)
from tracker.utils import fetch_harness_config

PRESIGNED_URL_EXPIRES_SECONDS = 300

router = APIRouter(prefix="/agents")


def _valid_name(name: str) -> str:
    try:
        return validate_agent_name(name)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.get("", response_model=AgentsResponse)
async def list_agents_endpoint(
    _org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> AgentsResponse:
    """List the organization's agents."""
    agents = await list_agents(
        aws=harness_config.aws,
        s3_bucket=harness_config.s3_bucket,
        s3_prefix=harness_config.s3_prefix,
    )
    return AgentsResponse(
        agents=[
            AgentEntry(name=name, last_modified=str(last_modified) if last_modified else None)
            for name, last_modified in agents
        ]
    )


@router.get("/{name}/download-url", response_model=AgentDownloadURLResponse)
async def get_agent_download_url(
    name: str,
    _org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> AgentDownloadURLResponse:
    """Return a 5-minute presigned URL to download agents/<name>.zip."""
    name = _valid_name(name)
    key = get_contract_s3_key(name, harness_config.s3_prefix)
    if not await s3_object_exists(key, aws=harness_config.aws, s3_bucket=harness_config.s3_bucket):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")
    if await get_s3_object_size(key, aws=harness_config.aws, s3_bucket=harness_config.s3_bucket) > MAX_AGENT_ZIP_BYTES:
        raise HTTPException(status_code=413, detail="Agent zip exceeds the 100 MiB limit")

    url = await create_presigned_url(
        key,
        aws=harness_config.aws,
        s3_bucket=harness_config.s3_bucket,
        expiration=PRESIGNED_URL_EXPIRES_SECONDS,
    )
    return AgentDownloadURLResponse(name=name, download_url=url, expires_in=PRESIGNED_URL_EXPIRES_SECONDS)


@router.post("/{name}/upload-url", response_model=AgentUploadURLResponse)
async def get_agent_upload_url(
    name: str,
    size_bytes: int = Query(gt=0, le=MAX_AGENT_ZIP_BYTES),
    _org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> AgentUploadURLResponse:
    """Return a 5-minute presigned URL to upload an organization agent."""
    name = _valid_name(name)
    key = get_contract_s3_key(name, harness_config.s3_prefix)
    upload = await create_presigned_upload(
        key,
        aws=harness_config.aws,
        s3_bucket=harness_config.s3_bucket,
        max_bytes=size_bytes,
        expiration=PRESIGNED_URL_EXPIRES_SECONDS,
    )
    return AgentUploadURLResponse(
        name=name,
        upload_url=upload["url"],
        fields=upload["fields"],
        expires_in=PRESIGNED_URL_EXPIRES_SECONDS,
    )


@router.delete("/{name}", response_model=StatusResponse)
async def delete_agent(
    name: str,
    _org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> StatusResponse:
    """Delete an organization agent."""
    name = _valid_name(name)
    key = get_contract_s3_key(name, harness_config.s3_prefix)
    if not await s3_object_exists(key, aws=harness_config.aws, s3_bucket=harness_config.s3_bucket):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")

    await delete_from_s3(key, aws=harness_config.aws, s3_bucket=harness_config.s3_bucket)
    return StatusResponse(status="success")
