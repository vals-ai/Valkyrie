"""GET /agents — list agents from S3 under the org's bucket."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from tracker.api.dependencies import get_access_key_aws_runtime
from tracker.auth import get_current_org
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import create_presigned_url, list_agents, s3_object_exists
from tracker.database.models import Org
from tracker.types import AgentDownloadURLResponse, AgentEntry, AgentsResponse

PRESIGNED_URL_EXPIRES_SECONDS = 300

router = APIRouter(prefix="/agents")


@router.get("", response_model=AgentsResponse)
async def list_agents_endpoint(
    _org: Org = Depends(get_current_org),
    aws_runtime: AWSRuntime = Depends(get_access_key_aws_runtime),
) -> AgentsResponse:
    """List agent zips under the org's S3 bucket."""
    agents = await list_agents(aws_runtime)
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
    aws_runtime: AWSRuntime = Depends(get_access_key_aws_runtime),
) -> AgentDownloadURLResponse:
    """Return a 5-minute presigned URL to download agents/<name>.zip."""
    key = f"agents/{name}.zip"
    if not await s3_object_exists(key, aws_runtime):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")

    url = await create_presigned_url(
        key,
        aws_runtime,
        expiration=PRESIGNED_URL_EXPIRES_SECONDS,
    )
    return AgentDownloadURLResponse(name=name, download_url=url, expires_in=PRESIGNED_URL_EXPIRES_SECONDS)
