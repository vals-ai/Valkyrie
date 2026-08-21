"""Agent-library storage routes: list, presigned download/upload URLs, delete."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from tracker.api.dependencies import get_agent_library_aws_runtime, validated_agent_name
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import create_presigned_url, delete_from_s3, list_agents, s3_object_exists
from tracker.types import AgentDownloadURLResponse, AgentEntry, AgentsResponse, AgentUploadURLResponse

PRESIGNED_URL_EXPIRES_SECONDS = 300

router = APIRouter(prefix="/agents")


@router.get("", response_model=AgentsResponse)
async def list_agents_endpoint(
    aws_runtime: AWSRuntime = Depends(get_agent_library_aws_runtime),
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
    aws_runtime: AWSRuntime = Depends(get_agent_library_aws_runtime),
) -> AgentDownloadURLResponse:
    """Return a 5-minute presigned URL to download agents/<name>.zip."""
    key = f"agents/{name}.zip"
    if not await s3_object_exists(key, aws_runtime):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")

    expires_in = aws_runtime.clients.maximum_presign_ttl(PRESIGNED_URL_EXPIRES_SECONDS)
    url = await create_presigned_url(
        key,
        aws_runtime,
        expiration=expires_in,
    )
    return AgentDownloadURLResponse(name=name, download_url=url, expires_in=expires_in)


@router.post("/{name}/upload-url", response_model=AgentUploadURLResponse)
async def get_agent_upload_url(
    name: str,
    aws_runtime: AWSRuntime = Depends(get_agent_library_aws_runtime),
) -> AgentUploadURLResponse:
    """Return a presigned single-part PUT URL for agents/<name>.zip."""
    key = f"agents/{validated_agent_name(name)}.zip"
    expires_in = aws_runtime.clients.maximum_presign_ttl(PRESIGNED_URL_EXPIRES_SECONDS)
    url = await create_presigned_url(key, aws_runtime, expiration=expires_in, client_method="put_object")
    return AgentUploadURLResponse(name=name, upload_url=url, expires_in=expires_in)


@router.delete("/{name}", status_code=204)
async def delete_agent(
    name: str,
    aws_runtime: AWSRuntime = Depends(get_agent_library_aws_runtime),
) -> None:
    """Delete agents/<name>.zip from the org's bucket."""
    key = f"agents/{validated_agent_name(name)}.zip"
    if not await s3_object_exists(key, aws_runtime):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")

    await delete_from_s3(key, aws_runtime)
