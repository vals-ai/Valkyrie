"""GET /agents — list agents from S3 under the org's bucket."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from tracker.auth import get_current_org
from tracker.aws.s3 import create_presigned_url, list_s3_agent_names, s3_object_exists
from tracker.database.models import Org
from tracker.types import AgentDownloadURLResponse, AgentEntry, AgentsResponse, HarnessConfig
from tracker.utils import fetch_harness_config

PRESIGNED_URL_EXPIRES_SECONDS = 300

router = APIRouter(prefix="/agents")


@router.get("", response_model=AgentsResponse)
def list_agents(
    _org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> AgentsResponse:
    """List agent zips under the org's S3 bucket."""
    agents = list_s3_agent_names(aws=harness_config.aws, s3_bucket=harness_config.s3_bucket)
    return AgentsResponse(
        agents=[
            AgentEntry(
                name=str(agent["name"]),
                last_modified=str(agent["last_modified"]) if agent.get("last_modified") else None,
            )
            for agent in agents
        ]
    )


@router.get("/{name}/download-url", response_model=AgentDownloadURLResponse)
def get_agent_download_url(
    name: str,
    _org: Org = Depends(get_current_org),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> AgentDownloadURLResponse:
    """Return a 5-minute presigned URL to download agents/<name>.zip."""
    key = f"agents/{name}.zip"
    if not s3_object_exists(key, aws=harness_config.aws, s3_bucket=harness_config.s3_bucket):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")

    url = create_presigned_url(
        key,
        aws=harness_config.aws,
        s3_bucket=harness_config.s3_bucket,
        expiration=PRESIGNED_URL_EXPIRES_SECONDS,
    )
    return AgentDownloadURLResponse(name=name, download_url=url, expires_in=PRESIGNED_URL_EXPIRES_SECONDS)
