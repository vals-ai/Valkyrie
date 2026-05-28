"""GET /agents — list agents from S3 under the org's bucket."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Org, OrgConfig, User
from tracker.database.session import get_session
from tracker.s3 import generate_presigned_get_url, list_s3_agent_names, s3_object_exists
from tracker.types import AgentDownloadURLResponse, AgentEntry, AgentsResponse, AWSCredentials

router = APIRouter()


@router.get("/agents", response_model=AgentsResponse)
def list_agents(
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> AgentsResponse:
    _, org = user_and_org
    cfg = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()
    if cfg is None:
        return AgentsResponse(agents=[])

    aws = AWSCredentials.from_org_config(cfg)
    rows = list_s3_agent_names(aws=aws, s3_bucket=cfg.s3_bucket)
    return AgentsResponse(
        agents=[
            AgentEntry(
                name=str(r["name"]),
                last_modified=str(r["last_modified"]) if r.get("last_modified") else None,
            )
            for r in rows
        ]
    )


@router.get("/agents/{name}/download-url", response_model=AgentDownloadURLResponse)
def get_agent_download_url(
    name: str,
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> AgentDownloadURLResponse:
    _, org = user_and_org
    cfg = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()
    if cfg is None:
        raise HTTPException(status_code=404, detail="Org config not set")

    aws = AWSCredentials.from_org_config(cfg)
    key = f"agents/{name}.zip"
    if not s3_object_exists(key, aws=aws, s3_bucket=cfg.s3_bucket):
        raise HTTPException(status_code=404, detail=f"Agent '{name}' not found in S3")

    expires_in = 300
    url = generate_presigned_get_url(key, aws=aws, s3_bucket=cfg.s3_bucket, ttl_seconds=expires_in)
    return AgentDownloadURLResponse(name=name, download_url=url, expires_in=expires_in)
