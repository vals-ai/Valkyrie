"""GET /agents — list agents from S3 under the org's bucket."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Org, OrgConfig, User
from tracker.database.session import get_session
from tracker.s3 import list_s3_agent_names
from tracker.types import AgentEntry, AgentsResponse, AWSCredentials

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

    aws = AWSCredentials(
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
        aws_default_region=cfg.aws_default_region,
    )
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
