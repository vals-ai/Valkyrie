"""GET and PUT /org-config endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Org, OrgConfig, User
from tracker.database.session import get_session
from tracker.types import MASKED_SECRET, OrgConfigResponse, OrgConfigUpdate

router = APIRouter()


@router.get("/org-config", response_model=OrgConfigResponse)
def get_org_config(
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> OrgConfigResponse:
    _, org = user_and_org
    config = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()
    if config is None:
        raise HTTPException(status_code=404, detail="OrgConfig not yet configured")
    return OrgConfigResponse.from_org_config(config)


@router.put("/org-config", response_model=OrgConfigResponse)
def put_org_config(
    update: OrgConfigUpdate,
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> OrgConfigResponse:
    _, org = user_and_org
    config = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()

    if config is None:
        # Upsert path: secrets must be concrete (not masked).
        if update.aws_secret_access_key == MASKED_SECRET or update.daytona_secret_name == MASKED_SECRET:
            raise HTTPException(
                status_code=400,
                detail="Cannot create initial OrgConfig with masked secrets; provide real values",
            )
        config = OrgConfig(
            org_id=org.id,
            aws_access_key_id=update.aws_access_key_id,
            aws_secret_access_key=update.aws_secret_access_key,
            aws_default_region=update.aws_default_region,
            s3_bucket=update.s3_bucket,
            daytona_secret_name=update.daytona_secret_name,
            log_group=update.log_group,
            log_retention_policy=update.log_retention_policy,
            webhook=update.webhook if update.webhook != MASKED_SECRET else None,
        )
        session.add(config)
    else:
        update.apply_to(config)
        session.add(config)

    session.commit()
    session.refresh(config)
    return OrgConfigResponse.from_org_config(config)
