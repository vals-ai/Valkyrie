"""Map each sandbox provider to the organization secret that holds its configuration."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, col, select

from tracker.api.secrets import ADMINS_ONLY, require_own_secret, require_secret_writer
from tracker.auth import get_current_org
from tracker.database.models import Org, OrgProviderSecret
from tracker.database.session import get_session
from tracker.types import ProviderName, ProviderSecretRequest, ProvidersResponse

router = APIRouter(prefix="/sandbox-providers")


def provider_secrets(session: Session, org_id: UUID) -> dict[str, str]:
    rows = session.exec(select(OrgProviderSecret).where(col(OrgProviderSecret.org_id) == org_id))
    return {row.provider: row.secret_name for row in rows}


@router.get("", response_model=ProvidersResponse)
def list_providers(org: Org = Depends(get_current_org), session: Session = Depends(get_session)) -> ProvidersResponse:
    """List which secret holds each sandbox provider's configuration, by name."""
    return ProvidersResponse(providers=provider_secrets(session, org.id))


@router.put(
    "/{provider}",
    response_model=ProvidersResponse,
    responses={403: ADMINS_ONLY, 404: {"description": "Secret does not exist"}},
)
async def set_provider(
    provider: ProviderName,
    body: ProviderSecretRequest,
    org: Org = Depends(require_secret_writer),
    session: Session = Depends(get_session),
) -> ProvidersResponse:
    """Use the named secret for this sandbox provider's runs."""
    await require_own_secret(body.secret_name, org)
    session.merge(OrgProviderSecret(org_id=org.id, provider=provider, secret_name=body.secret_name))
    session.commit()
    return ProvidersResponse(providers=provider_secrets(session, org.id))


@router.delete(
    "/{provider}",
    response_model=ProvidersResponse,
    responses={403: ADMINS_ONLY, 404: {"description": "Provider is not mapped"}},
)
def remove_provider(
    provider: ProviderName, org: Org = Depends(require_secret_writer), session: Session = Depends(get_session)
) -> ProvidersResponse:
    """Stop using a secret for this sandbox provider."""
    row = session.get(OrgProviderSecret, (org.id, provider))
    if row is None:
        raise HTTPException(status_code=404, detail=f"Provider '{provider}' has no secret")
    session.delete(row)
    session.commit()
    return ProvidersResponse(providers=provider_secrets(session, org.id))
