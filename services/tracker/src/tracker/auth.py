"""Authentication and org resolution for the tracker service."""

from __future__ import annotations

from collections.abc import Mapping

from descope import AuthException, DescopeClient
from fastapi import Depends, HTTPException, Request
from sqlmodel import Session, select

from tracker.config import AUTH_REQUIRED, DESCOPE_PROJECT_ID
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.logging import get_logger

logger = get_logger(__name__)

BENCHMARK_SERVICE_API_KEY_HEADER = "X-Descope-Api-Key"

_cached_default_org: Org | None = None
_descope_client: DescopeClient | None = (
    DescopeClient(project_id=DESCOPE_PROJECT_ID) if AUTH_REQUIRED and DESCOPE_PROJECT_ID else None
)


def get_default_org(session: Session) -> Org:
    """Fetch the default org, cached after first load. Used in self-hosted mode."""
    global _cached_default_org
    if _cached_default_org is not None:
        return _cached_default_org
    org = session.exec(select(Org).where(Org.name == DEFAULT_ORG_NAME)).first()
    if not org:
        raise RuntimeError("Default org not found — run the migration")
    _cached_default_org = org
    return org


def extract_api_key(request: Request) -> str:
    """Extract API key from request headers. Raises 401 if missing."""
    api_key = request.headers.get("x-api-key")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API key")
    return api_key


def resolve_descope_tenant(api_key: str) -> str:
    """Validate an API key against Descope and return the tenant name (no DB lookup)."""
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")
    try:
        jwt_response = _descope_client.exchange_access_key(api_key)
    except AuthException as e:
        raise HTTPException(status_code=401, detail=f"Invalid API key: {e.error_message}") from e

    tenants = list(jwt_response.get("tenants", {}).keys())
    if len(tenants) != 1:
        raise HTTPException(
            status_code=400,
            detail=f"Access key must be scoped to exactly one tenant, got {len(tenants)}",
        )
    return tenants[0]


def find_org_by_tenant(tenant_name: str, session: Session) -> Org | None:
    """Look up an org by Descope tenant name. Returns None if not found."""
    return session.exec(select(Org).where(Org.name == tenant_name)).first()


def forward_tracker_api_key(
    service_headers: Mapping[str, str] | None,
    tracker_api_key: str | None,
) -> dict[str, str]:
    """Copy service headers and inject the tracker API key for benchmark-service auth.

    The downstream benchmark-service auth header must not reuse ``X-Api-Key`` because that
    header is already reserved for Daytona sandbox credentials.
    """
    forwarded_headers = dict(service_headers or {})
    if not tracker_api_key:
        return forwarded_headers

    has_explicit_override = any(key.lower() == BENCHMARK_SERVICE_API_KEY_HEADER.lower() for key in forwarded_headers)
    if not has_explicit_override:
        forwarded_headers[BENCHMARK_SERVICE_API_KEY_HEADER] = tracker_api_key

    return forwarded_headers


def get_current_org(request: Request, session: Session = Depends(get_session)) -> Org:
    """FastAPI dependency that resolves the current org.

    Self-hosted (AUTH_REQUIRED=false): returns default org.
    Hosted (AUTH_REQUIRED=true): validates Descope API key and resolves org.
    """
    if not AUTH_REQUIRED:
        return get_default_org(session)

    api_key = extract_api_key(request)
    tenant_name = resolve_descope_tenant(api_key)
    org = find_org_by_tenant(tenant_name, session)
    if not org:
        raise HTTPException(
            status_code=404,
            detail=f"Organization '{tenant_name}' not configured — run valk config init",
        )
    return org
