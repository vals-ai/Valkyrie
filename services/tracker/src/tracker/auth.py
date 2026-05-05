"""Authentication and org resolution for the tracker service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from descope import AuthException, DescopeClient
from fastapi import Depends, HTTPException, Request
from sqlmodel import Session, select

from tracker.config import AUTH_REQUIRED, DESCOPE_PROJECT_ID
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.logging import get_logger

logger = get_logger(__name__)

BENCHMARK_SERVICE_API_KEY_HEADER = "X-Descope-Api-Key"


@dataclass(frozen=True)
class RequestIdentity:
    """Identity that authenticated the current request.

    In hosted mode `access_key_id` is always set; `email` and `name` are populated only
    when the corresponding custom claims are present on the Descope access key. In
    self-hosted mode all three are None.
    """

    org: Org
    access_key_id: str | None
    email: str | None
    name: str | None


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


def resolve_descope_identity(api_key: str) -> tuple[str, str, str | None, str | None]:
    """Validate an API key and return (tenant_name, access_key_id, email, name).

    Pulls all four from the same JWT response — no extra Descope round-trips. Logs a
    warning when the `email` custom claim is missing so admins can locate keys that
    need updating in Descope.
    """
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

    access_key_id = jwt_response.get("sub")
    if not access_key_id:
        raise HTTPException(status_code=400, detail="Descope JWT missing 'sub' claim")

    raw_email = jwt_response.get("email")
    email = raw_email.strip().lower() if isinstance(raw_email, str) and raw_email.strip() else None
    raw_name = jwt_response.get("name")
    name = raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else None

    if email is None:
        logger.warning(
            "Access key %s has no 'email' custom claim; run attribution will be empty for runs started with this key",
            access_key_id,
        )

    return tenants[0], access_key_id, email, name


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


def get_current_starter(request: Request, session: Session = Depends(get_session)) -> RequestIdentity:
    """FastAPI dependency that returns the full identity behind the current request.

    Self-hosted (AUTH_REQUIRED=False): returns RequestIdentity with default org and Nones.
    Hosted (AUTH_REQUIRED=True): validates Descope API key and resolves org + identity.
    """
    if not AUTH_REQUIRED:
        return RequestIdentity(org=get_default_org(session), access_key_id=None, email=None, name=None)

    api_key = extract_api_key(request)
    tenant_name, access_key_id, email, name = resolve_descope_identity(api_key)
    org = find_org_by_tenant(tenant_name, session)
    if not org:
        raise HTTPException(
            status_code=404,
            detail=f"Organization '{tenant_name}' not configured — run valk config init",
        )
    return RequestIdentity(org=org, access_key_id=access_key_id, email=email, name=name)


def get_current_org(request: Request, session: Session = Depends(get_session)) -> Org:
    """FastAPI dependency that resolves the current org. Thin shim over get_current_starter."""
    return get_current_starter(request, session).org
