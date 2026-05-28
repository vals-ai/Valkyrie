"""Authentication and org resolution for the tracker service."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from descope import AuthException, DescopeClient
from fastapi import Depends, HTTPException, Request
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from tracker.config import AUTH_REQUIRED, DESCOPE_PROJECT_ID
from tracker.database.models import DEFAULT_ORG_NAME, Org, User
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


def get_or_create_user(
    session: Session,
    *,
    descope_user_id: str,
    email: str,
    org: Org,
) -> User:
    """Idempotent lookup-or-insert keyed on descope_user_id. Updates email if changed."""
    existing = session.exec(select(User).where(User.descope_user_id == descope_user_id)).first()
    if existing is not None:
        # Only update email if the new value is non-empty; access-key auth
        # doesn't carry email, so we never overwrite a real address with "".
        if email and existing.email != email:
            existing.email = email
            session.add(existing)
            session.commit()
            session.refresh(existing)
        return existing

    user = User(org_id=org.id, email=email, descope_user_id=descope_user_id)
    session.add(user)
    try:
        session.commit()
        session.refresh(user)
        return user
    except IntegrityError:
        session.rollback()
        # Another worker won the race — re-fetch.
        existing = session.exec(select(User).where(User.descope_user_id == descope_user_id)).first()
        if existing is None:
            raise  # Truly unexpected
        return existing


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


def _extract_bearer_token(request: Request) -> str | None:
    auth = request.headers.get("authorization")
    if not auth:
        return None
    parts = auth.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1]


def resolve_bearer_session(jwt: str, session: Session) -> tuple[User, Org]:
    """Validate a Descope session JWT and resolve the (user, org) pair."""
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")

    try:
        jwt_response = _descope_client.validate_session(jwt)
    except AuthException as e:
        raise HTTPException(status_code=401, detail=f"Invalid session: {e.error_message}") from e

    tenants = list(jwt_response.get("tenants", {}).keys())
    if not tenants:
        raise HTTPException(status_code=400, detail="Session token has no tenant")
    # Platform enforces single-tenancy per user (see master plan decision #15); take the first.
    tenant_name = tenants[0]

    org = find_org_by_tenant(tenant_name, session)
    if not org:
        raise HTTPException(status_code=404, detail=f"Organization '{tenant_name}' not configured")

    descope_user_id = jwt_response.get("userId") or jwt_response.get("user_id")
    email = jwt_response.get("email") or jwt_response.get("user", {}).get("email") or ""
    if not descope_user_id:
        raise HTTPException(status_code=400, detail="Session token missing userId")

    user = get_or_create_user(session, descope_user_id=descope_user_id, email=email, org=org)
    return user, org


def get_current_user_and_org(request: Request, session: Session = Depends(get_session)) -> tuple[User | None, Org]:
    """Dispatch table for the two supported auth header conventions.

    - Authorization: Bearer <session_jwt> -> validate_session -> (User, Org)
    - x-api-key: <access_key>             -> exchange_access_key -> (User|None, Org)

    Self-hosted mode (AUTH_REQUIRED=false) short-circuits to the default org with no user.
    """
    if not AUTH_REQUIRED:
        return None, get_default_org(session)

    bearer = _extract_bearer_token(request)
    api_key = request.headers.get("x-api-key")

    if bearer and api_key:
        raise HTTPException(status_code=401, detail="Send Authorization OR x-api-key, not both")
    if not bearer and not api_key:
        raise HTTPException(status_code=401, detail="Missing Authorization or x-api-key header")

    if bearer:
        return resolve_bearer_session(bearer, session)

    # x-api-key path — reuse existing validation, then lift user_id if present
    if api_key is None:
        raise HTTPException(status_code=401, detail="Missing x-api-key header")
    if not _descope_client:
        raise RuntimeError("Descope client not initialized")
    try:
        jwt_response = _descope_client.exchange_access_key(api_key)
    except AuthException as e:
        raise HTTPException(status_code=401, detail=f"Invalid API key: {e.error_message}") from e

    tenants = list(jwt_response.get("tenants", {}).keys())
    if len(tenants) != 1:
        raise HTTPException(status_code=400, detail="Access key must be scoped to exactly one tenant")
    org = find_org_by_tenant(tenants[0], session)
    if not org:
        raise HTTPException(status_code=404, detail=f"Organization '{tenants[0]}' not configured")

    # Custom claims live inside sessionToken for exchange_access_key responses.
    session_token = jwt_response.get("sessionToken") or {}
    descope_user_id = (
        session_token.get("user_id")
        or session_token.get("userId")
        or jwt_response.get("user_id")
        or jwt_response.get("userId")
    )
    email = session_token.get("email") or jwt_response.get("email") or ""
    user: User | None = None
    if descope_user_id:
        user = get_or_create_user(session, descope_user_id=descope_user_id, email=email, org=org)
    return user, org


def get_current_org(request: Request, session: Session = Depends(get_session)) -> Org:
    """FastAPI dependency that resolves the current org.

    Accepts both ``Authorization: Bearer <session_jwt>`` (web UI) and
    ``x-api-key: <access_key>`` (CLI/CI). Delegates to ``get_current_user_and_org``
    so all endpoints get dual-auth without per-handler changes.
    """
    _, org = get_current_user_and_org(request, session)
    return org


def resolve_registry_auth_headers(
    custom_benchmark_service: str | None,
    org_config: object,
    secret_resolver: Callable[[str], str],
) -> dict[str, str]:
    """Look up auth header for a benchmark service URL in OrgConfig.benchmark_services.

    Returns {<auth_header_name>: <resolved_secret>} if the URL matches an entry
    with both auth_header_name and auth_secret_name set. Returns {} otherwise.
    """
    if not custom_benchmark_service:
        return {}

    benchmark_services: list[dict[str, object]] | None = getattr(org_config, "benchmark_services", None)
    if not benchmark_services:
        return {}

    for entry in benchmark_services:
        if entry.get("url") == custom_benchmark_service:
            secret_name = entry.get("auth_secret_name")
            header_name = entry.get("auth_header_name")
            if not secret_name or not header_name:
                return {}
            return {str(header_name): secret_resolver(str(secret_name))}

    return {}
