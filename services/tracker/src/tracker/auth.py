"""Authentication and org resolution for the tracker service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import hmac
import math
import secrets
from threading import Condition
import time
from typing import Any, cast

from cachetools import TLRUCache, cached
from descope import DescopeClient
from descope.exceptions import AuthException
from fastapi import Depends, HTTPException, Request
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout
from sqlmodel import Session, select
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tracker.config import (
    AUTH_REQUIRED,
    DESCOPE_MANAGEMENT_KEY,
    DESCOPE_PROJECT_ID,
    BenchmarkServiceDestination,
)
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.logging import get_logger
from tracker.outbound_security import validate_service_headers

logger = get_logger(__name__)

BENCHMARK_SERVICE_API_KEY_HEADER = "X-Descope-Api-Key"
DESCOPE_ACCESS_KEY_ID_FIELD = "keyId"
DESCOPE_CUSTOM_CLAIMS_FIELD = "customClaims"
DESCOPE_SESSION_TOKEN_FIELD = "sessionToken"
DESCOPE_USER_ID_CLAIM = "user_id"
_ACCESS_KEY_CACHE_MAX_SIZE = 1024
_ACCESS_KEY_CACHE_TTL_SECONDS = 10.0
_ACCESS_KEY_EXPIRY_SKEW_SECONDS = 1.0


@dataclass(frozen=True)
class RequestIdentity:
    """Identity that authenticated the current request.

    In hosted mode `access_key_id` is always set. `email` and `name` are populated
    from Descope claims or the bound user profile when the caller requests it. In
    self-hosted mode all three are None. The access key id is persisted as
    `Benchmark.started_by_id` to preserve the exact credential used to start the run.
    """

    org: Org
    access_key_id: str | None
    email: str | None
    name: str | None


@dataclass(frozen=True)
class DescopeUserProfile:
    email: str | None
    name: str | None


@dataclass(frozen=True)
class DescopeIdentity:
    tenant_name: str
    access_key_id: str
    email: str | None
    name: str | None


@dataclass(frozen=True)
class CachedAccessKeyClaims:
    tenant_name: str
    access_key_id: str
    email: str | None
    name: str | None
    user_id: str | None
    cache_deadline_monotonic: float


_cached_default_org: Org | None = None
_access_key_digest_secret = secrets.token_bytes(32)
_access_key_cache_condition = Condition()
_access_key_cache: TLRUCache[bytes, CachedAccessKeyClaims, float] = TLRUCache(
    maxsize=_ACCESS_KEY_CACHE_MAX_SIZE,
    ttu=lambda _key, claims, _now: claims.cache_deadline_monotonic,
    timer=time.monotonic,
)
_descope_client: DescopeClient | None = (
    DescopeClient(
        project_id=DESCOPE_PROJECT_ID,
        management_key=DESCOPE_MANAGEMENT_KEY or None,
    )
    if AUTH_REQUIRED and DESCOPE_PROJECT_ID
    else None
)


def _get_descope_claim(jwt_response: Mapping[str, object], claim_name: str) -> object:
    """Read a claim from the exchange response or its nested session token."""
    session_token = jwt_response.get(DESCOPE_SESSION_TOKEN_FIELD)
    if isinstance(session_token, Mapping) and claim_name in session_token:
        return session_token.get(claim_name)

    return jwt_response.get(claim_name)


def _normalize_optional_string(value: object, *, lowercase: bool = False) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    return normalized.lower() if lowercase else normalized


def _get_descope_string_claim(
    jwt_response: Mapping[str, object], claim_name: str, *, lowercase: bool = False
) -> str | None:
    return _normalize_optional_string(_get_descope_claim(jwt_response, claim_name), lowercase=lowercase)


def _get_descope_custom_string_claim(
    jwt_response: Mapping[str, object], claim_name: str, *, lowercase: bool = False
) -> str | None:
    """Read a string claim, including values nested under customClaims."""
    claim = _get_descope_string_claim(jwt_response, claim_name, lowercase=lowercase)
    if claim is not None:
        return claim

    for claim_source in (jwt_response, jwt_response.get(DESCOPE_SESSION_TOKEN_FIELD)):
        if not isinstance(claim_source, Mapping):
            continue

        custom_claims = claim_source.get(DESCOPE_CUSTOM_CLAIMS_FIELD)
        if isinstance(custom_claims, Mapping) and claim_name in custom_claims:
            return _normalize_optional_string(custom_claims.get(claim_name), lowercase=lowercase)

    return None


def _load_descope_user_profile(user_id: str) -> DescopeUserProfile:
    """Load email/name from the Descope user record bound to an access key."""
    if not _descope_client:
        return DescopeUserProfile(email=None, name=None)

    try:
        user_response = _descope_client.mgmt.user.load_by_user_id(user_id)
    except Exception:
        logger.warning("Failed to load Descope user profile")
        return DescopeUserProfile(email=None, name=None)

    user = user_response.get("user")
    if not isinstance(user, Mapping):
        logger.warning("Descope user profile response did not include a user object")
        return DescopeUserProfile(email=None, name=None)

    email = _normalize_optional_string(user.get("email"), lowercase=True)
    name = _normalize_optional_string(user.get("name") or user.get("displayName"))
    return DescopeUserProfile(email=email, name=name)


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


@retry(
    retry=retry_if_exception_type((ReadTimeout, RequestsConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=2),
    reraise=True,
)
def _exchange_access_key(api_key: str, descope_client: DescopeClient) -> dict[str, Any]:
    """Call Descope with retries on transient network errors."""
    return cast(dict[str, Any], descope_client.exchange_access_key(api_key))


def _access_key_digest(api_key: str) -> bytes:
    """Return a process-keyed digest without retaining the presented credential."""
    return hmac.digest(_access_key_digest_secret, api_key.encode(), hashlib.sha256)


def _cache_deadline(jwt_expires_at: float, *, wall_now: float, monotonic_now: float) -> float:
    remaining = jwt_expires_at - wall_now - _ACCESS_KEY_EXPIRY_SKEW_SECONDS
    cache_seconds = min(_ACCESS_KEY_CACHE_TTL_SECONDS, max(0.0, remaining))
    return monotonic_now + cache_seconds


def _normalize_access_key_claims(jwt_response: Mapping[str, object]) -> CachedAccessKeyClaims:
    tenants_value = jwt_response.get("tenants")
    tenant_mapping: Mapping[object, object] = (
        cast(Mapping[object, object], tenants_value) if isinstance(tenants_value, Mapping) else {}
    )
    tenants = [str(tenant) for tenant in tenant_mapping]
    if len(tenants) != 1:
        raise HTTPException(
            status_code=400,
            detail=f"Access key must be scoped to exactly one tenant, got {len(tenants)}",
        )

    access_key_id = _get_descope_string_claim(jwt_response, DESCOPE_ACCESS_KEY_ID_FIELD) or _get_descope_string_claim(
        jwt_response, "sub"
    )
    if not access_key_id:
        raise HTTPException(status_code=400, detail="Descope JWT missing access key id")

    expires_value = _get_descope_claim(jwt_response, "exp")
    if isinstance(expires_value, (int, float)) and not isinstance(expires_value, bool):
        try:
            jwt_expires_at = float(expires_value)
        except OverflowError:
            jwt_expires_at = 0.0
        if not math.isfinite(jwt_expires_at):
            jwt_expires_at = 0.0
    else:
        jwt_expires_at = 0.0
    cache_deadline = _cache_deadline(
        jwt_expires_at,
        wall_now=time.time(),
        monotonic_now=time.monotonic(),
    )
    return CachedAccessKeyClaims(
        tenant_name=str(tenants[0]),
        access_key_id=access_key_id,
        email=_get_descope_string_claim(jwt_response, "email", lowercase=True),
        name=_get_descope_string_claim(jwt_response, "name"),
        user_id=_get_descope_custom_string_claim(jwt_response, DESCOPE_USER_ID_CLAIM),
        cache_deadline_monotonic=cache_deadline,
    )


@cached(cache=_access_key_cache, key=_access_key_digest, condition=_access_key_cache_condition)
def _exchange_and_normalize_access_key(api_key: str) -> CachedAccessKeyClaims:
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")

    try:
        jwt_response = _exchange_access_key(api_key, _descope_client)
    except AuthException as exc:
        logger.warning("Descope API key validation failed")
        raise HTTPException(status_code=401, detail="Invalid API key") from exc
    except Exception as exc:
        logger.warning("Descope API key validation failed because the provider is unavailable")
        raise HTTPException(status_code=503, detail="Auth service unavailable") from exc

    return _normalize_access_key_claims(jwt_response)


def resolve_descope_identity(api_key: str, *, include_user_profile: bool = False) -> DescopeIdentity:
    """Validate an API key and return its Descope tenant and attribution identity."""
    claims = _exchange_and_normalize_access_key(api_key)
    email = claims.email
    name = claims.name
    if include_user_profile and email is None and claims.user_id is not None:
        profile = _load_descope_user_profile(claims.user_id)
        email = profile.email
        name = name or profile.name

    return DescopeIdentity(
        tenant_name=claims.tenant_name,
        access_key_id=claims.access_key_id,
        email=email,
        name=name,
    )


def find_org_by_tenant(tenant_name: str, session: Session) -> Org | None:
    """Look up an org by Descope tenant name. Returns None if not found."""
    return session.exec(select(Org).where(Org.name == tenant_name)).first()


def forward_tracker_api_key(
    service_headers: Mapping[str, str] | None,
    tracker_api_key: str | None,
    *,
    destination: BenchmarkServiceDestination,
) -> dict[str, str]:
    """Copy service headers and inject the tracker API key only for hosted services.

    The downstream benchmark-service auth header must not reuse ``X-Api-Key`` because that
    header is already reserved for Daytona sandbox credentials.
    """
    forwarded_headers = dict(service_headers or {})
    try:
        validate_service_headers(forwarded_headers)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Unsupported benchmark service header") from exc
    if not tracker_api_key or destination is not BenchmarkServiceDestination.HOSTED:
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


def resolve_bearer_session(jwt: str, session: Session) -> Org:
    """Validate a Descope session JWT and resolve the org."""
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")

    try:
        jwt_response = _descope_client.validate_session(jwt)
    except AuthException as exc:
        logger.warning("Descope session validation failed")
        raise HTTPException(status_code=401, detail="Invalid session") from exc

    tenants = list(jwt_response.get("tenants", {}).keys())
    if not tenants:
        raise HTTPException(status_code=400, detail="Session token has no tenant")
    tenant_name = tenants[0]

    org = find_org_by_tenant(tenant_name, session)
    if not org:
        raise HTTPException(status_code=404, detail=f"Organization '{tenant_name}' not configured")

    return org


def get_current_org(request: Request, session: Session = Depends(get_session)) -> Org:
    """Resolve the current org from either an Authorization: Bearer or x-api-key header."""
    if not AUTH_REQUIRED:
        return get_default_org(session)

    bearer = _extract_bearer_token(request)
    api_key = request.headers.get("x-api-key") or ""

    if bearer and api_key:
        raise HTTPException(status_code=401, detail="Send Authorization OR x-api-key, not both")
    if not bearer and not api_key:
        raise HTTPException(status_code=401, detail="Missing Authorization or x-api-key header")

    if bearer:
        return resolve_bearer_session(bearer, session)

    identity = resolve_descope_identity(api_key)
    org = find_org_by_tenant(identity.tenant_name, session)
    if not org:
        raise HTTPException(status_code=404, detail=f"Organization '{identity.tenant_name}' not configured")

    return org


def get_current_starter(request: Request, session: Session = Depends(get_session)) -> RequestIdentity:
    """FastAPI dependency that returns the full identity behind the current request.

    Self-hosted (AUTH_REQUIRED=False): returns RequestIdentity with default org and Nones.
    Hosted (AUTH_REQUIRED=True): validates Descope API key and resolves org + identity.
    """
    if not AUTH_REQUIRED:
        return RequestIdentity(org=get_default_org(session), access_key_id=None, email=None, name=None)

    api_key = extract_api_key(request)
    identity = resolve_descope_identity(api_key, include_user_profile=True)
    org = find_org_by_tenant(identity.tenant_name, session)
    if not org:
        raise HTTPException(
            status_code=404,
            detail=f"Organization '{identity.tenant_name}' not configured — run valk config init",
        )
    return RequestIdentity(org=org, access_key_id=identity.access_key_id, email=identity.email, name=identity.name)
