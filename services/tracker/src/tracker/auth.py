"""Authentication and org resolution for the tracker service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Literal, cast

from descope.descope_client import DescopeClient
from descope.exceptions import AuthException
from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tracker import config
from tracker.config import AUTH_REQUIRED, DESCOPE_MANAGEMENT_KEY, DESCOPE_PROJECT_ID
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.logging import get_logger
from tracker.outbound_security import validate_service_headers

logger = get_logger(__name__)

BENCHMARK_SERVICE_API_KEY_HEADER = "X-Descope-Api-Key"
DESCOPE_ACCESS_KEY_ID_FIELD = "keyId"
DESCOPE_CUSTOM_CLAIMS_FIELD = "customClaims"
DESCOPE_BENCHMARK_SERVICE_PURPOSE = "valkyrie_benchmark_service"
DESCOPE_SESSION_TOKEN_FIELD = "sessionToken"
DESCOPE_USER_ID_CLAIM = "user_id"


@dataclass(frozen=True)
class BearerIdentity:
    org: Org
    principal_id: str
    email: str | None
    kind: Literal["bearer"] = "bearer"


@dataclass(frozen=True)
class AccessKeyIdentity:
    org: Org
    principal_id: str
    email: str | None
    kind: Literal["access_key"] = "access_key"


@dataclass(frozen=True)
class SelfHostedIdentity:
    org: Org
    principal_id: None = None
    email: None = None
    kind: Literal["self_hosted"] = "self_hosted"


RequestIdentity = BearerIdentity | AccessKeyIdentity | SelfHostedIdentity


@dataclass(frozen=True)
class DescopeIdentity:
    tenant_name: str
    principal_id: str
    email: str | None


@dataclass(frozen=True)
class BearerCredential:
    token: str


@dataclass(frozen=True)
class AccessKeyCredential:
    cleartext: str


RequestCredential = BearerCredential | AccessKeyCredential

_bearer_scheme = HTTPBearer(
    auto_error=False,
    bearerFormat="JWT",
    scheme_name="DescopeBearer",
)
_access_key_scheme = APIKeyHeader(
    name="x-api-key",
    auto_error=False,
    scheme_name="DescopeAccessKey",
)
BearerSecurity = Annotated[
    HTTPAuthorizationCredentials | None,
    Security(_bearer_scheme),
]
AccessKeySecurity = Annotated[str | None, Security(_access_key_scheme)]

_cached_default_org: Org | None = None
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
    if isinstance(session_token, Mapping):
        session_claims = cast(Mapping[str, object], session_token)
        if claim_name in session_claims:
            return session_claims.get(claim_name)

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

        claims = cast(Mapping[str, object], claim_source)
        custom_claims = claims.get(DESCOPE_CUSTOM_CLAIMS_FIELD)
        if isinstance(custom_claims, Mapping):
            typed_custom_claims = cast(Mapping[str, object], custom_claims)
            if claim_name in typed_custom_claims:
                return _normalize_optional_string(typed_custom_claims.get(claim_name), lowercase=lowercase)

    return None


def _get_descope_tenants(jwt_response: Mapping[str, object]) -> list[str]:
    tenant_claims = jwt_response.get("tenants")
    if not isinstance(tenant_claims, Mapping):
        return []
    return [tenant for tenant in cast(Mapping[object, object], tenant_claims) if isinstance(tenant, str)]


def _eligible_bearer_tenant(jwt_response: Mapping[str, object]) -> str:
    allowed_tenants = config.managed_tenant_ids()
    eligible_tenants = [tenant for tenant in _get_descope_tenants(jwt_response) if tenant in allowed_tenants]
    if not eligible_tenants:
        raise HTTPException(status_code=403, detail="Session is not eligible for managed Valkyrie")
    if len(eligible_tenants) > 1:
        raise HTTPException(status_code=400, detail="Session matches multiple managed Valkyrie tenants")
    return eligible_tenants[0]


def _load_descope_user_email(user_id: str) -> str | None:
    """Load email from a Descope user record."""
    if not _descope_client:
        return None

    try:
        user_response = cast(
            Mapping[str, object],
            _descope_client.mgmt.user.load_by_user_id(user_id),  # pyright: ignore[reportUnknownMemberType]
        )
    except Exception:
        logger.warning("Failed to load Descope user profile for user_id=%s", user_id, exc_info=True)
        return None

    user = user_response.get("user")
    if not isinstance(user, Mapping):
        logger.warning("Descope user profile response did not include a user object for user_id=%s", user_id)
        return None

    user_claims = cast(Mapping[str, object], user)
    return _normalize_optional_string(user_claims.get("email"), lowercase=True)


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


def _extract_request_credential(request: Request) -> RequestCredential:
    authorization = request.headers.get("authorization")
    api_key = request.headers.get("x-api-key")

    if authorization is not None and api_key is not None:
        raise HTTPException(status_code=401, detail="Send Authorization OR x-api-key, not both")
    if api_key is not None:
        if not api_key.strip():
            raise HTTPException(status_code=401, detail="Missing Authorization or x-api-key header")
        return AccessKeyCredential(api_key)
    if authorization is None:
        raise HTTPException(status_code=401, detail="Missing Authorization or x-api-key header")

    scheme, separator, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or separator != " " or not token or any(char.isspace() for char in token):
        raise HTTPException(status_code=401, detail="Authorization must be Bearer <session JWT>")
    return BearerCredential(token)


def extract_api_key(request: Request) -> str:
    """Extract API key from request headers. Raises 401 if missing."""
    credential = _extract_request_credential(request)
    if not isinstance(credential, AccessKeyCredential):
        raise HTTPException(status_code=401, detail="This endpoint requires x-api-key")
    return credential.cleartext


@retry(
    retry=retry_if_exception_type((ReadTimeout, RequestsConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=2),
    reraise=True,
)
def _exchange_access_key(api_key: str, descope_client: DescopeClient) -> Mapping[str, object]:
    """Call Descope with retries on transient network errors."""
    return cast(
        Mapping[str, object],
        descope_client.exchange_access_key(api_key),  # pyright: ignore[reportUnknownMemberType]
    )


def resolve_descope_identity(api_key: str, *, include_user_profile: bool = False) -> DescopeIdentity:
    """Validate an API key and return its Descope tenant and attribution identity.

    Lightweight callers use only the exchanged access-key JWT. Callers that need
    attribution can request the bound user profile, which adds one Descope
    management API lookup when the JWT carries user_id but lacks email.

    Supported access-key response shape:
        {
            "tenants": {"vals.ai": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "customClaims": {"user_id": "U2abc"},
            },
        }
    """
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")
    try:
        jwt_response = _exchange_access_key(api_key, _descope_client)
    except AuthException as exc:
        logger.warning("Descope API key validation failed: %s", exc.error_message)
        raise HTTPException(status_code=401, detail="Invalid API key") from exc
    except Exception as exc:
        logger.exception("Descope API key validation failed")
        raise HTTPException(status_code=503, detail="Auth service unavailable") from exc

    purpose = _get_descope_custom_string_claim(jwt_response, "purpose")
    if purpose == DESCOPE_BENCHMARK_SERVICE_PURPOSE:
        raise HTTPException(status_code=401, detail="Invalid API key")

    tenants = _get_descope_tenants(jwt_response)
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

    email = _get_descope_string_claim(jwt_response, "email", lowercase=True)
    user_id = _get_descope_custom_string_claim(jwt_response, DESCOPE_USER_ID_CLAIM)

    if include_user_profile and email is None and user_id is not None:
        email = _load_descope_user_email(user_id)

    return DescopeIdentity(tenant_name=tenants[0], principal_id=access_key_id, email=email)


def find_org_by_tenant(tenant_name: str, session: Session) -> Org | None:
    """Look up an org by Descope tenant name. Returns None if not found."""
    return session.exec(select(Org).where(Org.name == tenant_name)).first()


def _find_or_create_org(tenant_name: str, session: Session) -> Org:
    org = find_org_by_tenant(tenant_name, session)
    if org:
        return org

    org = Org(name=tenant_name)
    session.add(org)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        org = find_org_by_tenant(tenant_name, session)
        assert org is not None
        return org

    session.refresh(org)
    return org


def forward_tracker_api_key(
    service_headers: Mapping[str, str] | None,
    tracker_api_key: str | None,
) -> dict[str, str]:
    """Copy service headers and inject the tracker API key for benchmark-service auth.

    The downstream benchmark-service auth header must not reuse ``X-Api-Key`` because that
    header is already reserved for Daytona sandbox credentials.
    """
    forwarded_headers = dict(service_headers or {})
    try:
        validate_service_headers(forwarded_headers)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Unsupported benchmark service header") from exc
    if not tracker_api_key:
        return forwarded_headers

    has_explicit_override = any(key.lower() == BENCHMARK_SERVICE_API_KEY_HEADER.lower() for key in forwarded_headers)
    if not has_explicit_override:
        forwarded_headers[BENCHMARK_SERVICE_API_KEY_HEADER] = tracker_api_key

    return forwarded_headers


def resolve_bearer_session(
    jwt: str,
    session: Session,
    *,
    include_user_profile: bool = False,
) -> BearerIdentity:
    """Validate a Descope session JWT and resolve its user and organization."""
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")

    try:
        jwt_response = cast(
            Mapping[str, object],
            _descope_client.validate_session(jwt),  # pyright: ignore[reportUnknownMemberType]
        )
    except AuthException as exc:
        logger.warning("Descope session validation failed: %s", exc.error_message)
        raise HTTPException(status_code=401, detail="Invalid session") from exc

    if _get_descope_custom_string_claim(jwt_response, "purpose") == DESCOPE_BENCHMARK_SERVICE_PURPOSE:
        raise HTTPException(status_code=401, detail="Invalid session")

    tenant = _eligible_bearer_tenant(jwt_response)

    subject = (
        _get_descope_string_claim(jwt_response, "userId")
        or _get_descope_string_claim(jwt_response, DESCOPE_USER_ID_CLAIM)
        or _get_descope_string_claim(jwt_response, "sub")
    )
    if subject is None:
        raise HTTPException(status_code=400, detail="Descope session missing user subject")

    email = _get_descope_string_claim(jwt_response, "email", lowercase=True)
    user = jwt_response.get("user")
    if isinstance(user, Mapping):
        user_claims = cast(Mapping[str, object], user)
        email = email or _normalize_optional_string(user_claims.get("email"), lowercase=True)

    if include_user_profile and email is None:
        email = _load_descope_user_email(subject)

    return BearerIdentity(
        org=_find_or_create_org(tenant, session),
        principal_id=subject,
        email=email,
    )


def _resolve_hosted_identity(
    request: Request,
    session: Session,
    *,
    include_user_profile: bool,
) -> RequestIdentity:
    credential = _extract_request_credential(request)
    if isinstance(credential, BearerCredential):
        return resolve_bearer_session(
            credential.token,
            session,
            include_user_profile=include_user_profile,
        )

    identity = resolve_descope_identity(credential.cleartext, include_user_profile=include_user_profile)
    org = find_org_by_tenant(identity.tenant_name, session)
    if not org:
        raise HTTPException(
            status_code=404,
            detail=f"Organization '{identity.tenant_name}' not configured — run valk config init",
        )
    return AccessKeyIdentity(
        org=org,
        principal_id=identity.principal_id,
        email=identity.email,
    )


def get_current_org(
    request: Request,
    session: Session = Depends(get_session),
    _bearer: BearerSecurity = None,
    _api_key: AccessKeySecurity = None,
) -> Org:
    """Resolve the current org from either an Authorization: Bearer or x-api-key header."""
    if not AUTH_REQUIRED:
        return get_default_org(session)
    return _resolve_hosted_identity(request, session, include_user_profile=False).org


def get_current_starter(
    request: Request,
    session: Session = Depends(get_session),
    _bearer: BearerSecurity = None,
    _api_key: AccessKeySecurity = None,
) -> RequestIdentity:
    """FastAPI dependency that returns the full identity behind the current request.

    Self-hosted (AUTH_REQUIRED=False): returns the default organization identity.
    Hosted (AUTH_REQUIRED=True): validates one Descope credential and resolves org + identity.
    """
    if not AUTH_REQUIRED:
        return SelfHostedIdentity(org=get_default_org(session))
    return _resolve_hosted_identity(request, session, include_user_profile=True)
