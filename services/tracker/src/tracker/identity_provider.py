"""Identity-provider seam for tracker authentication.

Core Valkyrie defines the surface the tracker uses to resolve request credentials
into a tenant identity: access-key exchange, session-token validation, and bound
user-profile lookup. The concrete provider is selected by configuration
(``IDENTITY_PROVIDER``, default ``descope``); provider-specific project and
management credentials are deployment configuration owned by the deploying
organization.

Provider methods return the provider's own claims mapping. Today the only
provider is Descope, so the claims follow Descope's access-key exchange and
session-JWT shapes; consumers of :class:`IdentityProvider` interpret that shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from descope.descope_client import DescopeClient
from descope.exceptions import AuthException

from tracker.logging import get_logger

logger = get_logger(__name__)

DESCOPE_PROVIDER = "descope"


class CredentialRejectedError(Exception):
    """The provider rejected the presented credential (invalid or expired)."""


@dataclass(frozen=True)
class UserProfile:
    """Identity attributes from the user record bound to a credential."""

    email: str | None
    name: str | None


def normalize_optional_string(value: object, *, lowercase: bool = False) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    return normalized.lower() if lowercase else normalized


class IdentityProvider(Protocol):
    """Credential-validation surface the tracker uses for hosted authentication.

    ``exchange_access_key`` and ``validate_session`` return the provider's
    claims mapping and raise :class:`CredentialRejectedError` when the
    credential is rejected; transient provider errors propagate unchanged so
    callers can apply their own retry and availability policy.
    """

    def exchange_access_key(self, api_key: str) -> Mapping[str, Any]: ...

    def validate_session(self, session_token: str) -> Mapping[str, Any]: ...

    def load_user_profile(self, user_id: str) -> UserProfile: ...


class DescopeIdentityProvider:
    """Identity provider backed by a Descope project."""

    def __init__(self, *, project_id: str, management_key: str | None) -> None:
        self._client = DescopeClient(project_id=project_id, management_key=management_key)

    def exchange_access_key(self, api_key: str) -> Mapping[str, Any]:
        try:
            return cast(Mapping[str, Any], self._client.exchange_access_key(api_key))
        except AuthException as exc:
            raise CredentialRejectedError("Descope rejected the access key") from exc

    def validate_session(self, session_token: str) -> Mapping[str, Any]:
        try:
            return cast(Mapping[str, Any], self._client.validate_session(session_token))
        except AuthException as exc:
            raise CredentialRejectedError("Descope rejected the session token") from exc

    def load_user_profile(self, user_id: str) -> UserProfile:
        """Load email/name from the Descope user record bound to an access key."""
        try:
            user_response = self._client.mgmt.user.load_by_user_id(user_id)
        except Exception:
            logger.warning("Failed to load Descope user profile")
            return UserProfile(email=None, name=None)

        user = user_response.get("user")
        if not isinstance(user, Mapping):
            logger.warning("Descope user profile response did not include a user object")
            return UserProfile(email=None, name=None)

        email = normalize_optional_string(user.get("email"), lowercase=True)
        name = normalize_optional_string(user.get("name") or user.get("displayName"))
        return UserProfile(email=email, name=name)


def build_identity_provider(
    *,
    auth_required: bool,
    provider_name: str,
    descope_project_id: str,
    descope_management_key: str,
) -> IdentityProvider | None:
    """Build the configured identity provider, or None when auth is disabled or
    the provider's project credentials are not set (lazy failure on use, matching
    previous behavior so self-hosted deployments boot without provider config).
    """
    if not auth_required:
        return None
    if provider_name != DESCOPE_PROVIDER:
        raise ValueError(f"Unsupported IDENTITY_PROVIDER {provider_name!r}; supported providers: {DESCOPE_PROVIDER!r}")
    if not descope_project_id:
        return None
    return DescopeIdentityProvider(
        project_id=descope_project_id,
        management_key=descope_management_key or None,
    )
