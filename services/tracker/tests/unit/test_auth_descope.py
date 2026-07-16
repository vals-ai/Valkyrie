"""Tests for Tracker Descope authentication boundaries.

Run: uv run pytest tests/unit/test_auth_descope.py
"""

from collections.abc import Generator
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from descope.exceptions import AuthException
from fastapi import HTTPException
from requests.exceptions import ReadTimeout
from sqlmodel import Session
from tenacity import wait_none

import tracker.auth as auth_module
from tracker.auth import (
    RequestIdentity,
    find_org_by_tenant,
    get_current_org,
    get_current_starter,
    resolve_bearer_session,
    resolve_descope_identity,
)
from tracker.database.models import DEFAULT_ORG_NAME, Org


@pytest.fixture
def test_org(empty_database_session: Session) -> Org:
    org = Org(id=uuid4(), name="test-tenant")
    empty_database_session.add(org)
    empty_database_session.commit()
    return org


@pytest.fixture
def mock_descope() -> Generator[MagicMock, None, None]:
    mock_client = MagicMock()
    with patch("tracker.auth._descope_client", mock_client):
        yield mock_client


def descope_access_key_response(
    *,
    tenant: str = "test-tenant",
    key_id: str = "K2abc",
    email: str | None = None,
    name: str | None = None,
    user_id: str | None = None,
) -> dict[str, object]:
    session_token: dict[str, object] = {
        "sub": key_id,
        "tenants": {tenant: {}},
    }
    if email is not None:
        session_token["email"] = email
    if name is not None:
        session_token["name"] = name
    if user_id is not None:
        session_token["customClaims"] = {"user_id": user_id}

    return {
        "tenants": {tenant: {}},
        "keyId": key_id,
        "sessionToken": session_token,
    }


def disable_auth_retry_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep retry behavior while removing production backoff from unit tests."""
    exchange_access_key = getattr(auth_module, "_exchange_access_key")
    monkeypatch.setattr(exchange_access_key.retry, "wait", wait_none())


class TestDescopeIdentityResolution:
    """Hosted identity, bearer session, and organization resolution."""

    def test_valid_api_key_resolves_identity(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()

        identity = resolve_descope_identity("valid-key")
        assert identity.tenant_name == "test-tenant"
        assert identity.access_key_id == "K2abc"

    def test_valid_api_key_finds_org(
        self, mock_descope: MagicMock, empty_database_session: Session, test_org: Org
    ) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()

        identity = resolve_descope_identity("valid-key")
        org = find_org_by_tenant(identity.tenant_name, empty_database_session)
        assert org is not None
        assert org.id == test_org.id

    def test_resolve_descope_identity_invalid_api_key_raises_401(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.side_effect = AuthException(status_code=401, error_message="Invalid key")

        with pytest.raises(HTTPException) as exc_info:
            resolve_descope_identity("bad-key")

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == "Invalid API key"
        assert "Invalid key" not in str(exc_info.value.detail)

    def test_resolve_bearer_session_invalid_token_raises_safe_401(
        self, mock_descope: MagicMock, empty_database_session: Session
    ) -> None:
        mock_descope.validate_session.side_effect = AuthException(
            status_code=401, error_message="Sensitive provider detail"
        )

        with pytest.raises(HTTPException) as exc_info:
            resolve_bearer_session("bad-session", empty_database_session)

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == "Invalid session"
        assert "Sensitive provider detail" not in str(exc_info.value.detail)

    def test_org_not_in_db_returns_none(self, empty_database_session: Session) -> None:
        org = find_org_by_tenant("nonexistent-org", empty_database_session)
        assert org is None

    def test_resolve_descope_identity_full_claims(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(
            email="Alice@Vals.AI",
            name="Alice Smith",
        )

        identity = resolve_descope_identity("valid-key")
        assert identity.tenant_name == "test-tenant"
        assert identity.access_key_id == "K2abc"
        assert identity.email == "alice@vals.ai"
        assert identity.name == "Alice Smith"
        mock_descope.mgmt.user.load_by_user_id.assert_not_called()

    def test_resolve_descope_identity_loads_user_profile_when_requested(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")
        mock_descope.mgmt.user.load_by_user_id.return_value = {
            "user": {
                "email": "Alice@Vals.AI",
                "displayName": "Alice Smith",
            },
        }

        identity = resolve_descope_identity("valid-key", include_user_profile=True)

        assert identity.tenant_name == "test-tenant"
        assert identity.access_key_id == "K2abc"
        assert identity.email == "alice@vals.ai"
        assert identity.name == "Alice Smith"
        mock_descope.mgmt.user.load_by_user_id.assert_called_once_with("U2abc")

    def test_resolve_descope_identity_skips_user_profile_lookup_by_default(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")

        identity = resolve_descope_identity("valid-key")

        assert identity.access_key_id == "K2abc"
        assert identity.email is None
        assert identity.name is None
        mock_descope.mgmt.user.load_by_user_id.assert_not_called()

    def test_resolve_descope_identity_missing_email_returns_none(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()

        identity = resolve_descope_identity("valid-key")
        assert identity.access_key_id == "K2abc"
        assert identity.email is None
        assert identity.name is None

    def test_resolve_descope_identity_missing_name_returns_none(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(email="alice@vals.ai")

        identity = resolve_descope_identity("valid-key")
        assert identity.email == "alice@vals.ai"
        assert identity.name is None

    def test_resolve_descope_identity_whitespace_only_email_treated_as_missing(self, mock_descope: MagicMock) -> None:
        """A whitespace-only email claim is treated identically to a missing one."""
        mock_descope.exchange_access_key.return_value = descope_access_key_response(email="   ")

        identity = resolve_descope_identity("valid-key")
        assert identity.email is None

    def test_resolve_descope_identity_multiple_tenants_raises_400(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = {
            "tenants": {"a": {}, "b": {}},
            "keyId": "K2abc",
            "sessionToken": {"sub": "K2abc", "email": "alice@vals.ai"},
        }

        with pytest.raises(HTTPException) as exc_info:
            resolve_descope_identity("multi-tenant-key")
        assert exc_info.value.status_code == 400

    def test_resolve_descope_identity_retries_read_timeout(
        self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Transient Descope timeouts must retry and return the eventual identity.

        Test cases:
        - The first exchange times out.
        - The second exchange succeeds without a real retry delay.
        """
        disable_auth_retry_wait(monkeypatch)
        mock_descope.exchange_access_key.side_effect = [
            ReadTimeout("HTTPSConnectionPool(host='api.descope.com', port=443): Read timed out. (read timeout=60)"),
            descope_access_key_response(),
        ]

        identity = resolve_descope_identity("some-key")

        assert identity.tenant_name == "test-tenant"
        assert mock_descope.exchange_access_key.call_count == 2

    def test_resolve_descope_identity_returns_503_when_retries_exhausted(
        self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exhausted Descope retries must return a stable service-unavailable error.

        Test cases:
        - All three exchanges time out.
        - The client receives a safe 503 response after immediate test retries.
        """
        disable_auth_retry_wait(monkeypatch)
        mock_descope.exchange_access_key.side_effect = ReadTimeout(
            "HTTPSConnectionPool(host='api.descope.com', port=443): Read timed out. (read timeout=60)"
        )

        with patch("tracker.auth.logger.exception") as log_exception:
            with pytest.raises(HTTPException) as exc_info:
                resolve_descope_identity("some-key")

        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == "Auth service unavailable"
        log_exception.assert_called_once_with("Descope API key validation failed")
        assert mock_descope.exchange_access_key.call_count == 3


class TestCurrentStarterResolution:
    """Starter and organization dependencies across hosting modes."""

    def test_get_current_starter_self_hosted(
        self, monkeypatch: pytest.MonkeyPatch, empty_database_session: Session
    ) -> None:
        empty_database_session.add(Org(id=uuid4(), name=DEFAULT_ORG_NAME))
        empty_database_session.commit()

        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", False)
        monkeypatch.setattr("tracker.auth._cached_default_org", None)

        mock_request = MagicMock()
        identity = get_current_starter(mock_request, empty_database_session)

        assert isinstance(identity, RequestIdentity)
        assert identity.org.name == DEFAULT_ORG_NAME
        assert identity.access_key_id is None
        assert identity.email is None
        assert identity.name is None

    def test_get_current_starter_hosted_full_claims(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        mock_descope.exchange_access_key.return_value = descope_access_key_response(email="alice@vals.ai", name="Alice")

        mock_request = MagicMock()
        mock_request.headers = {"x-api-key": "valid-key"}

        identity = get_current_starter(mock_request, empty_database_session)

        assert isinstance(identity, RequestIdentity)
        assert identity.org.id == test_org.id
        assert identity.access_key_id == "K2abc"
        assert identity.email == "alice@vals.ai"
        assert identity.name == "Alice"

    def test_get_current_starter_hosted_missing_email(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")
        mock_descope.mgmt.user.load_by_user_id.return_value = {
            "user": {
                "email": "alice@vals.ai",
                "displayName": "Alice",
            },
        }

        mock_request = MagicMock()
        mock_request.headers = {"x-api-key": "valid-key"}

        identity = get_current_starter(mock_request, empty_database_session)

        assert identity.org.id == test_org.id
        assert identity.access_key_id == "K2abc"
        assert identity.email == "alice@vals.ai"
        assert identity.name == "Alice"

    def test_get_current_org_hosted_skips_user_profile_lookup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")

        mock_request = MagicMock()
        mock_request.headers = {"x-api-key": "valid-key"}

        org = get_current_org(mock_request, empty_database_session)

        assert org.id == test_org.id
        mock_descope.mgmt.user.load_by_user_id.assert_not_called()
