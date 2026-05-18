from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from descope import AuthException
from fastapi import HTTPException
from requests.exceptions import ReadTimeout
from sqlmodel import Session, SQLModel, create_engine

from tracker.auth import (
    RequestIdentity,
    find_org_by_tenant,
    get_current_org,
    get_current_starter,
    resolve_descope_identity,
)
from tracker.database.models import DEFAULT_ORG_NAME, Org


@pytest.fixture
def session():
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def test_org(session: Session) -> Org:
    org = Org(id=uuid4(), name="test-tenant")
    session.add(org)
    session.commit()
    return org


@pytest.fixture
def mock_descope():
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


def test_valid_api_key_resolves_identity(mock_descope):
    mock_descope.exchange_access_key.return_value = descope_access_key_response()

    identity = resolve_descope_identity("valid-key")
    assert identity.tenant_name == "test-tenant"
    assert identity.access_key_id == "K2abc"


def test_valid_api_key_finds_org(mock_descope, session, test_org):
    mock_descope.exchange_access_key.return_value = descope_access_key_response()

    identity = resolve_descope_identity("valid-key")
    org = find_org_by_tenant(identity.tenant_name, session)
    assert org is not None
    assert org.id == test_org.id


def test_resolve_descope_identity_invalid_api_key_raises_401(mock_descope):
    mock_descope.exchange_access_key.side_effect = AuthException(status_code=401, error_message="Invalid key")

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_identity("bad-key")
    assert exc_info.value.status_code == 401


def test_org_not_in_db_returns_none(mock_descope, session):
    org = find_org_by_tenant("nonexistent-org", session)
    assert org is None


def test_resolve_descope_identity_full_claims(mock_descope):
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


def test_resolve_descope_identity_loads_user_profile_when_requested(mock_descope):
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


def test_resolve_descope_identity_skips_user_profile_lookup_by_default(mock_descope):
    mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")

    identity = resolve_descope_identity("valid-key")

    assert identity.access_key_id == "K2abc"
    assert identity.email is None
    assert identity.name is None
    mock_descope.mgmt.user.load_by_user_id.assert_not_called()


def test_resolve_descope_identity_missing_email_returns_none(mock_descope):
    mock_descope.exchange_access_key.return_value = descope_access_key_response()

    identity = resolve_descope_identity("valid-key")
    assert identity.access_key_id == "K2abc"
    assert identity.email is None
    assert identity.name is None


def test_resolve_descope_identity_missing_name_returns_none(mock_descope):
    mock_descope.exchange_access_key.return_value = descope_access_key_response(email="alice@vals.ai")

    identity = resolve_descope_identity("valid-key")
    assert identity.email == "alice@vals.ai"
    assert identity.name is None


def test_resolve_descope_identity_whitespace_only_email_treated_as_missing(mock_descope):
    """A whitespace-only email claim is treated identically to a missing one."""
    mock_descope.exchange_access_key.return_value = descope_access_key_response(email="   ")

    identity = resolve_descope_identity("valid-key")
    assert identity.email is None


def test_resolve_descope_identity_multiple_tenants_raises_400(mock_descope):
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"a": {}, "b": {}},
        "keyId": "K2abc",
        "sessionToken": {"sub": "K2abc", "email": "alice@vals.ai"},
    }

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_identity("multi-tenant-key")
    assert exc_info.value.status_code == 400


def test_resolve_descope_identity_retries_read_timeout(mock_descope):
    mock_descope.exchange_access_key.side_effect = [
        ReadTimeout("HTTPSConnectionPool(host='api.descope.com', port=443): Read timed out. (read timeout=60)"),
        descope_access_key_response(),
    ]

    identity = resolve_descope_identity("some-key")

    assert identity.tenant_name == "test-tenant"
    assert mock_descope.exchange_access_key.call_count == 2


def test_resolve_descope_identity_returns_503_when_retries_exhausted(mock_descope):
    mock_descope.exchange_access_key.side_effect = ReadTimeout(
        "HTTPSConnectionPool(host='api.descope.com', port=443): Read timed out. (read timeout=60)"
    )

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_identity("some-key")
    assert exc_info.value.status_code == 503
    assert mock_descope.exchange_access_key.call_count == 3


def test_get_current_starter_self_hosted(monkeypatch, session):
    session.add(Org(id=uuid4(), name=DEFAULT_ORG_NAME))
    session.commit()

    monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", False)
    monkeypatch.setattr("tracker.auth._cached_default_org", None)

    fake_request = MagicMock()
    identity = get_current_starter(fake_request, session)

    assert isinstance(identity, RequestIdentity)
    assert identity.org.name == DEFAULT_ORG_NAME
    assert identity.access_key_id is None
    assert identity.email is None
    assert identity.name is None


def test_get_current_starter_hosted_full_claims(monkeypatch, mock_descope, session, test_org):
    monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
    mock_descope.exchange_access_key.return_value = descope_access_key_response(email="alice@vals.ai", name="Alice")

    fake_request = MagicMock()
    fake_request.headers = {"x-api-key": "valid-key"}

    identity = get_current_starter(fake_request, session)

    assert isinstance(identity, RequestIdentity)
    assert identity.org.id == test_org.id
    assert identity.access_key_id == "K2abc"
    assert identity.email == "alice@vals.ai"
    assert identity.name == "Alice"


def test_get_current_starter_hosted_missing_email(monkeypatch, mock_descope, session, test_org):
    monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
    mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")
    mock_descope.mgmt.user.load_by_user_id.return_value = {
        "user": {
            "email": "alice@vals.ai",
            "displayName": "Alice",
        },
    }

    fake_request = MagicMock()
    fake_request.headers = {"x-api-key": "valid-key"}

    identity = get_current_starter(fake_request, session)

    assert identity.org.id == test_org.id
    assert identity.access_key_id == "K2abc"
    assert identity.email == "alice@vals.ai"
    assert identity.name == "Alice"


def test_get_current_org_hosted_skips_user_profile_lookup(monkeypatch, mock_descope, session, test_org):
    monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
    mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U2abc")

    fake_request = MagicMock()
    fake_request.headers = {"x-api-key": "valid-key"}

    org = get_current_org(fake_request, session)

    assert org.id == test_org.id
    mock_descope.mgmt.user.load_by_user_id.assert_not_called()
