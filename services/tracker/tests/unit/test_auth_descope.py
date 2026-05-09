from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine

from tracker.auth import RequestIdentity, get_current_starter, resolve_descope_identity
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


def test_resolve_descope_identity_invalid_api_key_raises_401(mock_descope):
    from descope import AuthException

    mock_descope.exchange_access_key.side_effect = AuthException(status_code=401, error_message="Invalid key")

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_identity("bad-key")
    assert exc_info.value.status_code == 401


def test_org_not_in_db_returns_none(mock_descope, session):
    from tracker.auth import find_org_by_tenant

    org = find_org_by_tenant("nonexistent-org", session)
    assert org is None


def test_resolve_descope_identity_full_claims(mock_descope):
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "sub": "K2abc",
        "email": "Alice@Vals.AI",
        "name": "Alice Smith",
    }

    tenant, key_id, email, name = resolve_descope_identity("valid-key")
    assert tenant == "test-tenant"
    assert key_id == "K2abc"
    assert email == "alice@vals.ai"
    assert name == "Alice Smith"


def test_resolve_descope_identity_missing_email_returns_none(mock_descope):
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "sub": "K2abc",
    }

    _tenant, key_id, email, name = resolve_descope_identity("valid-key")
    assert key_id == "K2abc"
    assert email is None
    assert name is None


def test_resolve_descope_identity_missing_name_returns_none(mock_descope):
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "sub": "K2abc",
        "email": "alice@vals.ai",
    }

    _tenant, _key_id, email, name = resolve_descope_identity("valid-key")
    assert email == "alice@vals.ai"
    assert name is None


def test_resolve_descope_identity_whitespace_only_email_treated_as_missing(mock_descope):
    """A whitespace-only email claim is treated identically to a missing one."""
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "sub": "K2abc",
        "email": "   ",
    }

    _tenant, _key_id, email, _name = resolve_descope_identity("valid-key")
    assert email is None


def test_resolve_descope_identity_multiple_tenants_raises_400(mock_descope):
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"a": {}, "b": {}},
        "sub": "K2abc",
        "email": "alice@vals.ai",
    }

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_identity("multi-tenant-key")
    assert exc_info.value.status_code == 400


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
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "sub": "K2abc",
        "email": "alice@vals.ai",
        "name": "Alice",
    }

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
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "sub": "K2abc",
    }

    fake_request = MagicMock()
    fake_request.headers = {"x-api-key": "valid-key"}

    identity = get_current_starter(fake_request, session)

    assert identity.org.id == test_org.id
    assert identity.access_key_id == "K2abc"
    assert identity.email is None
