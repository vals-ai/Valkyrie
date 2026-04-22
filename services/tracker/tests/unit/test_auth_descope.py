from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine

from tracker.database.models import Org


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


def test_valid_api_key_resolves_tenant(mock_descope):
    from tracker.auth import resolve_descope_tenant

    mock_descope.exchange_access_key.return_value = {"tenants": {"test-tenant": {}}}

    tenant = resolve_descope_tenant("valid-key")
    assert tenant == "test-tenant"


def test_valid_api_key_finds_org(mock_descope, session, test_org):
    from tracker.auth import find_org_by_tenant, resolve_descope_tenant

    mock_descope.exchange_access_key.return_value = {"tenants": {"test-tenant": {}}}

    tenant = resolve_descope_tenant("valid-key")
    org = find_org_by_tenant(tenant, session)
    assert org is not None
    assert org.id == test_org.id


def test_invalid_api_key_raises_401(mock_descope):
    from descope import AuthException
    from tracker.auth import resolve_descope_tenant

    mock_descope.exchange_access_key.side_effect = AuthException(status_code=401, error_message="Invalid key")

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_tenant("bad-key")
    assert exc_info.value.status_code == 401


def test_multiple_tenants_raises_400(mock_descope):
    from tracker.auth import resolve_descope_tenant

    mock_descope.exchange_access_key.return_value = {"tenants": {"tenant-a": {}, "tenant-b": {}}}

    with pytest.raises(HTTPException) as exc_info:
        resolve_descope_tenant("multi-tenant-key")
    assert exc_info.value.status_code == 400


def test_org_not_in_db_returns_none(mock_descope, session):
    from tracker.auth import find_org_by_tenant

    org = find_org_by_tenant("nonexistent-org", session)
    assert org is None
