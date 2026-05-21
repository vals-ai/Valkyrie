from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, Request
from sqlmodel import Session, select

from tracker.database.models import Org


@pytest.fixture
def mock_descope():
    mock_client = MagicMock()
    with patch("tracker.auth._descope_client", mock_client):
        yield mock_client


def _make_request(headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    return Request(scope)


def _ensure_org(session: Session, name: str = "test-tenant") -> Org:
    existing = session.exec(select(Org).where(Org.name == name)).first()
    if existing:
        return existing
    org = Org(name=name)
    session.add(org)
    session.commit()
    session.refresh(org)
    return org


def test_bearer_valid_session_resolves_user_and_org(mock_descope, database_session):
    from tracker.auth import resolve_bearer_session

    org = _ensure_org(database_session, "test-tenant")
    mock_descope.validate_session.return_value = {
        "tenants": {"test-tenant": {}},
        "userId": "U_alice",
        "email": "alice@example.com",
    }

    user, resolved_org = resolve_bearer_session("session-jwt-string", database_session)
    assert resolved_org.id == org.id
    assert user is not None
    assert user.descope_user_id == "U_alice"
    assert user.email == "alice@example.com"


def test_bearer_expired_session_raises_401(mock_descope, database_session):
    from descope import AuthException
    from tracker.auth import resolve_bearer_session

    mock_descope.validate_session.side_effect = AuthException(
        status_code=401, error_type="expired", error_message="Session expired"
    )

    with pytest.raises(HTTPException) as exc_info:
        resolve_bearer_session("expired-jwt", database_session)
    assert exc_info.value.status_code == 401


def test_bearer_unknown_tenant_returns_404(mock_descope, database_session):
    from tracker.auth import resolve_bearer_session

    mock_descope.validate_session.return_value = {
        "tenants": {"unknown-tenant": {}},
        "userId": "U_x",
        "email": "x@x.com",
    }

    with pytest.raises(HTTPException) as exc_info:
        resolve_bearer_session("jwt", database_session)
    assert exc_info.value.status_code == 404


def test_get_current_user_and_org_bearer_path(mock_descope, database_session):
    from tracker.auth import get_current_user_and_org

    _ensure_org(database_session, "test-tenant")
    mock_descope.validate_session.return_value = {
        "tenants": {"test-tenant": {}},
        "userId": "U_alice",
        "email": "alice@example.com",
    }

    request = _make_request({"authorization": "Bearer some-jwt"})
    with patch("tracker.auth.AUTH_REQUIRED", True):
        user, org = get_current_user_and_org(request, database_session)
    assert user is not None
    assert org.name == "test-tenant"


def test_get_current_user_and_org_apikey_path(mock_descope, database_session):
    from tracker.auth import get_current_user_and_org

    _ensure_org(database_session, "test-tenant")
    mock_descope.exchange_access_key.return_value = {
        "tenants": {"test-tenant": {}},
        "userId": "U_ci",
        "email": "ci@example.com",
    }

    request = _make_request({"x-api-key": "ak_xxx"})
    with patch("tracker.auth.AUTH_REQUIRED", True):
        user, org = get_current_user_and_org(request, database_session)
    assert org.name == "test-tenant"
    assert user is not None and user.descope_user_id == "U_ci"


def test_both_headers_returns_401(mock_descope, database_session):
    from tracker.auth import get_current_user_and_org

    request = _make_request({"authorization": "Bearer x", "x-api-key": "ak_x"})
    with patch("tracker.auth.AUTH_REQUIRED", True):
        with pytest.raises(HTTPException) as exc_info:
            get_current_user_and_org(request, database_session)
    assert exc_info.value.status_code == 401


def test_no_headers_returns_401(mock_descope, database_session):
    from tracker.auth import get_current_user_and_org

    request = _make_request({})
    with patch("tracker.auth.AUTH_REQUIRED", True):
        with pytest.raises(HTTPException) as exc_info:
            get_current_user_and_org(request, database_session)
    assert exc_info.value.status_code == 401
