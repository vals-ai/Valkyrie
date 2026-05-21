from unittest.mock import patch, MagicMock

import pytest
from sqlmodel import Session, select

from tracker.database.models import Org


@pytest.fixture
def mock_descope():
    mock_client = MagicMock()
    with patch("tracker.auth._descope_client", mock_client):
        yield mock_client


def _ensure_org(session: Session, name: str = "test-tenant") -> Org:
    existing = session.exec(select(Org).where(Org.name == name)).first()
    if existing:
        return existing
    org = Org(name=name)
    session.add(org)
    session.commit()
    session.refresh(org)
    return org


def test_bearer_lifts_email_from_user_claim(mock_descope, database_session):
    """The Descope SDK's validate_session puts email under jwt_response['user']['email'], not top-level."""
    from tracker.auth import resolve_bearer_session

    _ensure_org(database_session, "test-tenant")
    mock_descope.validate_session.return_value = {
        "tenants": {"test-tenant": {}},
        "userId": "U_nested",
        "user": {"email": "nested@example.com"},
    }

    user, _ = resolve_bearer_session("jwt", database_session)
    assert user.email == "nested@example.com"


def test_bearer_email_top_level_still_works(mock_descope, database_session):
    """Top-level email continues to work as a fallback."""
    from tracker.auth import resolve_bearer_session

    _ensure_org(database_session, "test-tenant-2")
    mock_descope.validate_session.return_value = {
        "tenants": {"test-tenant-2": {}},
        "userId": "U_topfallback",
        "email": "top@example.com",
    }

    user, _ = resolve_bearer_session("jwt", database_session)
    assert user.email == "top@example.com"
