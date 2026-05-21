from uuid import UUID

from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.auth import get_or_create_user
from tracker.database.models import Org


def _org(session: Session) -> Org:
    return session.exec(select(Org).where(Org.id == TEST_ORG_ID)).one()


def test_get_or_create_creates_when_missing(database_session: Session) -> None:
    org = _org(database_session)
    user = get_or_create_user(database_session, descope_user_id="U_new", email="new@x.com", org=org)
    assert isinstance(user.id, UUID)
    assert user.descope_user_id == "U_new"
    assert user.email == "new@x.com"
    assert user.org_id == org.id


def test_get_or_create_returns_existing(database_session: Session) -> None:
    org = _org(database_session)
    first = get_or_create_user(database_session, descope_user_id="U_dupe", email="dupe@x.com", org=org)
    second = get_or_create_user(database_session, descope_user_id="U_dupe", email="dupe@x.com", org=org)
    assert first.id == second.id


def test_get_or_create_updates_email_if_changed(database_session: Session) -> None:
    org = _org(database_session)
    first = get_or_create_user(database_session, descope_user_id="U_emailchange", email="old@x.com", org=org)
    second = get_or_create_user(database_session, descope_user_id="U_emailchange", email="new@x.com", org=org)
    assert first.id == second.id
    assert second.email == "new@x.com"


def test_get_or_create_preserves_email_when_new_value_empty(database_session: Session) -> None:
    """Access-key auth doesn't carry email; don't overwrite a real address with ''."""
    org = _org(database_session)
    first = get_or_create_user(database_session, descope_user_id="U_keepemail", email="real@x.com", org=org)
    second = get_or_create_user(database_session, descope_user_id="U_keepemail", email="", org=org)
    assert first.id == second.id
    assert second.email == "real@x.com"
