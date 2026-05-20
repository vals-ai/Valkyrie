from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.database.models import User


def test_create_user_persists_fields(database_session: Session) -> None:
    user = User(
        org_id=TEST_ORG_ID,
        email="alice@example.com",
        descope_user_id="U_abc123",
    )
    database_session.add(user)
    database_session.commit()

    fetched = database_session.exec(select(User).where(User.descope_user_id == "U_abc123")).one()
    assert fetched.email == "alice@example.com"
    assert fetched.org_id == TEST_ORG_ID
    assert isinstance(fetched.id, UUID)
    assert fetched.created_at is not None


def test_descope_user_id_is_unique(database_session: Session) -> None:
    user_a = User(org_id=TEST_ORG_ID, email="a@example.com", descope_user_id="U_dupe")
    database_session.add(user_a)
    database_session.commit()

    user_b = User(org_id=TEST_ORG_ID, email="b@example.com", descope_user_id="U_dupe")
    database_session.add(user_b)

    with pytest.raises(IntegrityError):
        database_session.commit()
