import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import User


def test_descope_user_id_is_unique(database_session: Session) -> None:
    user_a = User(org_id=TEST_ORG_ID, email="a@example.com", descope_user_id="U_dupe")
    database_session.add(user_a)
    database_session.commit()

    user_b = User(org_id=TEST_ORG_ID, email="b@example.com", descope_user_id="U_dupe")
    database_session.add(user_b)

    with pytest.raises(IntegrityError):
        database_session.commit()
