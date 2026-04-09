import pytest
from sqlmodel import Session, SQLModel, create_engine

from tests.conftest import TEST_ORG_ID
from tracker.database.models import DEFAULT_ORG_NAME, Org


@pytest.fixture
def session():
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def test_get_default_org_returns_vals_org(session: Session):
    from tracker.auth import get_default_org

    vals_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    session.add(vals_org)
    session.commit()

    result = get_default_org(session)
    assert result.id == TEST_ORG_ID
    assert result.name == DEFAULT_ORG_NAME


def test_get_default_org_raises_if_missing(session: Session):
    import tracker.auth as auth_module
    auth_module._cached_default_org = None  # clear cache from prior test

    with pytest.raises(RuntimeError, match="Default org not found"):
        auth_module.get_default_org(session)
