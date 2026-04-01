import pytest
from sqlmodel import Session, SQLModel, create_engine

from tracker.database.models import Org, VALS_ORG_ID


@pytest.fixture
def session():
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def test_get_default_org_returns_vals_org(session: Session):
    from tracker.auth import get_default_org

    vals_org = Org(id=VALS_ORG_ID, name="Vals")
    session.add(vals_org)
    session.commit()

    result = get_default_org(session)
    assert result.id == VALS_ORG_ID
    assert result.name == "Vals"


def test_get_default_org_raises_if_missing(session: Session):
    import tracker.auth as auth_module
    auth_module._cached_default_org = None  # clear cache from prior test

    with pytest.raises(RuntimeError, match="Default org not found"):
        auth_module.get_default_org(session)
