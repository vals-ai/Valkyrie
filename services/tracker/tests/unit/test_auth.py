import pytest
from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

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


def test_forward_tracker_api_key_preserves_explicit_override_case_insensitive():
    from tracker.auth import forward_tracker_api_key

    original_headers = {"x-descope-api-key": "override-key"}

    headers = forward_tracker_api_key(original_headers, "tracker-api-key")

    assert headers == original_headers
    assert headers is not original_headers


def _request_with_header(name: str, value: str) -> Request:
    return Request({"type": "http", "headers": [(name.lower().encode(), value.encode())]})


def test_extract_bearer_token_uses_case_insensitive_scheme_matching():
    from tracker.auth import _extract_bearer_token

    assert _extract_bearer_token(_request_with_header("Authorization", "Bearer token-1")) == "token-1"
    assert _extract_bearer_token(_request_with_header("authorization", "bearer token-2")) == "token-2"


def test_extract_bearer_token_rejects_missing_or_wrong_scheme():
    from tracker.auth import _extract_bearer_token

    assert _extract_bearer_token(_request_with_header("Authorization", "Basic token")) is None
    assert _extract_bearer_token(_request_with_header("Authorization", "Bearer   ")) is None
