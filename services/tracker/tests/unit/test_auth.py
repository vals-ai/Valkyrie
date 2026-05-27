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


def test_forward_tracker_api_key_adds_benchmark_service_header():
    from tracker.auth import BENCHMARK_SERVICE_API_KEY_HEADER, forward_tracker_api_key

    headers = forward_tracker_api_key({"Authorization": "Bearer benchmark-token"}, "tracker-api-key")

    assert headers == {
        "Authorization": "Bearer benchmark-token",
        BENCHMARK_SERVICE_API_KEY_HEADER: "tracker-api-key",
    }


def test_forward_tracker_api_key_preserves_explicit_override_case_insensitive():
    from tracker.auth import forward_tracker_api_key

    original_headers = {"x-descope-api-key": "override-key"}

    headers = forward_tracker_api_key(original_headers, "tracker-api-key")

    assert headers == original_headers
    assert headers is not original_headers


def test_forward_tracker_api_key_skips_missing_tracker_key():
    from tracker.auth import forward_tracker_api_key

    headers = forward_tracker_api_key({"Authorization": "Bearer benchmark-token"}, None)

    assert headers == {"Authorization": "Bearer benchmark-token"}


def test_resolve_registry_auth_headers_matches_url():
    from tracker.auth import resolve_registry_auth_headers

    class FakeCfg:
        benchmark_services = [
            {
                "name": "swebench",
                "url": "https://swebench.x",
                "auth_header_name": "X-Api-Key",
                "auth_secret_name": "swebenchKey",
            },
            {"name": "fab", "url": "https://fab.x", "auth_header_name": None, "auth_secret_name": None},
        ]

    headers = resolve_registry_auth_headers("https://swebench.x", FakeCfg(), lambda _: "resolved-secret")
    assert headers == {"X-Api-Key": "resolved-secret"}


def test_resolve_registry_auth_headers_no_match():
    from tracker.auth import resolve_registry_auth_headers

    class FakeCfg:
        benchmark_services = []

    headers = resolve_registry_auth_headers("https://anywhere.x", FakeCfg(), lambda _: "x")
    assert headers == {}


def test_resolve_registry_auth_headers_entry_without_auth():
    from tracker.auth import resolve_registry_auth_headers

    class FakeCfg:
        benchmark_services = [
            {"name": "x", "url": "https://x.x", "auth_header_name": None, "auth_secret_name": None},
        ]

    headers = resolve_registry_auth_headers("https://x.x", FakeCfg(), lambda _: "x")
    assert headers == {}


def test_resolve_registry_auth_headers_none_custom_service():
    from tracker.auth import resolve_registry_auth_headers

    class FakeCfg:
        benchmark_services = [
            {"name": "x", "url": "https://x.x", "auth_header_name": "H", "auth_secret_name": "S"},
        ]

    headers = resolve_registry_auth_headers(None, FakeCfg(), lambda _: "x")
    assert headers == {}
