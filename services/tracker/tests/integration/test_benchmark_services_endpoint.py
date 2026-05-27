import importlib
from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import OrgConfig
from tracker.types import AWSCredentials, HarnessConfig


@pytest.fixture
def daytona_secret_name() -> str:
    """Shadow the session-scoped fixture to avoid requiring TEST_DAYTONA_SECRET_NAME."""
    return "fake-daytona-secret"


@pytest.fixture
def harness_config() -> HarnessConfig:
    """Shadow the session-scoped harness_config to avoid requiring AWS credentials."""
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_default_region="us-east-1",
        ),
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=30,
        daytona_secret_name="fake-daytona-secret",
    )


@pytest.fixture
def client(monkeypatch, database_session: Session):
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "http://localhost:5173")
    import tracker.config as config_mod; importlib.reload(config_mod)
    import tracker.auth as auth_mod; importlib.reload(auth_mod)
    import main as main_mod; importlib.reload(main_mod)
    from tracker.database.session import get_session as get_session_dep

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    main_mod.app.dependency_overrides[get_session_dep] = get_test_session
    monkeypatch.setattr("tracker.database.session.engine", database_session.bind)

    with patch.object(auth_mod, "_descope_client") as mock_client:
        mock_client.validate_session.return_value = {
            "tenants": {"default": {}},
            "userId": "U_caller",
            "user": {"email": "caller@example.com"},
        }
        yield TestClient(main_mod.app)


def _make_cfg(session: Session, services: list[dict]) -> None:
    cfg = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A", aws_secret_access_key="s",
        aws_default_region="us-east-2", s3_bucket="b", daytona_secret_name="d",
        benchmark_services=services,
    )
    session.add(cfg)
    session.commit()


def test_benchmark_services_returns_pings(client, database_session, monkeypatch):
    _make_cfg(database_session, [
        {"name": "swebench", "url": "http://up:8001", "auth_header_name": None, "auth_secret_name": None},
        {"name": "fab", "url": "http://down:8002", "auth_header_name": None, "auth_secret_name": None},
    ])

    async def fake_ping(name: str, url: str):
        if name == "swebench":
            return {"name": "swebench", "url": url, "healthy": True, "latency_ms": 12, "error": None}
        return {"name": name, "url": url, "healthy": False, "latency_ms": None, "error": "timeout"}

    # Also clear the in-memory cache between tests
    import tracker.api.benchmark_services as bs
    bs._health_cache.clear()
    monkeypatch.setattr(bs, "_ping_service", AsyncMock(side_effect=fake_ping))

    resp = client.get("/benchmark-services", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    by_name = {s["name"]: s for s in data["services"]}
    assert by_name["swebench"]["healthy"] is True
    assert by_name["fab"]["healthy"] is False


def test_benchmark_services_empty_when_no_config(client, monkeypatch):
    import tracker.api.benchmark_services as bs
    bs._health_cache.clear()
    monkeypatch.setattr(bs, "_ping_service", AsyncMock())

    resp = client.get("/benchmark-services", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200
    assert resp.json()["services"] == []


def test_benchmark_services_unauth_401(client):
    resp = client.get("/benchmark-services")
    assert resp.status_code == 401
