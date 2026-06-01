import importlib
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import OrgConfig
from tracker.types import AWSCredentials, HarnessConfig


@pytest.fixture
def harness_config() -> HarnessConfig:
    """Shadow the session-scoped harness_config to avoid requiring AWS credentials."""
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="test-aws-access-key-id",
            aws_secret_access_key="test-aws-secret-access-key",
            aws_default_region="us-east-1",
        ),
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=30,
        daytona_secret_name="test-daytona-secret",
    )


@pytest.fixture
def client(monkeypatch, database_session: Session):
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "http://localhost:5173")
    import tracker.config as config_mod

    importlib.reload(config_mod)
    import tracker.auth as auth_mod

    importlib.reload(auth_mod)
    import main as main_mod

    importlib.reload(main_mod)
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


def _make_org_config(session: Session) -> None:
    cfg = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="AKIA",
        aws_secret_access_key="s",
        aws_default_region="us-east-1",
        s3_bucket="agentic-harness",
        daytona_secret_name="d",
    )
    session.add(cfg)
    session.commit()


def test_agents_returns_list(client, database_session, monkeypatch):
    _make_org_config(database_session)
    monkeypatch.setattr(
        "tracker.api.agents.list_s3_agent_names",
        MagicMock(
            return_value=[
                {"name": "claude_code", "last_modified": None},
                {"name": "mini_sweagent", "last_modified": None},
            ]
        ),
    )

    resp = client.get("/agents", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    names = sorted(a["name"] for a in data["agents"])
    assert names == ["claude_code", "mini_sweagent"]


def test_agents_empty_when_no_org_config(client):
    resp = client.get("/agents", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200
    assert resp.json()["agents"] == []


def test_agents_unauth_401(client):
    resp = client.get("/agents")
    assert resp.status_code == 401
