import importlib
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
)
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


def _make(session: Session, name: str, agent: str) -> Benchmark:
    b = Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=BenchmarkStatus.FINISHED,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name=agent, install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(b)
    session.commit()
    return b


def test_filter_options_returns_distinct(client, database_session):
    _make(database_session, "swebench", "mini_sweagent")
    _make(database_session, "swebench", "claude_code")
    _make(database_session, "fab", "mini_sweagent")
    _make(database_session, "swebench", "mini_sweagent")  # duplicate

    resp = client.get(
        "/benchmarks/filter-options",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["benchmark_names"] == ["fab", "swebench"]
    assert sorted(data["agent_names"]) == ["claude_code", "mini_sweagent"]


def test_filter_options_unauth_401(client):
    resp = client.get("/benchmarks/filter-options")
    assert resp.status_code == 401
