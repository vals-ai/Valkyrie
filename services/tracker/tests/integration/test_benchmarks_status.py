import importlib
from collections.abc import Generator
from unittest.mock import patch
from uuid import uuid4

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
    """Override session-scoped harness_config to avoid requiring AWS credentials."""
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


def _make_bench(name: str, status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS) -> Benchmark:
    return Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=status,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="x", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )


def test_status_returns_requested_ids(client, database_session):
    a = _make_bench("a", BenchmarkStatus.IN_PROGRESS)
    b = _make_bench("b", BenchmarkStatus.FINISHED)
    database_session.add_all([a, b])
    database_session.commit()

    resp = client.get(
        f"/benchmarks/status?ids={a.id},{b.id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    ids = {e["id"] for e in data["entries"]}
    assert ids == {str(a.id), str(b.id)}


def test_status_ignores_foreign_ids(client, database_session):
    bench = _make_bench("own")
    database_session.add(bench)
    database_session.commit()
    foreign = uuid4()

    resp = client.get(
        f"/benchmarks/status?ids={bench.id},{foreign}",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert len(data["entries"]) == 1
    assert data["entries"][0]["id"] == str(bench.id)


def test_status_no_ids_returns_empty(client):
    resp = client.get("/benchmarks/status", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200
    assert resp.json() == {"entries": []}


def test_status_unauthenticated_returns_401(client):
    resp = client.get("/benchmarks/status?ids=00000000-0000-0000-0000-000000000001")
    assert resp.status_code == 401
