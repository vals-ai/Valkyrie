import importlib
from collections.abc import Generator
from unittest.mock import MagicMock, patch
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
    OrgConfig,
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


def _make_bench(session: Session) -> Benchmark:
    b = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(b)
    session.commit()
    return b


def _make_org_config(session: Session) -> OrgConfig:
    cfg = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="AKIA",
        aws_secret_access_key="s",
        aws_default_region="us-east-1",
        s3_bucket="b",
        daytona_secret_name="d",
        log_group="benchmarks",
    )
    session.add(cfg)
    session.commit()
    return cfg


def test_logs_returns_events(client, database_session, monkeypatch):
    b = _make_bench(database_session)
    _make_org_config(database_session)

    mock_filter = MagicMock(
        return_value={
            "events": [
                {"timestamp": 1, "message": "hello", "log_stream": "s1"},
                {"timestamp": 2, "message": "world", "log_stream": "s1"},
            ],
            "next_token": None,
        }
    )
    monkeypatch.setattr("tracker.api.logs.filter_log_events", mock_filter)

    resp = client.get(
        f"/benchmarks/{b.id}/logs",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["events"]) == 2
    assert data["events"][0]["message"] == "hello"


def test_logs_passes_next_token(client, database_session, monkeypatch):
    b = _make_bench(database_session)
    _make_org_config(database_session)

    mock_filter = MagicMock(return_value={"events": [], "next_token": "tok2"})
    monkeypatch.setattr("tracker.api.logs.filter_log_events", mock_filter)

    client.get(
        f"/benchmarks/{b.id}/logs?next_token=tok1&limit=50",
        headers={"Authorization": "Bearer fake"},
    )
    kwargs = mock_filter.call_args.kwargs
    assert kwargs["next_token"] == "tok1"
    assert kwargs["limit"] == 50


def test_logs_unknown_benchmark_404(client, monkeypatch):
    monkeypatch.setattr("tracker.api.logs.filter_log_events", MagicMock())
    resp = client.get(
        f"/benchmarks/{uuid4()}/logs",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 404


def test_logs_no_org_config_returns_empty(client, database_session, monkeypatch):
    b = _make_bench(database_session)
    monkeypatch.setattr("tracker.api.logs.filter_log_events", MagicMock())

    resp = client.get(
        f"/benchmarks/{b.id}/logs",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200
    assert resp.json()["events"] == []


def test_logs_unauthenticated_401(client):
    resp = client.get(f"/benchmarks/{uuid4()}/logs")
    assert resp.status_code == 401
