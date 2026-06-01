import importlib
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    OrgConfig,
    Task,
    TaskStatus,
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


def _make_bench_with_task(session: Session, task_id: str = "astropy__12907") -> tuple[Benchmark, Task]:
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
    t = Task(org_id=b.org_id, benchmark=b.id, task_id=task_id, status=TaskStatus.FINISHED)
    session.add(t)
    session.commit()
    return b, t


def _make_org_config(session: Session) -> OrgConfig:
    cfg = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="AKIA",
        aws_secret_access_key="s",
        aws_default_region="us-east-1",
        s3_bucket="agentic-harness",
        daytona_secret_name="d",
        log_group="benchmarks",
    )
    session.add(cfg)
    session.commit()
    return cfg


def test_get_single_task_returns_payload(client, database_session):
    b, t = _make_bench_with_task(database_session)
    database_session.add(
        EvaluationResult(
            org_id=t.org_id,
            task=t.id,
            instance_id=f"{b.id}/{t.task_id}",
            result={"resolved": True, "f2p_score": 0.9},
        )
    )
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["task_id"] == t.task_id
    assert data["status"] == "FINISHED"
    assert data["evaluation_result"]["resolved"] is True


def test_get_single_task_404_unknown(client, database_session):
    b, _ = _make_bench_with_task(database_session)
    resp = client.get(
        f"/benchmarks/{b.id}/tasks/nonexistent",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 404


def test_list_task_files(client, database_session, monkeypatch):
    b, t = _make_bench_with_task(database_session)
    _make_org_config(database_session)
    mock_list = MagicMock(
        return_value=[
            {
                "key": f"benchmarks/{b.id}/{t.task_id}/trajectory.json",
                "size": 1024,
                "last_modified": "2026-05-21T00:00:00+00:00",
            },
            {
                "key": f"benchmarks/{b.id}/{t.task_id}/agent.log",
                "size": 5000,
                "last_modified": "2026-05-21T00:01:00+00:00",
            },
        ]
    )
    monkeypatch.setattr("tracker.api.single_task.list_s3_objects_detailed", mock_list)

    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}/files",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["files"]) == 2
    assert data["files"][0]["key"].endswith("/trajectory.json")


def test_list_task_files_no_org_config_empty(client, database_session):
    b, t = _make_bench_with_task(database_session)
    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}/files",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200
    assert resp.json()["files"] == []


def test_presigned_url_succeeds(client, database_session, monkeypatch):
    b, t = _make_bench_with_task(database_session)
    _make_org_config(database_session)
    mock_presign = MagicMock(return_value="https://example.com/presigned")
    monkeypatch.setattr("tracker.api.single_task.generate_presigned_get_url", mock_presign)

    key = f"benchmarks/{b.id}/{t.task_id}/trajectory.json"
    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}/files/url?key={key}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["url"] == "https://example.com/presigned"
    assert data["expires_in"] == 300


def test_presigned_url_rejects_path_traversal(client, database_session, monkeypatch):
    b, t = _make_bench_with_task(database_session)
    _make_org_config(database_session)
    monkeypatch.setattr("tracker.api.single_task.generate_presigned_get_url", MagicMock())

    bad_key = f"benchmarks/{b.id}/other_task/evil.txt"
    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}/files/url?key={bad_key}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 400

    bad_key2 = f"benchmarks/{b.id}/{t.task_id}/../something"
    resp2 = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}/files/url?key={bad_key2}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp2.status_code == 400


def test_unauthenticated_returns_401(client, database_session):
    b, t = _make_bench_with_task(database_session)
    assert client.get(f"/benchmarks/{b.id}/tasks/{t.task_id}").status_code == 401
    assert client.get(f"/benchmarks/{b.id}/tasks/{t.task_id}/files").status_code == 401
    assert client.get(f"/benchmarks/{b.id}/tasks/{t.task_id}/files/url?key=x").status_code == 401
