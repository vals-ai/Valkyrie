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
    FinalEvaluation,
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


def _make_bench(name="bench-1", status=BenchmarkStatus.FINISHED) -> Benchmark:
    return Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=status,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )


def test_get_single_benchmark_returns_payload(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()

    resp = client.get(f"/benchmarks/{b.id}", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"] == str(b.id)
    assert data["name"] == "bench-1"
    assert data["status"] == "FINISHED"
    assert data["final_score"] is None
    assert data["total_tasks"] == 0


def test_get_single_benchmark_includes_final_score(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(FinalEvaluation(org_id=b.org_id, benchmark=b.id, final_score=0.42))
    database_session.commit()

    resp = client.get(f"/benchmarks/{b.id}", headers={"Authorization": "Bearer fake"})
    assert resp.json()["final_score"] == 0.42


def test_get_single_benchmark_unknown_returns_404(client):
    bogus = uuid4()
    resp = client.get(f"/benchmarks/{bogus}", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 404


def test_get_benchmark_tasks_paginates(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    for i in range(3):
        database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id=f"t{i}", status=TaskStatus.PENDING))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?limit=2&offset=0",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["tasks"]) == 2
    assert data["total_count"] == 3


def test_get_benchmark_tasks_filters_by_status(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="ok", status=TaskStatus.FINISHED))
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="err", status=TaskStatus.ERROR, error_message="boom")
    )
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?status=ERROR",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert len(data["tasks"]) == 1
    assert data["tasks"][0]["task_id"] == "err"
    assert data["tasks"][0]["error_message"] == "boom"


def test_unauth_returns_401(client):
    bogus = uuid4()
    assert client.get(f"/benchmarks/{bogus}").status_code == 401
    assert client.get(f"/benchmarks/{bogus}/tasks").status_code == 401


def test_get_benchmark_tasks_filters_by_task_id_search(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__astropy-12907", status=TaskStatus.FINISHED)
    )
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="django__django-11400", status=TaskStatus.FINISHED)
    )
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__astropy-13033", status=TaskStatus.ERROR)
    )
    database_session.commit()

    # substring match (case-insensitive)
    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=astropy",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 2
    ids = sorted(t["task_id"] for t in data["tasks"])
    assert ids == ["astropy__astropy-12907", "astropy__astropy-13033"]


def test_get_benchmark_tasks_search_treats_like_wildcards_literally(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="task_1", status=TaskStatus.PENDING))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="taskA1", status=TaskStatus.PENDING))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="task%1", status=TaskStatus.PENDING))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=task_1",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "task_1"

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=task%251",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "task%1"


def test_get_benchmark_tasks_search_case_insensitive(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="DJANGO__django-1", status=TaskStatus.PENDING))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__1", status=TaskStatus.PENDING))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=django",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "DJANGO__django-1"


def test_get_benchmark_tasks_search_combines_with_status(client, database_session):
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__12907", status=TaskStatus.FINISHED))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__13033", status=TaskStatus.ERROR))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="django__11400", status=TaskStatus.ERROR))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=astropy&status=ERROR",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "astropy__13033"
