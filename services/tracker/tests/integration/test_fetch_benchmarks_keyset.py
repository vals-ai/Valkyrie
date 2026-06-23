import importlib
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
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
        sandbox_provider_secret_name="test-daytona-secret",
    )


@pytest.fixture
def client(monkeypatch, database_session: Session):
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")
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
        mock_client.exchange_access_key.return_value = {
            "tenants": {"default": {}},
            "keyId": "K_caller",
            "sub": "K_caller",
            "userId": "U_caller",
            "user_id": "U_caller",
            "email": "caller@example.com",
        }
        yield TestClient(main_mod.app)

    main_mod.app.dependency_overrides.clear()


def _seed(session: Session, n: int, run_by_email: str | None = None, start_status=None) -> list[Benchmark]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        status = (
            start_status if start_status else (BenchmarkStatus.IN_PROGRESS if i % 2 == 0 else BenchmarkStatus.FINISHED)
        )
        b = Benchmark(
            org_id=TEST_ORG_ID,
            name=f"bench-{i}",
            started_at=base + timedelta(minutes=i),
            status=status,
            run_by_email=run_by_email,
            arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
                concurrency=1,
            ),
        )
        session.add(b)
        rows.append(b)
    session.commit()
    return rows


def test_keyset_paginates_with_cursor(client, database_session):
    _seed(database_session, 5)
    resp = client.get("/fetch-benchmarks?limit=2&cursor=", headers={"x-api-key": "fake-key"})
    assert resp.status_code == 200, resp.text
    page1 = resp.json()
    assert len(page1["benchmarks"]) == 2
    assert page1["next_cursor"] is not None

    resp2 = client.get(
        f"/fetch-benchmarks?limit=2&cursor={page1['next_cursor']}",
        headers={"x-api-key": "fake-key"},
    )
    page2 = resp2.json()
    assert len(page2["benchmarks"]) == 2
    ids_seen = {row["id"] for row in page1["benchmarks"]} | {row["id"] for row in page2["benchmarks"]}
    assert len(ids_seen) == 4  # no duplicates across pages


def test_filter_by_status(client, database_session):
    _seed(database_session, 4)
    resp = client.get(
        "/fetch-benchmarks?status=IN_PROGRESS",
        headers={"x-api-key": "fake-key"},
    )
    data = resp.json()
    assert all(r["status"] == "IN_PROGRESS" for r in data["benchmarks"])


def test_filter_by_started_after(client, database_session):
    _seed(database_session, 5)
    # Use params dict so httpx handles URL-encoding (+ in timezone offset must not become space)
    resp = client.get(
        "/fetch-benchmarks",
        params={"started_after": "2026-01-01T00:02:00+00:00"},
        headers={"x-api-key": "fake-key"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    # Rows started at 00:00..00:04, after 00:02 means started_at > 00:02 (2 rows: 00:03, 00:04)
    assert 2 <= len(data["benchmarks"]) <= 3


def test_legacy_offset_limit_still_works(client, database_session):
    _seed(database_session, 3)
    resp = client.get(
        "/fetch-benchmarks?offset=0&limit=10",
        headers={"x-api-key": "fake-key"},
    )
    data = resp.json()
    assert len(data["benchmarks"]) == 3
    assert data["total_count"] == 3


def test_table_row_includes_run_by_email(client, database_session):
    _seed(database_session, 1, run_by_email="emailtest@x.com")

    resp = client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})
    data = resp.json()
    assert data["benchmarks"][0]["run_by_email"] == "emailtest@x.com"


def test_table_row_run_by_email_null_when_unattributed(client, database_session):
    _seed(database_session, 1, run_by_email=None)
    resp = client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})
    data = resp.json()
    assert data["benchmarks"][0]["run_by_email"] is None
