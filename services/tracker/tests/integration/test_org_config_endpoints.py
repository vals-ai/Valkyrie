"""Integration tests for GET /org-config and PUT /org-config."""

import importlib
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tracker.types import AWSCredentials, HarnessConfig

_TEST_ORG_NAME = "default"


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

    # Also patch the engine used directly in the DB-verify assertion in tests
    monkeypatch.setattr("tracker.database.session.engine", database_session.bind)

    with patch.object(auth_mod, "_descope_client") as mock_client:
        mock_client.validate_session.return_value = {
            "tenants": {"default": {}},
            "userId": "U_alice",
            "user": {"email": "alice@example.com"},
        }
        yield TestClient(main_mod.app)

    main_mod.app.dependency_overrides.clear()


def test_get_org_config_404_when_missing(client):
    resp = client.get("/org-config", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 404


def test_put_then_get_round_trip_masks_secrets(client):
    body = {
        "aws_access_key_id": "AKIAEXAMPLE",
        "aws_secret_access_key": "real-aws-secret",
        "aws_default_region": "us-east-2",
        "s3_bucket": "my-bucket",
        "daytona_secret_name": "daytona/prod",
        "log_group": None,
        "log_retention_policy": None,
        "webhook": "https://hooks.example.com/T/abc/xyz",
    }
    put_resp = client.put("/org-config", json=body, headers={"Authorization": "Bearer fake"})
    assert put_resp.status_code == 200, put_resp.text

    get_resp = client.get("/org-config", headers={"Authorization": "Bearer fake"})
    assert get_resp.status_code == 200
    data = get_resp.json()

    assert data["aws_access_key_id"] == "AKIAEXAMPLE"
    assert data["aws_default_region"] == "us-east-2"
    assert data["s3_bucket"] == "my-bucket"

    assert data["aws_secret_access_key"] == "********"
    assert data["daytona_secret_name"] == "********"
    assert data["webhook"] == "********"


def test_put_with_masked_sentinel_preserves_secret(client):
    body = {
        "aws_access_key_id": "AKIA1",
        "aws_secret_access_key": "real-secret",
        "aws_default_region": "us-east-2",
        "s3_bucket": "b1",
        "daytona_secret_name": "d1",
        "webhook": None,
    }
    client.put("/org-config", json=body, headers={"Authorization": "Bearer fake"})

    body["aws_secret_access_key"] = "********"
    body["daytona_secret_name"] = "********"
    body["s3_bucket"] = "b2"
    resp = client.put("/org-config", json=body, headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200

    # Verify via the DB that real secrets are still there.
    from sqlmodel import Session, select
    import tracker.database.session as session_mod
    from tracker.database.models import OrgConfig

    with Session(session_mod.engine) as s:
        row = s.exec(select(OrgConfig)).one()
        assert row.aws_secret_access_key == "real-secret"
        assert row.daytona_secret_name == "d1"
        assert row.s3_bucket == "b2"


def test_unauthenticated_returns_401(client):
    resp = client.get("/org-config")
    assert resp.status_code == 401
