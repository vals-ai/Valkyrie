"""Integration test: a real FastAPI endpoint accepting Bearer auth.

Uses a mocked Descope client; otherwise hits the full app stack.
"""
import importlib
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

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
def client(monkeypatch):
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "http://localhost:5173")

    import tracker.config as config_mod
    importlib.reload(config_mod)
    import tracker.auth as auth_mod
    importlib.reload(auth_mod)
    import main as main_mod
    importlib.reload(main_mod)

    with patch.object(auth_mod, "_descope_client") as mock_client:
        mock_client.validate_session.return_value = {
            "tenants": {"default": {}},
            "userId": "U_alice",
            "email": "alice@example.com",
        }
        yield TestClient(main_mod.app)


def test_whoami_with_bearer_returns_user(client):
    resp = client.get("/whoami", headers={"Authorization": "Bearer fake-jwt"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "alice@example.com"
    assert data["org_name"] == "default"


def test_whoami_without_auth_returns_401(client):
    resp = client.get("/whoami")
    assert resp.status_code == 401
