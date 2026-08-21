"""Run with `uv run pytest tests/integration/local/api/test_agents.py`.

Exercise authentication for agent routes through the real app.
"""

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from pytest import MonkeyPatch


def test_agents_empty_when_bucket_empty(client: TestClient, monkeypatch: MonkeyPatch) -> None:
    """Authenticated agent listing must accept explicit harness credentials."""
    monkeypatch.setattr("tracker.api.agents.list_agents", AsyncMock(return_value=[]))

    response = client.get(
        "/agents",
        headers={
            "Authorization": "Bearer fake",
            "X-Harness-AWS-Access-Key-Id": "test-access-key",
            "X-Harness-AWS-Secret-Access-Key": "test-secret-key",
            "X-Harness-AWS-Default-Region": "us-east-1",
            "X-Harness-S3-Bucket": "test-bucket",
        },
    )

    assert response.status_code == 200
    assert response.json()["agents"] == []


def test_agents_unauth_401(client: TestClient) -> None:
    """The agents catalog must not be readable without authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    response = client.get("/agents")

    assert response.status_code == 401
