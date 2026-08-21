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


_HARNESS_HEADERS = {
    "X-Harness-AWS-Access-Key-Id": "test-access-key",
    "X-Harness-AWS-Secret-Access-Key": "test-secret-key",
    "X-Harness-AWS-Default-Region": "us-east-1",
    "X-Harness-S3-Bucket": "test-bucket",
}


def test_agent_upload_url_and_delete_accept_api_key_auth(
    access_key_client: TestClient,
    monkeypatch: MonkeyPatch,
) -> None:
    """The keyless CLI's api-key auth mode must reach the new agent storage routes.

    Test cases:
    - upload-url returns a signed URL for an api-key-authenticated caller.
    - delete removes an existing agent for the same caller.
    """
    monkeypatch.setattr(
        "tracker.api.agents.create_presigned_url",
        AsyncMock(return_value="https://example.test/put"),
    )
    monkeypatch.setattr("tracker.api.agents.s3_object_exists", AsyncMock(return_value=True))
    monkeypatch.setattr("tracker.api.agents.delete_from_s3", AsyncMock(return_value=None))
    headers = {"x-api-key": "fake-key", **_HARNESS_HEADERS}

    upload_response = access_key_client.post("/agents/demo/upload-url", headers=headers)
    assert upload_response.status_code == 200, upload_response.text
    assert upload_response.json()["upload_url"] == "https://example.test/put"

    delete_response = access_key_client.delete("/agents/demo", headers=headers)
    assert delete_response.status_code == 204, delete_response.text


def test_agent_upload_url_unauth_401(client: TestClient) -> None:
    response = client.post("/agents/demo/upload-url")

    assert response.status_code == 401
