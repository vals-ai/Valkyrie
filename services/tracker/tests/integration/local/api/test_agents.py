"""Local integration tests for the agents API."""

from unittest.mock import AsyncMock


def test_agents_empty_when_bucket_empty(client, monkeypatch):
    """The agents route must preserve an empty catalog as a successful response.

    Test cases:
    - An authenticated request receives an empty agents list when S3 has no bundles.
    """
    monkeypatch.setattr("tracker.api.agents.list_agents", AsyncMock(return_value=[]))

    resp = client.get("/agents", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200
    assert resp.json()["agents"] == []


def test_agents_unauth_401(client):
    """The agents catalog must not be readable without authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    resp = client.get("/agents")
    assert resp.status_code == 401
