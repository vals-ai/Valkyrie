from unittest.mock import MagicMock


def test_agents_empty_when_bucket_empty(client, monkeypatch):
    monkeypatch.setattr("tracker.api.agents.list_s3_agent_names", MagicMock(return_value=[]))

    resp = client.get("/agents", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200
    assert resp.json()["agents"] == []


def test_agents_unauth_401(client):
    resp = client.get("/agents")
    assert resp.status_code == 401
