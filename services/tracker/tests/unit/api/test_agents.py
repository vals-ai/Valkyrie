"""Run with `uv run pytest tests/unit/api/test_agents.py`.

Cover agent listing and download-link routes.
"""

from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

import tracker.api.agents as agents_api
from main import app

_client = TestClient(app)


class TestAgentRoutes:
    """Agent catalog and download route behavior."""

    def test_list_agents_returns_storage_metadata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Agent listing must expose the names and timestamps returned by storage.

        Test cases:
        - The route serializes one stored agent and its last-modified timestamp.
        """

        async def fake_list_agents(**_kwargs: object) -> list[tuple[str, datetime]]:
            return [("agent-a", datetime(2026, 1, 2, tzinfo=timezone.utc))]

        monkeypatch.setattr(agents_api, "list_agents", fake_list_agents)

        response = _client.get("/agents")

        assert response.status_code == 200
        assert response.json() == {"agents": [{"name": "agent-a", "last_modified": "2026-01-02 00:00:00+00:00"}]}

    def test_agent_download_url_uses_route_expiration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Download links must use the route's configured expiration in both the signer and response.

        Test cases:
        - An existing agent receives a URL signed with the configured TTL.
        """
        captured_expiration: list[int] = []

        async def fake_presigned_url(*_args: Any, expiration: int, **_kwargs: Any) -> str:
            captured_expiration.append(expiration)
            return "https://example.test/agent-a.zip"

        async def fake_exists(*_args: Any, **_kwargs: Any) -> bool:
            return True

        monkeypatch.setattr(agents_api, "s3_object_exists", fake_exists)
        monkeypatch.setattr(agents_api, "create_presigned_url", fake_presigned_url)

        response = _client.get("/agents/agent-a/download-url")

        assert response.status_code == 200
        assert response.json() == {
            "name": "agent-a",
            "download_url": "https://example.test/agent-a.zip",
            "expires_in": agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        }
        assert captured_expiration == [agents_api.PRESIGNED_URL_EXPIRES_SECONDS]

    def test_agent_download_url_returns_not_found_for_missing_agent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Missing agent artifacts must return not found instead of a useless signed URL.

        Test cases:
        - A missing storage object receives a stable 404 response.
        """

        async def fake_exists(*_args: Any, **_kwargs: Any) -> bool:
            return False

        monkeypatch.setattr(agents_api, "s3_object_exists", fake_exists)

        response = _client.get("/agents/missing/download-url")

        assert response.status_code == 404
        assert response.json()["detail"] == "Agent 'missing' not found in S3"
