"""Run with `uv run pytest tests/unit/api/test_agents.py`.

Cover agent listing and download-link routes.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

import tracker.api.agents as agents_api
from main import app
from tracker.aws.runtime import AWSRuntime

_client = TestClient(app)


class TestAgentRoutes:
    """Agent catalog and download route behavior."""

    def test_list_agents_returns_storage_metadata(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        harness_headers: dict[str, str],
    ) -> None:
        """Agent listing must expose the names and timestamps returned by storage.

        Test cases:
        - The route serializes one stored agent and its last-modified timestamp.
        """

        list_agents = AsyncMock(return_value=[("agent-a", datetime(2026, 1, 2, tzinfo=timezone.utc))])
        monkeypatch.setattr(agents_api, "list_agents", list_agents)

        response = _client.get("/agents", headers=harness_headers)

        assert response.status_code == 200
        assert response.json() == {"agents": [{"name": "agent-a", "last_modified": "2026-01-02 00:00:00+00:00"}]}
        list_agents.assert_awaited_once_with(aws_runtime)

    def test_agent_download_url_uses_route_expiration(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        harness_headers: dict[str, str],
    ) -> None:
        """Download links must use the route's configured expiration in both the signer and response.

        Test cases:
        - An existing agent receives a URL signed with the configured TTL.
        """
        exists = AsyncMock(return_value=True)
        presigned_url = AsyncMock(return_value="https://example.test/agent-a.zip")
        monkeypatch.setattr(agents_api, "s3_object_exists", exists)
        monkeypatch.setattr(agents_api, "create_presigned_url", presigned_url)

        response = _client.get("/agents/agent-a/download-url", headers=harness_headers)

        assert response.status_code == 200
        assert response.json() == {
            "name": "agent-a",
            "download_url": "https://example.test/agent-a.zip",
            "expires_in": agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        }
        exists.assert_awaited_once_with("agents/agent-a.zip", aws_runtime)
        presigned_url.assert_awaited_once_with(
            "agents/agent-a.zip",
            aws_runtime,
            expiration=agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        )

    def test_agent_download_url_returns_not_found_for_missing_agent(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        harness_headers: dict[str, str],
    ) -> None:
        """Missing agent artifacts must return not found instead of a useless signed URL.

        Test cases:
        - A missing storage object receives a stable 404 response.
        """

        exists = AsyncMock(return_value=False)
        monkeypatch.setattr(agents_api, "s3_object_exists", exists)

        response = _client.get("/agents/missing/download-url", headers=harness_headers)

        assert response.status_code == 404
        assert response.json()["detail"] == "Agent 'missing' not found in S3"
        exists.assert_awaited_once_with("agents/missing.zip", aws_runtime)
