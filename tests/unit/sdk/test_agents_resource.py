"""Tests for hosted agent discovery workflows."""

from __future__ import annotations

import httpx
import pytest


async def test_list_returns_typed_hosted_agents(make_client) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/agents"
        return httpx.Response(
            200,
            json={
                "agents": [
                    {"name": "sweagent", "last_modified": "2026-07-08 12:00:00+00:00"},
                    {"name": "terminal-agent", "last_modified": None},
                ]
            },
        )

    async with make_client(handler) as client:
        result = await client.agents.list()

    assert [agent.name for agent in result.agents] == ["sweagent", "terminal-agent"]
    assert result.agents[0].last_modified == "2026-07-08 12:00:00+00:00"


async def test_download_url_escapes_agent_name_path_segment(make_client) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.raw_path == b"/agents/agent%20one/download-url"
        return httpx.Response(
            200,
            json={
                "name": "agent one",
                "download_url": "https://download.test/agent",
                "expires_in": 300,
            },
        )

    async with make_client(handler) as client:
        result = await client.agents.download_url("agent one")

    assert result.name == "agent one"
    assert result.download_url == "https://download.test/agent"
    assert result.expires_in == 300


async def test_download_url_rejects_blank_agent_name(make_client) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        with pytest.raises(ValueError, match="agent name must not be blank"):
            await client.agents.download_url("  ")


async def test_download_url_rejects_path_separators(make_client) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        with pytest.raises(ValueError, match="agent name must not contain '/'"):
            await client.agents.download_url("team/agent")


@pytest.mark.parametrize("name", [".", ".."])
async def test_download_url_rejects_normalized_dot_segments(make_client, name: str) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        with pytest.raises(ValueError, match=r"agent name must not be '\.' or '\.\.'"):
            await client.agents.download_url(name)
