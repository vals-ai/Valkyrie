"""Hosted agent discovery operations."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

from valkyrie.sdk.models import AgentDownloadURLResponse, AgentsResponse

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class AgentsResource:
    """Async operations for agents uploaded to the configured tenant."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def list(self) -> AgentsResponse:
        """List uploaded agents visible to the configured tenant."""
        return await self._sdk.request_model("GET", "/agents", AgentsResponse)

    async def download_url(self, name: str) -> AgentDownloadURLResponse:
        """Create a temporary download URL for an uploaded agent."""
        if not name.strip():
            raise ValueError("agent name must not be blank")
        if name in {".", ".."}:
            raise ValueError("agent name must not be '.' or '..'")
        if "/" in name:
            raise ValueError("agent name must not contain '/'")
        name_segment = quote(name, safe="")
        return await self._sdk.request_model(
            "GET",
            f"/agents/{name_segment}/download-url",
            AgentDownloadURLResponse,
        )
