"""Sandbox provider mappings for the organization, and each user's run settings."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

from valkyrie.sdk.models import ProvidersResponse, RunSettingsResponse

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


def _segment(name: str) -> str:
    """Escape a provider or benchmark name for the request path; '.' and '..' would change the route."""
    if name in {"", ".", ".."}:
        raise ValueError(f"Invalid name: {name!r}")
    return quote(name, safe="")


class ProvidersResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def list(self) -> ProvidersResponse:
        """List the secret behind each sandbox provider, by name."""
        return await self._sdk.request_model("GET", "/sandbox-providers", ProvidersResponse)

    async def set(self, provider: str, secret_name: str) -> ProvidersResponse:
        """Use an organization secret for every member's runs on this provider.

        The secret must already exist; store it with `client.secrets.set`.
        """
        return await self._sdk.request_model(
            "PUT", f"/sandbox-providers/{_segment(provider)}", ProvidersResponse, json={"secret_name": secret_name}
        )

    async def remove(self, provider: str) -> ProvidersResponse:
        """Stop using a secret for this provider; hosted runs on it fail until one is set again."""
        return await self._sdk.request_model("DELETE", f"/sandbox-providers/{_segment(provider)}", ProvidersResponse)


class SettingsResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def get(self) -> RunSettingsResponse:
        """Show your default sandbox provider and saved benchmark services."""
        return await self._sdk.request_model("GET", "/settings", RunSettingsResponse)

    async def set_default_provider(self, provider: str) -> RunSettingsResponse:
        """Run on this sandbox provider unless a run names another. Needs a login, not an access key."""
        return await self._sdk.request_model(
            "PUT", "/settings/default-provider", RunSettingsResponse, json={"provider": provider}
        )

    async def set_benchmark_service(
        self, benchmark: str, url: str, *, auth_secret: str | None = None, auth_header: str | None = None
    ) -> RunSettingsResponse:
        """Use this URL for your runs of a benchmark, replacing the URL and credential saved before.

        Tracker sends the `auth_secret` secret's value in `auth_header`, `Authorization` by default. Needs a
        login, not an access key.
        """
        return await self._sdk.request_model(
            "PUT",
            f"/settings/benchmark-services/{_segment(benchmark)}",
            RunSettingsResponse,
            json={"url": url, "auth_secret_name": auth_secret, "auth_header_name": auth_header},
        )

    async def remove_benchmark_service(self, benchmark: str) -> RunSettingsResponse:
        """Go back to the hosted service for your runs of a benchmark."""
        return await self._sdk.request_model(
            "DELETE", f"/settings/benchmark-services/{_segment(benchmark)}", RunSettingsResponse
        )
