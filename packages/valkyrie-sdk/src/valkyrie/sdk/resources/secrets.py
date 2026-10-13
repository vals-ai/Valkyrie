"""Tracker-backed storage for the tenant's own run secrets."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from valkyrie.sdk.models import SecretEntry, SecretsResponse

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


def validate_secret_name(name: str) -> str:
    """Validate a stored secret's name before it becomes a request path segment."""
    if not re.fullmatch(r"[A-Za-z0-9_+=.@-]{1,128}", name) or name in {".", ".."}:
        raise ValueError("Invalid secret name: use 1-128 letters, digits, or the characters _+=.@-")
    return name


class SecretsResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def list(self) -> SecretsResponse:
        """List stored secrets by name and reference, never their values.

        Secrets set or removed in the last few minutes may not show up yet.
        """
        return await self._sdk.request_model("GET", "/secrets", SecretsResponse)

    async def set(self, name: str, value: str) -> SecretEntry:
        """Create the secret or replace its value; runs reference it by the returned reference."""
        return await self._sdk.request_model(
            "PUT", f"/secrets/{validate_secret_name(name)}", SecretEntry, json={"value": value}
        )

    async def remove(self, name: str) -> SecretEntry:
        """Delete the secret immediately; it cannot be recovered.

        On hosted Valkyrie a missing secret raises a 404 API error. Self-hosted deployments without a
        tenant access role can't detect a missing secret, so removing one succeeds.
        """
        return await self._sdk.request_model("DELETE", f"/secrets/{validate_secret_name(name)}", SecretEntry)
