"""Async client for the Valkyrie API."""

from __future__ import annotations

import os
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel

from valkyrie.sdk.config import DEFAULT_CONFIG_PATH, ValkyrieConfig
from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieTransportError
from valkyrie.sdk.resources.runs import RunsResource

DEFAULT_BASE_URL = "https://benchmark-tracker.vals.ai"
ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


class ValkyrieClient:
    """Async client for Valkyrie API resources."""

    def __init__(
        self,
        config: ValkyrieConfig,
        *,
        base_url: str | None = None,
        timeout: float | httpx.Timeout = 120,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        resolved_base_url = base_url or os.environ.get("TRACKER_SERVICE_URL", DEFAULT_BASE_URL)
        self._timeout = timeout if isinstance(timeout, httpx.Timeout) else httpx.Timeout(timeout)
        self._client = httpx.AsyncClient(
            base_url=resolved_base_url.rstrip("/"),
            timeout=self._timeout,
            headers=config.request_headers(),
            transport=transport,
        )
        self.runs = RunsResource(self)

    @classmethod
    def from_config(
        cls,
        path: str | Path = DEFAULT_CONFIG_PATH,
        *,
        base_url: str | None = None,
        timeout: float | httpx.Timeout = 120,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> ValkyrieClient:
        """Load a client from a CLI-compatible YAML config."""
        return cls(
            ValkyrieConfig.from_yaml(path),
            base_url=base_url,
            timeout=timeout,
            transport=transport,
        )

    async def __aenter__(self) -> ValkyrieClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    async def request_model(
        self,
        method: str,
        path: str,
        model: type[ResponseModel],
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> ResponseModel:
        """Send a request and validate its response as a Pydantic model."""
        try:
            response = await self._client.request(method, path, params=params, json=json)
        except httpx.HTTPError as exc:
            raise ValkyrieTransportError(f"Valkyrie request failed: {exc}") from exc

        self.raise_for_status(response)
        return model.model_validate(response.json())

    @staticmethod
    def raise_for_status(response: httpx.Response) -> None:
        """Convert a non-success response into an SDK API error."""
        if response.is_success:
            return
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text
        detail = body.get("detail", body) if isinstance(body, dict) else body
        raise ValkyrieAPIError(response.status_code, detail)

    def stream_response(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> AbstractAsyncContextManager[httpx.Response]:
        """Open a streaming response without a read timeout."""
        timeout = httpx.Timeout(
            connect=self._timeout.connect,
            read=None,
            write=self._timeout.write,
            pool=self._timeout.pool,
        )
        return self._client.stream(method, path, params=params, timeout=timeout)
