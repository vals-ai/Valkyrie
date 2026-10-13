"""Async client for the Valkyrie API."""

from __future__ import annotations

from collections.abc import AsyncIterable
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel

from valkyrie.sdk.config import ValkyrieConfig
from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieTransportError
from valkyrie.sdk.resources.agents import AgentsResource
from valkyrie.sdk.resources.artifacts import ArtifactsResource
from valkyrie.sdk.resources.benchmarks import BenchmarksResource
from .resources.logs import LogsResource
from valkyrie.sdk.resources.runs import RunsResource
from valkyrie.sdk.resources.scheduler import SchedulerResource
from valkyrie.sdk.resources.secrets import SecretsResource
from valkyrie.sdk.resources.settings import ProvidersResource, SettingsResource
from valkyrie.sdk.resources.services import BenchmarkServicesResource

ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


class ValkyrieClient:
    """Async client for Valkyrie API resources; without a config it uses `ValkyrieConfig()`."""

    def __init__(
        self,
        config: ValkyrieConfig | None = None,
        *,
        timeout: float | httpx.Timeout = 120,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config or ValkyrieConfig()
        self._timeout = timeout if isinstance(timeout, httpx.Timeout) else httpx.Timeout(timeout)
        self._client = httpx.AsyncClient(
            base_url=self.config.url,
            timeout=self._timeout,
            auth=self.config.http_auth(),
            transport=transport,
        )
        self.runs = RunsResource(self)
        self.logs = LogsResource(self)
        self.benchmarks = BenchmarksResource(self)
        self.agents = AgentsResource(self)
        self.artifacts = ArtifactsResource(self)
        self.services = BenchmarkServicesResource(self)
        self.scheduler = SchedulerResource(self)
        self.secrets = SecretsResource(self)
        self.providers = ProvidersResource(self)
        self.settings = SettingsResource(self)

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
        content: AsyncIterable[bytes] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ResponseModel:
        """Send a request and validate its response as a Pydantic model."""
        try:
            response = await self._client.request(
                method,
                path,
                params=params,
                json=json,
                content=content,
                headers=headers,
            )
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

    def download_auth(self, url: str) -> httpx.Auth | None:
        """Authenticate downloads only on this client's Tracker origin."""
        target = httpx.URL(url)
        tracker = self._client.base_url
        if target.scheme in {"http", "https"} and (target.scheme, target.host, target.port) == (
            tracker.scheme,
            tracker.host,
            tracker.port,
        ):
            return self._client.auth
        return None

    def stream_response(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> AbstractAsyncContextManager[httpx.Response]:
        """Open a streaming response without a read timeout."""
        timeout = httpx.Timeout(
            connect=self._timeout.connect,
            read=None,
            write=self._timeout.write,
            pool=self._timeout.pool,
        )
        return self._client.stream(method, path, params=params, json=json, timeout=timeout)
