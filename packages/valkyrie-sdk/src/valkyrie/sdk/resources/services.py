"""Benchmark-service discovery and task selection operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from valkyrie.sdk.models import (
    BenchmarkServiceCatalogResponse,
    BenchmarkServiceEntry,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
    FetchBenchmarkTasksRequest,
    TaskIDsResponse,
)

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class BenchmarkServicesResource:
    """Async operations for hosted and custom benchmark services."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def catalog(self) -> BenchmarkServiceCatalogResponse:
        """List hosted benchmark services visible to the configured tenant."""
        return await self._sdk.request_model(
            "GET",
            "/benchmark-services",
            BenchmarkServiceCatalogResponse,
        )

    async def _check(self, services: Sequence[BenchmarkServiceEntry]) -> BenchmarkServicesResponse:
        """Health-check services returned by the Tracker catalog."""
        payload = BenchmarkServicesRequest(services=list(services))
        return await self._sdk.request_model(
            "POST",
            "/benchmark-services",
            BenchmarkServicesResponse,
            json=payload.model_dump(mode="json"),
        )

    async def list(self) -> BenchmarkServicesResponse:
        """List hosted services and return their current health."""
        catalog = await self.catalog()
        if not catalog.services:
            return BenchmarkServicesResponse(services=[])
        return await self._check(catalog.services)

    async def task_ids(
        self,
        benchmark: str,
        *,
        dataset: str | None = None,
        service_headers: Mapping[str, str] | None = None,
        ignore_custom_services: bool = False,
    ) -> list[str]:
        """Fetch task IDs for a benchmark dataset."""
        if not benchmark.strip():
            raise ValueError("benchmark must not be blank")

        headers: dict[str, str] = {}
        if credential := self._sdk.config.benchmark_auth.get(benchmark):
            headers["Authorization"] = credential.get_secret_value()
        headers.update(service_headers or {})

        payload = FetchBenchmarkTasksRequest(
            benchmark_name=benchmark,
            dataset=dataset,
            custom_benchmark_service=(
                None if ignore_custom_services else self._sdk.config.custom_benchmark_services.get(benchmark)
            ),
            service_headers=headers,
        )
        response = await self._sdk.request_model(
            "POST",
            "/fetch-benchmark-tasks",
            TaskIDsResponse,
            json=payload.model_dump(mode="json"),
        )
        return response.task_ids
