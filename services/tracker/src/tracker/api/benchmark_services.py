"""POST /benchmark-services — health-check caller-provided benchmark services."""

from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter, Depends, HTTPException, Request
import httpx

from tracker.auth import get_current_org
from tracker.config import BENCHMARK_CATALOG_URL
from tracker.database.models import Org
from tracker.types import (
    BenchmarkServiceCatalogResponse,
    BenchmarkServiceEntry,
    BenchmarkServiceHealth,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
)

CATALOG_REQUEST_TIMEOUT_SECONDS = 10.0
HEALTH_CHECK_TIMEOUT_SECONDS = 1.0

router = APIRouter(prefix="/benchmark-services")


async def _ping_service(client: httpx.AsyncClient, name: str, url: str) -> BenchmarkServiceHealth:
    """Ping <url>/health and return the service health shape."""
    health_url = f"{url}/health"
    start = time.monotonic()
    try:
        resp = await client.get(health_url)
        latency_ms = int((time.monotonic() - start) * 1000)
        return BenchmarkServiceHealth(
            name=name,
            url=url,
            healthy=resp.status_code == 200,
            latency_ms=latency_ms,
            error=None if resp.status_code == 200 else f"HTTP {resp.status_code}",
        )
    except (httpx.TimeoutException, httpx.RequestError) as e:
        return BenchmarkServiceHealth(name=name, url=url, healthy=False, latency_ms=None, error=str(e))


@router.get("", response_model=BenchmarkServiceCatalogResponse)
async def catalog_benchmark_services(
    request: Request,
    _org: Org = Depends(get_current_org),
) -> BenchmarkServiceCatalogResponse:
    """Fetch catalog benchmark services visible to the caller."""
    if not BENCHMARK_CATALOG_URL:
        return BenchmarkServiceCatalogResponse(services=[])

    headers: dict[str, str] = {}
    if api_key := request.headers.get("x-api-key"):
        headers["X-Api-Key"] = api_key

    try:
        async with httpx.AsyncClient(timeout=CATALOG_REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(f"{BENCHMARK_CATALOG_URL}/benchmark-services", headers=headers)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Failed to list benchmark services: {e}") from e

    if response.status_code != 200:
        try:
            body = response.json()
            detail = body.get("detail", response.text) if isinstance(body, dict) else response.text
        except ValueError:
            detail = response.text
        raise HTTPException(status_code=response.status_code, detail=detail)

    return BenchmarkServiceCatalogResponse(
        services=[BenchmarkServiceEntry.model_validate(service) for service in response.json().get("services", [])]
    )


@router.post("", response_model=BenchmarkServicesResponse)
async def list_benchmark_services(
    request: BenchmarkServicesRequest,
    _org: Org = Depends(get_current_org),
) -> BenchmarkServicesResponse:
    """Health-check the caller-provided benchmark services with concurrent pings."""
    if not request.services:
        return BenchmarkServicesResponse(services=[])

    async with httpx.AsyncClient(timeout=HEALTH_CHECK_TIMEOUT_SECONDS) as client:
        health_entries = await asyncio.gather(
            *[_ping_service(client, service.name, service.url) for service in request.services]
        )
    return BenchmarkServicesResponse(services=health_entries)
