"""List and health-check benchmark services."""

from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter, Depends, HTTPException, Request
import httpx

from tracker.auth import get_current_org
from tracker.config import BENCHMARK_CATALOG_URL, create_benchmark_service_url, list_benchmark_service_names
from tracker.database.models import Org
from tracker.types import (
    BenchmarkServiceEntry,
    BenchmarkServiceHealth,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
)

HEALTH_CHECK_TIMEOUT_SECONDS = 2.0
CATALOG_TIMEOUT_SECONDS = 5.0

router = APIRouter(prefix="/benchmark-services")


async def _ping_service(
    client: httpx.AsyncClient,
    name: str,
    url: str,
    source: str = "custom",
) -> BenchmarkServiceHealth:
    """Ping <url>/health with 2s timeout. Returns dict matching BenchmarkServiceHealth shape."""
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
            source=source,
        )
    except (httpx.TimeoutException, httpx.RequestError) as e:
        return BenchmarkServiceHealth(name=name, url=url, healthy=False, latency_ms=None, error=str(e), source=source)


async def _health_check_services(
    services: list[BenchmarkServiceEntry],
    source: str,
) -> BenchmarkServicesResponse:
    """Health-check benchmark services with concurrent pings."""
    if not services:
        return BenchmarkServicesResponse(services=[])

    async with httpx.AsyncClient(timeout=HEALTH_CHECK_TIMEOUT_SECONDS) as client:
        health_entries = await asyncio.gather(
            *[_ping_service(client, service.name, service.url, source=source) for service in services]
        )
    return BenchmarkServicesResponse(services=health_entries)


def _catalog_url() -> str:
    if BENCHMARK_CATALOG_URL.endswith("/benchmark-services"):
        return BENCHMARK_CATALOG_URL
    return f"{BENCHMARK_CATALOG_URL}/benchmark-services"


def _catalog_auth_headers(request: Request) -> dict[str, str]:
    headers: dict[str, str] = {}
    for header_name in ("x-api-key", "x-descope-api-key"):
        if value := request.headers.get(header_name):
            headers[header_name] = value
            break
    return headers


async def _list_catalog_services(headers: dict[str, str]) -> list[BenchmarkServiceEntry]:
    if not BENCHMARK_CATALOG_URL:
        return [
            BenchmarkServiceEntry(name=name, url=create_benchmark_service_url(name))
            for name in list_benchmark_service_names()
        ]

    try:
        async with httpx.AsyncClient(timeout=CATALOG_TIMEOUT_SECONDS) as client:
            response = await client.get(_catalog_url(), headers=headers)
            response.raise_for_status()
    except httpx.HTTPStatusError as error:
        raise HTTPException(status_code=error.response.status_code, detail=error.response.text) from error
    except httpx.RequestError as error:
        raise HTTPException(status_code=503, detail=f"Failed to list benchmark catalog: {error}") from error

    return [BenchmarkServiceEntry.model_validate(service) for service in response.json().get("services", [])]


@router.get("", response_model=BenchmarkServicesResponse)
async def list_benchmark_services(
    request: Request,
    _org: Org = Depends(get_current_org),
) -> BenchmarkServicesResponse:
    """List tenant-visible hosted benchmark services and health-check them."""
    services = await _list_catalog_services(_catalog_auth_headers(request))
    return await _health_check_services(services, source="hosted")


@router.post("", response_model=BenchmarkServicesResponse)
async def check_benchmark_services(
    request: BenchmarkServicesRequest,
    _org: Org = Depends(get_current_org),
) -> BenchmarkServicesResponse:
    """Health-check caller-provided benchmark services."""
    return await _health_check_services(request.services, source="custom")
