"""List and health-check benchmark services."""

from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter, Depends
import httpx

from tracker.auth import get_current_org
from tracker.config import create_benchmark_service_url, list_benchmark_service_names
from tracker.database.models import Org
from tracker.types import BenchmarkServiceEntry, BenchmarkServiceHealth, BenchmarkServicesRequest, BenchmarkServicesResponse

HEALTH_CHECK_TIMEOUT_SECONDS = 2.0

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


@router.get("", response_model=BenchmarkServicesResponse)
async def list_benchmark_services(
    _org: Org = Depends(get_current_org),
) -> BenchmarkServicesResponse:
    """List configured hosted benchmark services and health-check them."""
    services = [
        BenchmarkServiceEntry(name=name, url=create_benchmark_service_url(name))
        for name in list_benchmark_service_names()
    ]
    return await _health_check_services(services, source="hosted")


@router.post("", response_model=BenchmarkServicesResponse)
async def check_benchmark_services(
    request: BenchmarkServicesRequest,
    _org: Org = Depends(get_current_org),
) -> BenchmarkServicesResponse:
    """Health-check caller-provided benchmark services."""
    return await _health_check_services(request.services, source="custom")
