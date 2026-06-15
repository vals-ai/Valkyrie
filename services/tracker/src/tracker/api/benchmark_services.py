"""POST /benchmark-services — health-check caller-provided benchmark services."""

from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter, Depends
import httpx

from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.types import BenchmarkServiceHealth, BenchmarkServicesRequest, BenchmarkServicesResponse

HEALTH_CHECK_TIMEOUT_SECONDS = 2.0

router = APIRouter(prefix="/benchmark-services")


async def _ping_service(client: httpx.AsyncClient, name: str, url: str) -> BenchmarkServiceHealth:
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
        )
    except (httpx.TimeoutException, httpx.RequestError) as e:
        return BenchmarkServiceHealth(name=name, url=url, healthy=False, latency_ms=None, error=str(e))


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
