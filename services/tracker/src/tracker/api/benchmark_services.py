"""POST /benchmark-services — health-check caller-provided benchmark services."""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Depends, HTTPException, Request
import httpx

from tracker.auth import BENCHMARK_SERVICE_API_KEY_HEADER, RequestIdentity, get_current_org, get_current_starter
from tracker.aws.resolver import resolve_non_run_aws_runtime
from tracker.benchmark_service_credentials import ensure_managed_benchmark_service_headers
from tracker.config import AUTH_REQUIRED, BENCHMARK_CATALOG_URL
from tracker.database.models import Org
from tracker.types import (
    BenchmarkServiceCatalogResponse,
    BenchmarkServiceHealth,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
)

CATALOG_REQUEST_TIMEOUT_SECONDS = 10.0
HEALTH_CHECK_TIMEOUT_SECONDS = 1.0

logger = logging.getLogger(__name__)
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
    except (httpx.TimeoutException, httpx.RequestError) as exc:
        logger.warning("Benchmark service health check failed for %s: %s", name, exc)
        return BenchmarkServiceHealth(
            name=name,
            url=url,
            healthy=False,
            latency_ms=None,
            error="Benchmark service request failed",
        )


@router.get("", response_model=BenchmarkServiceCatalogResponse)
async def catalog_benchmark_services(
    request: Request,
    org: Org = Depends(get_current_org),
) -> BenchmarkServiceCatalogResponse:
    """Fetch catalog benchmark services visible to the caller."""
    if not BENCHMARK_CATALOG_URL:
        return BenchmarkServiceCatalogResponse(services=[])

    api_key = request.headers.get("x-api-key")
    if AUTH_REQUIRED and not api_key:
        runtime = resolve_non_run_aws_runtime(request, org.name)
        service_headers = ensure_managed_benchmark_service_headers(org, runtime.clients)
        headers = {"X-Api-Key": service_headers[BENCHMARK_SERVICE_API_KEY_HEADER]}
    else:
        headers = {"X-Api-Key": api_key} if api_key else {}

    try:
        async with httpx.AsyncClient(timeout=CATALOG_REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(
                f"{BENCHMARK_CATALOG_URL}/benchmark-services",
                headers=headers,
            )
    except httpx.HTTPError as exc:
        logger.warning("Benchmark catalog request failed: %s", exc)
        raise HTTPException(status_code=502, detail="Failed to list benchmark services") from exc

    if response.status_code != 200:
        logger.warning("Benchmark catalog returned HTTP %s", response.status_code)
        raise HTTPException(status_code=502, detail="Failed to list benchmark services")

    try:
        payload: object = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Benchmark catalog response must be an object")
        if "services" not in payload:
            return BenchmarkServiceCatalogResponse(services=[])
        return BenchmarkServiceCatalogResponse.model_validate(payload)
    except ValueError as exc:
        logger.warning("Benchmark catalog returned an invalid response", exc_info=True)
        raise HTTPException(status_code=502, detail="Failed to list benchmark services") from exc


@router.post("", response_model=BenchmarkServicesResponse)
async def list_benchmark_services(
    request: BenchmarkServicesRequest,
    identity: RequestIdentity = Depends(get_current_starter),
) -> BenchmarkServicesResponse:
    """Health-check the caller-provided benchmark services with concurrent pings."""
    match identity.kind:
        case "bearer":
            raise HTTPException(status_code=403, detail="Bearer sessions cannot health-check custom services")
        case "access_key" | "self_hosted":
            pass

    if not request.services:
        return BenchmarkServicesResponse(services=[])

    async with httpx.AsyncClient(timeout=HEALTH_CHECK_TIMEOUT_SECONDS) as client:
        health_entries = await asyncio.gather(
            *[_ping_service(client, service.name, service.url) for service in request.services]
        )
    return BenchmarkServicesResponse(services=health_entries)
