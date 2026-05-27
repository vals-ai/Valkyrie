"""GET /benchmark-services — list configured benchmark services with health pings."""
from __future__ import annotations

import asyncio
import time

import httpx
from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Org, OrgConfig, User
from tracker.database.session import get_session
from tracker.types import BenchmarkServiceHealth, BenchmarkServicesResponse

router = APIRouter()


_CACHE_TTL_S = 30
# Cache: (org_id, url-set) -> (cached_at_monotonic, results)
_health_cache: dict[tuple[str, tuple[str, ...]], tuple[float, list[BenchmarkServiceHealth]]] = {}


async def _ping_service(name: str, url: str) -> dict[str, object]:
    """Ping <url>/health with 2s timeout. Returns dict matching BenchmarkServiceHealth shape."""
    health_url = url.rstrip("/") + "/health"
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(health_url)
            latency_ms = int((time.monotonic() - start) * 1000)
            return {
                "name": name,
                "url": url,
                "healthy": resp.status_code == 200,
                "latency_ms": latency_ms,
                "error": None if resp.status_code == 200 else f"HTTP {resp.status_code}",
            }
    except (httpx.TimeoutException, httpx.RequestError) as e:
        return {
            "name": name,
            "url": url,
            "healthy": False,
            "latency_ms": None,
            "error": str(e),
        }


@router.get("/benchmark-services", response_model=BenchmarkServicesResponse)
async def list_benchmark_services(
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> BenchmarkServicesResponse:
    _, org = user_and_org

    cfg = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()
    if cfg is None or not cfg.benchmark_services:
        return BenchmarkServicesResponse(services=[])

    services = cfg.benchmark_services
    cache_key = (str(org.id), tuple(sorted(s["url"] for s in services)))
    now = time.monotonic()
    cached = _health_cache.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return BenchmarkServicesResponse(services=cached[1])

    ping_results = await asyncio.gather(
        *[_ping_service(s["name"], s["url"]) for s in services]
    )
    health_entries = [BenchmarkServiceHealth(**r) for r in ping_results]
    _health_cache[cache_key] = (now, health_entries)

    return BenchmarkServicesResponse(services=health_entries)
