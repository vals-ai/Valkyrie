"""GET /benchmark-services — list configured benchmark services with health pings."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
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
_MAX_HEALTH_CACHE_ENTRIES = 256
# Cache: (org_id, url-set) -> (cached_at_monotonic, results)
_health_cache: OrderedDict[tuple[str, tuple[str, ...]], tuple[float, list[BenchmarkServiceHealth]]] = OrderedDict()


def _prune_health_cache(now: float) -> None:
    expired_keys = [key for key, (cached_at, _) in _health_cache.items() if now - cached_at >= _CACHE_TTL_S]
    for key in expired_keys:
        _health_cache.pop(key, None)
    while len(_health_cache) > _MAX_HEALTH_CACHE_ENTRIES:
        _health_cache.popitem(last=False)


async def _ping_service(name: str, url: str) -> BenchmarkServiceHealth:
    """Ping <url>/health with 2s timeout. Returns dict matching BenchmarkServiceHealth shape."""
    health_url = url.rstrip("/") + "/health"
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
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
        _health_cache.move_to_end(cache_key)
        return BenchmarkServicesResponse(services=cached[1])
    _prune_health_cache(now)

    health_entries = await asyncio.gather(*[_ping_service(s["name"], s["url"]) for s in services])
    _health_cache[cache_key] = (now, health_entries)
    _prune_health_cache(now)

    return BenchmarkServicesResponse(services=health_entries)
