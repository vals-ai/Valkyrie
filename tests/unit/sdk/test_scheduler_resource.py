"""Scheduler SDK request validation and typed snapshots.

Run: uv run pytest tests/unit/sdk/test_scheduler_resource.py
"""

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, cast

import httpx
import pytest

from valkyrie.sdk import SchedulerOverviewResponse, ValkyrieClient


@pytest.mark.parametrize(
    ("waiting_limit", "active_limit", "waiting_offset", "active_offset", "include_capacity"),
    [(100, 100, 0, 0, False), (1, 200, 200, 10, True)],
)
async def test_overview_uses_typed_snapshot_and_query_options(
    make_client: Callable[..., ValkyrieClient],
    waiting_limit: int,
    active_limit: int,
    waiting_offset: int,
    active_offset: int,
    include_capacity: bool,
) -> None:
    """Fetch scheduler state with typed capacity and server-compatible query options."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/scheduler/overview"
        assert request.url.params["waiting_limit"] == str(waiting_limit)
        assert request.url.params["active_limit"] == str(active_limit)
        assert request.url.params["waiting_offset"] == str(waiting_offset)
        assert request.url.params["active_offset"] == str(active_offset)
        if include_capacity:
            assert request.url.params["include_capacity"] == "true"
        else:
            assert "include_capacity" not in request.url.params
        return httpx.Response(
            200,
            json={
                "observed_at": "2026-09-10T12:00:00Z",
                "summary": {"waiting": 3},
                "pools": [
                    {"pool_id": "omitted", "waiting": 0},
                    {"pool_id": "null", "waiting": 0, "provider": None, "capacity_domains": None},
                    {"pool_id": "empty", "waiting": 0, "provider": "daytona", "capacity_domains": []},
                    {
                        "pool_id": "populated",
                        "waiting": 3,
                        "provider": "daytona",
                        "capacity_domains": [
                            {
                                "target_id": "target-a",
                                "sandbox_class": "small",
                                "capacity": {
                                    "cpu": {"available": 3.5, "total": 4},
                                    "memory": {"available": 7, "total": 8},
                                    "disk": {"available": 15, "total": 20},
                                },
                            }
                        ],
                    },
                ],
                "waiting_entries": [],
                "active_entries": [],
                "waiting_capped": True,
                "active_capped": False,
                "waiting_next_offset": 201,
                "active_next_offset": None,
            },
        )

    async with make_client(handler) as client:
        result = await client.scheduler.overview(
            waiting_limit=waiting_limit,
            active_limit=active_limit,
            waiting_offset=waiting_offset,
            active_offset=active_offset,
            include_capacity=include_capacity,
        )

    assert isinstance(result, SchedulerOverviewResponse)
    assert result.observed_at == datetime(2026, 9, 10, 12, tzinfo=timezone.utc)
    assert result.summary.waiting == 3
    omitted, null, empty, populated = result.pools
    assert omitted.provider is omitted.capacity_domains is None
    assert null.provider is null.capacity_domains is None
    assert empty.provider == "daytona"
    assert empty.capacity_domains == []
    assert populated.provider == "daytona"
    assert populated.capacity_domains is not None
    domain = populated.capacity_domains[0]
    assert (domain.target_id, domain.sandbox_class) == ("target-a", "small")
    assert domain.capacity.cpu.model_dump() == {"available": 3.5, "total": 4.0}
    assert domain.capacity.memory.model_dump() == {"available": 7.0, "total": 8.0}
    assert domain.capacity.disk.model_dump() == {"available": 15.0, "total": 20.0}
    assert result.waiting_capped is True
    assert result.waiting_next_offset == 201
    assert result.active_next_offset is None


@pytest.mark.parametrize(
    ("name", "value"),
    [(name, value) for name in ("waiting_limit", "active_limit") for value in (0, 201, True, 1.5)]
    + [(name, value) for name in ("waiting_offset", "active_offset") for value in (-1, True, 1.5)],
)
async def test_overview_rejects_invalid_limits(
    make_client: Callable[..., ValkyrieClient], name: str, value: object
) -> None:
    """Reject non-integer or out-of-range limits before sending a request."""

    def handler(_request: httpx.Request) -> httpx.Response:
        pytest.fail("unexpected request")

    async with make_client(handler) as client:
        with pytest.raises(ValueError, match=name):
            await client.scheduler.overview(**cast(Any, {name: value}))


@pytest.mark.parametrize("value", [0, 1, None, "true"])
async def test_overview_rejects_non_boolean_capacity_option(
    make_client: Callable[..., ValkyrieClient], value: object
) -> None:
    """Reject non-boolean capacity options before sending a request."""

    def handler(_request: httpx.Request) -> httpx.Response:
        pytest.fail("unexpected request")

    async with make_client(handler) as client:
        with pytest.raises(ValueError, match="include_capacity"):
            await client.scheduler.overview(include_capacity=cast(bool, value))
