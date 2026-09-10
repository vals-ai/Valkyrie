"""Scheduler SDK request validation and typed snapshots.

Run: uv run pytest tests/unit/sdk/test_scheduler_resource.py
"""

from collections.abc import Callable
from datetime import datetime, timezone
from typing import cast

import httpx
import pytest

from valkyrie.sdk import SchedulerOverviewResponse, ValkyrieClient


@pytest.mark.parametrize(
    "limits", [{}, {"waiting_limit": 1, "active_limit": 200, "waiting_offset": 200, "active_offset": 10}]
)
async def test_overview_uses_typed_snapshot_and_query_limits(
    make_client: Callable[..., ValkyrieClient], limits: dict[str, int]
) -> None:
    """Fetch scheduler state using server-compatible limits and preserve total counts."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/scheduler/overview"
        assert request.url.params["waiting_limit"] == str(limits.get("waiting_limit", 100))
        assert request.url.params["active_limit"] == str(limits.get("active_limit", 100))
        assert request.url.params["waiting_offset"] == str(limits.get("waiting_offset", 0))
        assert request.url.params["active_offset"] == str(limits.get("active_offset", 0))
        return httpx.Response(
            200,
            json={
                "observed_at": "2026-09-10T12:00:00Z",
                "summary": {"waiting": 3},
                "pools": [{"pool_id": "shared", "waiting": 3}],
                "waiting_entries": [],
                "active_entries": [],
                "waiting_capped": True,
                "active_capped": False,
                "waiting_next_offset": 201,
                "active_next_offset": None,
            },
        )

    async with make_client(handler) as client:
        result = await client.scheduler.overview(**limits)

    assert isinstance(result, SchedulerOverviewResponse)
    assert result.observed_at == datetime(2026, 9, 10, 12, tzinfo=timezone.utc)
    assert result.summary.waiting == 3
    assert result.pools[0].pool_id == "shared"
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
            await client.scheduler.overview(**{name: cast(int, value)})
