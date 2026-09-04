"""Tests for the async SDK log resource.

Run: uv run pytest tests/unit/sdk/test_logs_resource.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from importlib import import_module
from typing import Any
from urllib.parse import parse_qs
from uuid import uuid4

import httpx  # pyright: ignore[reportMissingImports]
import pytest  # pyright: ignore[reportMissingImports]

from valkyrie.sdk.errors import ValkyrieRunError, ValkyrieStreamError  # pyright: ignore[reportMissingImports]


def test_log_types_and_resource_are_public() -> None:
    """The SDK must export log models and its resources package must export the namespace."""
    sdk = import_module("valkyrie.sdk")
    resources = import_module("valkyrie.sdk.resources")

    assert getattr(sdk, "LogEvent").__name__ == "LogEvent"
    assert getattr(sdk, "LogPage").__name__ == "LogPage"
    assert getattr(resources, "LogsResource").__name__ == "LogsResource"


async def test_fetch_run_follows_empty_pages_and_sorts_events(make_client) -> None:
    """Aggregate fetches must follow changing cursors even when a page has no events."""
    requests: list[httpx.Request] = []
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(200, json={"events": [], "next_cursor": "page-2"})
        return httpx.Response(
            200,
            json={
                "events": [
                    {"timestamp": "2026-01-01T00:00:02Z", "message": "second", "task_id": "task-2"},
                    {"timestamp": "2026-01-01T00:00:01Z", "message": "first", "task_id": "task-1"},
                ]
            },
        )

    client = make_client(handler)
    async with client:
        events = await client.logs.fetch_run(
            run_id,
            query="needle",
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

    assert [event.message for event in events] == ["first", "second"]
    assert [request.url.path for request in requests] == [f"/benchmarks/{run_id}/logs"] * 2
    assert requests[0].url.params["query"] == "needle"
    assert requests[1].url.params["cursor"] == "page-2"


async def test_task_page_validates_bounds_and_builds_provider_neutral_request(make_client) -> None:
    """Task reads must validate timestamps and send no CloudWatch location fields."""
    requests: list[httpx.Request] = []
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"events": []})

    client = make_client(handler)
    async with client:
        await client.logs.page_task(run_id, "task-1", query="literal", cursor="cursor", limit=25)
        with pytest.raises(ValkyrieRunError, match="timezone offset"):
            await client.logs.page_task(run_id, "task-1", start_time=datetime(2026, 1, 1))
        with pytest.raises(ValkyrieRunError, match="later than"):
            await client.logs.page_task(
                run_id,
                "task-1",
                start_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        with pytest.raises(ValkyrieRunError, match="later than"):
            await client.logs.page_task(
                run_id,
                "task-1",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        await client.logs.page_task(run_id, "provider/model:fast")

    request = requests[0]
    assert request.url.path == f"/benchmarks/{run_id}/logs/task"
    params = parse_qs(request.url.query.decode())
    assert params == {
        "task_id": ["task-1"],
        "query": ["literal"],
        "cursor": ["cursor"],
        "limit": ["25"],
    }
    assert requests[-1].url.params["task_id"] == "provider/model:fast"
    assert "log_group" not in params
    assert "log_stream" not in params


async def test_stream_task_parses_log_and_error_events(make_client) -> None:
    """Task following must parse typed SSE records and surface provider errors."""
    run_id = uuid4()
    payload: dict[str, Any] = {
        "timestamp": "2026-01-01T00:00:00Z",
        "message": "hello",
        "task_id": "task-1",
    }

    requests: list[httpx.Request] = []

    def success_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = f": connected\n\nevent: log\ndata: {json.dumps(payload)}\n\nevent: end\ndata: {{}}\n\n"
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    client = make_client(success_handler)
    async with client:
        events = [event async for event in client.logs.stream_task(run_id, "provider/model:fast", query="hello")]

    assert [event.message for event in events] == ["hello"]
    assert requests[0].url.path == f"/benchmarks/{run_id}/logs/task/stream"
    assert requests[0].url.params["task_id"] == "provider/model:fast"

    def error_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text='event: error\ndata: {"detail":"denied"}\n\n',
            headers={"content-type": "text/event-stream"},
        )

    failing_client = make_client(error_handler)
    async with failing_client:
        with pytest.raises(ValkyrieStreamError, match="denied"):
            _ = [event async for event in failing_client.logs.stream_task(run_id, "task-1")]
