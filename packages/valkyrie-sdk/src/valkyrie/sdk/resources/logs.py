"""Benchmark log operations for the async Valkyrie SDK."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from .._sse import error_detail, server_sent_events
from ..errors import ValkyrieRunError, ValkyrieStreamError
from ..models.logs import LogEvent, LogPage

if TYPE_CHECKING:
    from ..client import ValkyrieClient


class LogsResource:
    """Fetch, filter, and follow benchmark logs through the tracker."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def page_task(
        self,
        run_id: UUID,
        task_id: str,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        """Fetch one page from a task's current log stream, including IDs containing `/`."""
        return await self._page(run_id, task_id, query, start_time, end_time, cursor, limit)

    async def fetch_task(
        self,
        run_id: UUID,
        task_id: str,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[LogEvent]:
        """Fetch every matching event from a task stream, including IDs containing `/`."""
        return await self._fetch(run_id, task_id, query, start_time, end_time)

    async def page_run(
        self,
        run_id: UUID,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        """Fetch one page across every stream in a run."""
        return await self._page(run_id, None, query, start_time, end_time, cursor, limit)

    async def fetch_run(
        self,
        run_id: UUID,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[LogEvent]:
        """Fetch every matching event across a run."""
        return await self._fetch(run_id, None, query, start_time, end_time)

    async def _page(
        self,
        run_id: UUID,
        task_id: str | None,
        query: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
        cursor: str | None,
        limit: int,
    ) -> LogPage:
        params = _request_params(task_id, query, start_time, end_time, cursor, limit)
        return await self._sdk.request_model("GET", f"/benchmarks/{run_id}/logs", LogPage, params=params)

    async def _fetch(
        self,
        run_id: UUID,
        task_id: str | None,
        query: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> list[LogEvent]:
        events: list[LogEvent] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            page = await self._page(run_id, task_id, query, start_time, end_time, cursor, 1_000)
            events.extend(page.events)
            cursor = page.next_cursor
            if cursor is None:
                events.sort(key=_event_sort_key)
                return events
            if cursor in seen_cursors:
                raise ValkyrieStreamError("Valkyrie log pagination returned a repeated cursor")
            seen_cursors.add(cursor)

    async def stream_task(
        self,
        run_id: UUID,
        task_id: str,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[LogEvent]:
        """Yield events from one task's log stream until the server sends its `end` event.

        Task IDs may contain `/`. Raises ValkyrieStreamError when the server reports an error or
        the body ends without `end`.
        """
        params = _request_params(task_id, query, start_time, end_time)
        async with self._sdk.stream_response("GET", f"/benchmarks/{run_id}/logs/stream", params=params) as response:
            async for event_name, data in server_sent_events(response):
                if event_name == "end":
                    return
                if event := _log_event(event_name, data):
                    yield event
            raise ValkyrieStreamError("Valkyrie log stream ended before its end event")


def _request_params(
    task_id: str | None,
    query: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    cursor: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    if query is not None and not query.strip():
        raise ValkyrieRunError("query must not be blank")
    for name, value in (("start_time", start_time), ("end_time", end_time)):
        if value is not None and value.utcoffset() is None:
            raise ValkyrieRunError(f"{name} must include a timezone offset")
    if start_time is not None and end_time is not None and end_time <= start_time:
        raise ValkyrieRunError("end_time must be later than start_time")
    if limit is not None and not 1 <= limit <= 10_000:
        raise ValkyrieRunError("limit must be between 1 and 10000")

    params = {
        "task_id": task_id,
        "query": query,
        "start_time": start_time and start_time.isoformat(),
        "end_time": end_time and end_time.isoformat(),
        "cursor": cursor,
        "limit": limit,
    }
    return {key: value for key, value in params.items() if value is not None}


def _log_event(event_name: str, data: str) -> LogEvent | None:
    if event_name == "error":
        raise ValkyrieStreamError(f"Valkyrie log stream returned an error: {error_detail(data, 'detail')}")
    if event_name != "log" or not data:
        return None
    try:
        return LogEvent.model_validate_json(data)
    except (ValueError, TypeError) as error:
        raise ValkyrieStreamError("Invalid Valkyrie log stream event") from error


def _event_sort_key(event: LogEvent) -> tuple[datetime, datetime, str]:
    return (event.timestamp, event.ingestion_time or event.timestamp, event.event_id or "")
