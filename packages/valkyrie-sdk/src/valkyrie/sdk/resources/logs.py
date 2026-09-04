"""Benchmark log operations for the async Valkyrie SDK."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from ..errors import ValkyrieRunError, ValkyrieStreamError, handle_httpx_stream_errors
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
        params = _request_params(query, start_time, end_time, cursor, limit)
        params["task_id"] = task_id
        return await self._sdk.request_model(
            "GET",
            f"/benchmarks/{run_id}/logs/task",
            LogPage,
            params=params,
        )

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
        events: list[LogEvent] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            page = await self.page_task(
                run_id,
                task_id,
                query=query,
                start_time=start_time,
                end_time=end_time,
                cursor=cursor,
            )
            events.extend(page.events)
            cursor = _next_cursor(page.next_cursor, seen_cursors)
            if cursor is None:
                break
        events.sort(key=_event_sort_key)
        return events

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
        params = _request_params(query, start_time, end_time, cursor, limit)
        return await self._sdk.request_model(
            "GET",
            f"/benchmarks/{run_id}/logs",
            LogPage,
            params=params,
        )

    async def fetch_run(
        self,
        run_id: UUID,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[LogEvent]:
        """Fetch every matching event across a run."""
        events: list[LogEvent] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            page = await self.page_run(
                run_id,
                query=query,
                start_time=start_time,
                end_time=end_time,
                cursor=cursor,
            )
            events.extend(page.events)
            cursor = _next_cursor(page.next_cursor, seen_cursors)
            if cursor is None:
                break
        events.sort(key=_event_sort_key)
        return events

    @handle_httpx_stream_errors("Valkyrie log stream failed")
    async def stream_task(
        self,
        run_id: UUID,
        task_id: str,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[LogEvent]:
        """Yield events from one task stream, including task IDs containing `/`."""
        params = _request_params(query, start_time, end_time, None, 1_000)
        params.pop("limit")
        params["task_id"] = task_id
        async with self._sdk.stream_response(
            "GET",
            f"/benchmarks/{run_id}/logs/task/stream",
            params=params,
        ) as response:
            if not response.is_success:
                await response.aread()
                self._sdk.raise_for_status(response)

            event_name = ""
            data_lines: list[str] = []
            async for line in response.aiter_lines():
                if line == "":
                    event = _parse_stream_event(event_name, data_lines)
                    if event is not None:
                        yield event
                    if event_name == "end":
                        return
                    event_name = ""
                    data_lines = []
                elif line.startswith("event:"):
                    event_name = line.removeprefix("event:").strip()
                elif line.startswith("data:"):
                    data_lines.append(line.removeprefix("data:").lstrip())

            event = _parse_stream_event(event_name, data_lines)
            if event is not None:
                yield event


def _request_params(
    query: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    cursor: str | None,
    limit: int,
) -> dict[str, Any]:
    if query is not None and not query.strip():
        raise ValkyrieRunError("query must not be blank")
    for name, value in (("start_time", start_time), ("end_time", end_time)):
        if value is not None and value.utcoffset() is None:
            raise ValkyrieRunError(f"{name} must include a timezone offset")
    if start_time is not None and end_time is not None and end_time <= start_time:
        raise ValkyrieRunError("end_time must be later than start_time")
    if limit < 1 or limit > 10_000:
        raise ValkyrieRunError("limit must be between 1 and 10000")

    params: dict[str, Any] = {"limit": limit}
    if query is not None:
        params["query"] = query
    if start_time is not None:
        params["start_time"] = start_time.isoformat()
    if end_time is not None:
        params["end_time"] = end_time.isoformat()
    if cursor is not None:
        params["cursor"] = cursor
    return params


def _next_cursor(cursor: str | None, seen_cursors: set[str]) -> str | None:
    if cursor is None:
        return None
    if cursor in seen_cursors:
        raise ValkyrieStreamError("Valkyrie log pagination returned a repeated cursor")
    seen_cursors.add(cursor)
    return cursor


def _parse_stream_event(event_name: str, data_lines: Sequence[str]) -> LogEvent | None:
    data = "\n".join(data_lines)
    if event_name == "error":
        try:
            payload: Any = json.loads(data) if data else {}
        except json.JSONDecodeError:
            payload = data
        detail = payload.get("detail", payload) if isinstance(payload, dict) else payload
        raise ValkyrieStreamError(f"Valkyrie log stream returned an error: {detail}")
    if event_name == "end" or not data:
        return None
    if event_name != "log":
        return None
    try:
        return LogEvent.model_validate_json(data)
    except (ValueError, TypeError) as error:
        raise ValkyrieStreamError("Invalid Valkyrie log stream event") from error


def _event_sort_key(event: LogEvent) -> tuple[datetime, datetime, str]:
    return (event.timestamp, event.ingestion_time or event.timestamp, event.event_id or "")
