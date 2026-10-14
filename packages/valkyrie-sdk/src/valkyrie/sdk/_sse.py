"""Server-sent event parsing shared by the streaming resources."""

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx


async def server_sent_events(response: httpx.Response) -> AsyncIterator[tuple[str, str]]:
    """Yield `(event, data)` for each event in a `text/event-stream` body.

    `event` is "" when the server sent only data lines, and multi-line data is joined with
    newlines. Comments and events with neither a name nor data are skipped. An event left
    unterminated when the body ends is still yielded.
    """
    event_name = ""
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if line.startswith("event:"):
            event_name = line.removeprefix("event:").strip()
        elif line.startswith("data:"):
            data_lines.append(line.removeprefix("data:").lstrip())
        elif line == "":
            if event_name or data_lines:
                yield event_name, "\n".join(data_lines)
            event_name, data_lines = "", []

    if event_name or data_lines:
        yield event_name, "\n".join(data_lines)


def error_detail(data: str, key: str) -> Any:
    """Return `data[key]` from a JSON error event, or the raw data when it isn't a JSON object."""
    try:
        payload: Any = json.loads(data) if data else {}
    except json.JSONDecodeError:
        return data
    return payload.get(key, payload) if isinstance(payload, dict) else payload
