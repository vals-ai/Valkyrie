"""Tests for the run logs command.

Run: uv run pytest tests/unit/cli/run/test_run_logs.py
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import AsyncIterator, Coroutine
from datetime import datetime, timezone
from importlib import import_module
from typing import Any
from uuid import UUID, uuid4

import pytest  # pyright: ignore[reportMissingImports]
from click.testing import CliRunner  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.models.logs import LogEvent, LogPage  # pyright: ignore[reportMissingImports]

from valkyrie.cli.run.logs import logs  # pyright: ignore[reportMissingImports]

logs_module = import_module("valkyrie.cli.run.logs")


class MockLogsResource:
    """Return configured events and retain command arguments."""

    def __init__(self, events: list[LogEvent]) -> None:
        self.events = events
        self.pages = deque([LogPage(events=events)])
        self.operation: str | None = None
        self.arguments: dict[str, Any] = {}
        self.timeline: list[str] = []

    async def page_run(self, run_id: UUID, **arguments: Any) -> LogPage:
        self.operation = "run"
        self.arguments = {"run_id": run_id, **arguments}
        self.timeline.append(f"page:{arguments.get('cursor')}")
        return self.pages.popleft()

    async def page_task(self, run_id: UUID, task_id: str, **arguments: Any) -> LogPage:
        self.operation = "task"
        self.arguments = {"run_id": run_id, "task_id": task_id, **arguments}
        self.timeline.append(f"page:{arguments.get('cursor')}")
        return self.pages.popleft()

    async def stream_task(self, run_id: UUID, task_id: str, **arguments: Any) -> AsyncIterator[LogEvent]:
        self.operation = "follow"
        self.arguments = {"run_id": run_id, "task_id": task_id, **arguments}
        for event in self.events:
            yield event


class MockClient:
    """Provide the async client context used by the command."""

    def __init__(self, events: list[LogEvent]) -> None:
        self.logs = MockLogsResource(events)

    async def __aenter__(self) -> "MockClient":
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    client: MockClient,
    run_id: UUID,
    arguments: list[str],
):
    monkeypatch.setattr(logs_module.ValkyrieClient, "from_config", lambda: client)
    return CliRunner().invoke(logs, [str(run_id), *arguments])


def test_logs_fetches_aggregate_with_filters_and_sanitizes_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default mode must aggregate run logs and remove terminal control characters."""
    run_id = uuid4()
    event = LogEvent(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        message="\x1b[31mfailed\r",
        task_id="task-1",
    )
    client = MockClient([event])

    result = _invoke(
        monkeypatch,
        client,
        run_id,
        [
            "--query",
            "failed",
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-01-01T01:00:00+00:00",
        ],
    )

    assert result.exit_code == 0, result.output
    assert client.logs.operation == "run"
    assert client.logs.arguments["query"] == "failed"
    assert client.logs.arguments["start_time"] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert "\x1b" not in result.output
    assert "[task-1] \\u001b[31mfailed\\r" in result.output


def test_logs_fetches_one_task_as_jsonl_and_follows_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """Task selection must work for snapshots, JSONL output, and follow mode."""
    run_id = uuid4()
    event = LogEvent(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        message="hello\x1b",
        task_id="task-1",
    )
    client = MockClient([event])

    result = _invoke(monkeypatch, client, run_id, ["--task-id", "provider/model:fast", "--format", "jsonl"])

    assert result.exit_code == 0, result.output
    assert client.logs.operation == "task"
    assert client.logs.arguments["task_id"] == "provider/model:fast"
    assert json.loads(result.output)["message"] == "hello\x1b"
    assert "\x1b" not in result.output

    follow_result = _invoke(
        monkeypatch,
        client,
        run_id,
        ["--task-id", "provider/model:fast", "--query", "hello", "--follow"],
    )

    assert follow_result.exit_code == 0, follow_result.output
    assert client.logs.operation == "follow"
    assert client.logs.arguments["query"] == "hello"


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--follow"], "requires --task-id"),
        (["--since", "2026-01-01T00:00:00"], "timezone offset"),
        (
            ["--since", "2026-01-02T00:00:00Z", "--until", "2026-01-01T00:00:00Z"],
            "later than",
        ),
        (
            ["--since", "2026-01-01T00:00:00Z", "--until", "2026-01-01T00:00:00Z"],
            "later than",
        ),
        (["--query", "   "], "must not be blank"),
    ],
)
def test_logs_rejects_invalid_modes_and_bounds(arguments: list[str], message: str) -> None:
    """Invalid follow, query, and timestamp combinations must fail before network access."""
    result = CliRunner().invoke(logs, [str(uuid4()), *arguments])

    assert result.exit_code == 2
    assert message in result.output


def test_non_follow_logs_write_each_page_before_fetching_the_next(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot mode must not accumulate all SDK pages before writing output."""
    run_id = uuid4()
    first = LogEvent(timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc), message="first")
    second = LogEvent(timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc), message="second")
    client = MockClient([])
    client.logs.pages = deque(
        [
            LogPage(events=[first], next_cursor="next"),
            LogPage(events=[second]),
        ]
    )

    def record_write(event: LogEvent, _output_format: str) -> None:
        client.logs.timeline.append(f"write:{event.message}")

    monkeypatch.setattr(logs_module, "_write_event", record_write)

    result = _invoke(monkeypatch, client, run_id, [])

    assert result.exit_code == 0, result.output
    assert client.logs.timeline == ["page:None", "write:first", "page:next", "write:second"]


def test_text_output_escapes_line_breaks_tabs_and_task_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Text records must stay on one prefixed line even when event fields contain controls."""
    run_id = uuid4()
    event = LogEvent(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        message="first\nFORGED\tfield",
        task_id="task\nFORGED",
    )

    result = _invoke(monkeypatch, MockClient([event]), run_id, [])

    assert result.exit_code == 0, result.output
    assert result.output.count("\n") == 1
    assert "task\\nFORGED" in result.output
    assert "first\\nFORGED\\tfield" in result.output
    assert "\t" not in result.output


def test_logs_handles_ctrl_c_without_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ctrl-C during follow must exit successfully without rendering a traceback."""

    def interrupt(coroutine: Coroutine[Any, Any, Any]) -> None:
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(logs_module.asyncio, "run", interrupt)

    result = CliRunner().invoke(logs, [str(uuid4()), "--task-id", "task-1", "--follow"])

    assert result.exit_code == 0
    assert "Traceback" not in result.output
