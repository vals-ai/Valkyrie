"""Tests for the run logs command.

Run: uv run pytest tests/unit/cli/run/test_run_logs.py
"""

from __future__ import annotations

import json
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.logs import logs

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_LOGS_PATH = f"/benchmarks/{_RUN_ID}/logs"


def _event(message: str, task_id: str | None = "task-1") -> dict[str, object]:
    return {"timestamp": "2026-01-01T00:00:00Z", "message": message, "task_id": task_id}


def _stream(*events: dict[str, object], end: bool = True) -> httpx.Response:
    body = "".join(f"event: log\ndata: {json.dumps(event)}\n\n" for event in events)
    return httpx.Response(
        200, text=body + ("event: end\ndata: {}\n\n" if end else ""), headers={"content-type": "text/event-stream"}
    )


@pytest.mark.parametrize(
    ("arguments", "path", "task_id"),
    [
        ([], _LOGS_PATH, None),
        (["--task-id", "provider/model:fast", "--format", "jsonl"], _LOGS_PATH, "provider/model:fast"),
        (
            ["--task-id", "provider/model:fast", "--follow", "--format", "jsonl"],
            f"{_LOGS_PATH}/stream",
            "provider/model:fast",
        ),
    ],
    ids=["run", "task", "follow"],
)
def test_logs_select_the_stream_and_forward_filters(
    tracker: MockTracker, cli_runner: CliRunner, arguments: list[str], path: str, task_id: str | None
) -> None:
    tracker.reply("GET", _LOGS_PATH, httpx.Response(200, json={"events": [_event("hello")]}))
    tracker.reply("GET", f"{_LOGS_PATH}/stream", _stream(_event("hello")))

    result = cli_runner.invoke(logs, [str(_RUN_ID), "--query", "hello", "--since", "2026-01-01T00:00:00Z", *arguments])

    assert result.exit_code == 0, result.output

    (request,) = tracker.requests
    assert request.url.path == path
    assert request.url.params.get("task_id") == task_id
    assert request.url.params["query"] == "hello"
    assert request.url.params["start_time"] == "2026-01-01T00:00:00+00:00"
    if "jsonl" in arguments:
        assert json.loads(result.output)["message"] == "hello"
    else:
        assert result.output == "2026-01-01T00:00:00Z [task-1] hello\n"


def test_text_output_keeps_each_record_on_one_escaped_line(tracker: MockTracker, cli_runner: CliRunner) -> None:
    event = _event("\x1b[31mfirst\nFORGED\tfield\r", task_id="task\nFORGED")
    tracker.reply("GET", _LOGS_PATH, httpx.Response(200, json={"events": [event]}))

    result = cli_runner.invoke(logs, [str(_RUN_ID)])

    assert result.exit_code == 0, result.output
    assert result.output == "2026-01-01T00:00:00Z [task\\nFORGED] \\u001b[31mfirst\\nFORGED\\tfield\\r\n"


def test_logs_write_each_page_before_fetching_the_next(tracker: MockTracker, cli_runner: CliRunner) -> None:
    """A failure on the second page still leaves the first page's records on stdout."""
    tracker.reply(
        "GET",
        _LOGS_PATH,
        httpx.Response(200, json={"events": [_event("first")], "next_cursor": "next"}),
        httpx.ConnectError("tracker went away"),
    )

    result = cli_runner.invoke(logs, [str(_RUN_ID)])

    assert result.exit_code == 1
    assert result.output.index("first") < result.output.index("tracker went away")


def test_follow_fails_when_the_stream_ends_without_its_end_event(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply("GET", f"{_LOGS_PATH}/stream", _stream(_event("final"), end=False))

    result = cli_runner.invoke(logs, [str(_RUN_ID), "--task-id", "task-1", "--follow"])

    assert result.exit_code == 1
    assert "final" in result.output
    assert "ended before its end event" in result.output


def test_ctrl_c_while_following_exits_quietly(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply("GET", f"{_LOGS_PATH}/stream", KeyboardInterrupt())

    result = cli_runner.invoke(logs, [str(_RUN_ID), "--task-id", "task-1", "--follow"])

    assert result.exit_code == 0
    assert result.output == ""


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--follow"], "requires --task-id"),
        (["--since", "2026-01-01T00:00:00"], "timezone offset"),
        (["--since", "2026-01-02T00:00:00Z", "--until", "2026-01-01T00:00:00Z"], "later than"),
        (["--since", "2026-01-01T00:00:00Z", "--until", "2026-01-01T00:00:00Z"], "later than"),
        (["--query", "   "], "must not be blank"),
    ],
)
def test_logs_rejects_invalid_modes_and_bounds(arguments: list[str], message: str) -> None:
    """Invalid follow, query, and timestamp combinations must fail before network access."""
    result = CliRunner().invoke(logs, [str(_RUN_ID), *arguments])

    assert result.exit_code == 2
    assert message in result.output
