"""Tests for shedding the newest tasks from a running run.

Run: uv run pytest tests/unit/cli/run/test_shed.py

Covers the preview-then-shed call order and the tracker rejections that must leave the run alone.
"""

import json
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.shed import shed

from tests.unit.cli.conftest import MockTracker

RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


def serve_shed(tracker: MockTracker, task_ids: list[str], error: str | None = None) -> None:
    """Tracker selects the tasks, so preview and real shed answer with the same task IDs."""

    def handle(request: httpx.Request) -> httpx.Response:
        if error is not None:
            return httpx.Response(409, json={"detail": error})
        concurrency = json.loads(request.content)["concurrency"]
        return httpx.Response(200, json={"benchmark_id": str(RUN_ID), "concurrency": concurrency, "task_ids": task_ids})

    tracker.reply("POST", f"/benchmarks/{RUN_ID}/shed", handle)


def shed_calls(tracker: MockTracker) -> list[tuple[int, bool]]:
    return [(body["concurrency"], body["dry_run"]) for body in (json.loads(r.content) for r in tracker.requests)]


def test_shed_previews_then_sheds_after_confirmation(cli_runner: CliRunner, tracker: MockTracker) -> None:
    """A dry run shows the plan, the prompt is answered, and one real shed request follows."""
    serve_shed(tracker, ["late", "b"])

    result = cli_runner.invoke(shed, [str(RUN_ID), "--concurrency", "1"], input="y\n")

    assert result.exit_code == 0, result.output
    assert shed_calls(tracker) == [(1, True), (1, False)]
    assert "Would force stop 2 task(s):" in result.output
    assert "Run concurrency updated to 1." in result.output
    assert "Force stopped 2 task(s):" in result.output
    assert result.output.count("late") == 2


def test_shed_reports_when_nothing_is_above_the_limit(cli_runner: CliRunner, tracker: MockTracker) -> None:
    serve_shed(tracker, [])

    result = cli_runner.invoke(shed, [str(RUN_ID), "--concurrency", "4"], input="y\n")

    assert result.exit_code == 0, result.output
    assert shed_calls(tracker) == [(4, True), (4, False)]
    assert result.output.count("No building or in-progress tasks are above the limit.") == 2


@pytest.mark.parametrize(
    "detail",
    [
        "Run 123e4567-e89b-12d3-a456-426614174000 concurrency is 4; shed can only lower it.",
        "Run 123e4567-e89b-12d3-a456-426614174000 is currently in the STOPPING state.",
    ],
)
def test_shed_surfaces_tracker_rejections_from_the_preview(
    cli_runner: CliRunner, tracker: MockTracker, detail: str
) -> None:
    """The tracker refuses to raise the limit or touch an inactive run; the preview fails before any prompt."""
    serve_shed(tracker, [], error=detail)

    result = cli_runner.invoke(shed, [str(RUN_ID), "--concurrency", "4"], input="y\n")

    assert result.exit_code == 1
    assert shed_calls(tracker) == [(4, True)]
    assert detail in result.output


def test_shed_dry_run_and_cancel_only_preview(cli_runner: CliRunner, tracker: MockTracker) -> None:
    serve_shed(tracker, ["b"])

    dry_run = cli_runner.invoke(shed, [str(RUN_ID), "--concurrency", "1", "--dry-run"])
    declined = cli_runner.invoke(shed, [str(RUN_ID), "--concurrency", "1"], input="n\n")

    assert dry_run.exit_code == 0, dry_run.output
    assert "Would force stop 1 task(s):" in dry_run.output
    assert declined.exit_code == 0, declined.output
    assert "Cancelled." in declined.output
    assert shed_calls(tracker) == [(1, True), (1, True)]
