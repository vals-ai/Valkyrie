"""Tests for task-scoped run stops.

Run: uv run pytest tests/unit/cli/run/test_stop.py

Covers CLI task selection and conflicting task sources for `valkyrie run stop`.
"""

import json
from pathlib import Path
from uuid import UUID

import httpx
from click.testing import CliRunner

from valkyrie.cli.run.stop import stop

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


def test_stop_task_selection(tracker: MockTracker, tmp_path: Path) -> None:
    """Resolve valid task selectors and reject ambiguous or empty input.

    Test cases:
    - Comma-separated task IDs are trimmed and deduplicated.
    - A task ID file uses the shared one-per-line parser.
    - Inline and file selectors cannot be combined.
    - A delimiter-only selector cannot widen into a full-run stop.
    """
    tracker.reply("POST", f"/stop-benchmark/{_RUN_ID}", httpx.Response(200, json={"status": "success"}))
    task_ids_file = tmp_path / "task-ids.txt"
    task_ids_file.write_text("task-c\ntask-d\n", encoding="utf-8")
    runner = CliRunner()

    inline_result = runner.invoke(stop, [str(_RUN_ID), "--task-ids", "task-a, task-b,task-a"], input="y\n")
    file_result = runner.invoke(stop, [str(_RUN_ID), "--task-ids-file", str(task_ids_file)], input="y\n")
    conflicting_result = runner.invoke(
        stop, [str(_RUN_ID), "--task-ids", "task-a", "--task-ids-file", str(task_ids_file)]
    )
    empty_result = runner.invoke(stop, [str(_RUN_ID), "--task-ids", ","])

    assert inline_result.exit_code == 0, inline_result.output
    assert file_result.exit_code == 0, file_result.output
    assert [json.loads(request.content)["task_ids"] for request in tracker.requests] == [
        ["task-a", "task-b"],
        ["task-c", "task-d"],
    ]
    assert conflicting_result.exit_code == 2
    assert "mutually exclusive" in conflicting_result.output
    assert empty_result.exit_code == 2
    assert "No task ids provided" in empty_result.output
