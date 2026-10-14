"""Tests for the run tasks command.

Run: uv run pytest tests/unit/cli/run/test_run_tasks.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
from click.testing import CliRunner
from valkyrie.sdk.models import FailureCategory, TaskStatus, TasksResponse, TaskSummary

from valkyrie.cli.run.tasks import tasks

from tests.unit.cli.conftest import MockTracker

RUN_ID = UUID(int=811)


def _summary(index: int, *, error: str | None = None, category: FailureCategory | None = None) -> TaskSummary:
    return TaskSummary(
        id=UUID(int=index + 1),
        task_id=f"task-{index}",
        status=TaskStatus.ERROR if error else TaskStatus.FINISHED,
        started_at=datetime(2026, 9, 17, 12, tzinfo=timezone.utc),
        finished_at=None,
        error_message=error,
        failure_category=category,
    )


def _invoke(tracker: MockTracker, response: TasksResponse, *arguments: str):
    tracker.reply("GET", f"/benchmarks/{RUN_ID}/tasks", httpx.Response(200, json=response.model_dump(mode="json")))
    return CliRunner().invoke(tasks, [str(RUN_ID), *arguments])


def test_tasks_text_shows_category_column_and_blank_for_legacy_rows(tracker: MockTracker) -> None:
    response = TasksResponse(
        tasks=[
            _summary(0, error="Agent command failed with exit code 2", category=FailureCategory.AGENT),
            _summary(1, error="legacy failure"),
            _summary(2),
        ],
        total_count=3,
    )

    result = _invoke(tracker, response)

    assert result.exit_code == 0, result.output
    header, *body = [line for line in result.output.splitlines() if line.strip()]
    assert header.split() == ["Task", "Status", "Category", "Error"]
    category_start = header.index("Category")
    error_start = header.index("Error")
    rows = {line.split()[0]: line[category_start:error_start].strip() for line in body if line.startswith("task-")}
    assert rows == {"task-0": "agent", "task-1": "", "task-2": ""}
    assert "Agent command failed with exit code 2" in result.output


def test_tasks_json_emits_failure_category(tracker: MockTracker) -> None:
    response = TasksResponse(
        tasks=[_summary(0, error="sandbox lost", category=FailureCategory.INFRASTRUCTURE), _summary(1)],
        total_count=2,
    )

    result = _invoke(tracker, response, "--format", "json")

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert [task["failure_category"] for task in payload["tasks"]] == ["infrastructure", None]
