"""Tests for task-scoped run stops.

Run: uv run pytest tests/unit/cli/run/test_stop.py

Covers CLI task selection and conflicting task sources for `valkyrie run stop`.
"""

import json
from importlib import import_module
from pathlib import Path
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.types import StopBenchmarkResponse

from valkyrie.cli.exceptions import TrackerServiceError

stop_module = import_module("valkyrie.cli.run.stop")
stop_command = stop_module.stop


class MockTrackerService:
    """Record stop requests made by the CLI command."""

    stop_calls: list[dict[str, object]] = []

    def __enter__(self) -> "MockTrackerService":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def stop_benchmark(
        self,
        benchmark_id: UUID,
        force: bool,
        task_ids: list[str] | None = None,
    ) -> StopBenchmarkResponse:
        self.stop_calls.append(
            {
                "benchmark_id": benchmark_id,
                "force": force,
                "task_ids": task_ids,
            }
        )

        return StopBenchmarkResponse(status="success")


@pytest.fixture(autouse=True)
def reset_stop_calls() -> None:
    """Reset recorded requests so each test is isolated."""
    MockTrackerService.stop_calls = []


def test_stop_task_selection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Resolve valid task selectors and reject ambiguous or empty input.

    Test cases:
    - Comma-separated task IDs are trimmed and deduplicated.
    - A task ID file uses the shared one-per-line parser.
    - Inline and file selectors cannot be combined.
    - A delimiter-only selector cannot widen into a full-run stop.
    """
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    task_ids_file = tmp_path / "task-ids.txt"
    task_ids_file.write_text("task-c\ntask-d\n", encoding="utf-8")
    runner = CliRunner()
    monkeypatch.setattr(stop_module, "TrackerService", MockTrackerService)

    inline_result = runner.invoke(
        stop_command,
        [str(run_id), "--task-ids", "task-a, task-b,task-a"],
        input="y\n",
    )

    file_result = runner.invoke(
        stop_command,
        [str(run_id), "--task-ids-file", str(task_ids_file)],
        input="y\n",
    )

    conflicting_result = runner.invoke(
        stop_command,
        [
            str(run_id),
            "--task-ids",
            "task-a",
            "--task-ids-file",
            str(task_ids_file),
        ],
    )

    empty_result = runner.invoke(
        stop_command,
        [str(run_id), "--task-ids", ","],
    )

    assert inline_result.exit_code == 0, inline_result.output
    assert file_result.exit_code == 0, file_result.output
    assert [call["task_ids"] for call in MockTrackerService.stop_calls] == [
        ["task-a", "task-b"],
        ["task-c", "task-d"],
    ]
    assert conflicting_result.exit_code == 2
    assert "mutually exclusive" in conflicting_result.output
    assert empty_result.exit_code == 2
    assert "No task ids provided" in empty_result.output


def test_json_stop_covers_completed_cancelled_and_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    tracker = MockTrackerService()
    monkeypatch.setattr(stop_module, "TrackerService", lambda: tracker)
    runner = CliRunner()

    forced = runner.invoke(stop_command, [str(run_id), "--force", "--json"])

    assert forced.exit_code == 0, forced.output
    assert json.loads(forced.stdout) == {
        "action": "stop",
        "force": True,
        "kind": "run_stop",
        "run_id": str(run_id),
        "schema_version": 1,
        "status": "completed",
        "task_ids": None,
    }
    assert MockTrackerService.stop_calls == [{"benchmark_id": run_id, "force": True, "task_ids": None}]

    cancelled = runner.invoke(stop_command, [str(run_id), "--json"], input="n\n")

    assert cancelled.exit_code == 0, cancelled.output
    assert json.loads(cancelled.stdout)["status"] == "cancelled"

    blocked = runner.invoke(stop_command, [str(run_id), "--json"], input="")

    assert blocked.exit_code == 1
    records = [json.loads(line) for line in blocked.stdout.splitlines()]
    assert [record["kind"] for record in records] == ["run_stop", "error"]
    assert records[0]["status"] == "blocked"
    assert records[0]["reason"] == "confirmation_required"
    assert "--force" in records[1]["error_message"]
    assert MockTrackerService.stop_calls == [{"benchmark_id": run_id, "force": True, "task_ids": None}]


def test_stop_declines_and_force_messages_in_human_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    tracker = MockTrackerService()
    monkeypatch.setattr(stop_module, "TrackerService", lambda: tracker)
    runner = CliRunner()

    declined = runner.invoke(stop_command, [str(run_id)], input="n\n")

    assert declined.exit_code == 0, declined.output
    assert "Cancelled." in declined.output
    assert MockTrackerService.stop_calls == []

    forced = runner.invoke(stop_command, [str(run_id), "--force"])

    assert forced.exit_code == 0, forced.output
    assert "force stopped" in forced.output


def test_stop_reports_tracker_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")

    class FailingTrackerService(MockTrackerService):
        def stop_benchmark(
            self,
            benchmark_id: UUID,
            force: bool,
            task_ids: list[str] | None = None,
        ) -> StopBenchmarkResponse:
            raise TrackerServiceError("run not found")

    monkeypatch.setattr(stop_module, "TrackerService", FailingTrackerService)

    result = CliRunner().invoke(stop_command, [str(run_id), "--force"])

    assert result.exit_code == 1
    assert "run not found" in result.output
