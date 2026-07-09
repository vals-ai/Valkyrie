"""Tests for task-scoped run stops.

Run: uv run pytest tests/unit/cli/run/test_stop.py

Covers CLI task selection and conflicting task sources for `valkyrie run stop`.
"""

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.types import StopBenchmarkResponse

stop_module = import_module("valkyrie.cli.run.stop")
stop_command = stop_module.stop


class MockTrackerService:
    """Record stop requests made by the CLI command."""

    stop_calls: list[dict[str, object]] = []
    auth_credential: str | None = "benchmark-secret"

    def __enter__(self) -> "MockTrackerService":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark(self, _run_id: UUID) -> SimpleNamespace:
        return SimpleNamespace(benchmark_name="swebench")

    @classmethod
    def get_benchmark_auth(cls, _benchmark_name: str) -> str | None:
        return cls.auth_credential

    def stop_benchmark(
        self,
        benchmark_id: UUID,
        force: bool,
        task_ids: list[str] | None = None,
        service_headers: dict[str, str] | None = None,
    ) -> StopBenchmarkResponse:
        self.stop_calls.append(
            {
                "benchmark_id": benchmark_id,
                "force": force,
                "task_ids": task_ids,
                "service_headers": service_headers,
            }
        )

        return StopBenchmarkResponse(status="success")


@pytest.fixture(autouse=True)
def reset_stop_calls() -> None:
    """Reset recorded requests so each test is isolated."""
    MockTrackerService.stop_calls = []
    MockTrackerService.auth_credential = "benchmark-secret"


def test_stop_resolves_inline_and_file_task_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Resolve both supported task-selection inputs before stopping a run.

    Test cases:
    - Comma-separated task IDs are trimmed and deduplicated.
    - A task ID file uses the shared one-per-line parser.
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
    full_result = runner.invoke(stop_command, [str(run_id)], input="y\n")
    force_result = runner.invoke(
        stop_command,
        [str(run_id), "--task-ids", "task-e", "--force"],
        input="y\n",
    )

    assert inline_result.exit_code == 0, inline_result.output
    assert file_result.exit_code == 0, file_result.output
    assert full_result.exit_code == 0, full_result.output
    assert force_result.exit_code == 0, force_result.output
    assert [call["task_ids"] for call in MockTrackerService.stop_calls] == [
        ["task-a", "task-b"],
        ["task-c", "task-d"],
        None,
        ["task-e"],
    ]
    assert [call["force"] for call in MockTrackerService.stop_calls] == [False, False, False, True]
    assert [call["service_headers"] for call in MockTrackerService.stop_calls] == [
        {"Authorization": "benchmark-secret"},
        {"Authorization": "benchmark-secret"},
        None,
        {"Authorization": "benchmark-secret"},
    ]
    assert "Selected tasks force stopped" in force_result.output


def test_stop_rejects_conflicting_task_sources() -> None:
    """Reject simultaneous inline and file task-selection inputs.

    Test cases:
    - `--task-ids` and `--task-ids-file` return a Click usage error.
    """
    runner = CliRunner()

    result = runner.invoke(
        stop_command,
        [
            "123e4567-e89b-12d3-a456-426614174000",
            "--task-ids",
            "task-a",
            "--task-ids-file",
            "task-ids.txt",
        ],
    )

    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


@pytest.mark.parametrize("task_ids", ["", ",", " , "])
def test_stop_rejects_empty_task_selection(task_ids: str) -> None:
    """Reject an explicitly supplied task selector that resolves to no IDs.

    Test cases:
    - Empty and delimiter-only selections cannot widen into a full-run stop.
    """
    result = CliRunner().invoke(
        stop_command,
        ["123e4567-e89b-12d3-a456-426614174000", "--task-ids", task_ids],
    )

    assert result.exit_code == 2
    assert "No task ids provided" in result.output
