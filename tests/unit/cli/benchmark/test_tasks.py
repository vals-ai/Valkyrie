"""Tests for benchmark task-ID export commands.

Run: uv run pytest tests/unit/cli/benchmark/test_tasks.py
"""

from importlib import import_module
from pathlib import Path

import pytest
from click.testing import CliRunner

from valkyrie.cli.benchmark.tasks import tasks
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService

tasks_module = import_module("valkyrie.cli.benchmark.tasks")


class MockTaskTracker:
    """Return configured task IDs and record the benchmark request."""

    response: list[str] | TrackerServiceError = []
    calls: list[dict[str, object]] = []

    def __enter__(self) -> "MockTaskTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark_tasks(
        self,
        benchmark_name: str,
        dataset: str | None = None,
        ignore_custom_services: bool = False,
        service_headers: dict[str, str] | None = None,
    ) -> list[str]:
        self.calls.append(
            {
                "benchmark_name": benchmark_name,
                "dataset": dataset,
                "ignore_custom_services": ignore_custom_services,
                "service_headers": service_headers,
            }
        )
        if isinstance(self.response, TrackerServiceError):
            raise self.response

        return self.response


class TestBenchmarkTasksCommand:
    """Task export contents, request options, and failure safety."""

    def test_export_writes_task_ids_with_resolved_headers(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Task export must write one ID per line and preserve benchmark request options.

        Test cases:
        - Configured authorization is combined with CLI headers.
        - Dataset and custom-service opt-out reach the tracker.
        - The selected output file ends with a newline.
        """
        output_path = tmp_path / "tasks.txt"
        MockTaskTracker.response = ["task-a", "task-b"]
        MockTaskTracker.calls = []

        def configured_auth(_benchmark_name: str) -> str:
            return "Bearer configured"

        monkeypatch.setattr(tasks_module, "TrackerService", MockTaskTracker)
        monkeypatch.setattr(TrackerService, "get_benchmark_auth", staticmethod(configured_auth))

        result = cli_runner.invoke(
            tasks,
            [
                "swebench",
                "--dataset",
                "verified",
                "--header",
                "X-Test",
                "value",
                "--ignore-custom-services",
                "--output",
                str(output_path),
            ],
        )

        assert result.exit_code == 0, result.output
        assert output_path.read_text(encoding="utf-8") == "task-a\ntask-b\n"
        assert MockTaskTracker.calls == [
            {
                "benchmark_name": "swebench",
                "dataset": "verified",
                "ignore_custom_services": True,
                "service_headers": {"Authorization": "Bearer configured", "X-Test": "value"},
            }
        ]

    def test_tracker_failure_does_not_create_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """A failed tracker request must not leave a misleading task-ID file.

        Test cases:
        - Tracker errors produce a concise nonzero command result.
        - The requested output path remains absent.
        """
        output_path = tmp_path / "tasks.txt"
        MockTaskTracker.response = TrackerServiceError("benchmark service unavailable")
        MockTaskTracker.calls = []

        def no_auth(_benchmark_name: str) -> None:
            return None

        monkeypatch.setattr(tasks_module, "TrackerService", MockTaskTracker)
        monkeypatch.setattr(TrackerService, "get_benchmark_auth", staticmethod(no_auth))

        result = cli_runner.invoke(tasks, ["swebench", "--output", str(output_path)])

        assert result.exit_code == 1
        assert "benchmark service unavailable" in result.output
        assert not output_path.exists()
