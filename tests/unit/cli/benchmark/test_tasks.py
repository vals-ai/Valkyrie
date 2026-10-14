"""Tests for benchmark task-ID export commands.

Run: uv run pytest tests/unit/cli/benchmark/test_tasks.py
"""

import json
from pathlib import Path

import httpx
from click.testing import CliRunner

from valkyrie.cli.benchmark.tasks import tasks

from tests.unit.cli.conftest import MockTracker


class TestBenchmarkTasksCommand:
    """Task export contents, request options, and failure safety."""

    def test_export_writes_task_ids_and_sends_the_headers_and_service_choice(
        self, tracker: MockTracker, tmp_path: Path, cli_runner: CliRunner
    ) -> None:
        tracker.reply("POST", "/fetch-benchmark-tasks", httpx.Response(200, json={"task_ids": ["task-a", "task-b"]}))
        output_path = tmp_path / "tasks.txt"

        result = cli_runner.invoke(
            tasks,
            ["swebench", "--dataset", "verified", "-H", "X-Test", "value", "--ics", "--output", str(output_path)],
        )

        assert result.exit_code == 0, result.output
        assert output_path.read_text(encoding="utf-8") == "task-a\ntask-b\n"

        (request,) = tracker.requests
        assert json.loads(request.content) == {
            "benchmark_name": "swebench",
            "dataset": "verified",
            "service_headers": {"X-Test": "value"},
            "ignore_custom_services": True,
        }

    def test_tracker_failure_does_not_create_output(
        self, tracker: MockTracker, tmp_path: Path, cli_runner: CliRunner
    ) -> None:
        tracker.reply(
            "POST", "/fetch-benchmark-tasks", httpx.Response(502, json={"detail": "benchmark service unavailable"})
        )
        output_path = tmp_path / "tasks.txt"

        result = cli_runner.invoke(tasks, ["swebench", "--output", str(output_path)])

        assert result.exit_code == 1
        assert "benchmark service unavailable" in result.output
        assert not output_path.exists()
