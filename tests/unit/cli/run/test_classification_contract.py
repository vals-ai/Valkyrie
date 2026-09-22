"""Consume captured process-task output through SDK models and CLI rendering."""

import json
from pathlib import Path

import pytest

from tracker.types import FinalViewResponse
from valkyrie.sdk.models.runs import FinalViewResponse as SDKFinalViewResponse
from valkyrie.sdk.models.benchmarks import SingleTaskResponse, TasksResponse
from valkyrie.cli.run.errors import format_run_errors_json, format_run_errors_text


@pytest.mark.parametrize(
    "fixture_name,category", [("classification.json", "benchmark_service"), ("original-error.json", "agent")]
)
def test_stored_failure_crosses_sdk_and_cli(
    capsys: pytest.CaptureFixture[str], fixture_name: str, category: str
) -> None:
    """Fixture was emitted by the SQLite producer/API test, not hand-built SDK data."""
    path = Path(__file__).parents[3] / "fixtures" / "sdk_api" / fixture_name
    wire = json.loads(path.read_text())
    task = SingleTaskResponse.model_validate(wire["task"])
    tasks = TasksResponse.model_validate(wire["tasks"])
    results = SDKFinalViewResponse.model_validate(wire["results"])
    assert task.failure_category == category
    assert tasks.tasks[0].failure_category == task.failure_category
    assert results.task_failure_categories is not None
    assert results.task_errors is not None
    assert task.error_message is not None
    assert results.task_failure_categories[task.task_id] == task.failure_category
    assert results.task_errors[task.task_id] == task.error_message
    cli_results = FinalViewResponse.model_validate(results.model_dump(mode="json"))
    payload = json.loads(format_run_errors_json(cli_results))
    assert payload["task_failure_categories"] == {task.task_id: category}
    assert payload["task_errors"] == {task.task_id: task.error_message}
    format_run_errors_text(cli_results)
    rendered = capsys.readouterr().out
    assert f"[1 task, {category}] task_0" in rendered
    assert task.error_message in rendered

    if fixture_name == "original-error.json":
        assert "TimeoutError: VALKYRIE_282_ACCEPTANCE controlled upstream timeout" in task.error_message
        assert "VALKYRIE_282_STDOUT_SENTINEL" not in rendered
        assert "VALKYRIE_282_STDERR_SENTINEL" not in rendered
