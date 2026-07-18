"""Integration tests for CLI commands that change or export tracker state.

Run: uv run pytest tests/integration/local/cli/test_write_commands.py
"""

import json
from pathlib import Path

from click.testing import CliRunner
from sqlmodel import Session, select
from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus

from valkyrie.cli.main import cli


def test_cli_stops_only_selected_pending_tasks(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    """A selective stop must preserve the run and every unselected task.

    Test cases:
    - The CLI sends selected task IDs through the FastAPI request body.
    - The tracker stops the selected pending task without stopping the run.
    - An unselected active task keeps its current state.
    """
    running, _finished = seeded_runs

    result = cli_runner.invoke(
        cli,
        ["run", "stop", str(running.id), "--task-ids", "pending"],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    assert "Selected tasks are stopping" in result.output

    database_session.expire_all()
    task_rows = database_session.exec(select(Task).where(Task.benchmark == running.id)).all()
    task_statuses = {task.task_id: task.status for task in task_rows}
    assert task_statuses["pending"] == TaskStatus.STOPPED
    assert task_statuses["active"] == TaskStatus.IN_PROGRESS

    stored_run = database_session.get(Benchmark, running.id)
    assert stored_run is not None
    assert stored_run.status == BenchmarkStatus.IN_PROGRESS


def test_cli_updates_active_run_concurrency_through_tracker(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    """The public CLI must persist live concurrency through the production HTTP path.

    Test cases:
    - A live update travels through Click, the tracker client, and the FastAPI route.
    - Repeating the same update is idempotent.
    - A subsequent CLI read and direct database read return the new limit.
    - Updating concurrency preserves the rest of the stored benchmark arguments.
    """
    running, _finished = seeded_runs
    original_arguments = running.arguments

    for _ in range(2):
        result = cli_runner.invoke(cli, ["run", "update", str(running.id), "--concurrency", "4"])

        assert result.exit_code == 0, result.output
        assert result.output == "✓ Run concurrency updated to 4.\n"

    fetch_result = cli_runner.invoke(cli, ["run", "fetch", str(running.id), "--format", "json"])

    assert fetch_result.exit_code == 0, fetch_result.output
    assert json.loads(fetch_result.output)["max_concurrency"] == 4

    database_session.expire_all()
    stored_run = database_session.get(Benchmark, running.id)
    assert stored_run is not None
    assert stored_run.arguments == original_arguments.model_copy(update={"concurrency": 4})


def test_cli_rejects_concurrency_update_for_finished_run(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    """A terminal run must reject updates without changing its stored arguments."""
    _running, finished = seeded_runs
    original_arguments = finished.arguments

    result = cli_runner.invoke(cli, ["run", "update", str(finished.id), "--concurrency", "4"])

    assert result.exit_code == 1
    assert "Failed to update run concurrency" in result.output
    assert "BenchmarkStatus.FINISHED" in result.output

    database_session.expire_all()
    stored_run = database_session.get(Benchmark, finished.id)
    assert stored_run is not None
    assert stored_run.arguments == original_arguments


def test_cli_exports_tracker_results_without_private_contract_values(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    tmp_path: Path,
) -> None:
    """Result export must preserve tracker data without persisting private agent configuration.

    Test cases:
    - The CLI accepts the FastAPI final-view response and writes its evaluation and final score.
    - Agent secrets and runtime kwargs are absent from the saved JSON.
    """
    _running, finished = seeded_runs
    output_path = tmp_path / "results.json"

    result = cli_runner.invoke(cli, ["run", "results", str(finished.id), "--path", str(output_path)])

    assert result.exit_code == 0, result.output
    saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved_payload["benchmark_id"] == str(finished.id)
    assert saved_payload["final_evaluation"]["final_score"] == 0.75
    assert saved_payload["evaluation_results"]["complete"]["score"] == 1
    assert "secrets" not in saved_payload["benchmark_arguments"]["contract"]
    assert "kwargs" not in saved_payload["benchmark_arguments"]["contract"]
    assert "finished-secret-must-not-leak" not in output_path.read_text(encoding="utf-8")
