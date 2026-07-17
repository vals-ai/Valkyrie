"""Integration tests for read-only CLI commands against the tracker app.

Run: uv run pytest tests/integration/local/cli/test_read_commands.py
"""

import json

from click.testing import CliRunner
from tracker.database.models import Benchmark

from valkyrie.cli.main import cli


def test_cli_reads_persisted_tracker_state(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
) -> None:
    """Verify CLI JSON contracts using real tracker routes and database queries.

    Test cases:
    - Run list preserves database ordering, identity, score, and task counts.
    - Run status aggregates terminal task states through the batch endpoint.
    - Run fetch combines status and metadata without exposing contract secrets.
    """
    running, finished = seeded_runs

    list_result = cli_runner.invoke(cli, ["run", "list", "--format", "json", "--all"])
    assert list_result.exit_code == 0, list_result.output
    list_payload = json.loads(list_result.output)
    assert [run["run_id"] for run in list_payload["runs"]] == [str(finished.id), str(running.id)]
    assert list_payload["runs"][0]["final_score"] == 0.75
    assert list_payload["runs"][1]["task_state_counts"] == {
        "BUILDING": 0,
        "ERROR": 1,
        "EVALUATING": 0,
        "FINISHED": 1,
        "IN_PROGRESS": 1,
        "PENDING": 1,
        "STOPPED": 0,
    }

    status_result = cli_runner.invoke(
        cli,
        ["run", "status", "--ids", f"{running.id},{finished.id}", "--format", "json"],
    )
    assert status_result.exit_code == 0, status_result.output
    status_payload = json.loads(status_result.output)
    assert [(run["run_id"], run["finished_tasks"]) for run in status_payload["runs"]] == [
        (str(running.id), 2),
        (str(finished.id), 1),
    ]

    fetch_result = cli_runner.invoke(cli, ["run", "fetch", str(running.id), "--format", "json"])
    assert fetch_result.exit_code == 0, fetch_result.output
    fetch_payload = json.loads(fetch_result.output)
    assert fetch_payload["agent_name"] == "cli-agent"
    assert fetch_payload["dataset"] == "verified"
    assert fetch_payload["max_concurrency"] == 2
    assert "must-not-leak" not in fetch_result.output
