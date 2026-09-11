"""Integration tests for read-only CLI commands against the tracker app.

Run: uv run pytest tests/integration/local/cli/test_read_commands.py
"""

import json
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner
from fastapi import FastAPI
from sqlmodel import Session
from tracker.database.models import Benchmark
from valkyrie.sdk import ValkyrieClient, ValkyrieConfig

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


def test_queue_status_reads_persisted_scheduler_state(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
    local_tracker_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read a queued task through CLI, SDK, real tracker routing, and database queries."""
    running, _ = seeded_runs
    running.arguments = running.arguments.model_copy(update={"priority": 1, "queue_pool_id": "shared"})
    database_session.add(running)
    database_session.commit()

    def from_config(path: Path, *, base_url: str) -> ValkyrieClient:
        return ValkyrieClient(
            ValkyrieConfig.from_yaml(path),
            base_url=base_url,
            transport=httpx.ASGITransport(app=local_tracker_app),
        )

    monkeypatch.setattr(ValkyrieClient, "from_config", from_config)

    result = cli_runner.invoke(cli, ["queue", "status", "--format", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["summary"] == {"waiting": 1, "building": 0, "in_progress": 1, "evaluating": 0}
    assert payload["pools"] == [{"pool_id": "shared", "waiting": 1}]
    assert payload["waiting_entries"][0]["benchmark_uuid"] == str(running.id)
    assert payload["waiting_entries"][0]["external_task_id"] == "pending"
    assert payload["waiting_entries"][0]["position"] == 1
    assert payload["waiting_entries"][0]["priority"] == 1
    assert payload["active_entries"][0]["status"] == "IN_PROGRESS"
    assert payload["waiting_capped"] is False
    assert payload["waiting_next_offset"] is payload["active_next_offset"] is None
    assert "must-not-leak" not in result.output

    for exhausted, remaining in (("waiting", "active"), ("active", "waiting")):
        page_result = cli_runner.invoke(cli, ["queue", "status", "--format", "json", f"--{exhausted}-offset", "1"])

        assert page_result.exit_code == 0, page_result.output
        page = json.loads(page_result.output)
        assert page["summary"] == payload["summary"]
        assert page[f"{exhausted}_entries"] == []
        assert len(page[f"{remaining}_entries"]) == 1
        assert page["waiting_next_offset"] is page["active_next_offset"] is None


@pytest.mark.parametrize(
    ("parameter", "value"),
    [(name, value) for name in ("waiting_limit", "active_limit") for value in ("0", "201")]
    + [(name, value) for name in ("waiting_offset", "active_offset") for value in ("-1", "1.5")],
)
async def test_queue_api_rejects_invalid_page_bounds(local_tracker_app: FastAPI, parameter: str, value: str) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=local_tracker_app), base_url="http://tracker.test"
    ) as client:
        response = await client.get("/scheduler/overview", params={parameter: value})

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", parameter]


@pytest.mark.usefixtures("sdk_tracker_transport")
def test_cli_task_inspection(cli_runner: CliRunner, seeded_runs: tuple[Benchmark, Benchmark]) -> None:
    running, finished = seeded_runs
    result = cli_runner.invoke(
        cli,
        [
            "run",
            "tasks",
            str(running.id),
            "--status",
            "pending",
            "--search",
            "pend",
            "--sort",
            "task_id",
            "--limit",
            "1",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["total_count"] == 1
    assert [task["task_id"] for task in payload["tasks"]] == ["pending"]
    detail = cli_runner.invoke(cli, ["run", "task", str(finished.id), "complete", "--format", "json"])
    assert detail.exit_code == 0, detail.output
    assert json.loads(detail.output)["evaluation_result"] == {"score": 1}
    missing = cli_runner.invoke(cli, ["run", "task", str(running.id), "missing"])
    assert missing.exit_code == 1
    assert "404" in missing.output
    text = cli_runner.invoke(cli, ["run", "tasks", str(running.id), "--offset", "50"])
    assert text.exit_code == 0, text.output


def test_cli_task_artifact_links(
    cli_runner: CliRunner, seeded_runs: tuple[Benchmark, Benchmark], agent_library: dict[str, bytes]
) -> None:
    _, finished = seeded_runs
    result = cli_runner.invoke(cli, ["run", "task-artifacts", str(finished.id), "complete", "--format", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["agent_output_url"] is None
    text = cli_runner.invoke(cli, ["run", "task", str(finished.id), "complete"])
    assert text.exit_code == 0, text.output
    assert '"score": 1' in text.output
