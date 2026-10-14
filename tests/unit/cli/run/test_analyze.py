"""Tests for run analysis command behavior.

Run: uv run pytest tests/unit/cli/run/test_analyze.py
"""

import io
import json
import zipfile
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.analyze import analyze

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_metadata, ok

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_ANALYZE_PATH = f"/analyze-benchmark/{_RUN_ID}"
_CONTRACT = "name: mini_sweagent\ninstall_cmd: 'true'\nrun_cmd: 'run {problem_statement_path}'\n"


def serve_pushed_agent(tracker: MockTracker, contract: str | None) -> None:
    """Serve the run's metadata and its agent's current bundle, or a 404 when `contract` is None."""
    tracker.reply("GET", f"/fetch-benchmark-metadata/{_RUN_ID}", ok(make_fetch_metadata(_RUN_ID)))
    if contract is None:
        tracker.reply("GET", "/agents/mini_sweagent/download-url", httpx.Response(404, json={"detail": "Not found"}))
        return

    bundle = io.BytesIO()
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("mini_sweagent/contract.yaml", contract)
    download_url = {"name": "mini_sweagent", "download_url": "https://download.test/agent.zip", "expires_in": 60}
    tracker.reply("GET", "/agents/mini_sweagent/download-url", httpx.Response(200, json=download_url))
    tracker.reply("GET", "/agent.zip", httpx.Response(200, content=bundle.getvalue()))


def test_analysis_uses_the_pushed_contracts_lambda_and_reports_the_plan(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    """Only `ingest_lambda` is read, so a pushed contract missing required fields still works."""
    serve_pushed_agent(tracker, "name: mini_sweagent\ningest_lambda: docent-ingest\n")
    events = [
        ("started", {"lambda_function": "docent-ingest"}),
        ("heartbeat", {}),
        ("done", {"reading_plan_url": "https://docent.example/plan"}),
    ]
    body = "".join(f"event: {name}\ndata: {json.dumps(data)}\n\n" for name, data in events)
    tracker.reply("POST", _ANALYZE_PATH, httpx.Response(200, text=body, headers={"content-type": "text/event-stream"}))

    result = cli_runner.invoke(analyze, [str(_RUN_ID), "--no-cache"])

    assert result.exit_code == 0, result.output
    assert "Invoking docent-ingest" in result.output
    assert "Reading plan: https://docent.example/plan" in result.output
    assert "event:" not in result.output

    (request,) = tracker.requested("POST", _ANALYZE_PATH)
    assert json.loads(request.content) == {"no_cache": True, "lambda_function": "docent-ingest"}


@pytest.mark.parametrize(
    ("contract", "expected_message"),
    [
        (_CONTRACT, "has no `ingest_lambda` set"),
        ("name: [unclosed\n", "Could not load contract for agent 'mini_sweagent'"),
        (None, "If the agent has never been pushed"),
    ],
    ids=["no-lambda", "invalid-contract", "never-pushed"],
)
def test_unusable_analysis_contract_reports_remediation(
    tracker: MockTracker, cli_runner: CliRunner, contract: str | None, expected_message: str
) -> None:
    serve_pushed_agent(tracker, contract)

    result = cli_runner.invoke(analyze, [str(_RUN_ID)])

    assert result.exit_code == 1
    assert expected_message in result.output
    assert tracker.requested("POST", _ANALYZE_PATH) == []


@pytest.mark.parametrize(
    ("status", "expected_message"),
    [
        ("IN_PROGRESS", "is still in progress"),
        ("STOPPING", "is stopping"),
        ("STOPPED", "was stopped before completion"),
        ("ERROR", "errored before completing"),
    ],
)
def test_non_finished_runs_report_state_without_internal_error(
    tracker: MockTracker, cli_runner: CliRunner, status: str, expected_message: str
) -> None:
    serve_pushed_agent(tracker, _CONTRACT + "ingest_lambda: docent-ingest\n")
    detail = f"Cannot analyze run {_RUN_ID}: status is {status} (must be FINISHED)."
    tracker.reply("POST", _ANALYZE_PATH, httpx.Response(409, json={"detail": detail}))

    result = cli_runner.invoke(analyze, [str(_RUN_ID)])

    assert result.exit_code == 1
    assert expected_message in result.output
    assert "Valkyrie API request failed" not in result.output
