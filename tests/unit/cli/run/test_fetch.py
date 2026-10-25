"""Tests for run fetch output contracts.

Run: uv run pytest tests/unit/cli/run/test_fetch.py

Covers the JSON snapshot, the JSONL stream records, and the text stream's identity and outcome.
"""

import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk.models import BenchmarkStatus, FetchBenchmarkMetadataResponse, ProjectResponse, SpendType, TaskStatus

from valkyrie.cli.run.fetch import fetch
from valkyrie.cli.run.snapshot import build_run_snapshot, format_run_snapshot_json

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_metadata, make_fetch_response, run_stream, serve_run

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_SECRETS = ("MODEL_API_KEY", "classified-secret-name", "temperature")
_PROJECT = ProjectResponse(
    handle="google-report-xyz",
    name="Google Report XYZ",
    spend_type=SpendType.CUSTOMER,
    subcategory="report",
    customer="google",
    extra_tags={"engagement": "q4"},
    created_at=datetime(2026, 10, 21, 18, 0, tzinfo=timezone.utc),
    created_by_email="researcher@vals.ai",
)


def _tagged_metadata() -> FetchBenchmarkMetadataResponse:
    return make_fetch_metadata(_RUN_ID).model_copy(update={"project": _PROJECT})


def _records(output: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in output.splitlines()]


def test_run_snapshot_is_versioned_allowlisted_and_stable() -> None:
    response = make_fetch_response(_RUN_ID)
    metadata = make_fetch_metadata(_RUN_ID)
    observed_at = datetime(2026, 7, 9, 13, 0, tzinfo=timezone.utc)

    snapshot = build_run_snapshot(response, metadata, event="snapshot", observed_at=observed_at)
    serialized = format_run_snapshot_json(response, metadata, event="snapshot")

    assert snapshot["schema_version"] == 1
    assert snapshot["event"] == "snapshot"
    assert snapshot["observed_at"] == "2026-07-09T13:00:00Z"
    assert snapshot["run_id"] == str(_RUN_ID)
    assert snapshot["agent_name"] == "mini_sweagent"
    assert snapshot["model"] == "openai/gpt-5"
    assert snapshot["dataset"] == "verified"
    assert snapshot["progress_percent"] == 25.0
    assert snapshot["task_state_counts"] == {
        "PENDING": 0,
        "BUILDING": 0,
        "IN_PROGRESS": 3,
        "EVALUATING": 0,
        "STOPPED": 0,
        "FINISHED": 1,
        "ERROR": 0,
    }
    assert not any(secret in serialized for secret in _SECRETS)


@pytest.mark.parametrize(
    ("final_score", "metadata_available"),
    [(None, True), (float("nan"), True), (float("inf"), True), (float("-inf"), True), (None, False)],
)
def test_fetch_json_outputs_one_clean_object(
    tracker: MockTracker, cli_runner: CliRunner, final_score: float | None, metadata_available: bool
) -> None:
    """Non-finite scores become null, and missing metadata nulls the identity instead of failing."""
    metadata = make_fetch_metadata(_RUN_ID) if metadata_available else None
    serve_run(tracker, make_fetch_response(_RUN_ID, final_score=final_score), metadata=metadata)

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--format", "json"])

    assert result.exit_code == 0, result.output
    assert len(result.output.splitlines()) == 1
    assert not any(token in result.output for token in ("\x1b", "NaN", "Infinity"))

    payload = json.loads(result.output)
    assert payload["event"] == "snapshot"
    assert payload["run_id"] == str(_RUN_ID)
    assert payload["final_score"] is None
    assert payload["metadata_available"] is metadata_available
    assert payload["agent_name"] == ("mini_sweagent" if metadata_available else None)


@pytest.mark.parametrize(
    ("metadata", "spend_line"),
    [
        (_tagged_metadata(), "│ Spend:       customer · google-report-xyz · report · google · engagement=q4"),
        (make_fetch_metadata(_RUN_ID), "│ Spend:       untagged"),
        (None, "│ Spend:       unavailable"),
    ],
    ids=["tagged", "untagged", "metadata-unavailable"],
)
def test_fetch_text_shows_the_runs_project(
    tracker: MockTracker,
    cli_runner: CliRunner,
    metadata: FetchBenchmarkMetadataResponse | None,
    spend_line: str,
) -> None:
    serve_run(tracker, make_fetch_response(_RUN_ID), metadata=metadata)

    result = cli_runner.invoke(fetch, [str(_RUN_ID)])

    assert result.exit_code == 0, result.output
    assert spend_line in result.output


def test_fetch_text_escapes_terminal_control_characters_in_the_project(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    hostile = _PROJECT.model_copy(update={"handle": "Blog\x1b[2K", "extra_tags": {"note": "a\x1b[1Ab"}})
    metadata = make_fetch_metadata(_RUN_ID).model_copy(update={"project": hostile})
    serve_run(tracker, make_fetch_response(_RUN_ID), metadata=metadata)

    result = cli_runner.invoke(fetch, [str(_RUN_ID)])

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    assert "Blog\\x1b[2K" in result.output
    assert "note=a\\x1b[1Ab" in result.output


@pytest.mark.parametrize(
    ("metadata", "project"),
    [
        (
            _tagged_metadata(),
            {
                "handle": "google-report-xyz",
                "name": "Google Report XYZ",
                "spend_type": "customer",
                "subcategory": "report",
                "customer": "google",
                "extra_tags": {"engagement": "q4"},
                "created_at": "2026-10-21T18:00:00+00:00",
                "created_by_email": "researcher@vals.ai",
            },
        ),
        (make_fetch_metadata(_RUN_ID), None),
    ],
    ids=["tagged", "untagged"],
)
def test_fetch_json_carries_the_runs_project(
    tracker: MockTracker,
    cli_runner: CliRunner,
    metadata: FetchBenchmarkMetadataResponse,
    project: dict[str, object] | None,
) -> None:
    serve_run(tracker, make_fetch_response(_RUN_ID), metadata=metadata)

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["project"] == project


def test_fetch_jsonl_writes_snapshot_updates_and_the_terminal_record(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    finished = make_fetch_response(_RUN_ID, status=BenchmarkStatus.FINISHED, finished_tasks=4, final_score=75.0)
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=run_stream(finished), metadata=make_fetch_metadata(_RUN_ID))

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--connect", "--format", "jsonl"])

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    assert not any(secret in result.output for secret in _SECRETS)

    records = _records(result.output)
    assert [record["event"] for record in records] == ["snapshot", "update", "complete"]
    assert all(record["schema_version"] == 1 and record["run_id"] == str(_RUN_ID) for record in records)
    assert records[-1]["status"] == "FINISHED"
    assert records[-1]["final_score"] == 75.0


@pytest.mark.parametrize(
    ("stream", "events", "exit_code"),
    [
        (run_stream(make_fetch_response(_RUN_ID, status=BenchmarkStatus.ERROR)), ["update", "error"], 0),
        (run_stream(make_fetch_response(_RUN_ID, status=BenchmarkStatus.STOPPED)), ["update", "stopped"], 0),
        (run_stream(end="error"), ["error"], 0),
        (run_stream(end="disconnect"), ["disconnect"], 0),
        (KeyboardInterrupt(), ["interrupted"], 0),
        (run_stream(end=None), ["disconnect"], 1),
    ],
    ids=["errored-run", "stopped-run", "error-event", "disconnect", "ctrl-c", "body-ends-early"],
)
def test_fetch_jsonl_names_how_the_stream_ended(
    tracker: MockTracker,
    cli_runner: CliRunner,
    stream: httpx.Response | BaseException,
    events: list[str],
    exit_code: int,
) -> None:
    """`complete` maps to the run's final status; a body without an ending event exits 1."""
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=stream, metadata=make_fetch_metadata(_RUN_ID))

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--connect", "--format", "jsonl"])

    assert result.exit_code == exit_code
    assert [record["event"] for record in _records(result.stdout)] == ["snapshot", *events]
    assert ("ended without a terminal event" in result.stderr) is bool(exit_code)


def test_connected_text_shows_identity_then_the_errored_outcome(tracker: MockTracker, cli_runner: CliRunner) -> None:
    """A live run that ends ERROR prints the failure, even though the stream's event is `complete`."""
    error_message = "No tasks were completed successfully.\n- 4/4 tasks: Secret error"
    errored = make_fetch_response(
        _RUN_ID, status=BenchmarkStatus.ERROR, task_breakdown={TaskStatus.ERROR: 4}, error_message=error_message
    )
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=run_stream(errored), metadata=make_fetch_metadata(_RUN_ID))

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--connect"])

    assert result.exit_code == 0, result.output
    assert "Run Details" in result.output
    assert all(value in result.output for value in ("mini_sweagent", "openai/gpt-5", "verified", "runner@vals.ai"))
    assert not any(secret in result.output for secret in _SECRETS)
    assert "✗ Run errored." in result.output
    assert "✓ Run completed!" not in result.output
    assert f"Error: {error_message}" in result.output


def test_connected_fetch_of_a_finished_run_prints_its_status_without_streaming(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    errored = make_fetch_response(_RUN_ID, status=BenchmarkStatus.ERROR, error_message="Dominant task error")
    serve_run(tracker, errored, metadata=make_fetch_metadata(_RUN_ID))

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--connect"])

    assert result.exit_code == 0, result.output
    assert "Run Status" in result.output
    assert "Error: Dominant task error" in result.output
    assert "Streaming run updates" not in result.output
    assert all(request.url.params.get("connect") is None for request in tracker.requests)


@pytest.mark.parametrize(
    ("metadata", "spend_line"),
    [
        (_tagged_metadata(), "customer · google-report-xyz · report · google · engagement=q4"),
        (make_fetch_metadata(_RUN_ID), "untagged"),
    ],
    ids=["tagged", "untagged"],
)
def test_connected_fetch_of_an_active_run_shows_the_runs_project(
    tracker: MockTracker, cli_runner: CliRunner, metadata: FetchBenchmarkMetadataResponse, spend_line: str
) -> None:
    finished = make_fetch_response(_RUN_ID, status=BenchmarkStatus.FINISHED)
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=run_stream(finished), metadata=metadata)

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--connect"])

    assert result.exit_code == 0, result.output
    assert "Run Details" in result.output
    assert "Spend:" in result.output
    assert spend_line in result.output


@pytest.mark.parametrize(
    ("metadata", "spend_line"),
    [
        (_tagged_metadata(), "customer · google-report-xyz · report · google · engagement=q4"),
        (make_fetch_metadata(_RUN_ID), "untagged"),
        (None, "unavailable"),
    ],
    ids=["tagged", "untagged", "metadata-unavailable"],
)
def test_connected_fetch_of_a_finished_run_shows_the_runs_project(
    tracker: MockTracker, cli_runner: CliRunner, metadata: FetchBenchmarkMetadataResponse | None, spend_line: str
) -> None:
    finished = make_fetch_response(_RUN_ID, status=BenchmarkStatus.FINISHED)
    serve_run(tracker, finished, metadata=metadata)

    result = cli_runner.invoke(fetch, [str(_RUN_ID), "--connect"])

    assert result.exit_code == 0, result.output
    assert "Run Status" in result.output
    assert "Spend:" in result.output
    assert spend_line in result.output


@pytest.mark.parametrize(
    ("arguments", "expected_error"),
    [(["--connect", "--format", "json"], "use --format jsonl"), (["--format", "jsonl"], "requires --connect")],
)
def test_fetch_rejects_mismatched_machine_format(
    tracker: MockTracker, cli_runner: CliRunner, arguments: list[str], expected_error: str
) -> None:
    result = cli_runner.invoke(fetch, [str(_RUN_ID), *arguments])

    assert result.exit_code == 2
    assert expected_error in result.output
    assert tracker.requests == []
