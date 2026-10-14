"""Tests for batched run status output.

Run: uv run pytest tests/unit/cli/run/test_status.py
"""

import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk.models import BenchmarkStatus, BenchmarkStatusEntry, BenchmarkStatusResponse

from valkyrie.cli.run.status import status_runs

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import ok


def make_entry(index: int, *, status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS) -> BenchmarkStatusEntry:
    return BenchmarkStatusEntry(
        id=UUID(int=index + 1),
        status=status,
        finished_at=datetime(2026, 7, 9, 13, 0, tzinfo=timezone.utc) if status == BenchmarkStatus.FINISHED else None,
        total_tasks=4,
        finished_tasks=1,
        task_state_counts={"FINISHED": 1, "IN_PROGRESS": 3},
    )


def invoke_with_entries(tracker: MockTracker, entries: list[BenchmarkStatusEntry], args: list[str]) -> Result:
    """Answer status requests with the known entries among the requested IDs."""

    def statuses(request: httpx.Request) -> httpx.Response:
        requested = set(request.url.params["ids"].split(","))
        return ok(BenchmarkStatusResponse(entries=[entry for entry in entries if str(entry.id) in requested]))

    tracker.reply("GET", "/benchmarks/status", statuses)
    return CliRunner().invoke(status_runs, args)


def _requested_ids(tracker: MockTracker) -> list[list[str]]:
    return [request.url.params["ids"].split(",") for request in tracker.requests]


def test_status_json_deduplicates_and_restores_requested_order(tracker: MockTracker) -> None:
    first = make_entry(0)
    second = make_entry(1, status=BenchmarkStatus.FINISHED)

    result = invoke_with_entries(
        tracker, [second, first], ["--ids", f" {first.id}, {second.id}, {first.id} ", "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    assert len(result.stdout.splitlines()) == 1
    assert "\x1b" not in result.stdout
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["kind"] == "run_status"
    assert payload["requested_count"] == 2
    assert payload["returned_count"] == 2
    assert payload["missing_run_ids"] == []
    assert [entry["run_id"] for entry in payload["runs"]] == [str(first.id), str(second.id)]
    assert payload["runs"][0]["progress_percent"] == 25.0
    assert payload["runs"][1]["finished_at"] == "2026-07-09T13:00:00Z"
    assert _requested_ids(tracker) == [[str(first.id), str(second.id)]]


def test_status_json_reports_missing_ids_and_exits_nonzero(tracker: MockTracker) -> None:
    found = make_entry(0)
    missing = UUID(int=2)

    result = invoke_with_entries(tracker, [found], ["--ids", f"{found.id},{missing}", "--format", "json"])

    assert result.exit_code != 0
    payload = json.loads(result.stdout)
    assert payload["returned_count"] == 1
    assert payload["missing_run_ids"] == [str(missing)]
    assert "not found or are not accessible" in result.stderr


def test_status_json_chunks_large_id_sets(tracker: MockTracker) -> None:
    entries = [make_entry(index) for index in range(51)]

    result = invoke_with_entries(
        tracker, entries, ["--ids", ",".join(str(entry.id) for entry in entries), "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    assert [len(ids) for ids in _requested_ids(tracker)] == [50, 1]
    assert json.loads(result.stdout)["returned_count"] == 51


@pytest.mark.parametrize("ids_value", ["", " , ", "not-a-uuid"])
def test_status_rejects_invalid_ids_before_any_request(tracker: MockTracker, ids_value: str) -> None:
    result = CliRunner().invoke(status_runs, ["--ids", ids_value, "--format", "json"])

    assert result.exit_code == 2
    assert "--ids" in result.output
    assert tracker.requests == []
