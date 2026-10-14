"""Tests for run list output and pagination contracts.

Run: uv run pytest tests/unit/cli/run/test_list.py
"""

import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk.models import BenchmarkStatus, BenchmarkTableRow, FetchBenchmarksResponse

from valkyrie.cli.run.list_runs import list_runs

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import ok


def make_row(index: int) -> BenchmarkTableRow:
    return BenchmarkTableRow(
        id=UUID(int=index + 1),
        name="swebench",
        agent_name="mini_sweagent",
        model="openai/gpt-5",
        dataset="verified",
        label="release-candidate",
        started_by_email="runner@vals.ai",
        started_at=datetime(2026, 7, 9, 12, 30, tzinfo=timezone.utc),
        finished_at=None,
        status=BenchmarkStatus.IN_PROGRESS,
        total_tasks=4,
        finished_tasks=1,
        task_state_counts={"FINISHED": 1, "IN_PROGRESS": 3},
        final_score=None,
        error_message="provider response contained sensitive raw detail",
    )


def invoke_with_pages(
    tracker: MockTracker, pages: dict[str, FetchBenchmarksResponse | Exception], args: list[str]
) -> Result:
    """Serve run-list pages keyed by the request's cursor."""

    def page(request: httpx.Request) -> httpx.Response:
        response = pages[request.url.params.get("cursor", "")]
        if isinstance(response, Exception):
            raise response

        return ok(response)

    tracker.reply("GET", "/fetch-benchmarks", page)
    return CliRunner().invoke(list_runs, args)


def test_list_json_all_exhausts_cursor_pages_and_preserves_filters(tracker: MockTracker) -> None:
    first_page = [make_row(index) for index in range(500)]
    final_row = make_row(500)
    pages: dict[str, FetchBenchmarksResponse | Exception] = {
        "": FetchBenchmarksResponse(benchmarks=first_page, next_cursor="page-2"),
        "page-2": FetchBenchmarksResponse(benchmarks=[final_row], next_cursor=None),
    }

    result = invoke_with_pages(
        tracker,
        pages,
        [
            "--format",
            "json",
            "--all",
            "--agent-name",
            "mini_sweagent",
            "--benchmark-name",
            "swebench",
            "--model",
            "openai/gpt-5",
            "--dataset",
            "verified",
            "--label",
            "release-candidate",
            "--status",
            "IN_PROGRESS",
            "--order-by",
            "ASC",
            "--started-by",
            "runner@vals.ai",
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(result.stdout.splitlines()) == 1
    assert "\x1b" not in result.stdout
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["kind"] == "run_list"
    assert payload["returned_count"] == 501
    assert [run["run_id"] for run in payload["runs"]] == [str(row.id) for row in [*first_page, final_row]]
    assert "error_message" not in payload["runs"][0]
    assert "sensitive raw detail" not in result.stdout

    assert [request.url.params["cursor"] for request in tracker.requests] == ["", "page-2"]
    for request in tracker.requests:
        assert {key: value for key, value in request.url.params.items() if key != "cursor"} == {
            "agent_name": "mini_sweagent",
            "benchmark_name": "swebench",
            "model": "openai/gpt-5",
            "dataset": "verified",
            "label": "release-candidate",
            "status": "IN_PROGRESS",
            "started_by": "runner@vals.ai",
            "order_by": "asc",
            "limit": "500",
            "offset": "0",
        }


@pytest.mark.parametrize("final_score", [None, float("nan"), float("inf"), float("-inf")])
def test_list_json_all_normalizes_non_finite_scores(tracker: MockTracker, final_score: float | None) -> None:
    rows = [] if final_score is None else [make_row(0).model_copy(update={"final_score": final_score})]
    pages: dict[str, FetchBenchmarksResponse | Exception] = {"": FetchBenchmarksResponse(benchmarks=rows)}

    result = invoke_with_pages(tracker, pages, ["--format", "json", "--all"])

    assert result.exit_code == 0, result.output
    assert not any(token in result.stdout for token in ["NaN", "Infinity"])
    payload = json.loads(result.stdout)
    assert payload["returned_count"] == len(rows)
    assert [run["final_score"] for run in payload["runs"]] == [None] * len(rows)


def test_list_json_all_does_not_emit_partial_output_after_later_page_failure(tracker: MockTracker) -> None:
    pages: dict[str, FetchBenchmarksResponse | Exception] = {
        "": FetchBenchmarksResponse(benchmarks=[make_row(0)], next_cursor="page-2"),
        "page-2": httpx.ConnectError("second page failed"),
    }

    result = invoke_with_pages(tracker, pages, ["--format", "json", "--all"])

    assert result.exit_code != 0
    assert result.stdout == ""
    assert "second page failed" in result.stderr


@pytest.mark.parametrize(
    ("args", "expected_error"),
    [
        (["--format", "json"], "requires --all"),
        (["--all"], "requires --format json"),
    ],
)
def test_list_machine_flags_validate_before_any_request(
    tracker: MockTracker, args: list[str], expected_error: str
) -> None:
    result = CliRunner().invoke(list_runs, args)

    assert result.exit_code == 2
    assert expected_error in result.output
    assert tracker.requests == []


@pytest.mark.parametrize("count", [2, 0])
def test_list_text_renders_one_page_or_the_active_filters(tracker: MockTracker, count: int) -> None:
    rows = [make_row(index) for index in range(count)]
    pages: dict[str, FetchBenchmarksResponse | Exception] = {
        "": FetchBenchmarksResponse(benchmarks=rows, total_count=count)
    }

    result = invoke_with_pages(tracker, pages, ["--agent-name", "mini_sweagent"])

    assert result.exit_code == 0, result.output
    (request,) = tracker.requests
    assert (request.url.params["offset"], request.url.params["limit"]) == ("0", "5")
    if count:
        assert all(str(row.id) in result.output for row in rows)
        assert "Total: 2 run(s)" in result.output
    else:
        assert "No runs found matching the specified filters." in result.output
        assert "Agent: mini_sweagent" in result.output
