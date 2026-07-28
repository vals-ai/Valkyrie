"""Tests for run list output and pagination contracts.

Run: uv run pytest tests/unit/cli/run/test_list.py
"""

import json
from datetime import datetime, timezone
from importlib import import_module
from typing import Never
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus
from tracker.types import BenchmarkTableRow, FetchBenchmarksRequest, FetchBenchmarksResponse, Order

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.list_runs import list_runs

list_runs_module = import_module("valkyrie.cli.run.list_runs")


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


class StubListTracker:
    def __init__(self, pages: dict[str, FetchBenchmarksResponse | TrackerServiceError]) -> None:
        self.pages = pages
        self.requests: list[FetchBenchmarksRequest] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmarks(self, request: FetchBenchmarksRequest) -> FetchBenchmarksResponse:
        self.requests.append(request)
        page = self.pages[request.cursor or ""]
        if isinstance(page, TrackerServiceError):
            raise page
        return page


def invoke_with_tracker(monkeypatch: pytest.MonkeyPatch, tracker: StubListTracker, args: list[str]):
    monkeypatch.setattr(list_runs_module, "TrackerService", lambda: tracker)
    return CliRunner().invoke(list_runs, args)


def test_list_json_all_exhausts_cursor_pages_and_preserves_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    first_page = [make_row(index) for index in range(500)]
    final_row = make_row(500)
    tracker = StubListTracker(
        {
            "": FetchBenchmarksResponse(benchmarks=first_page, next_cursor="page-2"),
            "page-2": FetchBenchmarksResponse(benchmarks=[final_row], next_cursor=None),
        }
    )

    def fail_interactive_pager(*_args: object, **_kwargs: object) -> Never:
        pytest.fail("machine output must not invoke the interactive pager")

    monkeypatch.setattr(list_runs_module, "paginate_benchmarks", fail_interactive_pager)

    result = invoke_with_tracker(
        monkeypatch,
        tracker,
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

    assert [request.cursor for request in tracker.requests] == ["", "page-2"]
    assert all(request.limit == 500 for request in tracker.requests)
    assert all(request.agent_name == ["mini_sweagent"] for request in tracker.requests)
    assert all(request.benchmark_name == ["swebench"] for request in tracker.requests)
    assert all(request.model == "openai/gpt-5" for request in tracker.requests)
    assert all(request.dataset == "verified" for request in tracker.requests)
    assert all(request.label == "release-candidate" for request in tracker.requests)
    assert all(request.status == [BenchmarkStatus.IN_PROGRESS] for request in tracker.requests)
    assert all(request.order_by == Order.ASC for request in tracker.requests)
    assert all(request.started_by == ["runner@vals.ai"] for request in tracker.requests)


def test_list_json_all_emits_empty_document(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = StubListTracker({"": FetchBenchmarksResponse(benchmarks=[], next_cursor=None)})

    result = invoke_with_tracker(monkeypatch, tracker, ["--format", "json", "--all"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["returned_count"] == 0
    assert payload["runs"] == []


@pytest.mark.parametrize("final_score", [float("nan"), float("inf"), float("-inf")])
def test_list_json_all_normalizes_non_finite_scores(
    monkeypatch: pytest.MonkeyPatch,
    final_score: float,
) -> None:
    row = make_row(0).model_copy(update={"final_score": final_score})
    tracker = StubListTracker({"": FetchBenchmarksResponse(benchmarks=[row], next_cursor=None)})

    result = invoke_with_tracker(monkeypatch, tracker, ["--format", "json", "--all"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["runs"][0]["final_score"] is None
    assert not any(token in result.stdout for token in ["NaN", "Infinity"])


def test_list_json_all_does_not_emit_partial_output_after_later_page_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = StubListTracker(
        {
            "": FetchBenchmarksResponse(benchmarks=[make_row(0)], next_cursor="page-2"),
            "page-2": TrackerServiceError("second page failed"),
        }
    )

    result = invoke_with_tracker(monkeypatch, tracker, ["--format", "json", "--all"])

    assert result.exit_code != 0
    assert result.stdout == ""
    assert "second page failed" in result.stderr


def test_list_json_all_rejects_repeated_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = StubListTracker(
        {
            "": FetchBenchmarksResponse(benchmarks=[make_row(0)], next_cursor=""),
        }
    )

    result = invoke_with_tracker(monkeypatch, tracker, ["--format", "json", "--all"])

    assert result.exit_code != 0
    assert result.stdout == ""
    assert "repeated run-list cursor" in result.stderr


@pytest.mark.parametrize(
    ("args", "expected_error"),
    [
        (["--format", "json"], "requires --all"),
        (["--all"], "requires --format json"),
    ],
)
def test_list_machine_flags_validate_before_tracker_construction(
    monkeypatch: pytest.MonkeyPatch,
    args: list[str],
    expected_error: str,
) -> None:
    def unexpected_tracker():
        raise AssertionError("flag validation should happen before constructing the tracker")

    monkeypatch.setattr(list_runs_module, "TrackerService", unexpected_tracker)

    result = CliRunner().invoke(list_runs, args)

    assert result.exit_code == 2
    assert expected_error in result.output
