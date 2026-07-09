import json
from datetime import datetime, timezone
from importlib import import_module
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus
from tracker.types import BenchmarkStatusEntry, BenchmarkStatusResponse

from valkyrie.cli.run import run
from valkyrie.cli.run.status import status_runs
from valkyrie.cli.tracker_client import TrackerService

status_module = import_module("valkyrie.cli.run.status")


def make_entry(index: int, *, status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS) -> BenchmarkStatusEntry:
    return BenchmarkStatusEntry(
        id=UUID(int=index + 1),
        status=status,
        finished_at=datetime(2026, 7, 9, 13, 0, tzinfo=timezone.utc) if status == BenchmarkStatus.FINISHED else None,
        total_tasks=4,
        finished_tasks=1,
        task_state_counts={"FINISHED": 1, "IN_PROGRESS": 3},
    )


class StubStatusTracker:
    def __init__(self, entries: list[BenchmarkStatusEntry]) -> None:
        self.entries = entries
        self.calls: list[list[UUID]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark_statuses(self, run_ids: list[UUID]) -> BenchmarkStatusResponse:
        self.calls.append(run_ids)
        requested = set(run_ids)
        return BenchmarkStatusResponse(entries=[entry for entry in self.entries if entry.id in requested])


def invoke_with_tracker(monkeypatch: pytest.MonkeyPatch, tracker: StubStatusTracker, args: list[str]):
    monkeypatch.setattr(status_module, "TrackerService", lambda: tracker)
    return CliRunner().invoke(status_runs, args)


def test_status_json_deduplicates_and_restores_requested_order(monkeypatch: pytest.MonkeyPatch) -> None:
    first = make_entry(0)
    second = make_entry(1, status=BenchmarkStatus.FINISHED)
    tracker = StubStatusTracker([second, first])

    result = invoke_with_tracker(
        monkeypatch,
        tracker,
        ["--ids", f" {first.id}, {second.id}, {first.id} ", "--format", "json"],
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
    assert tracker.calls == [[first.id, second.id]]


def test_status_json_reports_missing_ids_and_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    found = make_entry(0)
    missing = UUID(int=2)
    tracker = StubStatusTracker([found])

    result = invoke_with_tracker(
        monkeypatch,
        tracker,
        ["--ids", f"{found.id},{missing}", "--format", "json"],
    )

    assert result.exit_code != 0
    payload = json.loads(result.stdout)
    assert payload["returned_count"] == 1
    assert payload["missing_run_ids"] == [str(missing)]
    assert "not found or are not accessible" in result.stderr


def test_status_json_chunks_large_id_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    entries = [make_entry(index) for index in range(51)]
    tracker = StubStatusTracker(entries)

    result = invoke_with_tracker(
        monkeypatch,
        tracker,
        ["--ids", ",".join(str(entry.id) for entry in entries), "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    assert [len(call) for call in tracker.calls] == [50, 1]
    assert json.loads(result.stdout)["returned_count"] == 51


@pytest.mark.parametrize("ids_value", ["", " , ", "not-a-uuid"])
def test_status_rejects_invalid_ids_before_tracker_construction(
    monkeypatch: pytest.MonkeyPatch,
    ids_value: str,
) -> None:
    def unexpected_tracker():
        raise AssertionError("ID validation should happen before constructing the tracker")

    monkeypatch.setattr(status_module, "TrackerService", unexpected_tracker)

    result = CliRunner().invoke(status_runs, ["--ids", ids_value, "--format", "json"])

    assert result.exit_code == 2
    assert "--ids" in result.output


def test_tracker_client_fetches_batch_status_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = UUID(int=1)

    class FakeStatusClient:
        def __init__(self) -> None:
            self.url: str | None = None
            self.params: dict[str, str] | None = None

        def get(self, url: str, *, params: dict[str, str]) -> httpx.Response:
            self.url = url
            self.params = params
            return httpx.Response(
                200,
                json={
                    "entries": [
                        {
                            "id": str(run_id),
                            "status": "IN_PROGRESS",
                            "finished_at": None,
                            "total_tasks": 1,
                            "finished_tasks": 0,
                            "task_state_counts": {"PENDING": 1},
                        }
                    ]
                },
            )

        def close(self) -> None:
            return None

    client = FakeStatusClient()
    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(lambda: {}))
    monkeypatch.setattr(TrackerService, "parse_config_keys", lambda _self: {})
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", lambda **_kwargs: client)

    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_benchmark_statuses([run_id])

    assert client.url == "http://tracker/benchmarks/status"
    assert client.params == {"ids": str(run_id)}
    assert [entry.id for entry in response.entries] == [run_id]


def test_status_command_is_registered() -> None:
    assert "status" in run.commands
