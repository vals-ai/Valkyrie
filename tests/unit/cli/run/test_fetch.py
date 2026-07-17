"""Tests for run fetch output contracts.

Run: uv run pytest tests/unit/cli/run/test_fetch.py
"""

import json
from collections.abc import Generator
from datetime import datetime, timezone
from importlib import import_module
from uuid import UUID, uuid4

import pytest
from click.testing import CliRunner, Result
from tracker.database.models import BenchmarkStatus
from tracker.types import FetchBenchmarkMetadataResponse, FetchBenchmarkResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.fetch import fetch
from valkyrie.cli.run.snapshot import build_run_snapshot, format_run_snapshot_json

from tests.unit.cli.factories import make_fetch_metadata, make_fetch_response

fetch_module = import_module("valkyrie.cli.run.fetch")


class StubFetchTracker:
    def __init__(
        self,
        response: FetchBenchmarkResponse,
        metadata: FetchBenchmarkMetadataResponse | TrackerServiceError,
        events: list[str] | None = None,
        *,
        interrupt: bool = False,
    ) -> None:
        self.response = response
        self.metadata = metadata
        self.events = events or []
        self.interrupt = interrupt
        self.metadata_calls = 0

    def __enter__(self) -> "StubFetchTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark(self, _run_id: UUID) -> FetchBenchmarkResponse:
        return self.response

    def fetch_benchmark_metadata(self, _run_id: UUID) -> FetchBenchmarkMetadataResponse:
        self.metadata_calls += 1
        if isinstance(self.metadata, TrackerServiceError):
            raise self.metadata
        return self.metadata

    def stream_benchmark(self, _run_id: UUID) -> Generator[str, None, None]:
        if self.interrupt:
            raise KeyboardInterrupt
        yield from self.events


def invoke_with_tracker(
    monkeypatch: pytest.MonkeyPatch,
    tracker: StubFetchTracker,
    args: list[str],
) -> Result:
    monkeypatch.setattr(fetch_module, "TrackerService", lambda: tracker)
    return CliRunner().invoke(fetch, args)


def test_run_snapshot_is_versioned_allowlisted_and_stable() -> None:
    run_id = uuid4()
    response = make_fetch_response(run_id)
    metadata = make_fetch_metadata(run_id)
    observed_at = datetime(2026, 7, 9, 13, 0, tzinfo=timezone.utc)

    snapshot = build_run_snapshot(response, metadata, event="snapshot", observed_at=observed_at)
    serialized = format_run_snapshot_json(response, metadata, event="snapshot")

    assert snapshot["schema_version"] == 1
    assert snapshot["event"] == "snapshot"
    assert snapshot["observed_at"] == "2026-07-09T13:00:00Z"
    assert snapshot["run_id"] == str(run_id)
    assert snapshot["benchmark_name"] == "swebench"
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
    assert "MODEL_API_KEY" not in serialized
    assert "classified-secret-name" not in serialized
    assert "temperature" not in serialized


def test_fetch_json_outputs_one_clean_object(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubFetchTracker(make_fetch_response(run_id), make_fetch_metadata(run_id))

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--format", "json"])

    assert result.exit_code == 0, result.output
    assert len(result.output.splitlines()) == 1
    assert "\x1b" not in result.output
    assert "Run Status" not in result.output
    payload = json.loads(result.output)
    assert payload["event"] == "snapshot"
    assert payload["run_id"] == str(run_id)
    assert payload["agent_name"] == "mini_sweagent"
    assert tracker.metadata_calls == 1


@pytest.mark.parametrize("final_score", [float("nan"), float("inf"), float("-inf")])
def test_fetch_json_normalizes_non_finite_scores(
    monkeypatch: pytest.MonkeyPatch,
    final_score: float,
) -> None:
    run_id = uuid4()
    tracker = StubFetchTracker(make_fetch_response(run_id, final_score=final_score), make_fetch_metadata(run_id))

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["final_score"] is None
    assert not any(token in result.stdout for token in ["NaN", "Infinity"])


def test_fetch_json_uses_null_identity_when_metadata_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubFetchTracker(make_fetch_response(run_id), TrackerServiceError("metadata unavailable"))

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--format", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["metadata_available"] is False
    assert payload["agent_name"] is None
    assert payload["model"] is None
    assert payload["dataset"] is None


def test_fetch_jsonl_outputs_only_snapshot_update_and_terminal_records(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    finished = make_fetch_response(run_id, status=BenchmarkStatus.FINISHED, finished_tasks=4, final_score=75.0)
    tracker = StubFetchTracker(
        make_fetch_response(run_id),
        make_fetch_metadata(run_id),
        events=[f"data: {finished.model_dump_json()}", "event: complete"],
    )

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--connect", "--format", "jsonl"])

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    assert "Streaming run updates" not in result.output
    assert "Next Steps" not in result.output
    assert "MODEL_API_KEY" not in result.output
    records = [json.loads(line) for line in result.output.splitlines()]
    assert [record["event"] for record in records] == ["snapshot", "update", "complete"]
    assert all(record["schema_version"] == 1 for record in records)
    assert all(record["run_id"] == str(run_id) for record in records)
    assert records[-1]["status"] == "FINISHED"
    assert records[-1]["final_score"] == 75.0
    assert tracker.metadata_calls == 1


@pytest.mark.parametrize(
    ("status", "expected_event"),
    [
        (BenchmarkStatus.ERROR, "error"),
        (BenchmarkStatus.STOPPED, "stopped"),
    ],
)
def test_fetch_jsonl_maps_completed_stream_to_terminal_run_status(
    monkeypatch: pytest.MonkeyPatch,
    status: BenchmarkStatus,
    expected_event: str,
) -> None:
    run_id = uuid4()
    terminal = make_fetch_response(run_id, status=status, finished_tasks=4)
    tracker = StubFetchTracker(
        make_fetch_response(run_id),
        make_fetch_metadata(run_id),
        events=[f"data: {terminal.model_dump_json()}", "event: complete"],
    )

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--connect", "--format", "jsonl"])

    assert result.exit_code == 0, result.output
    records = [json.loads(line) for line in result.output.splitlines()]
    assert records[-1]["event"] == expected_event
    assert records[-1]["status"] == status.value


@pytest.mark.parametrize("terminal_event", ["error", "disconnect"])
def test_fetch_jsonl_preserves_other_terminal_events(
    monkeypatch: pytest.MonkeyPatch,
    terminal_event: str,
) -> None:
    run_id = uuid4()
    tracker = StubFetchTracker(
        make_fetch_response(run_id),
        make_fetch_metadata(run_id),
        events=[f"event: {terminal_event}"],
    )

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--connect", "--format", "jsonl"])

    assert result.exit_code == 0, result.output
    records = [json.loads(line) for line in result.output.splitlines()]
    assert [record["event"] for record in records] == ["snapshot", terminal_event]


def test_fetch_jsonl_reports_keyboard_interrupt_as_json(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubFetchTracker(make_fetch_response(run_id), make_fetch_metadata(run_id), interrupt=True)

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--connect", "--format", "jsonl"])

    assert result.exit_code == 0, result.output
    records = [json.loads(line) for line in result.output.splitlines()]
    assert [record["event"] for record in records] == ["snapshot", "interrupted"]


def test_fetch_jsonl_reports_clean_stream_exhaustion_as_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubFetchTracker(make_fetch_response(run_id), make_fetch_metadata(run_id))

    result = invoke_with_tracker(monkeypatch, tracker, [str(run_id), "--connect", "--format", "jsonl"])

    assert result.exit_code != 0
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert [record["event"] for record in records] == ["snapshot", "disconnect"]
    assert "ended without a terminal event" in result.stderr


@pytest.mark.parametrize(
    ("args", "expected_error"),
    [
        (["--connect", "--format", "json"], "use --format jsonl"),
        (["--format", "jsonl"], "requires --connect"),
    ],
)
def test_fetch_rejects_mismatched_machine_format(
    monkeypatch: pytest.MonkeyPatch,
    args: list[str],
    expected_error: str,
) -> None:
    run_id = uuid4()

    def unexpected_tracker():
        raise AssertionError("format validation should happen before constructing the tracker")

    monkeypatch.setattr(fetch_module, "TrackerService", unexpected_tracker)

    result = CliRunner().invoke(fetch, [str(run_id), *args])

    assert result.exit_code == 2
    assert expected_error in result.output
