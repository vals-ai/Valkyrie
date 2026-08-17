"""Tests for run error output contracts.

Run: uv run pytest tests/unit/cli/run/test_errors.py
"""

from datetime import datetime
import json
from importlib import import_module
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from click.testing import CliRunner, Result
from tracker.database.models import (
    BenchmarkStatus,
    FailureCategory,
    FailureClassificationState,
    FailureTerminalEffect,
)
from tracker.types import FailureDetail, FailureSummary, RetrieveResultsResponse, S3UploadResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.errors import build_run_errors_payload, errors, group_task_errors

from tests.unit.cli.factories import make_final_view

errors_module = import_module("valkyrie.cli.run.errors")


def make_failure_summary(
    run_id: UUID,
    *,
    message: str,
    task_row_id: UUID | None = None,
    attempt_id: UUID | None = None,
    category: FailureCategory = FailureCategory.VALKYRIE,
    producer: str | None = "tracker",
    operation: str | None = "process_run",
    classification_state: FailureClassificationState = FailureClassificationState.UNCLASSIFIED,
    cause_code: str | None = None,
    terminal_effect: FailureTerminalEffect = FailureTerminalEffect.TERMINAL,
    retry_sequence: int | None = None,
) -> FailureSummary:
    return FailureSummary(
        id=uuid4(),
        schema_version=1,
        category=category,
        benchmark_id=run_id,
        task_row_id=task_row_id,
        task_attempt_id=attempt_id,
        retry_sequence=retry_sequence,
        occurred_at=datetime(2026, 7, 10, 12, 4),
        producer=producer,
        operation=operation,
        error_type="SyntheticFailure",
        message=message,
        classification_state=classification_state,
        cause_code=cause_code,
        terminal_effect=terminal_effect,
    )


class StubErrorsTracker:
    def __init__(self, response: RetrieveResultsResponse | TrackerServiceError) -> None:
        self.response = response
        self.calls: list[tuple[UUID, bool, list[str] | None]] = []

    def __enter__(self) -> "StubErrorsTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def retrieve_results(
        self,
        run_id: UUID,
        s3: bool,
        task_ids: list[str] | None = None,
    ) -> RetrieveResultsResponse:
        self.calls.append((run_id, s3, task_ids))
        if isinstance(self.response, TrackerServiceError):
            raise self.response
        return self.response


def invoke_with_tracker(
    monkeypatch: pytest.MonkeyPatch,
    tracker: StubErrorsTracker,
    run_id: UUID,
    *args: str,
) -> Result:
    monkeypatch.setattr(errors_module, "TrackerService", lambda: tracker)
    return CliRunner().invoke(errors, [str(run_id), *args])


def test_errors_text_groups_identical_messages_without_writing_files(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    shared_message = "Required output artifact was not produced."
    task_errors = {f"task-{index:03}": shared_message for index in range(100)}
    task_errors["other-task"] = "Evaluator returned an invalid response."
    tracker = StubErrorsTracker(make_final_view(run_id, task_errors=task_errors))
    runner = CliRunner()
    monkeypatch.setattr(errors_module, "TrackerService", lambda: tracker)

    with runner.isolated_filesystem():
        result = runner.invoke(errors, [str(run_id)])
        assert list(Path.cwd().iterdir()) == []

    assert result.exit_code == 0, result.output
    assert "Run Errors" in result.stdout
    assert "Stored run error" in result.stdout
    assert "Task errors (101 tasks, 2 distinct messages)" in result.stdout
    assert "[100 tasks]" in result.stdout
    assert result.stdout.count(shared_message) == 1
    assert "task-000" in result.stdout
    assert "task-004" in result.stdout
    assert "task-005" not in result.stdout
    assert "(+95 more)" in result.stdout
    assert tracker.calls == [(run_id, False, None)]


@pytest.mark.parametrize(
    ("error_message", "task_errors", "status", "expected", "unexpected"),
    [
        ("Run failed before task execution.", None, BenchmarkStatus.ERROR, "Stored run error", "Task errors ("),
        ("Previous attempt failed.", None, BenchmarkStatus.IN_PROGRESS, "Stored run error", "Task errors ("),
        (None, {"task-a": "Task failed."}, BenchmarkStatus.FINISHED, "Task errors (1 task", "Stored run error"),
        (None, None, BenchmarkStatus.ERROR, "No current error messages recorded.", "Stored run error"),
    ],
)
def test_errors_text_handles_run_task_and_empty_states(
    monkeypatch: pytest.MonkeyPatch,
    error_message: str | None,
    task_errors: dict[str, str] | None,
    status: BenchmarkStatus,
    expected: str,
    unexpected: str,
) -> None:
    run_id = uuid4()
    tracker = StubErrorsTracker(
        make_final_view(run_id, status=status, error_message=error_message, task_errors=task_errors)
    )

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    assert expected in result.stdout
    assert unexpected not in result.stdout
    assert status.value.replace("_", " ").title() in result.stdout


def test_errors_text_preserves_empty_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubErrorsTracker(make_final_view(run_id, error_message="", task_errors={"task-a": ""}))

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    assert result.stdout.count("(empty error message)") == 2


def test_errors_text_sanitizes_terminal_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    unsafe_message = "\x1b]8;;https://example.invalid\x07click\x1b]8;;\x07\rrewritten\nnext\tline\u202e"
    run_failure = make_failure_summary(
        run_id,
        message=unsafe_message,
        category=FailureCategory.HARNESS,
        producer="benchmark_service\x1b[31m",
        operation="websocket\x07",
        classification_state=FailureClassificationState.CLASSIFIED,
        cause_code="cause\r",
    )
    response = make_final_view(
        run_id,
        error_message=unsafe_message,
        task_errors={"task\x1b[31m": "boom\b"},
    ).model_copy(update={"run_failure": run_failure})
    tracker = StubErrorsTracker(response)

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    for control in ("\x1b", "\x07", "\r", "\b", "\t", "\u202e"):
        assert control not in result.stdout
    assert "Harness / Benchmark Service\\x1b[31m / WebSocket\\x07" in result.stdout
    assert "Cause: cause\\r" in result.stdout
    assert "\\x1b" in result.stdout
    assert "\\x07" in result.stdout
    assert "\\r" in result.stdout
    assert "\\u202e" in result.stdout
    assert "next    line" in result.stdout


def test_errors_json_is_versioned_allowlisted_and_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    raw_error = "task failed\n\x1b[31mred"
    run_failure = make_failure_summary(run_id, message="structured run failure")
    response = make_final_view(
        run_id,
        error_message=None,
        task_errors={"task-b": raw_error, "task-a": "another failure"},
    ).model_copy(
        update={
            "run_failure": run_failure,
            "task_failures": {"task-b": run_failure},
            "recovered_failure_count": 3,
            "secondary_failure_count": 2,
        }
    )
    tracker = StubErrorsTracker(response)

    result = invoke_with_tracker(monkeypatch, tracker, run_id, "--format", "json")

    assert result.exit_code == 0, result.output
    assert len(result.stdout.splitlines()) == 1
    assert "\x1b" not in result.stdout
    payload = json.loads(result.stdout)
    assert set(payload) == {
        "schema_version",
        "kind",
        "observed_at",
        "run_id",
        "benchmark_name",
        "status",
        "error_message",
        "task_error_count",
        "task_errors",
    }
    assert payload["schema_version"] == 1
    assert payload["kind"] == "run_errors"
    assert payload["observed_at"].endswith("Z")
    assert payload["run_id"] == str(run_id)
    assert payload["benchmark_name"] == "demo-bench"
    assert payload["status"] == "ERROR"
    assert payload["error_message"] is None
    assert payload["task_error_count"] == 2
    assert list(payload["task_errors"]) == ["task-a", "task-b"]
    assert payload["task_errors"]["task-b"] == raw_error
    assert not any(
        marker in result.stdout
        for marker in [
            "run_failure",
            "task_failures",
            "recovered_failure_count",
            "secondary_failure_count",
            "structured run failure",
            "excluded-secret-name",
            "excluded-kwarg-value",
            "excluded-evaluation-value",
        ]
    )

    fixed_payload = build_run_errors_payload(
        response,
        observed_at=datetime(2026, 7, 10, 20, 15),
    )
    assert fixed_payload["observed_at"] == "2026-07-10T20:15:00Z"


def test_errors_json_v2_is_structured_allowlisted_and_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    run_failure = FailureDetail(
        **make_failure_summary(
            run_id,
            message="Run stream closed.",
            category=FailureCategory.HARNESS,
            producer="benchmark_service",
            operation="stream",
            classification_state=FailureClassificationState.CLASSIFIED,
            cause_code="websocket_closed",
        ).model_dump(),
        safe_details={"status_code": 503},
    )
    task_failure = make_failure_summary(
        run_id,
        message="Task retry exhausted.",
        task_row_id=uuid4(),
        attempt_id=uuid4(),
        producer="tracker",
        operation="process_task",
        retry_sequence=1,
    )
    response = make_final_view(
        run_id,
        error_message=run_failure.message,
        task_errors={"task-b": task_failure.message, "task-a": "Another task failed."},
    ).model_copy(
        update={
            "run_failure": run_failure,
            "task_failures": {"task-b": task_failure, "task-a": task_failure},
            "recovered_failure_count": 4,
            "secondary_failure_count": 1,
        }
    )
    tracker = StubErrorsTracker(response)

    result = invoke_with_tracker(
        monkeypatch,
        tracker,
        run_id,
        "--format",
        "json",
        "--schema-version",
        "2",
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert set(payload) == {
        "schema_version",
        "kind",
        "observed_at",
        "run_id",
        "benchmark_name",
        "status",
        "error_message",
        "task_error_count",
        "task_errors",
        "run_failure",
        "task_failures",
        "recovered_failure_count",
        "secondary_failure_count",
    }
    assert payload["schema_version"] == 2
    assert payload["recovered_failure_count"] == 4
    assert payload["secondary_failure_count"] == 1
    assert list(payload["task_failures"]) == ["task-a", "task-b"]
    assert set(payload["run_failure"]) == {
        "id",
        "schema_version",
        "category",
        "benchmark_id",
        "task_row_id",
        "task_attempt_id",
        "retry_sequence",
        "occurred_at",
        "producer",
        "operation",
        "error_type",
        "message",
        "classification_state",
        "cause_code",
        "terminal_effect",
    }
    assert payload["run_failure"]["category"] == "harness"
    assert payload["run_failure"]["cause_code"] == "websocket_closed"
    assert payload["task_failures"]["task-b"]["task_attempt_id"] == str(task_failure.task_attempt_id)
    assert "safe_details" not in result.stdout


def test_errors_json_normalizes_empty_error_state(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubErrorsTracker(
        make_final_view(
            run_id,
            status=BenchmarkStatus.FINISHED,
            error_message=None,
            task_errors=None,
        )
    )

    result = invoke_with_tracker(monkeypatch, tracker, run_id, "--format", "json")

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "FINISHED"
    assert payload["error_message"] is None
    assert payload["task_error_count"] == 0
    assert payload["task_errors"] == {}

    v2_result = invoke_with_tracker(
        monkeypatch,
        tracker,
        run_id,
        "--format",
        "json",
        "--schema-version",
        "2",
    )
    assert v2_result.exit_code == 0, v2_result.output
    v2_payload = json.loads(v2_result.stdout)
    assert v2_payload["run_failure"] is None
    assert v2_payload["task_failures"] == {}
    assert v2_payload["recovered_failure_count"] == 0
    assert v2_payload["secondary_failure_count"] == 0


def test_errors_text_renders_structured_provenance_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    run_failure = make_failure_summary(
        run_id,
        message="Benchmark service disconnected",
        category=FailureCategory.HARNESS,
        producer="benchmark_service",
        operation="websocket",
        classification_state=FailureClassificationState.CLASSIFIED,
        cause_code="websocket_connection_closed",
    )
    platform_failure = make_failure_summary(
        run_id,
        message="Executor host failed",
        task_row_id=uuid4(),
        attempt_id=uuid4(),
        producer="executor_host",
        operation="run_executor_dispatch",
        retry_sequence=2,
    )
    unknown_failure = make_failure_summary(
        run_id,
        message="Sandbox setup failed",
        task_row_id=uuid4(),
        category=FailureCategory.UNKNOWN,
        producer="sandbox_provider",
        operation="setup",
        classification_state=FailureClassificationState.DETAILS_UNAVAILABLE,
    )
    legacy_failure = make_failure_summary(
        run_id,
        message="Legacy failure",
        task_row_id=uuid4(),
        category=FailureCategory.UNKNOWN,
        producer=None,
        operation=None,
        classification_state=FailureClassificationState.LEGACY_UNCLASSIFIED,
    )
    task_failures = {
        "task-legacy": legacy_failure,
        "task-platform": platform_failure,
        "task-unknown": unknown_failure,
    }
    response = make_final_view(
        run_id,
        error_message=run_failure.message,
        task_errors={task_id: failure.message for task_id, failure in task_failures.items()},
    ).model_copy(
        update={
            "run_failure": run_failure,
            "task_failures": task_failures,
            "recovered_failure_count": 3,
            "secondary_failure_count": 1,
        }
    )
    tracker = StubErrorsTracker(response)

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    assert "Task failure provenance" in result.stdout
    assert "Historical non-terminal failures" in result.stdout
    assert "Harness / Benchmark Service / WebSocket" in result.stdout
    assert "Cause: websocket_connection_closed" in result.stdout
    assert (
        "task-platform:\n"
        "  Platform / Executor Host / Run executor dispatch\n"
        "task-unknown:"
    ) in result.stdout
    assert "Unknown / Sandbox Provider / Setup" in result.stdout
    assert "Details unavailable" in result.stdout
    assert (
        "task-legacy:\n"
        "  Unknown / Unknown component / Unknown operation\n"
        "task-platform:"
    ) in result.stdout
    for message in (
        "Benchmark service disconnected",
        "Executor host failed",
        "Sandbox setup failed",
        "Legacy failure",
    ):
        assert message in result.stdout
    for technical_field in (
        "category=",
        "classification=",
        "effect=",
        "producer=",
        "operation=",
        "type=",
        "attempt=",
        "retry=",
    ):
        assert technical_field not in result.stdout
    assert "Recovered: 3" in result.stdout
    assert "Secondary: 1" in result.stdout


def test_errors_schema_v2_requires_json_without_tracker_access(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_tracker() -> None:
        pytest.fail("invalid option combination should fail before constructing a tracker")

    monkeypatch.setattr(errors_module, "TrackerService", unexpected_tracker)

    result = CliRunner().invoke(errors, [str(uuid4()), "--schema-version", "2"])

    assert result.exit_code == 2
    assert "--schema-version 2 requires --format json" in result.stderr


def test_group_task_errors_uses_raw_messages_and_stable_order() -> None:
    groups = group_task_errors(
        {
            "task-c": "same",
            "task-b": "different\x1b",
            "task-a": "same",
            "task-d": "different\\x1b",
        }
    )

    assert groups == [
        ("same", ("task-a", "task-c")),
        ("different\x1b", ("task-b",)),
        ("different\\x1b", ("task-d",)),
    ]


def test_errors_tracker_failure_has_no_partial_stdout(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    unsafe_error = "tracker unavailable\x1b]8;;https://example.invalid\x07link\x1b]8;;\x07\rrewritten\u202e"
    tracker = StubErrorsTracker(TrackerServiceError(unsafe_error))
    monkeypatch.setattr(errors_module, "TrackerService", lambda: tracker)

    result = CliRunner().invoke(errors, [str(run_id), "--format", "json"], color=True)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "tracker unavailable" in result.stderr
    assert "\x1b]8;" not in result.stderr
    assert "\x07" not in result.stderr
    assert "\r" not in result.stderr
    assert "\u202e" not in result.stderr
    assert "\\x1b" in result.stderr
    assert "\\x07" in result.stderr
    assert "\\r" in result.stderr
    assert "\\u202e" in result.stderr


def test_errors_rejects_unexpected_s3_response(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubErrorsTracker(
        S3UploadResultsResponse(
            s3_url="s3://example/results.json",
            presigned_url="https://example.invalid/download",
            console_url="https://example.invalid/console",
        )
    )

    result = invoke_with_tracker(monkeypatch, tracker, run_id, "--format", "json")

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "unexpected response" in result.stderr


def test_errors_rejects_invalid_uuid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid run IDs must fail before tracker construction.

    Test cases:
    - A non-UUID argument exits with Click's usage error without creating a tracker client.
    """

    def unexpected_tracker() -> None:
        pytest.fail("invalid UUID should fail before constructing a tracker")

    monkeypatch.setattr(errors_module, "TrackerService", unexpected_tracker)
    invalid_result = CliRunner().invoke(errors, ["not-a-uuid"])
    assert invalid_result.exit_code == 2
