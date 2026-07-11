import json
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from click.testing import CliRunner, Result
from tracker.database.models import AgentContractRequest, BenchmarkArguments, BenchmarkStatus
from tracker.types import FinalViewResponse, RetrieveResultsResponse, S3UploadResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run import run
from valkyrie.cli.run.errors import build_run_errors_payload, errors, group_task_errors

errors_module = import_module("valkyrie.cli.run.errors")


def make_response(
    run_id: UUID,
    *,
    status: BenchmarkStatus = BenchmarkStatus.ERROR,
    error_message: str | None = "No tasks were completed successfully",
    task_errors: dict[str, str] | None = None,
) -> FinalViewResponse:
    return FinalViewResponse(
        benchmark_id=run_id,
        benchmark_name="demo-bench",
        started_at=datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 10, 12, 5, tzinfo=timezone.utc),
        status=status,
        error_message=error_message,
        benchmark_arguments=BenchmarkArguments(
            contract=AgentContractRequest(
                name="demo-agent",
                secrets={"SYNTHETIC_KEY": "excluded-secret-name"},
                kwargs={"private-option": "excluded-kwarg-value"},
            ),
            concurrency=10,
        ),
        tasks_stopped=None,
        final_evaluation=None,
        average_task_breakdown=None,
        evaluation_results={"successful-task": {"private": "excluded-evaluation-value"}},
        task_errors=task_errors,
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
    tracker = StubErrorsTracker(make_response(run_id, task_errors=task_errors))
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
        make_response(run_id, status=status, error_message=error_message, task_errors=task_errors)
    )

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    assert expected in result.stdout
    assert unexpected not in result.stdout
    assert status.value.replace("_", " ").title() in result.stdout


def test_errors_text_preserves_empty_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubErrorsTracker(make_response(run_id, error_message="", task_errors={"task-a": ""}))

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    assert result.stdout.count("(empty error message)") == 2


def test_errors_text_sanitizes_terminal_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    unsafe_message = "\x1b]8;;https://example.invalid\x07click\x1b]8;;\x07\rrewritten\nnext\tline\u202e"
    tracker = StubErrorsTracker(
        make_response(
            run_id,
            error_message=unsafe_message,
            task_errors={"task\x1b[31m": "boom\b"},
        )
    )

    result = invoke_with_tracker(monkeypatch, tracker, run_id)

    assert result.exit_code == 0, result.output
    for control in ("\x1b", "\x07", "\r", "\b", "\t", "\u202e"):
        assert control not in result.stdout
    assert "\\x1b" in result.stdout
    assert "\\x07" in result.stdout
    assert "\\r" in result.stdout
    assert "\\u202e" in result.stdout
    assert "next    line" in result.stdout


def test_errors_json_is_versioned_allowlisted_and_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    raw_error = "task failed\n\x1b[31mred"
    response = make_response(
        run_id,
        error_message=None,
        task_errors={"task-b": raw_error, "task-a": "another failure"},
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
        for marker in ["excluded-secret-name", "excluded-kwarg-value", "excluded-evaluation-value"]
    )

    fixed_payload = build_run_errors_payload(
        response,
        observed_at=datetime(2026, 7, 10, 20, 15),
    )
    assert fixed_payload["observed_at"] == "2026-07-10T20:15:00Z"


def test_errors_json_normalizes_empty_error_state(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    tracker = StubErrorsTracker(
        make_response(
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


def test_errors_command_is_registered_and_rejects_invalid_uuid(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "errors" in run.commands
    help_result = CliRunner().invoke(errors, ["--help"])
    assert help_result.exit_code == 0
    assert "--format [text|json]" in help_result.stdout

    def unexpected_tracker() -> None:
        pytest.fail("invalid UUID should fail before constructing a tracker")

    monkeypatch.setattr(errors_module, "TrackerService", unexpected_tracker)
    invalid_result = CliRunner().invoke(errors, ["not-a-uuid"])
    assert invalid_result.exit_code == 2
