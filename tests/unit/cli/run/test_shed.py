"""Tests for shedding the newest tasks from a running run.

Run: uv run pytest tests/unit/cli/run/test_shed.py

Covers newest-first selection, the concurrency-then-stop order, and tracker states that must not stop tasks.
"""

from datetime import datetime, timedelta, timezone
from importlib import import_module
from uuid import UUID, uuid4

import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus
from tracker.types import StopBenchmarkResponse, UpdateBenchmarkConcurrencyResponse
from valkyrie.sdk.models import TaskStatus, TaskSummary

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run import run

shed_module = import_module("valkyrie.cli.run.shed")
RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
BASE_TIME = datetime(2026, 9, 30, tzinfo=timezone.utc)


def summary(task_id: str, status: TaskStatus, minute: int) -> TaskSummary:
    return TaskSummary(
        id=uuid4(),
        task_id=task_id,
        status=status,
        started_at=BASE_TIME + timedelta(minutes=minute),
        finished_at=None,
    )


class MockShedTracker:
    """Record tracker calls in order; optionally reject the concurrency update like a run that is not in progress."""

    def __init__(self, update_error: str | None = None) -> None:
        self.update_error = update_error
        self.calls: list[tuple[str, object]] = []

    def __enter__(self) -> "MockShedTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def update_benchmark_concurrency(self, run_id: UUID, concurrency: int) -> UpdateBenchmarkConcurrencyResponse:
        self.calls.append(("update", concurrency))
        if self.update_error:
            raise TrackerServiceError(self.update_error)
        return UpdateBenchmarkConcurrencyResponse(
            benchmark_id=run_id, status=BenchmarkStatus.IN_PROGRESS, concurrency=concurrency
        )

    def stop_benchmark(
        self, benchmark_id: UUID, force: bool, task_ids: list[str] | None = None
    ) -> StopBenchmarkResponse:
        assert benchmark_id == RUN_ID
        self.calls.append(("stop", (force, task_ids)))
        return StopBenchmarkResponse(status="success")


def install(
    monkeypatch: pytest.MonkeyPatch,
    tracker: MockShedTracker,
    snapshots: list[list[TaskSummary]],
) -> None:
    """Serve one active-task snapshot per listing, recording each listing in the tracker call log."""
    remaining = iter(snapshots)

    async def active_tasks(run_id: UUID) -> list[TaskSummary]:
        assert run_id == RUN_ID
        tracker.calls.append(("list", None))
        return next(remaining)

    monkeypatch.setattr(shed_module, "_active_tasks", active_tasks)
    monkeypatch.setattr(shed_module, "TrackerService", lambda: tracker)


def test_newest_over_limit_picks_latest_stoppable_tasks() -> None:
    """Stop the newest building or in-progress tasks, never evaluating ones.

    Test cases:
    - Five active tasks at limit two stop the three newest stoppable tasks.
    - An evaluating task still counts toward the limit but is never selected.
    - A run already at or below the limit stops nothing.
    """
    active = [
        summary("oldest", TaskStatus.IN_PROGRESS, 0),
        summary("evaluating-newest", TaskStatus.EVALUATING, 9),
        summary("middle", TaskStatus.IN_PROGRESS, 3),
        summary("newer", TaskStatus.IN_PROGRESS, 5),
        summary("building", TaskStatus.BUILDING, 7),
    ]

    assert [task.task_id for task in shed_module._newest_over_limit(active, 2)] == ["building", "newer", "middle"]
    assert [task.task_id for task in shed_module._newest_over_limit(active, 1)] == [
        "building",
        "newer",
        "middle",
        "oldest",
    ]
    assert shed_module._newest_over_limit(active, 5) == []


def test_shed_lowers_concurrency_before_stopping_relisted_tasks(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lower the limit first, then force stop the newest tasks from a fresh listing.

    A task admitted between the preview and the concurrency update is the newest and must be the one stopped.
    """
    before = [summary("a", TaskStatus.IN_PROGRESS, 0), summary("b", TaskStatus.IN_PROGRESS, 1)]
    after = [*before, summary("late", TaskStatus.BUILDING, 2)]
    tracker = MockShedTracker()
    install(monkeypatch, tracker, [before, after])

    result = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1"], input="y\n")

    assert result.exit_code == 0, result.output
    assert tracker.calls == [("list", None), ("update", 1), ("list", None), ("stop", (True, ["late", "b"]))]
    assert "Force stopped:" in result.output


def test_shed_never_sends_an_empty_stop(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty task list would stop the whole run, so no stop request is sent when nothing is above the limit."""
    tracker = MockShedTracker()
    install(monkeypatch, tracker, [[summary("a", TaskStatus.IN_PROGRESS, 0)]] * 2)

    result = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "4"], input="y\n")

    assert result.exit_code == 0, result.output
    assert tracker.calls == [("list", None), ("update", 4), ("list", None)]
    assert "No building or in-progress tasks are above the limit." in result.output


def test_shed_does_not_stop_when_concurrency_update_is_rejected(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracker rejects the update for a run that is not in progress, so the limit is unchanged and nothing is stopped."""
    tracker = MockShedTracker(update_error="Run is currently in the STOPPING state.")
    install(monkeypatch, tracker, [[summary("a", TaskStatus.IN_PROGRESS, 0), summary("b", TaskStatus.IN_PROGRESS, 1)]])

    result = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1"], input="y\n")

    assert result.exit_code == 1
    assert tracker.calls == [("list", None), ("update", 1)]
    assert "STOPPING" in result.output


def test_shed_dry_run_and_cancel_change_nothing(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry runs and declined prompts only list tasks."""
    active = [summary("a", TaskStatus.IN_PROGRESS, 0), summary("b", TaskStatus.IN_PROGRESS, 1)]
    tracker = MockShedTracker()
    install(monkeypatch, tracker, [active, active])

    dry_run = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1", "--dry-run"])
    declined = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1"], input="n\n")

    assert dry_run.exit_code == 0, dry_run.output
    assert "Would force stop:" in dry_run.output
    assert "b" in dry_run.output
    assert declined.exit_code == 0, declined.output
    assert "Cancelled." in declined.output
    assert tracker.calls == [("list", None), ("list", None)]
