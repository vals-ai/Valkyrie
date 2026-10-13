"""Tests for shedding the newest tasks from a running run.

Run: uv run pytest tests/unit/cli/run/test_shed.py

Covers the preview-then-shed call order and the tracker rejections that must leave the run alone.
"""

from importlib import import_module
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.types import ShedBenchmarkResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run import run

shed_module = import_module("valkyrie.cli.run.shed")
RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


class MockShedTracker:
    """Record shed calls in order; the tracker owns selection, so every call answers with the same task ids."""

    def __init__(self, task_ids: list[str], error: str | None = None) -> None:
        self.task_ids = task_ids
        self.error = error
        self.calls: list[tuple[int, bool]] = []

    def __enter__(self) -> "MockShedTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def shed_benchmark(self, run_id: UUID, concurrency: int, dry_run: bool) -> ShedBenchmarkResponse:
        assert run_id == RUN_ID
        self.calls.append((concurrency, dry_run))
        if self.error:
            raise TrackerServiceError(self.error)
        return ShedBenchmarkResponse(benchmark_id=run_id, concurrency=concurrency, task_ids=self.task_ids)


def install(monkeypatch: pytest.MonkeyPatch, tracker: MockShedTracker) -> None:
    monkeypatch.setattr(shed_module, "TrackerService", lambda: tracker)


def test_shed_previews_then_sheds_after_confirmation(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dry run shows the plan, the prompt is answered, and one real shed request follows."""
    tracker = MockShedTracker(["late", "b"])
    install(monkeypatch, tracker)

    result = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1"], input="y\n")

    assert result.exit_code == 0, result.output
    assert tracker.calls == [(1, True), (1, False)]
    assert "Would force stop 2 task(s):" in result.output
    assert "Run concurrency updated to 1." in result.output
    assert "Force stopped 2 task(s):" in result.output
    assert result.output.count("late") == 2


def test_shed_reports_when_nothing_is_above_the_limit(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = MockShedTracker([])
    install(monkeypatch, tracker)

    result = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "4"], input="y\n")

    assert result.exit_code == 0, result.output
    assert tracker.calls == [(4, True), (4, False)]
    assert result.output.count("No building or in-progress tasks are above the limit.") == 2


@pytest.mark.parametrize(
    "detail",
    [
        "Run 123e4567-e89b-12d3-a456-426614174000 concurrency is 4; shed can only lower it.",
        "Run 123e4567-e89b-12d3-a456-426614174000 is currently in the STOPPING state.",
    ],
)
def test_shed_surfaces_tracker_rejections_from_the_preview(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    detail: str,
) -> None:
    """The tracker refuses to raise the limit or touch an inactive run; the preview fails before any prompt."""
    tracker = MockShedTracker([], error=detail)
    install(monkeypatch, tracker)

    result = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "4"], input="y\n")

    assert result.exit_code == 1
    assert tracker.calls == [(4, True)]
    assert detail in result.output


def test_shed_dry_run_and_cancel_only_preview(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = MockShedTracker(["b"])
    install(monkeypatch, tracker)

    dry_run = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1", "--dry-run"])
    declined = cli_runner.invoke(run, ["shed", str(RUN_ID), "--concurrency", "1"], input="n\n")

    assert dry_run.exit_code == 0, dry_run.output
    assert "Would force stop 1 task(s):" in dry_run.output
    assert declined.exit_code == 0, declined.output
    assert "Cancelled." in declined.output
    assert tracker.calls == [(1, True), (1, True)]
