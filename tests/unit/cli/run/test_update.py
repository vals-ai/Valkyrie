"""Tests for updating live run concurrency.

Run: uv run pytest tests/unit/cli/run/test_update.py

Covers successful updates, CLI validation, and tracker errors.
"""

from importlib import import_module
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus
from tracker.types import UpdateBenchmarkConcurrencyResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run import run

update_module = import_module("valkyrie.cli.run.update")


class MockUpdateTracker:
    """Record concurrency updates and return deterministic tracker results."""

    def __init__(
        self,
        response: UpdateBenchmarkConcurrencyResponse | TrackerServiceError,
    ) -> None:
        self.response = response
        self.calls: list[tuple[UUID, int]] = []

    def __enter__(self) -> "MockUpdateTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def update_benchmark_concurrency(
        self,
        run_id: UUID,
        concurrency: int,
    ) -> UpdateBenchmarkConcurrencyResponse:
        self.calls.append((run_id, concurrency))
        if isinstance(self.response, TrackerServiceError):
            raise self.response
        return self.response


def test_update_uses_effective_tracker_concurrency(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Print the effective concurrency returned by the tracker."""
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    tracker = MockUpdateTracker(
        UpdateBenchmarkConcurrencyResponse(
            benchmark_id=run_id,
            status=BenchmarkStatus.IN_PROGRESS,
            concurrency=6,
        )
    )
    monkeypatch.setattr(update_module, "TrackerService", lambda: tracker)

    result = cli_runner.invoke(run, ["update", str(run_id), "--concurrency", "9"])

    assert result.exit_code == 0, result.output
    assert tracker.calls == [(run_id, 9)]
    assert result.output == "✓ Run concurrency updated to 6.\n"


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "not-an-integer"])
def test_update_rejects_non_positive_integer(
    cli_runner: CliRunner,
    value: str,
) -> None:
    """Reject invalid concurrency values before contacting the tracker."""
    result = cli_runner.invoke(
        run,
        ["update", "123e4567-e89b-12d3-a456-426614174000", "--concurrency", value],
    )

    assert result.exit_code == 2
    assert "--concurrency" in result.output


def test_update_surfaces_tracker_error(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render tracker rejections as concise Click errors."""
    tracker = MockUpdateTracker(TrackerServiceError("Run is not in progress."))
    monkeypatch.setattr(update_module, "TrackerService", lambda: tracker)

    result = cli_runner.invoke(
        run,
        ["update", "123e4567-e89b-12d3-a456-426614174000", "--concurrency", "4"],
    )

    assert result.exit_code == 1
    assert result.output == "Error: Run is not in progress.\n"
