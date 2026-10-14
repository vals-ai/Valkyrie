"""Tests for updating live run concurrency.

Run: uv run pytest tests/unit/cli/run/test_update.py

Covers successful updates and CLI validation.
"""

import json
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.update import update

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


def test_update_prints_the_concurrency_tracker_applied(cli_runner: CliRunner, tracker: MockTracker) -> None:
    effective = {"benchmark_id": str(_RUN_ID), "status": "IN_PROGRESS", "concurrency": 6}
    tracker.reply("PATCH", f"/benchmarks/{_RUN_ID}/concurrency", httpx.Response(200, json=effective))

    result = cli_runner.invoke(update, [str(_RUN_ID), "--concurrency", "9"])

    assert result.exit_code == 0, result.output
    assert [json.loads(request.content) for request in tracker.requests] == [{"concurrency": 9}]
    assert result.output == "✓ Run concurrency updated to 6.\n"


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "not-an-integer"])
def test_update_rejects_non_positive_integer(
    cli_runner: CliRunner,
    value: str,
) -> None:
    """Reject invalid concurrency values before contacting the tracker."""
    result = cli_runner.invoke(update, [str(_RUN_ID), "--concurrency", value])

    assert result.exit_code == 2
    assert "--concurrency" in result.output
