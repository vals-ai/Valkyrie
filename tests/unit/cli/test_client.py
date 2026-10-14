"""Tests for running SDK calls from CLI commands.

Run: uv run pytest tests/unit/cli/test_client.py

Covers how SDK failures surface as command errors.
"""

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.filter_options import filter_options

from tests.unit.cli.conftest import MockTracker


@pytest.mark.parametrize(
    ("reply", "message"),
    [
        (
            httpx.Response(403, json={"detail": "invalid \x1b[31mkey"}),
            "Valkyrie API request failed (403): invalid \\x1b[31mkey",
        ),
        (httpx.ConnectError("tracker unavailable"), "Valkyrie request failed: tracker unavailable"),
        (httpx.Response(200, json={"benchmark_names": []}), "validation errors for FilterOptionsResponse"),
    ],
    ids=["api", "transport", "malformed"],
)
def test_sdk_failures_exit_1_with_an_escaped_message_and_no_partial_output(
    tracker: MockTracker, cli_runner: CliRunner, reply: httpx.Response | Exception, message: str
) -> None:
    tracker.reply("GET", "/benchmarks/filter-options", reply)

    result = cli_runner.invoke(filter_options, ["--format", "json"])

    assert result.exit_code == 1
    assert result.output.startswith("Error: ")
    assert message in result.output
    assert "\x1b" not in result.output
    assert "Traceback" not in result.output
