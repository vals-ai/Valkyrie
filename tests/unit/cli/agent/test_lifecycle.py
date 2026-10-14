"""Tests for CLI agent lifecycle commands.

Run: uv run pytest tests/unit/cli/agent/test_lifecycle.py

Push, download, removal and listing run end to end in tests/integration/local/cli/test_write_commands.py.
"""

import json

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.agent import agent

from tests.unit.cli.conftest import MockTracker


@pytest.mark.parametrize("count", [0, 12])
@pytest.mark.parametrize("arguments", [["--all"], ["--format", "json"], ["--all", "--format", "JSON"]])
def test_list_without_paging(tracker: MockTracker, cli_runner: CliRunner, count: int, arguments: list[str]) -> None:
    agents = [
        {"name": f"agent-{index}", "last_modified": "2026-07-17T12:00:00+00:00" if index else None}
        for index in range(count)
    ]
    tracker.reply("GET", "/agents", httpx.Response(200, json={"agents": agents}))

    result = cli_runner.invoke(agent, ["list", *arguments])

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    if "--format" in arguments:
        assert json.loads(result.output) == {"agents": agents}
    elif count:
        assert all(entry["name"] in result.output for entry in agents)
        assert "[q] quit" not in result.output
    else:
        assert "No agents found" in result.output


def test_list_reports_a_malformed_timestamp_without_a_traceback(tracker: MockTracker, cli_runner: CliRunner) -> None:
    agents = [{"name": "agent", "last_modified": "yesterday"}]
    tracker.reply("GET", "/agents", httpx.Response(200, json={"agents": agents}))

    result = cli_runner.invoke(agent, ["list", "--all"])

    assert result.exit_code == 1
    assert "Invalid isoformat string: 'yesterday'" in result.output
