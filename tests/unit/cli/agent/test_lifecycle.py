"""Tests for CLI agent lifecycle commands.

Run: uv run pytest tests/unit/cli/agent/test_lifecycle.py
"""

from collections.abc import Callable
from datetime import datetime, timezone
from importlib import import_module
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner
from tracker.exceptions import S3Error

from valkyrie.cli.agent import agent

lifecycle = import_module("valkyrie.cli.agent.lifecycle")


class TestAgentLifecycleCommands:
    """User-visible install, download, list, and removal behavior."""

    def test_commands_complete_successful_storage_flows(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Lifecycle commands must report completed operations and preserve user choices.

        Test cases:
        - Install reports the resolved contract name.
        - Download forwards the requested destination.
        - Removal requires confirmation before deleting.
        - Listing renders returned agent names and timestamps.
        """
        installed_at = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
        mock_install = AsyncMock(return_value="resolved-agent")
        mock_download = AsyncMock()
        mock_remove = AsyncMock()
        mock_list = AsyncMock(return_value=[("resolved-agent", installed_at), ("undated", None)])
        monkeypatch.setattr(lifecycle, "install_agent", mock_install)
        monkeypatch.setattr(lifecycle, "download_agent", mock_download)
        monkeypatch.setattr(lifecycle, "remove_agent", mock_remove)
        monkeypatch.setattr(lifecycle, "list_agents", mock_list)

        def render_first_page(
            load_page: Callable[[int, int], tuple[int, list[tuple[str, datetime | None]]]],
            render_page: Callable[[list[tuple[str, datetime | None]], int, int, int], None],
            **_kwargs: object,
        ) -> None:
            total_count, page = load_page(0, 10)
            render_page(page, 1, 1, total_count)

        monkeypatch.setattr(lifecycle, "paginate_cli_pages", render_first_page)

        install_result = cli_runner.invoke(agent, ["install", "https://github.com/vals-ai/agent", "--name", "chosen"])
        download_result = cli_runner.invoke(agent, ["download", "resolved-agent", "--output-dir", "/tmp/output"])
        remove_result = cli_runner.invoke(agent, ["remove", "resolved-agent"], input="y\n")
        list_result = cli_runner.invoke(agent, ["list"])

        assert install_result.exit_code == 0, install_result.output
        assert "Agent 'resolved-agent' installed successfully" in install_result.output
        mock_install.assert_awaited_once_with("chosen", "https://github.com/vals-ai/agent")
        assert download_result.exit_code == 0, download_result.output
        mock_download.assert_awaited_once()
        assert remove_result.exit_code == 0, remove_result.output
        mock_remove.assert_awaited_once_with("resolved-agent")
        assert list_result.exit_code == 0, list_result.output
        assert "resolved-agent" in list_result.output
        assert "undated" in list_result.output

    def test_remove_cancellation_does_not_delete_agent(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Declining removal confirmation must leave the remote agent untouched.

        Test cases:
        - A negative confirmation exits successfully with a cancellation message.
        - The storage deletion boundary is never called.
        """
        mock_remove = AsyncMock()
        monkeypatch.setattr(lifecycle, "remove_agent", mock_remove)

        result = cli_runner.invoke(agent, ["remove", "protected-agent"], input="n\n")

        assert result.exit_code == 0
        assert "Cancelled" in result.output
        mock_remove.assert_not_awaited()

    @pytest.mark.parametrize(
        ("arguments", "boundary_name"),
        [
            (["install", "https://github.com/vals-ai/agent"], "install_agent"),
            (["download", "agent"], "download_agent"),
            (["remove", "agent"], "remove_agent"),
            (["list"], "list_agents"),
        ],
    )
    def test_storage_failures_are_reported_without_tracebacks(
        self,
        arguments: list[str],
        boundary_name: str,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Storage failures must become concise command errors for every lifecycle action.

        Test cases:
        - Install, download, removal, and listing preserve the storage error message.
        - Failed commands exit nonzero without exposing an internal traceback.
        """
        monkeypatch.setattr(lifecycle, boundary_name, AsyncMock(side_effect=S3Error("storage unavailable")))
        command_input = "y\n" if arguments[0] == "remove" else None

        result = cli_runner.invoke(agent, arguments, input=command_input)

        assert result.exit_code == 1
        assert "storage unavailable" in result.output
        assert "Traceback" not in result.output
