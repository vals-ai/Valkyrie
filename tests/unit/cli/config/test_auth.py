"""Tests for benchmark-service authentication configuration.

Run: uv run pytest tests/unit/cli/config/test_auth.py
"""

from pathlib import Path

import yaml
from click.testing import CliRunner

from valkyrie.cli.config.auth import auth


class TestAuthCommands:
    """Credential persistence, redaction, and error behavior."""

    def test_set_list_and_remove_credentials(self, config_path: Path, cli_runner: CliRunner) -> None:
        """Credential commands must persist values while keeping listed secrets redacted.

        Test cases:
        - Setting credentials preserves unrelated config and supports multiple benchmarks.
        - Listing masks both long and short credentials.
        - Removing one credential leaves the other intact.
        """
        config_path.write_text("api_key: tracker-key\n", encoding="utf-8")

        first_result = cli_runner.invoke(auth, ["set", "swebench", "secret-value"])
        second_result = cli_runner.invoke(auth, ["set", "short", "key"])
        list_result = cli_runner.invoke(auth, ["list"])
        remove_result = cli_runner.invoke(auth, ["remove", "swebench"])

        assert first_result.exit_code == 0, first_result.output
        assert second_result.exit_code == 0, second_result.output
        assert "secr***" in list_result.output
        assert "key" not in list_result.output
        assert "***" in list_result.output
        assert remove_result.exit_code == 0, remove_result.output

        saved_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert saved_config == {"api_key": "tracker-key", "benchmark_auth": {"short": "key"}}

    def test_empty_and_missing_credentials_report_actionable_results(
        self,
        config_path: Path,
        cli_runner: CliRunner,
    ) -> None:
        """Empty credential state and unknown removals must not silently succeed.

        Test cases:
        - Listing an empty credential map reports that nothing is configured.
        - Removing an unknown benchmark returns a command error without changing config.
        """
        config_path.write_text("benchmark_auth: {}\n", encoding="utf-8")

        list_result = cli_runner.invoke(auth, ["list"])
        remove_result = cli_runner.invoke(auth, ["remove", "missing"])

        assert list_result.exit_code == 0
        assert "No benchmark auth credentials configured" in list_result.output
        assert remove_result.exit_code == 1
        assert "Auth for 'missing' not configured" in remove_result.output
        assert yaml.safe_load(config_path.read_text(encoding="utf-8")) == {"benchmark_auth": {}}
