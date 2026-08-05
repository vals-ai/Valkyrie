"""Tests for the agent-facing machine-output policy.

Run: uv run pytest tests/unit/cli/test_json_contract.py

Covers the supported run-command surface and strict serialization.
"""

import json
from collections.abc import Iterator
from importlib import import_module

import click
import pytest
from click.testing import CliRunner

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.machine_output import credential_free_url, redact_urls, strict_json
from valkyrie.cli.main import cli

_RUN_ID = "123e4567-e89b-12d3-a456-426614174000"
_EXPECTED_JSON_LEAVES = {
    f"run {name}" for name in "analyze errors fetch list outputs results resume retry start status stop update".split()
}


def _walk_leaves(group: click.Group, prefix: tuple[str, ...] = ()) -> Iterator[tuple[str, click.Command]]:
    for name, command in group.commands.items():
        path = (*prefix, name)
        if isinstance(command, click.Group):
            yield from _walk_leaves(command, path)
        else:
            yield " ".join(path), command


class TestJsonPolicy:
    """Machine-output availability and option compatibility across the Click tree."""

    def test_only_supported_run_leaves_expose_literal_json(self) -> None:
        leaves = dict(_walk_leaves(cli))

        commands_with_json = {
            path for path, command in leaves.items() if any("--json" in parameter.opts for parameter in command.params)
        }
        assert commands_with_json == _EXPECTED_JSON_LEAVES

    @pytest.mark.parametrize(
        "arguments",
        [
            ["run", "fetch", _RUN_ID],
            ["run", "status", "--ids", _RUN_ID],
            ["run", "errors", _RUN_ID],
            ["run", "list"],
        ],
    )
    def test_json_rejects_explicit_format_combination(
        self,
        arguments: list[str],
        cli_runner: CliRunner,
    ) -> None:
        result = cli_runner.invoke(cli, [*arguments, "--json", "--format", "json"])

        assert result.exit_code == 2
        assert result.stdout == ""
        assert "--json cannot be combined with --format" in result.stderr

    def test_json_failures_end_with_an_error_document_naming_the_invoked_command(
        self,
        cli_runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Machine callers must diagnose a failure from stdout, and usage errors must not.

        Test cases:
        - A command failure emits one error document naming the invoked alias.
        - Rejected usage keeps stdout empty.
        """
        monkeypatch.setattr(
            import_module("valkyrie.cli.run.resume"),
            "TrackerService",
            lambda: (_ for _ in ()).throw(TrackerServiceError("connection reset")),
        )

        result = cli_runner.invoke(cli, ["run", "retry", _RUN_ID, "--json"])

        assert result.exit_code == 1
        assert json.loads(result.stdout) == {
            "command": "run retry",
            "error_message": "connection reset",
            "kind": "error",
            "schema_version": 1,
        }
        assert cli_runner.invoke(cli, ["run", "retry", "not-a-uuid", "--json"]).stdout == ""

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("read https://b.test/t.txt?X-Amz-Signature=url-secret-sentinel failed", "read <redacted-url> failed"),
            ("no route to https://user:url-secret-sentinel@tracker.test/api", "no route to <redacted-url>"),
            ("no route to http://localhost:8000/api", "no route to http://localhost:8000/api"),
            ("unparseable https://[url-secret-sentinel", "unparseable <redacted-url>"),
            ("uppercase HTTPS://b.test/t?sig=url-secret-sentinel", "uppercase <redacted-url>"),
        ],
    )
    def test_redact_urls_drops_only_credential_bearing_urls(self, message: str, expected: str) -> None:
        """Receipts are persisted, so a quoted URL must not carry credentials into one."""
        assert redact_urls(message) == expected

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_strict_json_rejects_non_finite_numbers(self, value: float) -> None:
        with pytest.raises(ValueError):
            strict_json({"value": value})

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("https://example.test/path", "https://example.test/path"),
            ("https://user:password@example.test/path", None),
            ("https://example.test/path?token=query-secret-sentinel", None),
            ("https://example.test/path#fragment", None),
            ("javascript://example.test/path", None),
            ("file://example.test/path", None),
            ("https://[parser-error-secret-sentinel", None),
            ("https://example.test:invalid-port/path?token=secret-sentinel", None),
        ],
    )
    def test_credential_free_url_only_returns_already_safe_values(
        self,
        value: str,
        expected: str | None,
    ) -> None:
        assert credential_free_url(value) == expected
