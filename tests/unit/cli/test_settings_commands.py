"""Provider and benchmark service commands against a fake Tracker.

Run: uv run pytest tests/unit/cli/test_settings_commands.py
"""

import json
from typing import Any

import httpx
import pytest
from click.testing import CliRunner

from tests.unit.cli.fake_tracker import FakeTracker
from valkyrie.cli.main import cli


@pytest.mark.parametrize(
    ("arguments", "method", "path", "body", "output"),
    [
        (
            ["provider", "set", "daytona", "thissecret"],
            "PUT",
            "/sandbox-providers/daytona",
            {"secret_name": "thissecret"},
            "Provider daytona now uses the secret thissecret.\n",
        ),
        (
            ["provider", "remove", "daytona"],
            "DELETE",
            "/sandbox-providers/daytona",
            None,
            "Provider daytona no longer has a secret.\n",
        ),
        (
            ["provider", "default", "modal"],
            "PUT",
            "/settings/default-provider",
            {"provider": "modal"},
            "Your default provider is now modal.\n",
        ),
        (
            ["benchmark", "service", "set", "swebench", "https://bench.example"],
            "PUT",
            "/settings/benchmark-services/swebench",
            {"url": "https://bench.example", "auth_secret_name": None, "auth_header_name": None},
            "Runs of swebench now use https://bench.example.\n",
        ),
        (
            ["benchmark", "service", "set", "x", "https://x.dev", "--auth-secret", "k", "--auth-header", "X-Key"],
            "PUT",
            "/settings/benchmark-services/x",
            {"url": "https://x.dev", "auth_secret_name": "k", "auth_header_name": "X-Key"},
            "Runs of x now use https://x.dev, sending the secret k as X-Key.\n",
        ),
        (
            ["benchmark", "service", "remove", "swebench"],
            "DELETE",
            "/settings/benchmark-services/swebench",
            None,
            "Runs of swebench now use the hosted service.\n",
        ),
    ],
)
def test_commands_send_the_change_and_echo_the_saved_state(
    fake_tracker: FakeTracker, arguments: list[str], method: str, path: str, body: Any, output: str
) -> None:
    fake_tracker.providers["daytona"] = "oldsecret"
    fake_tracker.benchmark_services["swebench"] = {"url": "https://old.example"}

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code == 0, result.output
    assert result.output == output
    (request,) = fake_tracker.requests
    assert (request.method, request.url.path) == (method, path)
    assert (json.loads(request.content) if request.content else None) == body
    assert request.headers["x-api-key"] == "test-key"


@pytest.mark.parametrize(
    ("arguments", "raw_path"),
    [
        (["provider", "remove", "a/b"], b"/sandbox-providers/a%2Fb"),
        (["benchmark", "service", "remove", "a/b"], b"/settings/benchmark-services/a%2Fb"),
    ],
)
def test_names_are_escaped_in_the_path(fake_tracker: FakeTracker, arguments: list[str], raw_path: bytes) -> None:
    CliRunner().invoke(cli, arguments)

    assert fake_tracker.requests[0].url.raw_path == raw_path


@pytest.mark.parametrize(
    ("providers", "default", "output"),
    [
        (
            {"modal": "modalsecret", "daytona": "thissecret"},
            "daytona",
            "* daytona → thissecret\n  modal → modalsecret\n",
        ),
        (
            {"modal": "modalsecret"},
            "e2b",
            "* e2b (default; no secret set — run `valkyrie provider set e2b SECRET`)\n  modal → modalsecret\n",
        ),
        ({}, None, "No provider has a secret yet. Map one with `valkyrie provider set NAME SECRET`.\n"),
    ],
)
def test_provider_list_shows_secret_names_and_marks_the_default(
    fake_tracker: FakeTracker, providers: dict[str, str], default: str | None, output: str
) -> None:
    fake_tracker.providers = providers
    fake_tracker.default_provider = default

    result = CliRunner().invoke(cli, ["provider", "list"])

    assert result.exit_code == 0, result.output
    assert result.output == output


def test_tracker_validation_errors_reach_the_user(fake_tracker: FakeTracker) -> None:
    fake_tracker.refusal = httpx.Response(
        422, json={"detail": [{"loc": ["body", "url"], "msg": "Value error, Invalid benchmark service URL"}]}
    )

    result = CliRunner().invoke(cli, ["benchmark", "service", "set", "swebench", "my-tunnel.ngrok.io"])

    assert result.exit_code == 1
    assert result.output == "Error: url: Value error, Invalid benchmark service URL (422)\n"
