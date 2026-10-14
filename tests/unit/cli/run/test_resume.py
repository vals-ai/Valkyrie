"""Tests for resuming and retrying runs.

Run: uv run pytest tests/unit/cli/run/test_resume.py
"""

import json
from uuid import UUID

import click
import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk.models import BenchmarkStatus

from valkyrie.cli.run.resume import resume, retry_command

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_response, ok, run_stream, serve_run

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_RESTART_PATH = f"/retry-or-resume-benchmark/{_RUN_ID}"


_OVERRIDES = ["--benchmark-url", "https://new.example", "--lambda", "vals-format-lambda"]


@pytest.mark.parametrize(
    ("command", "arguments", "retry"),
    [(resume, [], "false"), (resume, ["--retry", "--update-agent"], "true"), (retry_command, _OVERRIDES, "true")],
    ids=["resume", "resume-retry", "retry-alias"],
)
def test_restart_forwards_headers_overrides_and_mode(
    tracker: MockTracker, cli_runner: CliRunner, command: click.Command, arguments: list[str], retry: str
) -> None:
    """`-H` headers reach custom benchmark services, and the retry alias always retries errored tasks."""
    tracker.reply("GET", "/fetch-benchmark", ok(make_fetch_response(_RUN_ID)))
    tracker.reply("POST", _RESTART_PATH, httpx.Response(200, json={"status": "success"}))

    result = cli_runner.invoke(command, [str(_RUN_ID), "-H", "x-descope-api-key", "secret-value", *arguments])

    assert result.exit_code == 0, result.output
    (request,) = tracker.requested("POST", _RESTART_PATH)
    assert request.url.params["retry"] == retry
    assert request.url.params["update_agent"] == str("--update-agent" in arguments).lower()

    body = json.loads(request.content)
    assert body["service_headers"] == {"x-descope-api-key": "secret-value"}
    if arguments == _OVERRIDES:
        assert (body["benchmark_url"], body["lambda_function"]) == ("https://new.example", "vals-format-lambda")


def test_connected_resume_streams_the_run_without_a_track_progress_hint(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    finished = make_fetch_response(_RUN_ID, status=BenchmarkStatus.FINISHED, finished_tasks=4)
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=run_stream(finished))
    tracker.reply("POST", _RESTART_PATH, httpx.Response(200, json={"status": "success"}))

    result = cli_runner.invoke(resume, [str(_RUN_ID), "--connect"])

    assert result.exit_code == 0, result.output
    assert "✓ Run completed!" in result.output
    assert "Track progress:" not in result.output
