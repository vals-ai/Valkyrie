"""Tests for resume and retry machine output.

Run: uv run pytest tests/unit/cli/run/test_resume.py

Covers safe action receipts and connected JSONL delegation for both command names.
"""

import json
from importlib import import_module
from uuid import UUID

import click
import pytest
from click.testing import CliRunner
from tracker.types import FetchBenchmarkResponse

from valkyrie.cli.run import run

from tests.unit.cli.factories import make_fetch_response

resume_module = import_module("valkyrie.cli.run.resume")

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


class MockResumeTracker:
    """Provide the owned tracker behavior exercised by resume and retry."""

    def __enter__(self) -> "MockResumeTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark(self, _run_id: UUID) -> FetchBenchmarkResponse:
        return make_fetch_response(_RUN_ID)

    def retry_or_resume_benchmark(self, *_args: object, **_kwargs: object) -> None:
        return None


@pytest.mark.parametrize(("command_name", "action"), [("resume", "resume"), ("retry", "retry")])
def test_json_action_receipts_exclude_secret_and_header_values(
    command_name: str,
    action: str,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
) -> None:
    tracker = MockResumeTracker()
    monkeypatch.setattr(resume_module, "TrackerService", lambda: tracker)
    monkeypatch.setattr(
        resume_module,
        "benchmark_service_headers",
        lambda _benchmark_name: {"Authorization": "header-secret-sentinel"},
    )

    result = cli_runner.invoke(
        run,
        [command_name, str(_RUN_ID), "--secret", "MODEL_KEY", "secret-name-sentinel", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["kind"] == "run_action"
    assert payload["action"] == action
    assert payload["status"] == "accepted"
    assert payload["run_id"] == str(_RUN_ID)
    assert "secret-name-sentinel" not in result.stdout
    assert "header-secret-sentinel" not in result.stdout


def test_connected_json_resume_emits_receipt_then_jsonl(
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
) -> None:
    tracker = MockResumeTracker()
    monkeypatch.setattr(resume_module, "TrackerService", lambda: tracker)
    monkeypatch.setattr(resume_module, "benchmark_service_headers", lambda _benchmark_name: {})

    def emit_stream(_tracker: MockResumeTracker, _run_id: UUID, *, output_format: str) -> None:
        assert output_format == "jsonl"
        click.echo('{"event":"snapshot","kind":"run_snapshot","schema_version":1}')

    monkeypatch.setattr(resume_module, "stream_benchmark_status", emit_stream)

    result = cli_runner.invoke(run, ["resume", str(_RUN_ID), "--connect", "--json"])

    assert result.exit_code == 0, result.output
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert [record["kind"] for record in records] == ["run_action", "run_snapshot"]
    assert [record["event"] for record in records] == ["accepted", "snapshot"]
