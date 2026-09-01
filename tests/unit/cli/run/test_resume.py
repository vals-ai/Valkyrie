"""Tests for resuming and retrying runs.

Run: uv run pytest tests/unit/cli/run/test_resume.py

Covers benchmark service header forwarding for custom services.
"""

from importlib import import_module
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.database.models import RetryMode
from tracker.types import FetchBenchmarkResponse, RetryOrResumeBenchmarkResponse

from tests.unit.cli.factories import make_fetch_response

resume_module = import_module("valkyrie.cli.run.resume")
service_headers_module = import_module("valkyrie.cli.service_headers")


class MockTrackerService:
    """Record retry/resume requests made by the CLI command."""

    calls: list[dict[str, object]] = []

    def __enter__(self) -> "MockTrackerService":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark(self, benchmark_id: UUID) -> FetchBenchmarkResponse:
        return make_fetch_response(benchmark_id)

    def retry_or_resume_benchmark(
        self,
        benchmark_id: UUID,
        retry: bool,
        retry_mode: RetryMode,
        concurrency: int | None,
        task_ids: list[str],
        service_headers: dict[str, str] | None = None,
        secrets: dict[str, str] | None = None,
        benchmark_url: str | None = None,
    ) -> RetryOrResumeBenchmarkResponse:
        self.calls.append({"benchmark_id": benchmark_id, "service_headers": service_headers})
        return RetryOrResumeBenchmarkResponse(status="success")


@pytest.fixture(autouse=True)
def reset_calls() -> None:
    """Reset recorded requests so each test is isolated."""
    MockTrackerService.calls = []


def test_resume_forwards_custom_headers(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send `-H` headers to the tracker so custom benchmark services authenticate."""
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    monkeypatch.setattr(resume_module, "TrackerService", MockTrackerService)

    def no_configured_auth(_benchmark_name: str) -> str | None:
        return None

    monkeypatch.setattr(
        service_headers_module.TrackerService,
        "get_benchmark_auth",
        staticmethod(no_configured_auth),
    )

    result = cli_runner.invoke(
        resume_module.resume,
        [str(run_id), "-H", "x-descope-api-key", "secret-value"],
    )

    assert result.exit_code == 0, result.output
    assert MockTrackerService.calls == [
        {"benchmark_id": run_id, "service_headers": {"x-descope-api-key": "secret-value"}}
    ]
