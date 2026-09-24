"""Tests for resuming and retrying runs.

Run: uv run pytest tests/unit/cli/run/test_resume.py

Covers benchmark service header forwarding for custom services.
"""

from importlib import import_module
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.database.models import RetryMode
from tracker.types import FetchBenchmarkMetadataResponse, FetchBenchmarkResponse, RetryOrResumeBenchmarkResponse

from tests.unit.cli.factories import make_fetch_metadata, make_fetch_response
from valkyrie.cli import s3_config

resume_module = import_module("valkyrie.cli.run.resume")
service_headers_module = import_module("valkyrie.cli.service_headers")


class MockTrackerService:
    """Record retry/resume requests made by the CLI command."""

    calls: list[dict[str, object]] = []
    retry_modes: list[RetryMode] = []

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
        lambda_function: str | None = None,
    ) -> RetryOrResumeBenchmarkResponse:
        self.calls.append({"benchmark_id": benchmark_id, "service_headers": service_headers})
        self.retry_modes.append(retry_mode)
        return RetryOrResumeBenchmarkResponse(status="success")


@pytest.fixture(autouse=True)
def reset_calls() -> None:
    """Reset recorded requests so each test is isolated."""
    MockTrackerService.calls = []
    MockTrackerService.retry_modes = []


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


@pytest.mark.parametrize("storage_bucket", [None, "shared-library", "vs-dev-acme-123"])
def test_update_agent_rejects_a_different_run_bucket_before_copy_or_resume(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    storage_bucket: str | None,
) -> None:
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.copy_object.return_value = {}

    class TrackerWithMetadata(MockTrackerService):
        def fetch_benchmark_metadata(self, benchmark_id: UUID) -> FetchBenchmarkMetadataResponse:
            return make_fetch_metadata(benchmark_id).model_copy(update={"storage_bucket": storage_bucket})

    monkeypatch.setattr(resume_module, "TrackerService", TrackerWithMetadata)
    monkeypatch.setattr(resume_module, "benchmark_service_headers", lambda *_arguments: {})
    monkeypatch.setattr(
        s3_config,
        "load_config",
        lambda: {
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "shared-library",
        },
    )
    monkeypatch.setattr(ExplicitCredentialsAWSClientProvider, "s3_client", lambda _provider: client)

    result = cli_runner.invoke(resume_module.resume, [str(run_id), "--update-agent"])

    if storage_bucket == "vs-dev-acme-123":
        assert result.exit_code == 1
        assert "does not match the run storage bucket" in result.output
        assert client.mock_calls == []
        assert MockTrackerService.calls == []
        return

    assert result.exit_code == 0, result.output
    client.copy_object.assert_awaited_once_with(
        Bucket="shared-library",
        CopySource={"Bucket": "shared-library", "Key": "agents/mini_sweagent.zip"},
        Key=f"benchmarks/{run_id}/mini_sweagent.zip",
    )
    assert MockTrackerService.calls == [{"benchmark_id": run_id, "service_headers": {}}]


def test_regrade_sends_regrade_retry_mode(cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_service_headers(*_arguments: object) -> dict[str, str]:
        return {}

    monkeypatch.setattr(resume_module, "TrackerService", MockTrackerService)
    monkeypatch.setattr(resume_module, "benchmark_service_headers", no_service_headers)

    result = cli_runner.invoke(resume_module.retry_command, ["123e4567-e89b-12d3-a456-426614174000", "--regrade"])

    assert result.exit_code == 0, result.output
    assert MockTrackerService.retry_modes == [RetryMode.REGRADE]


def test_regrade_rejects_from_scratch(cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resume_module, "TrackerService", MockTrackerService)

    result = cli_runner.invoke(
        resume_module.retry_command, ["123e4567-e89b-12d3-a456-426614174000", "--regrade", "--from-scratch"]
    )

    assert result.exit_code == 2
    assert "--from-scratch and --regrade cannot be used together." in result.output
    assert MockTrackerService.calls == []
