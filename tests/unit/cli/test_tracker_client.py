"""Tests for tracker client and related CLI rendering behavior.

Run: uv run pytest tests/unit/cli/test_tracker_client.py

Covers tracker client request construction, config handling, and CLI output helpers. Add cases here for
tracker-client behavior or CLI rendering that can regress without requiring live services.
"""

from collections.abc import Callable
from datetime import datetime
from functools import partial
from importlib import import_module
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import click
import httpx
import pytest
import yaml
from click.testing import CliRunner
from tracker.database.models import AgentContractRequest, BenchmarkStatus, DocentReadingStatus, RetryMode, TaskStatus
from tracker.types import (
    BenchmarkDetails,
    BenchmarkServiceEntry,
    BenchmarkServiceHealth,
    BenchmarkServicesResponse,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
)

from valkyrie.cli import main as cli_main
from valkyrie.cli import service_headers
import valkyrie.cli.config.benchmark_services as config_benchmark_services
from valkyrie.cli.config.benchmark_services import paginate_services
from valkyrie.cli.exceptions import TrackerNotFoundError
from valkyrie.cli.run import list_runs, start
from valkyrie.cli.run.list_runs import format_fetch_benchmarks_response
from valkyrie.cli.run.progress import format_benchmark_status
from valkyrie.cli.runtime_config import (
    DEV_TRACKER_URL,
    TRACKER_SERVICE_URL_ENV_VAR,
    VALKYRIE_CONFIG_PATH_ENV_VAR,
    VALKYRIE_ENV_ENV_VAR,
)
from valkyrie.cli.tracker_client import TrackerService, TrackerServiceError

run_resume = import_module("valkyrie.cli.run.resume")
run_start = import_module("valkyrie.cli.run.start")


class MockClient:
    """Record tracker HTTP requests and return deterministic responses."""

    def __init__(self) -> None:
        self.params: dict[str, object] | None = None
        self.json: dict[str, object] | None = None
        self.url: str | None = None

    def post(
        self,
        url: str,
        *,
        params: dict[str, object] | None = None,
        json: dict[str, object],
    ) -> httpx.Response:
        self.url = url
        self.params = params
        self.json = json
        return httpx.Response(200, json={"status": "success"})

    def get(
        self,
        url: str,
        *,
        params: dict[str, object] | None = None,
    ) -> httpx.Response:
        self.url = url
        self.params = params
        if "/fetch-run-outputs/" in url:
            return httpx.Response(200, content=b"tar")
        return httpx.Response(200, json={"benchmarks": [], "total_count": 0})

    def close(self) -> None:
        return None


@pytest.fixture
def mock_client() -> MockClient:
    """Provide a fresh recording HTTP client."""
    return MockClient()


def _mock_client_builder(mock_client: MockClient) -> Callable[..., MockClient]:
    def build_client(**_kwargs: object) -> MockClient:
        return mock_client

    return build_client


class MockTrackerService:
    """Provide deterministic tracker behavior for command tests in this module."""

    start_response: dict[str, object] = {}
    start_calls: list[dict[str, object]] = []
    retry_or_resume_calls: list[dict[str, object]] = []
    init_calls = 0
    provider_validations: list[str | None] = []
    require_config_values: list[bool] = []

    def __init__(self, *, require_config: bool = True) -> None:
        self.__class__.init_calls += 1
        self.require_config_values.append(require_config)

    @staticmethod
    def benchmark_service_health(
        name: str,
        url: str,
        *,
        healthy: bool = True,
        latency_ms: int | None = None,
        error: str | None = None,
    ) -> BenchmarkServiceHealth:
        return BenchmarkServiceHealth(name=name, url=url, healthy=healthy, latency_ms=latency_ms, error=error)

    @staticmethod
    def get_benchmark_auth(_benchmark_name: str) -> None:
        return None

    @staticmethod
    def get_webhook_secret() -> None:
        return None

    def __enter__(self) -> "MockTrackerService":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def start_benchmark(self, *_args: object, **_kwargs: object) -> httpx.Response:
        self.start_calls.append({"args": _args, "kwargs": _kwargs})
        return httpx.Response(200, json=self.start_response)

    def fetch_benchmark(self, _run_id: object) -> SimpleNamespace:
        return SimpleNamespace(benchmark_name="swebench")

    def retry_or_resume_benchmark(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        self.retry_or_resume_calls.append({"args": _args, "kwargs": _kwargs})

        return SimpleNamespace(status="success")

    @classmethod
    def validate_sandbox_provider(cls, provider: str | None = None) -> tuple[str, str]:
        cls.provider_validations.append(provider)
        return provider or "daytona", "DaytonaSecrets"

    def catalog_benchmark_services(self) -> list[BenchmarkServiceEntry]:
        return [
            BenchmarkServiceEntry(name="swebench", url="https://swebench.benchmarks.vals.ai"),
            BenchmarkServiceEntry(name="fab", url="https://fab.benchmarks.vals.ai"),
        ]

    def check_benchmark_services(self, services: list[BenchmarkServiceEntry]) -> BenchmarkServicesResponse:
        assert [(service.name, service.url) for service in services] == [
            ("swebench", "http://local-swebench"),
            ("fab", "https://fab.benchmarks.vals.ai"),
            ("custombench", "http://custombench"),
        ]
        return BenchmarkServicesResponse(
            services=[
                self.benchmark_service_health("swebench", "http://local-swebench", healthy=False, error="timeout"),
                self.benchmark_service_health("fab", "https://fab.benchmarks.vals.ai", latency_ms=20),
                self.benchmark_service_health("custombench", "http://custombench", latency_ms=5),
            ]
        )


@pytest.fixture
def mock_tracker_service() -> type[MockTrackerService]:
    """Reset and provide the tracker service mock used by this module."""
    MockTrackerService.start_calls = []
    MockTrackerService.retry_or_resume_calls = []
    MockTrackerService.init_calls = 0
    MockTrackerService.provider_validations = []
    MockTrackerService.require_config_values = []
    return MockTrackerService


@pytest.fixture
def connect_stream_testbed(
    monkeypatch: pytest.MonkeyPatch,
    mock_tracker_service: type[MockTrackerService],
) -> tuple[UUID, list[str], type[MockTrackerService]]:
    """Connect start and resume commands to a shared tracker mock."""
    started_run_id = uuid4()
    streamed_run_ids: list[str] = []
    mock_tracker_service.start_response = {
        "benchmark_name": "swebench",
        "agent_name": "agent",
        "benchmark_id": str(started_run_id),
        "concurrency": 5,
        "started_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "task_count": 1,
        "cloudwatch_url": "https://cloudwatch.example/run",
        "s3_bucket_url": "s3://bucket/benchmarks/run",
    }

    async def get_contract_from_s3(_agent: str, _agent_config: object) -> AgentContractRequest:
        return AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run")

    def stream_benchmark_status(_tracker: MockTrackerService, run_id: object) -> None:
        streamed_run_ids.append(str(run_id))

    monkeypatch.setattr(run_start, "TrackerService", mock_tracker_service)
    monkeypatch.setattr(run_start, "get_contract_from_s3", get_contract_from_s3)
    monkeypatch.setattr(run_start, "stream_benchmark_status", stream_benchmark_status)
    monkeypatch.setattr(run_resume, "TrackerService", mock_tracker_service)
    monkeypatch.setattr(run_resume, "stream_benchmark_status", stream_benchmark_status)

    return started_run_id, streamed_run_ids, mock_tracker_service


def _empty_config() -> dict[str, object]:
    return {}


def _empty_config_keys(_tracker: object) -> dict[str, str]:
    return {}


def _write_valkyrie_config(config_path: Path, **overrides: object) -> Path:
    config: dict[str, object] = {
        "AWS_ACCESS_KEY_ID": "aws-key",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
        "S3_BUCKET": "bucket",
        "LOG_GROUP": "benchmarks",
        "LOG_RETENTION_POLICY": 365,
    }
    for key, value in overrides.items():
        if value is None:
            config.pop(key, None)
        else:
            config[key] = value

    config_path.write_text(yaml.safe_dump(config, sort_keys=False))

    return config_path


@pytest.fixture(autouse=True)
def clear_runtime_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(VALKYRIE_ENV_ENV_VAR, raising=False)
    monkeypatch.delenv(TRACKER_SERVICE_URL_ENV_VAR, raising=False)
    monkeypatch.delenv(VALKYRIE_CONFIG_PATH_ENV_VAR, raising=False)


def test_tracker_client_uses_selected_environment_url(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, "dev")
    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    tracker = TrackerService()
    tracker.health_check()

    assert mock_client.url == f"{DEV_TRACKER_URL}/health"


def _handle_catalog_service_request(requests: list[httpx.Request], request: httpx.Request) -> httpx.Response:
    requests.append(request)

    if request.method == "GET":
        return httpx.Response(
            200,
            json={"services": [{"name": "swebench", "url": "https://swebench.benchmarks.vals.ai/"}]},
        )

    return httpx.Response(
        200,
        json={
            "services": [
                {
                    "name": "swebench",
                    "url": "https://swebench.benchmarks.vals.ai",
                    "healthy": True,
                    "latency_ms": 12,
                    "error": None,
                }
            ]
        },
    )


def test_tracker_client_lists_catalog_services_through_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalog service listing should use tracker-owned catalog lookup and health checks.

    Test cases:
    - Hosted entries are fetched from the tracker service catalog endpoint.
    - Hosted entries are health-checked by the existing tracker endpoint.
    """

    requests: list[httpx.Request] = []
    transport = httpx.MockTransport(partial(_handle_catalog_service_request, requests))
    original_client = httpx.Client

    def build_client(
        *,
        timeout: float | httpx.Timeout | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Client:

        return original_client(transport=transport, timeout=timeout, headers=headers)

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(lambda: {"api_key": "catalog-key"}))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    response = tracker.list_benchmark_services()

    assert requests[0].headers["X-Api-Key"] == "catalog-key"
    assert [str(request.url) for request in requests] == [
        "http://tracker/benchmark-services",
        "http://tracker/benchmark-services",
    ]
    assert json.loads(requests[1].content) == {
        "services": [
            {
                "name": "swebench",
                "url": "https://swebench.benchmarks.vals.ai",
                "auth_header_name": None,
                "auth_secret_name": None,
            }
        ]
    }
    assert [(service.name, service.latency_ms) for service in response.services] == [("swebench", 12)]


def test_fetch_run_outputs_uses_run_outputs_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_run_outputs(run_id, task_ids=["task-1", "task-2"])

    assert response.content == b"tar"
    assert mock_client.url == f"http://tracker/fetch-run-outputs/{run_id}"
    assert mock_client.params == {"task_ids": ["task-1", "task-2"]}


def test_fetch_run_outputs_omits_empty_task_ids(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    run_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_run_outputs(run_id)

    assert response.content == b"tar"
    assert mock_client.url == f"http://tracker/fetch-run-outputs/{run_id}"
    assert mock_client.params == {}


def test_stop_benchmark_sends_task_selection(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    """Send selected task IDs in stop requests.

    Test cases:
    - Force remains a query parameter.
    - Task IDs are sent in the request body.
    """

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    run_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")

    tracker.stop_benchmark(
        run_id,
        force=True,
        task_ids=["task-a", "task-b"],
    )

    assert mock_client.url == f"http://tracker/stop-benchmark/{run_id}"
    assert mock_client.params == {"force": True}
    assert mock_client.json == {"task_ids": ["task-a", "task-b"]}

    tracker.stop_benchmark(run_id, force=False)

    assert mock_client.json == {"task_ids": None}


def test_update_benchmark_concurrency_uses_patch_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Send concurrency updates to the dedicated tracker endpoint."""
    requests: list[httpx.Request] = []
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")

    def handle_request(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "benchmark_id": str(run_id),
                "status": "IN_PROGRESS",
                "concurrency": 9,
            },
        )

    transport = httpx.MockTransport(handle_request)
    original_client = httpx.Client

    def build_client(
        *,
        timeout: float | httpx.Timeout | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Client:
        return original_client(transport=transport, timeout=timeout, headers=headers)

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    response = TrackerService(base_url="http://tracker").update_benchmark_concurrency(run_id, 9)

    assert len(requests) == 1
    assert requests[0].method == "PATCH"
    assert str(requests[0].url) == f"http://tracker/benchmarks/{run_id}/concurrency"
    assert json.loads(requests[0].content) == {"concurrency": 9}
    assert response.benchmark_id == run_id
    assert response.concurrency == 9


def test_update_benchmark_concurrency_surfaces_tracker_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preserve the tracker's useful rejection detail for CLI callers."""
    original_client = httpx.Client
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(409, json={"detail": "Run is currently in the FINISHED state."})
    )

    def build_client(
        *,
        timeout: float | httpx.Timeout | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Client:
        return original_client(transport=transport, timeout=timeout, headers=headers)

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(
        TrackerServiceError,
        match="Failed to update run concurrency: Run is currently in the FINISHED state",
    ):
        tracker.update_benchmark_concurrency(uuid4(), 9)


def test_tracker_client_checks_health_on_context_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commands should fail before making tracker requests when the tracker is unhealthy.

    Test cases:
    - Entering the tracker context calls /health and raises a typed tracker error.
    """
    original_client = httpx.Client
    transport = httpx.MockTransport(lambda _request: httpx.Response(503, json={"detail": "not ready"}))

    def build_client(
        *,
        timeout: float | httpx.Timeout | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Client:
        return original_client(transport=transport, timeout=timeout, headers=headers)

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    with pytest.raises(TrackerNotFoundError, match="Tracker service failed to respond"):
        with TrackerService(base_url="http://tracker"):
            pass


def test_fetch_run_outputs_raises_tracker_error_for_non_ok_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class ErrorClient:
        def get(
            self,
            url: str,
            *,
            params: dict[str, object] | None = None,
        ) -> httpx.Response:
            self.url = url
            self.params = params
            return httpx.Response(404, json={"detail": "No outputs found"})

    client = ErrorClient()

    def build_client(**_kwargs: object) -> ErrorClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(TrackerServiceError, match="Failed to fetch run outputs: No outputs found"):
        tracker.fetch_run_outputs(uuid4())


def test_fetch_run_outputs_raises_tracker_error_for_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingClient:
        def get(
            self,
            url: str,
            *,
            params: dict[str, object] | None = None,
        ) -> httpx.Response:
            self.url = url
            self.params = params
            raise httpx.ConnectError("connection failed")

    client = FailingClient()

    def build_client(**_kwargs: object) -> FailingClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(TrackerServiceError, match="Failed to fetch run outputs: connection failed"):
        tracker.fetch_run_outputs(uuid4())


def harness_config_payload(_tracker: TrackerService, _provider: str | None = None) -> dict[str, object]:
    return {
        "aws": {
            "aws_access_key_id": "aws-key",
            "aws_secret_access_key": "aws-secret",
            "aws_default_region": "us-east-1",
        },
        "s3_bucket": "bucket",
        "log_group": "benchmarks",
        "log_retention_policy": 365,
        "sandbox_provider_secret_name": "DaytonaSecrets",
    }


def test_paginate_services_renders_latency_as_response_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Service list should show response latency when available and dash when not.

    Test cases:
    - Responding services render measured latency.
    - Non-responding services render a dash without healthy/error columns.
    """
    captured_rows: list[dict[str, str]] = []
    captured_headers: list[str] = []

    def mock_format_table(
        rows: list[dict[str, str]],
        headers: list[str],
        _current_page: int,
        _total_pages: int,
        _total_count: int,
        _item_name: str,
    ) -> None:
        captured_rows.extend(rows)
        captured_headers.extend(headers)

    monkeypatch.setattr("valkyrie.cli.config.benchmark_services.format_table", mock_format_table)

    paginate_services(
        [
            BenchmarkServiceHealth(
                name="swebench",
                url="https://swebench.benchmarks.vals.ai",
                healthy=True,
                latency_ms=23,
            ),
            BenchmarkServiceHealth(
                name="vcb",
                url="http://localhost:9000",
                healthy=False,
                latency_ms=None,
                error="[Errno -2] Name or service not known",
            ),
        ]
    )

    assert captured_headers == ["Benchmark", "Service URL", "Source", "Latency"]
    assert [row["Source"] for row in captured_rows] == ["benchmarks.vals.ai", "localhost:9000"]
    assert [row["Latency"] for row in captured_rows] == ["23 ms", "-"]


def test_paginate_services_health_checks_visible_pages_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Service pagination should defer health checks until a page is visible.

    Test cases:
    - The first page is health-checked before rendering.
    - Moving forward checks only the next page, and moving back reuses cached results.
    - Each page render clears the previous table output.
    """
    service_entries = [
        BenchmarkServiceEntry(name="one", url="https://one.example"),
        BenchmarkServiceEntry(name="two", url="https://two.example"),
        BenchmarkServiceEntry(name="three", url="https://three.example"),
    ]
    checked_pages: list[list[str]] = []
    rendered_pages: list[list[str]] = []
    keys = iter(["l", "h", "q"])

    def check_services(entries: list[BenchmarkServiceEntry]) -> list[BenchmarkServiceHealth]:
        checked_pages.append([entry.name for entry in entries])
        return [
            BenchmarkServiceHealth(
                name=entry.name,
                url=entry.url,
                healthy=True,
                latency_ms=len(checked_pages),
            )
            for entry in entries
        ]

    def mock_format_table(
        rows: list[dict[str, str]],
        _headers: list[str],
        _current_page: int,
        _total_pages: int,
        _total_count: int,
        _item_name: str,
    ) -> None:
        rendered_pages.append([row["Benchmark"] for row in rows])

    monkeypatch.setattr("valkyrie.cli.config.benchmark_services.click.getchar", lambda: next(keys))
    monkeypatch.setattr("valkyrie.cli.config.benchmark_services.format_table", mock_format_table)

    paginate_services(service_entries, limit=2, check_services=check_services)

    assert checked_pages == [["one", "two"], ["three"]]
    assert rendered_pages == [["one", "two"], ["three"], ["one", "two"]]
    assert capsys.readouterr().out.count("\033[2J\033[3J\033[1;1H") == 3


def test_retry_or_resume_sends_retry_mode(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    """Resume requests should carry retry mode and request overrides.

    Test cases:
    - Retry mode and concurrency are query parameters.
    - An explicit zero is forwarded instead of silently using the stored concurrency.
    - Secret and benchmark URL overrides are sent in the JSON body.
    """

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    tracker = TrackerService(base_url="http://tracker")
    result = tracker.retry_or_resume_benchmark(
        uuid4(),
        retry=True,
        retry_mode=RetryMode.FROM_SCRATCH,
        concurrency=3,
        task_ids=["task-1"],
        secrets={"ANTHROPIC_API_KEY": "new-secret"},
        benchmark_url="https://new.example",
    )

    assert result.status == "success"
    assert mock_client.params == {"retry": True, "retry_mode": "from_scratch", "concurrency": 3}
    assert mock_client.json == {
        "task_ids": ["task-1"],
        "service_headers": {},
        "secrets": {"ANTHROPIC_API_KEY": "new-secret"},
        "benchmark_url": "https://new.example",
    }

    tracker.retry_or_resume_benchmark(
        uuid4(),
        retry=False,
        retry_mode=RetryMode.AUTO,
        concurrency=0,
        task_ids=[],
    )

    assert mock_client.params == {"retry": False, "retry_mode": "auto", "concurrency": 0}


def test_tracker_client_requires_provider_secret_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing provider config should point users to the provider setup command.

    Test cases:
    - A config without legacy or named provider secrets fails with actionable remediation.
    """
    config_path = _write_valkyrie_config(tmp_path / "valkyrie.yaml")

    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))

    with pytest.raises(TrackerServiceError) as error:
        TrackerService(base_url="http://tracker")

    assert "Missing sandbox provider config" in str(error.value)
    assert "valkyrie config provider set <provider> <secret-name>" in str(error.value)


@pytest.mark.parametrize(
    ("config_overrides", "runtime_provider", "expected_provider", "expected_secret"),
    [
        pytest.param(
            {"DAYTONA_SECRET_NAME": "DaytonaSecrets"},
            None,
            "daytona",
            "DaytonaSecrets",
            id="legacy-daytona",
        ),
        pytest.param(
            {"sandbox_providers": {"daytona": "DaytonaSecrets", "modal": "ModalSecrets"}},
            None,
            "daytona",
            "DaytonaSecrets",
            id="first-named-provider",
        ),
        pytest.param(
            {
                "sandbox_providers": {"daytona": "DaytonaSecrets", "modal": "ModalSecrets"},
                "default_sandbox_provider": "modal",
            },
            None,
            "modal",
            "ModalSecrets",
            id="configured-default",
        ),
        pytest.param(
            {"sandbox_providers": {"daytona": "DaytonaSecrets", "modal": "ModalSecrets"}},
            "modal",
            "modal",
            "ModalSecrets",
            id="runtime-override",
        ),
        pytest.param(
            {"sandbox_providers": {"future": "FutureSecrets"}},
            "future",
            "future",
            "FutureSecrets",
            id="provider-not-in-tracker-enum",
        ),
    ],
)
def test_start_benchmark_resolves_provider_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
    config_overrides: dict[str, object],
    runtime_provider: str | None,
    expected_provider: str,
    expected_secret: str,
) -> None:
    """Start requests must resolve every supported provider configuration into the API payload.

    Test cases:
    - Legacy, first-named, configured-default, and runtime-selected providers resolve their secrets.
    - A newly configured provider name is forwarded without a tracker enum change.
    """
    config_path = _write_valkyrie_config(tmp_path / "valkyrie.yaml", **config_overrides)

    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    tracker = TrackerService(base_url="http://tracker")
    tracker.start_benchmark(
        contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
        benchmark_name="swebench",
        concurrency=1,
        ignore_custom_services=True,
        task_ids=None,
        slice_str=None,
        provider=runtime_provider,
    )

    assert mock_client.json is not None
    assert mock_client.json["sandbox_provider"] == expected_provider
    harness_config = mock_client.json["harness_config"]
    assert isinstance(harness_config, dict)
    assert harness_config["sandbox_provider_secret_name"] == expected_secret


def test_config_provider_commands_manage_named_provider_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Config provider commands should manage the sandbox_providers map.

    Test cases:
    - provider set creates named provider secrets without flat provider fields.
    - provider default writes a configured provider name and rejects unknown providers.
    - provider remove deletes only the requested provider.
    """
    config_path = _write_valkyrie_config(tmp_path / "valkyrie.yaml")
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))
    runner = CliRunner()

    result = runner.invoke(cli_main.cli, ["config", "provider", "set", "daytona", "DaytonaSecrets"])
    assert result.exit_code == 0
    result = runner.invoke(cli_main.cli, ["config", "provider", "set", "modal", "ModalSecrets"])
    assert result.exit_code == 0

    config = yaml.safe_load(config_path.read_text())
    assert config["sandbox_providers"] == {"daytona": "DaytonaSecrets", "modal": "ModalSecrets"}

    result = runner.invoke(cli_main.cli, ["config", "provider", "default", "modal"])
    assert result.exit_code == 0
    config = yaml.safe_load(config_path.read_text())
    assert config["default_sandbox_provider"] == "modal"

    result = runner.invoke(cli_main.cli, ["config", "provider", "default", "future"])
    assert result.exit_code != 0
    assert "not configured" in result.output

    result = runner.invoke(cli_main.cli, ["config", "provider", "remove", "daytona"])
    assert result.exit_code == 0

    config = yaml.safe_load(config_path.read_text())
    assert config["sandbox_providers"] == {"modal": "ModalSecrets"}
    assert config["default_sandbox_provider"] == "modal"


def _command_option_flags(command: click.Command, param_name: str) -> set[str]:
    param = next(param for param in command.params if param.name == param_name)
    assert isinstance(param, click.Option)
    return {*param.opts}


def test_run_commands_connect_after_success(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    """Connect should stream the run once start, resume, or retry succeeds.

    Test cases:
    - Start streams the new run ID returned by the tracker.
    - Resume and retry stream the run ID supplied by the user.
    - Connected commands skip the redundant track-progress next step.
    """
    started_run_id, streamed_run_ids, _mock_tracker_service = connect_stream_testbed
    resume_run_id = uuid4()
    retry_run_id = uuid4()

    runner = CliRunner()
    for command in (
        ["run", "start", "--agent", "agent", "--benchmark", "swebench", "--connect"],
        ["run", "resume", str(resume_run_id), "--connect"],
        ["run", "retry", str(retry_run_id), "--connect"],
    ):
        result = runner.invoke(cli_main.cli, command)
        assert result.exit_code == 0, result.output
        assert "Track progress:" not in result.output

    assert streamed_run_ids == [str(started_run_id), str(resume_run_id), str(retry_run_id)]


def test_run_retry_benchmark_url_reaches_tracker(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    """The retry command should send its benchmark URL override to the tracker.

    Test cases:
    - A retry accepts `--benchmark-url` and forwards its value.
    """
    _started_run_id, _streamed_run_ids, mock_tracker_service = connect_stream_testbed
    run_id = uuid4()

    result = CliRunner().invoke(
        cli_main.cli,
        ["run", "retry", str(run_id), "--benchmark-url", "https://new.example"],
    )

    assert result.exit_code == 0, result.output
    assert mock_tracker_service.retry_or_resume_calls[0]["kwargs"]["benchmark_url"] == "https://new.example"


def test_run_start_provider_option_reaches_tracker(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    """The CLI provider option should reach the tracker start request.

    Test cases:
    - `--provider modal` is forwarded as the runtime provider selection.
    - Provider prevalidation avoids constructing a throwaway tracker client.
    """
    _started_run_id, _streamed_run_ids, mock_tracker_service = connect_stream_testbed
    runner = CliRunner()

    result = runner.invoke(
        cli_main.cli,
        ["run", "start", "--agent", "agent", "--benchmark", "swebench", "--provider", "modal"],
    )

    assert result.exit_code == 0, result.output
    assert mock_tracker_service.provider_validations == ["modal"]
    assert mock_tracker_service.init_calls == 1
    start_kwargs = mock_tracker_service.start_calls[-1]["kwargs"]
    assert isinstance(start_kwargs, dict)
    assert start_kwargs["provider"] == "modal"


def test_run_start_sends_configured_service_auth_and_cli_headers(
    monkeypatch: pytest.MonkeyPatch,
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    """Run start should send configured benchmark auth and CLI headers to the tracker.

    Test cases:
    - Configured benchmark auth is included in the tracker start request.
    - CLI-provided Authorization overrides configured auth while preserving extra headers.
    """

    _started_run_id, _streamed_run_ids, mock_tracker_service = connect_stream_testbed

    def get_benchmark_auth(_benchmark_name: str) -> str:
        return "Bearer configured"

    monkeypatch.setattr(service_headers.TrackerService, "get_benchmark_auth", staticmethod(get_benchmark_auth))

    for header_args, expected_headers in (
        (["--header", "X-Test", "1"], {"Authorization": "Bearer configured", "X-Test": "1"}),
        (
            ["--header", "Authorization", "Bearer cli", "--header", "X-Test", "1"],
            {"Authorization": "Bearer cli", "X-Test": "1"},
        ),
    ):
        result = CliRunner().invoke(
            cli_main.cli,
            ["run", "start", "--agent", "agent", "--benchmark", "swebench", *header_args],
        )

        assert result.exit_code == 0, result.output
        start_kwargs = mock_tracker_service.start_calls[-1]["kwargs"]
        assert isinstance(start_kwargs, dict)
        assert start_kwargs["service_headers"] == expected_headers


def test_run_label_cli_options_and_client_requests(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    """Run labels should be accepted by start and sent as list filters.

    Test cases:
    - Click exposes `--label` and `-l` on run start and run list.
    - Tracker client start and list requests carry the label value.
    """
    assert _command_option_flags(start, "label") >= {"--label", "-l"}
    assert _command_option_flags(list_runs, "label") >= {"--label", "-l"}

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(_empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", _empty_config_keys)
    monkeypatch.setattr(TrackerService, "_build_harness_config_payload", harness_config_payload)

    def provider_config(_tracker: TrackerService, _provider: str | None = None) -> tuple[str, str]:
        return "daytona", "DaytonaSecrets"

    monkeypatch.setattr(TrackerService, "resolve_sandbox_provider", provider_config)
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    tracker = TrackerService(base_url="http://tracker")
    tracker.start_benchmark(
        contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
        benchmark_name="swebench",
        concurrency=1,
        ignore_custom_services=True,
        task_ids=None,
        slice_str=None,
        label="nightly",
    )
    assert mock_client.json is not None
    assert mock_client.json["label"] == "nightly"

    tracker.fetch_benchmarks(FetchBenchmarksRequest(label="nightly"))
    assert mock_client.params is not None
    assert mock_client.params["label"] == "nightly"


def test_run_label_fetch_and_list_output(capsys: pytest.CaptureFixture[str]) -> None:
    """Run fetch and list output should include labels when present.

    Test cases:
    - `valk run fetch` renders the label row.
    - `valk run list` includes a Label column.
    """
    run_id = uuid4()
    started_at = datetime.now(ZoneInfo("UTC"))
    details = BenchmarkDetails(
        status=BenchmarkStatus.IN_PROGRESS,
        started_at=started_at,
        total_tasks=1,
        finished_tasks=0,
        task_breakdown={TaskStatus.PENDING: 1},
        docent_reading_status=DocentReadingStatus.IDLE,
    )

    format_benchmark_status(
        FetchBenchmarkResponse(
            benchmark_name="swebench",
            benchmark_id=run_id,
            details=details,
            s3_bucket_url="s3://bucket/benchmarks/run",
            label="nightly",
        )
    )
    fetch_output = capsys.readouterr().out
    assert "Label:" in fetch_output
    assert "nightly" in fetch_output

    format_fetch_benchmarks_response(
        FetchBenchmarksResponse(
            benchmarks=[
                BenchmarkTableRow(
                    id=run_id,
                    name="swebench",
                    agent_name="agent",
                    model="openai/gpt-5.5",
                    dataset="default",
                    started_by_email=None,
                    started_at=started_at,
                    finished_at=None,
                    status=BenchmarkStatus.IN_PROGRESS,
                    total_tasks=1,
                    finished_tasks=0,
                    label="nightly",
                )
            ],
            total_count=1,
        )
    )
    list_output = capsys.readouterr().out
    assert "Label" in list_output
    assert "nightly" in list_output


def test_service_list_merges_hosted_and_custom_services(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_tracker_service: type[MockTrackerService],
) -> None:
    """Service list should show hosted services plus local custom overrides.

    Test cases:
    - Local custom services override hosted services with the same benchmark name.
    - Custom-only services are health-checked and included with the hosted rows.
    """
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "api_key": "test-key",
                "custom_benchmark_services": {
                    "swebench": "http://local-swebench",
                    "custombench": "http://custombench",
                },
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))

    captured_services: list[BenchmarkServiceHealth] = []

    def capture_paginated_services(
        services: list[BenchmarkServiceEntry],
        *,
        check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]],
    ) -> None:
        captured_services.extend(check_services(services))

    monkeypatch.setattr(config_benchmark_services, "TrackerService", mock_tracker_service)
    monkeypatch.setattr(config_benchmark_services, "paginate_services", capture_paginated_services)

    result = CliRunner().invoke(cli_main.cli, ["config", "service", "list"])

    assert result.exit_code == 0, result.output
    assert mock_tracker_service.require_config_values == [False]
    by_name = {service.name: service for service in captured_services}
    assert list(by_name) == ["swebench", "fab", "custombench"]
    assert by_name["swebench"].url == "http://local-swebench"
    assert by_name["fab"].url == "https://fab.benchmarks.vals.ai"
    assert by_name["custombench"].url == "http://custombench"
