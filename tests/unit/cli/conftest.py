from datetime import datetime
from importlib import import_module
from types import SimpleNamespace
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from click.testing import CliRunner
from tracker.database.models import AgentContractRequest
from tracker.types import BenchmarkServiceEntry, BenchmarkServiceHealth, BenchmarkServicesResponse

run_resume = import_module("valkyrie.cli.run.resume")
run_start = import_module("valkyrie.cli.run.start")


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide an isolated Click command runner."""
    return CliRunner()


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
        pass


@pytest.fixture
def mock_client() -> MockClient:
    """Provide a fresh recording HTTP client."""
    return MockClient()


class MockTrackerService:
    """Provide deterministic tracker behavior for CLI tests."""

    start_response: dict[str, object] = {}
    start_calls: list[dict[str, object]] = []
    init_calls: int = 0
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
        return SimpleNamespace(status="success")

    @classmethod
    def validate_sandbox_provider(cls, provider: str | None = None) -> tuple[str, str]:
        cls.provider_validations.append(provider)
        return provider or "daytona", "DaytonaSecrets"

    def resolve_sandbox_provider(self, provider: str | None = None) -> tuple[str, str]:
        return provider or "daytona", "DaytonaSecrets"

    def list_benchmark_services(self) -> BenchmarkServicesResponse:
        return self.check_benchmark_services(self.catalog_benchmark_services())

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
    """Reset and provide the shared tracker service mock."""
    MockTrackerService.start_calls = []
    MockTrackerService.init_calls = 0
    MockTrackerService.provider_validations = []
    MockTrackerService.require_config_values = []
    return MockTrackerService


@pytest.fixture
def connect_stream_testbed(
    monkeypatch: pytest.MonkeyPatch,
    mock_tracker_service: type[MockTrackerService],
) -> tuple[UUID, list[str], type[MockTrackerService]]:
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
