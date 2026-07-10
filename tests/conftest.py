from datetime import datetime
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
import yaml
from tracker.database.models import AgentContractRequest
from tracker.types import BenchmarkServiceEntry, BenchmarkServiceHealth, BenchmarkServicesResponse

run_resume = import_module("valkyrie.cli.run.resume")
run_start = import_module("valkyrie.cli.run.start")


class FakeClient:
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


def empty_config() -> dict[str, object]:
    return {}


def empty_config_keys(_tracker: object) -> dict[str, str]:
    return {}


def write_valkyrie_config(config_path: Path, **overrides: object) -> Path:
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


class FakeTrackerService:
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

    def __enter__(self) -> "FakeTrackerService":
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
def connect_stream_testbed(monkeypatch: pytest.MonkeyPatch) -> tuple[UUID, list[str]]:
    started_run_id = uuid4()
    streamed_run_ids: list[str] = []
    FakeTrackerService.start_calls = []
    FakeTrackerService.init_calls = 0
    FakeTrackerService.provider_validations = []
    FakeTrackerService.require_config_values = []
    FakeTrackerService.start_response = {
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

    def stream_benchmark_status(_tracker: FakeTrackerService, run_id: object) -> None:
        streamed_run_ids.append(str(run_id))

    monkeypatch.setattr(run_start, "TrackerService", FakeTrackerService)
    monkeypatch.setattr(run_start, "get_contract_from_s3", get_contract_from_s3)
    monkeypatch.setattr(run_start, "stream_benchmark_status", stream_benchmark_status)
    monkeypatch.setattr(run_resume, "TrackerService", FakeTrackerService)
    monkeypatch.setattr(run_resume, "stream_benchmark_status", stream_benchmark_status)

    return started_run_id, streamed_run_ids
