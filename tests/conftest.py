from datetime import datetime
from types import SimpleNamespace
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from tracker.database.models import AgentContractRequest

from valkyrie.cli import main as cli_main


class FakeTrackerService:
    start_response: dict[str, object] = {}

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
        return httpx.Response(200, json=self.start_response)

    def fetch_benchmark(self, _run_id: object) -> SimpleNamespace:
        return SimpleNamespace(benchmark_name="swebench")

    def retry_or_resume_benchmark(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(status="success")


@pytest.fixture
def connect_stream_testbed(monkeypatch: pytest.MonkeyPatch) -> tuple[UUID, list[str]]:
    started_run_id = uuid4()
    streamed_run_ids: list[str] = []
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

    monkeypatch.setattr(cli_main, "TrackerService", FakeTrackerService)
    monkeypatch.setattr(cli_main, "get_contract_from_s3", get_contract_from_s3)
    monkeypatch.setattr(cli_main, "check_tracker_service_health", lambda _tracker: True)
    monkeypatch.setattr(cli_main, "stream_benchmark_status", stream_benchmark_status)

    return started_run_id, streamed_run_ids
