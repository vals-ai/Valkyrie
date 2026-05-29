from uuid import uuid4

import httpx
import pytest
from tracker.database.models import RetryMode

from valkyrie.cli.tracker_service import TrackerService


class FakeClient:
    def __init__(self) -> None:
        self.params: dict[str, object] | None = None
        self.json: dict[str, object] | None = None
        self.path: str | None = None

    def post(
        self,
        _url: str,
        *,
        params: dict[str, object] | None = None,
        json: dict[str, object],
    ) -> httpx.Response:
        self.params = params
        self.json = json
        return httpx.Response(200, json={"status": "success"})

    def get(self, url: str, *, params: dict[str, object] | None = None) -> httpx.Response:
        self.path = url
        self.params = params
        return httpx.Response(
            200,
            json={
                "benchmark_id": "bench-1",
                "task_id": "task-1",
                "events": [{"timestamp": 123, "message": "hello\n", "log_stream_name": "task-1_abc"}],
                "next_token": "next",
            },
        )

    def close(self) -> None:
        pass


def empty_config() -> dict[str, object]:
    return {}


def empty_config_keys(_tracker: TrackerService) -> dict[str, str]:
    return {}


def test_retry_or_resume_sends_retry_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    result = tracker.retry_or_resume_benchmark(
        uuid4(),
        retry=True,
        retry_mode=RetryMode.FROM_SCRATCH,
        concurrency=3,
        task_ids=["task-1"],
    )

    assert result.status == "success"
    assert client.params == {"retry": True, "retry_mode": "from_scratch", "concurrency": 3}
    assert client.json == {"task_ids": ["task-1"], "service_headers": {}}


def test_fetch_benchmark_logs_sends_task_query(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    benchmark_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")
    result = tracker.fetch_benchmark_logs(benchmark_id, task_id="task-1", next_token="token", limit=25)

    assert client.path == f"http://tracker/fetch-benchmark-logs/{benchmark_id}"
    assert client.params == {"task_id": "task-1", "next_token": "token", "limit": 25}
    assert result["events"][0]["message"] == "hello\n"
