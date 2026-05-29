from uuid import uuid4

import httpx
import pytest
from tracker.database.models import AgentContractRequest, RetryMode

from valkyrie.cli.tracker_service import TrackerService


class FakeClient:
    def __init__(self) -> None:
        self.params: dict[str, object] | None = None
        self.json: dict[str, object] | None = None

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


def test_start_benchmark_without_config_sends_no_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unauthenticated hosted requests should rely on tracker deploy-time config.

    Test cases:
    - Missing local config does not block tracker client creation.
    - Start request sends no auth headers and omits harness_config.
    """
    client = FakeClient()

    def build_client(**kwargs: object) -> FakeClient:
        assert kwargs["headers"] == {}
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    response = tracker.start_benchmark(
        contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
        benchmark_name="swebench",
        concurrency=1,
        ignore_custom_services=True,
        task_ids=None,
        slice_str=None,
    )

    assert response.status_code == 200
    assert client.json is not None
    assert "harness_config" not in client.json


def test_configured_api_key_is_not_sent_for_unauthenticated_hosted_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy API keys should not be sent when tracker auth is disabled.

    Test cases:
    - API key-only config does not require AWS, S3, or Daytona config values.
    - X-Api-Key is omitted so it cannot be forwarded as benchmark auth.
    """
    client = FakeClient()

    def load_hosted_config() -> dict[str, object]:
        return {"api_key": "old-hosted-key"}

    def build_client(**kwargs: object) -> FakeClient:
        assert kwargs["headers"] == {}
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(load_hosted_config))
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    response = tracker.start_benchmark(
        contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
        benchmark_name="swebench",
        concurrency=1,
        ignore_custom_services=True,
        task_ids=None,
        slice_str=None,
    )

    assert response.status_code == 200
    assert client.json is not None
    assert "harness_config" not in client.json
