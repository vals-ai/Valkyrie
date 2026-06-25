from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import yaml
from tracker.database.models import AgentContractRequest, RetryMode

from valkyrie.cli import tracker_service as tracker_service_module
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
    """Resume requests should carry retry mode and override secrets.

    Test cases:
    - Retry mode and concurrency are query parameters.
    - Secret overrides are sent in the JSON body with task IDs and service headers.
    """
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
        secrets={"ANTHROPIC_API_KEY": "new-secret"},
    )

    assert result.status == "success"
    assert client.params == {"retry": True, "retry_mode": "from_scratch", "concurrency": 3}
    assert client.json == {
        "task_ids": ["task-1"],
        "service_headers": {},
        "secrets": {"ANTHROPIC_API_KEY": "new-secret"},
    }


def test_tracker_service_accepts_provider_secret_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tracker config should accept a sandbox provider secret key.

    Test cases:
    - SANDBOX_PROVIDER_SECRET_NAME satisfies provider secret config.
    - Harness payload carries the neutral provider secret field.
    """
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "AWS_ACCESS_KEY_ID": "aws-key",
                "AWS_SECRET_ACCESS_KEY": "aws-secret",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "bucket",
                "SANDBOX_PROVIDER_SECRET_NAME": "DaytonaSecrets",
                "LOG_GROUP": "benchmarks",
                "LOG_RETENTION_POLICY": 365,
            }
        )
    )

    monkeypatch.setattr(tracker_service_module, "_CONFIG_LOCATION", config_path)
    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    tracker.start_benchmark(
        contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
        benchmark_name="swebench",
        concurrency=1,
        ignore_custom_services=True,
        task_ids=None,
        slice_str=None,
    )

    assert client.json is not None
    harness_config = client.json["harness_config"]
    assert isinstance(harness_config, dict)
    assert harness_config["sandbox_provider_secret_name"] == "DaytonaSecrets"
