from uuid import uuid4

import httpx
import pytest
import yaml
from tracker.database.models import RetryMode

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


def test_tracker_service_accepts_neutral_provider_secret_config(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tracker config should not require Daytona-specific provider keys.

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
                "SANDBOX_PROVIDER_SECRET_NAME": "ModalSecrets",
                "LOG_GROUP": "benchmarks",
                "LOG_RETENTION_POLICY": 365,
            }
        )
    )

    monkeypatch.setattr(tracker_service_module, "_CONFIG_LOCATION", config_path)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", lambda **_kwargs: FakeClient())

    tracker = TrackerService(base_url="http://tracker")

    assert tracker._build_harness_config_payload()["sandbox_provider_secret_name"] == "ModalSecrets"
