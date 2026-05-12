from uuid import uuid4

import httpx
import pytest
from tracker.database.models import RetryMode

from valkyrie.cli.tracker_service import TrackerService


class FakeClient:
    def __init__(self) -> None:
        self.params: dict[str, object] | None = None
        self.json: dict[str, object] | None = None

    def post(self, _url: str, *, params: dict[str, object], json: dict[str, object]) -> httpx.Response:
        self.params = params
        self.json = json
        return httpx.Response(200, json={"status": "success"})

    def close(self) -> None:
        pass


def empty_config() -> dict[str, object]:
    return {}


def empty_config_keys(_tracker: TrackerService) -> dict[str, str]:
    return {}


def test_harness_payload_includes_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    def config_keys(_tracker: TrackerService) -> dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": "access-key",
            "AWS_SECRET_ACCESS_KEY": "secret-key",
            "AWS_SESSION_TOKEN": "session-token",
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "bucket",
            "LOG_GROUP": "logs",
            "LOG_RETENTION_POLICY": "365",
            "DAYTONA_SECRET_NAME": "daytona",
        }

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", config_keys)

    tracker = TrackerService(base_url="http://tracker")

    assert tracker._build_harness_config_payload()["aws"] == {
        "aws_access_key_id": "access-key",
        "aws_secret_access_key": "secret-key",
        "aws_session_token": "session-token",
        "aws_default_region": "us-east-1",
    }


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
