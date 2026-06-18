from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tracker.database.models import RetryMode

from valkyrie.cli.runtime_config import DEV_TRACKER_URL, VALKYRIE_CONFIG_PATH_ENV_VAR, VALKYRIE_ENV_ENV_VAR
from valkyrie.cli.tracker_service import TrackerService


class FakeClient:
    def __init__(self) -> None:
        self.get_url: str | None = None
        self.params: dict[str, object] | None = None
        self.json: dict[str, object] | None = None

    def get(self, url: str) -> httpx.Response:
        self.get_url = url
        return httpx.Response(200, json={"status": "ok"})

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


def test_tracker_service_uses_selected_environment_url(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, "dev")
    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService()
    tracker.health_check()

    assert client.get_url == f"{DEV_TRACKER_URL}/health"


def test_tracker_service_reads_config_path_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "dev.yaml"
    config_path.write_text(
        "\n".join(
            [
                "AWS_ACCESS_KEY_ID: access-key",
                "AWS_SECRET_ACCESS_KEY: secret-key",
                "AWS_DEFAULT_REGION: us-east-1",
                "S3_BUCKET: dev-bucket",
                "DAYTONA_SECRET_NAME: daytona-secret",
                "LOG_GROUP: benchmarks",
                "LOG_RETENTION_POLICY: 365",
            ]
        )
    )
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))

    config = TrackerService.parse_config_keys()

    assert config["S3_BUCKET"] == "dev-bucket"
    assert config["DAYTONA_SECRET_NAME"] == "daytona-secret"


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
