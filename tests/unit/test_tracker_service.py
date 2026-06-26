from datetime import datetime
from pathlib import Path
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
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
)

from valkyrie.cli import tracker_service as tracker_service_module
from valkyrie.cli import main as cli_main
from valkyrie.cli.main import list_benchmarks, start
from valkyrie.cli.tracker_service import TrackerService, TrackerServiceError
from valkyrie.cli.utils import format_benchmark_status, format_fetch_benchmarks_response


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


def empty_config_keys(_tracker: TrackerService) -> dict[str, str]:
    return {}


def test_fetch_run_outputs_uses_run_outputs_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    run_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_run_outputs(run_id, task_ids=["task-1", "task-2"])

    assert response.content == b"tar"
    assert client.url == f"http://tracker/fetch-run-outputs/{run_id}"
    assert client.params == {"task_ids": ["task-1", "task-2"]}


def test_fetch_run_outputs_omits_empty_task_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    run_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_run_outputs(run_id)

    assert response.content == b"tar"
    assert client.url == f"http://tracker/fetch-run-outputs/{run_id}"
    assert client.params == {}


def test_fetch_run_outputs_raises_tracker_error_for_non_ok_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class ErrorClient(FakeClient):
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

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(TrackerServiceError, match="Failed to fetch run outputs: No outputs found"):
        tracker.fetch_run_outputs(uuid4())


def test_fetch_run_outputs_raises_tracker_error_for_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingClient(FakeClient):
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

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(TrackerServiceError, match="Failed to fetch run outputs: connection failed"):
        tracker.fetch_run_outputs(uuid4())


def harness_config_payload(_tracker: TrackerService) -> dict[str, object]:
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


def test_tracker_service_accepts_legacy_daytona_secret_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "AWS_ACCESS_KEY_ID": "aws-key",
                "AWS_SECRET_ACCESS_KEY": "aws-secret",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "bucket",
                "DAYTONA_SECRET_NAME": "DaytonaSecrets",
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


def test_tracker_service_requires_provider_secret_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "AWS_ACCESS_KEY_ID": "aws-key",
                "AWS_SECRET_ACCESS_KEY": "aws-secret",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "bucket",
                "LOG_GROUP": "benchmarks",
                "LOG_RETENTION_POLICY": 365,
            }
        )
    )

    monkeypatch.setattr(tracker_service_module, "_CONFIG_LOCATION", config_path)

    with pytest.raises(TrackerServiceError, match="SANDBOX_PROVIDER_SECRET_NAME"):
        TrackerService(base_url="http://tracker")


def _command_option_flags(command: click.Command, param_name: str) -> set[str]:
    param = next(param for param in command.params if param.name == param_name)
    assert isinstance(param, click.Option)
    return {*param.opts}


def test_run_commands_connect_after_success(connect_stream_testbed: tuple[UUID, list[str]]) -> None:
    """Connect should stream the run once start, resume, or retry succeeds.

    Test cases:
    - Start streams the new run ID returned by the tracker.
    - Resume and retry stream the run ID supplied by the user.
    - Connected commands skip the redundant track-progress next step.
    """
    started_run_id, streamed_run_ids = connect_stream_testbed
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


def test_run_label_cli_options_and_client_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run labels should be accepted by start and sent as list filters.

    Test cases:
    - Click exposes `--label` and `-l` on run start and run list.
    - Tracker client start and list requests carry the label value.
    """
    assert _command_option_flags(start, "label") >= {"--label", "-l"}
    assert _command_option_flags(list_benchmarks, "label") >= {"--label", "-l"}

    client = FakeClient()

    def build_client(**_kwargs: object) -> FakeClient:
        return client

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(empty_config))
    monkeypatch.setattr(TrackerService, "parse_config_keys", empty_config_keys)
    monkeypatch.setattr(TrackerService, "_build_harness_config_payload", harness_config_payload)
    monkeypatch.setattr("valkyrie.cli.tracker_service.httpx.Client", build_client)

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
    assert client.json is not None
    assert client.json["label"] == "nightly"

    tracker.fetch_benchmarks(FetchBenchmarksRequest(label="nightly"))
    assert client.params is not None
    assert client.params["label"] == "nightly"


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
