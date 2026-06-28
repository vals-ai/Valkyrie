from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from uuid import uuid4

import click
import httpx
import pytest
import yaml
from click.testing import CliRunner
from tracker.database.models import AgentContractRequest, BenchmarkStatus, DocentReadingStatus, RetryMode, TaskStatus
from tracker.types import (
    BenchmarkDetails,
    BenchmarkServiceEntry,
    BenchmarkServiceHealth,
    BenchmarkServicesResponse,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
)

from valkyrie.cli import main as cli_main
from valkyrie.cli import tracker_service as tracker_service_module
from valkyrie.cli.main import cli, list_benchmarks, start
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import format_benchmark_status, format_fetch_benchmarks_response, paginate_services


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

    def get(self, _url: str, *, params: dict[str, object] | None = None) -> httpx.Response:
        self.params = params
        return httpx.Response(200, json={"benchmarks": [], "total_count": 0})

    def close(self) -> None:
        pass


def empty_config() -> dict[str, object]:
    return {}


def empty_config_keys(_tracker: TrackerService) -> dict[str, str]:
    return {}


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


def test_paginate_services_renders_latency_as_response_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Service list should show response latency when available and dash when not.

    Test cases:
    - Responding services render measured latency.
    - Non-responding services render a dash without healthy/error columns.
    """
    captured_rows: list[dict[str, str]] = []
    captured_headers: list[str] = []

    def fake_format_table(
        rows: list[dict[str, str]],
        headers: list[str],
        _current_page: int,
        _total_pages: int,
        _total_count: int,
        _item_name: str,
    ) -> None:
        captured_rows.extend(rows)
        captured_headers.extend(headers)

    monkeypatch.setattr(cli_main.click, "clear", lambda: None)
    monkeypatch.setattr("valkyrie.cli.utils.format_table", fake_format_table)

    paginate_services(
        [
            BenchmarkServiceHealth(
                name="swebench",
                url="https://swebench.benchmarks.vals.ai",
                healthy=True,
                latency_ms=23,
                source="hosted",
            ),
            BenchmarkServiceHealth(
                name="vcb",
                url="http://localhost:9000",
                healthy=False,
                latency_ms=None,
                error="[Errno -2] Name or service not known",
                source="custom",
            ),
        ]
    )

    assert captured_headers == ["Benchmark", "Service URL", "Source", "Latency"]
    assert [row["Source"] for row in captured_rows] == ["benchmarks.vals.ai", "localhost:9000"]
    assert [row["Latency"] for row in captured_rows] == ["23 ms", "-"]


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


def _command_option_flags(command: click.Command, param_name: str) -> set[str]:
    param = next(param for param in command.params if param.name == param_name)
    assert isinstance(param, click.Option)
    return {*param.opts}


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


def test_service_list_merges_hosted_and_custom_services(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Service list should show hosted services plus local custom overrides.

    Test cases:
    - Local custom services override hosted services with the same benchmark name.
    - Custom-only services are health-checked and included with the hosted rows.
    """
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "api_key": "test-key",
                "custom_benchmark_services": {
                    "swebench": "http://local-swebench",
                    "custombench": "http://custombench",
                },
            },
            sort_keys=False,
        )
    )
    monkeypatch.setattr(cli_main, "CONFIG_LOCATION", config_path)

    class FakeTracker:
        def __enter__(self) -> "FakeTracker":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def list_benchmark_services(self) -> BenchmarkServicesResponse:
            return BenchmarkServicesResponse(
                services=[
                    BenchmarkServiceHealth(
                        name="swebench",
                        url="https://swebench.benchmarks.vals.ai",
                        healthy=True,
                        latency_ms=10,
                        source="hosted",
                    ),
                    BenchmarkServiceHealth(
                        name="fab",
                        url="https://fab.benchmarks.vals.ai",
                        healthy=True,
                        latency_ms=20,
                        source="hosted",
                    ),
                ]
            )

        def check_benchmark_services(self, services: list[BenchmarkServiceEntry]) -> BenchmarkServicesResponse:
            assert [(service.name, service.url) for service in services] == [
                ("swebench", "http://local-swebench"),
                ("custombench", "http://custombench"),
            ]
            return BenchmarkServicesResponse(
                services=[
                    BenchmarkServiceHealth(
                        name="swebench",
                        url="http://local-swebench",
                        healthy=False,
                        latency_ms=None,
                        error="timeout",
                        source="custom",
                    ),
                    BenchmarkServiceHealth(
                        name="custombench",
                        url="http://custombench",
                        healthy=True,
                        latency_ms=5,
                        source="custom",
                    ),
                ]
            )

    captured_services: list[BenchmarkServiceHealth] = []

    def fake_paginate_services(services: list[BenchmarkServiceHealth]) -> None:
        captured_services.extend(services)

    monkeypatch.setattr(cli_main, "TrackerService", FakeTracker)
    monkeypatch.setattr(cli_main, "paginate_services", fake_paginate_services)

    result = CliRunner().invoke(cli, ["config", "service", "list"])

    assert result.exit_code == 0, result.output
    by_name = {service.name: service for service in captured_services}
    assert list(by_name) == ["swebench", "fab", "custombench"]
    assert by_name["swebench"].url == "http://local-swebench"
    assert by_name["swebench"].source == "custom override"
    assert by_name["fab"].source == "hosted"
    assert by_name["custombench"].source == "custom"
