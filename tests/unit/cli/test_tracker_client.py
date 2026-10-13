"""Tests for tracker client and related CLI rendering behavior.

Run: uv run pytest tests/unit/cli/test_tracker_client.py

Covers tracker client request construction, config handling, and CLI output helpers. Add cases here for
tracker-client behavior or CLI rendering that can regress without requiring live services.
"""

from collections.abc import Callable
from datetime import datetime
from functools import partial
from importlib import import_module
import json
from types import SimpleNamespace
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import click
import httpx
import pytest
from click.testing import CliRunner
from tracker.database.models import AgentContractRequest, BenchmarkStatus, DocentReadingStatus, RetryMode, TaskStatus
from tracker.types import (
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    StartBenchmarkRequest,
)

from valkyrie.cli import main as cli_main
from valkyrie.cli.exceptions import TrackerNotFoundError
from valkyrie.cli.run import list_runs, start
from valkyrie.cli.run.list_runs import format_fetch_benchmarks_response
from valkyrie.cli.run.progress import format_benchmark_status
from valkyrie.cli.tracker_client import TrackerService, TrackerServiceError

run_resume = import_module("valkyrie.cli.run.resume")
run_start = import_module("valkyrie.cli.run.start")


class MockClient:
    """Record tracker HTTP requests and return deterministic responses."""

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
        return None


@pytest.fixture
def mock_client() -> MockClient:
    """Provide a fresh recording HTTP client."""
    return MockClient()


def _mock_client_builder(mock_client: MockClient) -> Callable[..., MockClient]:
    def build_client(**_kwargs: object) -> MockClient:
        return mock_client

    return build_client


class MockTrackerService:
    """Provide deterministic tracker behavior for command tests in this module."""

    start_response: dict[str, object] = {}
    start_calls: list[dict[str, object]] = []
    retry_or_resume_calls: list[dict[str, object]] = []
    init_calls = 0

    def __init__(self) -> None:
        self.__class__.init_calls += 1

    def __enter__(self) -> "MockTrackerService":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def start_benchmark(self, *_args: object, **_kwargs: object) -> httpx.Response:
        self.start_calls.append({"args": _args, "kwargs": _kwargs})
        return httpx.Response(200, json=self.start_response)

    def retry_or_resume_benchmark(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
        self.retry_or_resume_calls.append({"args": _args, "kwargs": _kwargs})

        return SimpleNamespace(status="success")


@pytest.fixture
def mock_tracker_service() -> type[MockTrackerService]:
    """Reset and provide the tracker service mock used by this module."""
    MockTrackerService.start_calls = []
    MockTrackerService.retry_or_resume_calls = []
    MockTrackerService.init_calls = 0
    return MockTrackerService


@pytest.fixture
def connect_stream_testbed(
    monkeypatch: pytest.MonkeyPatch,
    mock_tracker_service: type[MockTrackerService],
) -> tuple[UUID, list[str], type[MockTrackerService]]:
    """Connect start and resume commands to a shared tracker mock."""
    started_run_id = uuid4()
    streamed_run_ids: list[str] = []
    mock_tracker_service.start_response = {
        "benchmark_name": "swebench",
        "agent_name": "agent",
        "benchmark_id": str(started_run_id),
        "concurrency": 5,
        "started_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "task_count": 1,
        "cloudwatch_url": "https://cloudwatch.example/run",
        "s3_bucket_url": "s3://bucket/benchmarks/run",
    }

    def stream_benchmark_status(_tracker: MockTrackerService, run_id: object) -> None:
        streamed_run_ids.append(str(run_id))

    monkeypatch.setattr(run_start, "TrackerService", mock_tracker_service)
    monkeypatch.setattr(run_start, "stream_benchmark_status", stream_benchmark_status)
    monkeypatch.setattr(run_resume, "TrackerService", mock_tracker_service)
    monkeypatch.setattr(run_resume, "stream_benchmark_status", stream_benchmark_status)

    return started_run_id, streamed_run_ids, mock_tracker_service


def _handle_catalog_service_request(requests: list[httpx.Request], request: httpx.Request) -> httpx.Response:
    requests.append(request)

    if request.method == "GET":
        return httpx.Response(
            200,
            json={"services": [{"name": "swebench", "url": "https://swebench.benchmarks.vals.ai/"}]},
        )

    return httpx.Response(
        200,
        json={
            "services": [
                {
                    "name": "swebench",
                    "url": "https://swebench.benchmarks.vals.ai",
                    "healthy": True,
                    "latency_ms": 12,
                    "error": None,
                }
            ]
        },
    )


def test_tracker_client_lists_catalog_services_through_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalog service listing should use tracker-owned catalog lookup and health checks.

    Test cases:
    - Hosted entries are fetched from the tracker service catalog endpoint.
    - Hosted entries are health-checked by the existing tracker endpoint.
    """

    requests: list[httpx.Request] = []
    transport = httpx.MockTransport(partial(_handle_catalog_service_request, requests))

    monkeypatch.setenv("VALKYRIE_API_KEY", "catalog-key")
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", partial(httpx.Client, transport=transport))

    tracker = TrackerService(base_url="http://tracker")
    response = tracker.list_benchmark_services()

    assert requests[0].headers["X-Api-Key"] == "catalog-key"
    assert [str(request.url) for request in requests] == [
        "http://tracker/benchmark-services",
        "http://tracker/benchmark-services",
    ]
    assert json.loads(requests[1].content) == {
        "services": [
            {
                "name": "swebench",
                "url": "https://swebench.benchmarks.vals.ai",
                "auth_header_name": None,
                "auth_secret_name": None,
            }
        ]
    }
    assert [(service.name, service.latency_ms) for service in response.services] == [("swebench", 12)]


def test_fetch_run_outputs_uses_run_outputs_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_run_outputs(run_id, task_ids=["task-1", "task-2"])

    assert response.content == b"tar"
    assert mock_client.url == f"http://tracker/fetch-run-outputs/{run_id}"
    assert mock_client.params == {"task_ids": ["task-1", "task-2"]}


def test_fetch_run_outputs_omits_empty_task_ids(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    run_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")
    response = tracker.fetch_run_outputs(run_id)

    assert response.content == b"tar"
    assert mock_client.url == f"http://tracker/fetch-run-outputs/{run_id}"
    assert mock_client.params == {}


def test_stop_benchmark_sends_task_selection(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    """Send selected task IDs in stop requests.

    Test cases:
    - Force remains a query parameter.
    - Task IDs are sent in the request body.
    """

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    run_id = uuid4()
    tracker = TrackerService(base_url="http://tracker")

    tracker.stop_benchmark(
        run_id,
        force=True,
        task_ids=["task-a", "task-b"],
    )

    assert mock_client.url == f"http://tracker/stop-benchmark/{run_id}"
    assert mock_client.params == {"force": True}
    assert mock_client.json == {"task_ids": ["task-a", "task-b"]}

    tracker.stop_benchmark(run_id, force=False)

    assert mock_client.json == {"task_ids": None}


def test_update_benchmark_concurrency_uses_patch_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Send concurrency updates to the dedicated tracker endpoint."""
    requests: list[httpx.Request] = []
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")

    def handle_request(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "benchmark_id": str(run_id),
                "status": "IN_PROGRESS",
                "concurrency": 9,
            },
        )

    transport = httpx.MockTransport(handle_request)

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", partial(httpx.Client, transport=transport))

    response = TrackerService(base_url="http://tracker").update_benchmark_concurrency(run_id, 9)

    assert len(requests) == 1
    assert requests[0].method == "PATCH"
    assert str(requests[0].url) == f"http://tracker/benchmarks/{run_id}/concurrency"
    assert json.loads(requests[0].content) == {"concurrency": 9}
    assert response.benchmark_id == run_id
    assert response.concurrency == 9


def test_update_benchmark_concurrency_surfaces_tracker_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preserve the tracker's useful rejection detail for CLI callers."""
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(409, json={"detail": "Run is currently in the FINISHED state."})
    )

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", partial(httpx.Client, transport=transport))

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(
        TrackerServiceError,
        match="Failed to update run concurrency: Run is currently in the FINISHED state",
    ):
        tracker.update_benchmark_concurrency(uuid4(), 9)


def test_shed_benchmark_posts_target_and_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Send shed requests to the tracker shed endpoint and surface its rejections.

    Test cases:
    - Concurrency and dry_run are sent in the request body and the stopped task IDs are returned.
    - A tracker rejection is raised as a TrackerServiceError with the tracker's detail.
    """
    requests: list[httpx.Request] = []
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")

    def handle_request(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if json.loads(request.content)["concurrency"] >= 4:
            return httpx.Response(400, json={"detail": "shed can only lower it."})
        return httpx.Response(
            200,
            json={"benchmark_id": str(run_id), "concurrency": 2, "task_ids": ["task-newest"]},
        )

    transport = httpx.MockTransport(handle_request)

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", partial(httpx.Client, transport=transport))
    tracker = TrackerService(base_url="http://tracker")

    response = tracker.shed_benchmark(run_id, 2, dry_run=True)

    assert requests[0].method == "POST"
    assert str(requests[0].url) == f"http://tracker/benchmarks/{run_id}/shed"
    assert json.loads(requests[0].content) == {"concurrency": 2, "dry_run": True}
    assert response.task_ids == ["task-newest"]

    with pytest.raises(TrackerServiceError, match="Failed to shed run: shed can only lower it."):
        tracker.shed_benchmark(run_id, 4, dry_run=False)


def test_tracker_client_checks_health_on_context_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commands should fail before making tracker requests when the tracker is unhealthy.

    Test cases:
    - Entering the tracker context calls /health and raises a typed tracker error.
    """
    transport = httpx.MockTransport(lambda _request: httpx.Response(503, json={"detail": "not ready"}))

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", partial(httpx.Client, transport=transport))

    with pytest.raises(TrackerNotFoundError, match="Tracker service failed to respond"):
        with TrackerService(base_url="http://tracker"):
            pass


def test_fetch_run_outputs_raises_tracker_error_for_non_ok_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class ErrorClient:
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

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(TrackerServiceError, match="Failed to fetch run outputs: No outputs found"):
        tracker.fetch_run_outputs(uuid4())


def test_fetch_run_outputs_raises_tracker_error_for_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingClient:
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

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)

    tracker = TrackerService(base_url="http://tracker")
    with pytest.raises(TrackerServiceError, match="Failed to fetch run outputs: connection failed"):
        tracker.fetch_run_outputs(uuid4())


def test_retry_or_resume_sends_retry_mode(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    """Resume requests should carry retry mode and request overrides.

    Test cases:
    - Retry mode and concurrency are query parameters.
    - An explicit zero is forwarded instead of silently using the stored concurrency.
    - Secret and benchmark URL overrides are sent in the JSON body.
    """

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

    tracker = TrackerService(base_url="http://tracker")
    result = tracker.retry_or_resume_benchmark(
        uuid4(),
        retry=True,
        retry_mode=RetryMode.FROM_SCRATCH,
        concurrency=3,
        task_ids=["task-1"],
        secrets={"ANTHROPIC_API_KEY": "new-secret"},
        benchmark_url="https://new.example",
        lambda_function="vals-format-lambda",
        update_agent=True,
    )

    assert result.status == "success"
    assert mock_client.params == {"retry": True, "retry_mode": "from_scratch", "concurrency": 3, "update_agent": True}
    assert mock_client.json == {
        "task_ids": ["task-1"],
        "service_headers": {},
        "secrets": {"ANTHROPIC_API_KEY": "new-secret"},
        "benchmark_url": "https://new.example",
        "lambda_function": "vals-format-lambda",
    }

    tracker.retry_or_resume_benchmark(
        uuid4(),
        retry=False,
        retry_mode=RetryMode.AUTO,
        concurrency=0,
        task_ids=[],
    )

    assert mock_client.params == {"retry": False, "retry_mode": "auto", "concurrency": 0, "update_agent": False}


@pytest.mark.parametrize(("runtime_provider", "ignore_custom_services"), [(None, False), ("modal", True)])
def test_start_benchmark_sends_the_provider_only_when_given(
    monkeypatch: pytest.MonkeyPatch, runtime_provider: str | None, ignore_custom_services: bool
) -> None:
    """The Tracker applies the user's default provider and the organization's secret unless a provider is named."""
    monkeypatch.setenv("VALKYRIE_API_KEY", "vals-key")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"status": "success"})

    monkeypatch.setattr(
        "valkyrie.cli.tracker_client.httpx.Client", partial(httpx.Client, transport=httpx.MockTransport(handler))
    )
    contract = AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run")

    TrackerService(base_url="http://tracker").start_benchmark(
        contract=contract,
        benchmark_name="swebench",
        concurrency=1,
        ignore_custom_services=ignore_custom_services,
        task_ids=None,
        slice_str=None,
        provider=runtime_provider,
    )

    (request,) = requests
    sent = StartBenchmarkRequest.model_validate_json(request.content)
    assert (request.url, request.headers["X-Api-Key"]) == ("http://tracker/start-benchmark", "vals-key")
    assert sent.contract.model_dump() == contract.model_dump()
    assert ("sandbox_provider" in sent.model_fields_set) is (runtime_provider is not None)
    assert sent.sandbox_provider == (runtime_provider or "daytona")
    assert not {"sandbox_provider_secret_name", "custom_benchmark_service"} & sent.model_fields_set
    assert ("ignore_custom_services" in sent.model_fields_set) is ignore_custom_services
    assert sent.ignore_custom_services is ignore_custom_services


def _command_option_flags(command: click.Command, param_name: str) -> set[str]:
    param = next(param for param in command.params if param.name == param_name)
    assert isinstance(param, click.Option)
    return {*param.opts}


def test_run_commands_connect_after_success(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    """Connect should stream the run once start, resume, or retry succeeds.

    Test cases:
    - Start streams the new run ID returned by the tracker.
    - Resume and retry stream the run ID supplied by the user.
    - Connected commands skip the redundant track-progress next step.
    """
    started_run_id, streamed_run_ids, _mock_tracker_service = connect_stream_testbed
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


def test_run_retry_benchmark_url_reaches_tracker(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    """The retry command should send its benchmark URL override to the tracker.

    Test cases:
    - A retry accepts `--benchmark-url` and forwards its value.
    """
    _started_run_id, _streamed_run_ids, mock_tracker_service = connect_stream_testbed
    run_id = uuid4()

    result = CliRunner().invoke(
        cli_main.cli,
        ["run", "retry", str(run_id), "--benchmark-url", "https://new.example", "--lambda", "vals-format-lambda"],
    )

    assert result.exit_code == 0, result.output
    assert mock_tracker_service.retry_or_resume_calls[0]["kwargs"]["benchmark_url"] == "https://new.example"
    assert mock_tracker_service.retry_or_resume_calls[0]["kwargs"]["lambda_function"] == "vals-format-lambda"


@pytest.mark.parametrize(
    ("options", "provider", "ignore_custom_services"),
    [([], None, False), (["--provider", "modal", "--ignore-custom-services"], "modal", True)],
)
def test_run_start_sends_only_the_provider_and_service_choice_it_was_given(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
    options: list[str],
    provider: str | None,
    ignore_custom_services: bool,
) -> None:
    """Without --provider, the Tracker picks the provider from the user's saved default."""
    _started_run_id, _streamed_run_ids, mock_tracker_service = connect_stream_testbed

    result = CliRunner().invoke(cli_main.cli, ["run", "start", "--agent", "agent", "--benchmark", "swebench", *options])

    assert result.exit_code == 0, result.output
    start_call = mock_tracker_service.start_calls[-1]
    assert isinstance(start_call["args"], tuple) and isinstance(start_call["kwargs"], dict)
    assert (start_call["kwargs"]["provider"], start_call["args"][3]) == (provider, ignore_custom_services)


def test_run_start_sends_cli_headers(
    connect_stream_testbed: tuple[UUID, list[str], type[MockTrackerService]],
) -> None:
    _started_run_id, _streamed_run_ids, mock_tracker_service = connect_stream_testbed
    header_args = ["--header", "Authorization", "Bearer cli", "--header", "X-Test", "1"]

    result = CliRunner().invoke(
        cli_main.cli, ["run", "start", "--agent", "agent", "--benchmark", "swebench", *header_args]
    )

    assert result.exit_code == 0, result.output
    start_kwargs = mock_tracker_service.start_calls[-1]["kwargs"]
    assert isinstance(start_kwargs, dict)
    assert start_kwargs["service_headers"] == {"Authorization": "Bearer cli", "X-Test": "1"}


def test_run_label_cli_options_and_client_requests(
    monkeypatch: pytest.MonkeyPatch,
    mock_client: MockClient,
) -> None:
    """Run labels should be accepted by start and sent as list filters.

    Test cases:
    - Click exposes `--label` and `-l` on run start and run list.
    - Tracker client start and list requests carry the label value.
    """
    assert _command_option_flags(start, "label") >= {"--label", "-l"}
    assert _command_option_flags(list_runs, "label") >= {"--label", "-l"}

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", _mock_client_builder(mock_client))

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
    assert mock_client.json is not None
    assert mock_client.json["label"] == "nightly"

    tracker.fetch_benchmarks(FetchBenchmarksRequest(label="nightly"))
    assert mock_client.params is not None
    assert mock_client.params["label"] == "nightly"


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
