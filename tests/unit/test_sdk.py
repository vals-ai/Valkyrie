"""Tests for the public async Valkyrie SDK.

Run: uv run pytest tests/unit/test_sdk.py

Covers config validation, request construction, response parsing, streaming, and SDK errors without live services.
"""

import json
from collections.abc import Callable
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import ValidationError
from tracker.database.models import AgentContractRequest
from tracker.types import FetchBenchmarksRequest

from valkyrie.sdk import (
    ValkyrieAPIError,
    ValkyrieClient,
    ValkyrieConfig,
    ValkyrieConfigError,
    ValkyrieStreamError,
    ValkyrieTransportError,
)


def config_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "api_key": "vals-key",
        "AWS_ACCESS_KEY_ID": "aws-key",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "AWS_DEFAULT_REGION": "us-west-2",
        "AWS_SESSION_TOKEN": "aws-session",
        "S3_BUCKET": "runs-bucket",
        "LOG_GROUP": "benchmarks",
        "LOG_RETENTION_POLICY": 30,
        "sandbox_providers": {"modal": "ModalSecret", "daytona": "DaytonaSecret"},
        "default_sandbox_provider": "modal",
        "custom_benchmark_services": {"swebench": "https://local.swebench/"},
        "benchmark_auth": {"swebench": "benchmark-token"},
        "webhook": "SlackWebhook",
    }
    values.update(overrides)
    return values


def sdk_config(**overrides: object) -> ValkyrieConfig:
    return ValkyrieConfig.model_validate(config_values(**overrides))


def fetch_response(run_id: UUID, *, status: str = "IN_PROGRESS") -> dict[str, object]:
    return {
        "benchmark_name": "swebench",
        "benchmark_id": str(run_id),
        "details": {
            "status": status,
            "started_at": "2026-07-08T12:00:00Z",
            "total_tasks": 2,
            "finished_tasks": 0,
            "task_breakdown": {status: 2},
            "docent_reading_status": "IDLE",
            "docent_reading_url": None,
        },
        "s3_bucket_url": "s3://runs-bucket/benchmarks/run",
        "label": "nightly",
        "final_score": None,
    }


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    config: ValkyrieConfig | None = None,
) -> ValkyrieClient:
    return ValkyrieClient(
        config or sdk_config(),
        base_url="https://tracker.test",
        transport=httpx.MockTransport(handler),
    )


def test_config_loads_existing_yaml_shape_and_builds_headers(tmp_path: Path) -> None:
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        """
api_key: vals-key
AWS_ACCESS_KEY_ID: aws-key
AWS_SECRET_ACCESS_KEY: aws-secret
AWS_DEFAULT_REGION: us-west-2
S3_BUCKET: runs-bucket
sandbox_providers:
  daytona: DaytonaSecret
default_sandbox_provider: daytona
""".strip(),
        encoding="utf-8",
    )

    config = ValkyrieConfig.from_yaml(config_path)

    assert config.resolve_sandbox_provider() == ("daytona", "DaytonaSecret")
    assert config.request_headers()["X-Api-Key"] == "vals-key"
    assert config.request_headers()["X-Harness-Aws-Access-Key-Id"] == "aws-key"


def test_config_rejects_missing_required_values_and_invalid_provider() -> None:
    values = config_values()
    values.pop("S3_BUCKET")
    with pytest.raises(ValidationError):
        ValkyrieConfig.model_validate(values)
    with pytest.raises(ValidationError):
        sdk_config(sandbox_providers={})
    with pytest.raises(ValidationError):
        sdk_config(LOG_GROUP=" ")

    config = sdk_config()
    with pytest.raises(ValkyrieConfigError, match="Unknown sandbox provider"):
        config.resolve_sandbox_provider("unknown")


def test_from_config_wraps_file_and_yaml_errors(tmp_path: Path) -> None:
    with pytest.raises(ValkyrieConfigError, match="Could not read"):
        ValkyrieClient.from_config(tmp_path / "missing.yaml")

    invalid_path = tmp_path / "invalid.yaml"
    invalid_path.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(ValkyrieConfigError, match="must contain a YAML mapping"):
        ValkyrieClient.from_config(invalid_path)

    malformed_path = tmp_path / "malformed.yaml"
    malformed_path.write_text("[", encoding="utf-8")
    with pytest.raises(ValkyrieConfigError, match="Invalid YAML"):
        ValkyrieClient.from_config(malformed_path)

    incomplete_path = tmp_path / "incomplete.yaml"
    incomplete_path.write_text("api_key: key\n", encoding="utf-8")
    with pytest.raises(ValkyrieConfigError, match="Invalid Valkyrie config"):
        ValkyrieClient.from_config(incomplete_path)


async def test_start_normalizes_agent_and_builds_configured_payload() -> None:
    requests: list[httpx.Request] = []
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(run_id),
                "concurrency": 10,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 2,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
            },
        )

    client = make_client(handler)
    async with client:
        response = await client.runs.start(
            "sweagent",
            "swebench",
            model="claude-sonnet",
            concurrency=10,
            task_ids=["task-1", "task-2"],
            dataset="default",
            label="nightly",
            agent_kwargs={"temperature": "0"},
            secrets={"ANTHROPIC_API_KEY": "AnthropicSecret"},
            service_headers={"X-Custom": "explicit"},
            webhook_intervals=[25, 100],
        )

    assert response.benchmark_id == run_id
    request = requests[0]
    body = json.loads(request.content)
    assert request.url.path == "/start-benchmark"
    assert request.headers["x-api-key"] == "vals-key"
    assert request.headers["x-harness-aws-session-token"] == "aws-session"
    assert body["contract"] == {
        "name": "sweagent",
        "model": "claude-sonnet",
        "install_cmd": "",
        "run_cmd": "",
        "final_output": None,
        "output_artifacts": [],
        "secrets": {"ANTHROPIC_API_KEY": "AnthropicSecret"},
        "kwargs": {"temperature": "0"},
    }
    assert body["custom_benchmark_service"] == "https://local.swebench"
    assert body["service_headers"] == {"Authorization": "benchmark-token", "X-Custom": "explicit"}
    assert body["sandbox_provider"] == "modal"
    assert body["harness_config"]["sandbox_provider_secret_name"] == "ModalSecret"
    assert body["webhook_secret_name"] == "SlackWebhook"
    assert body["webhook_intervals"] == [25, 100]


async def test_start_overlays_a_supplied_contract_without_mutating_it() -> None:
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "contract-agent",
                "benchmark_id": str(uuid4()),
                "concurrency": 5,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
            },
        )

    contract = AgentContractRequest(name="contract-agent", model="old", kwargs={"keep": "yes"})
    client = make_client(handler)
    async with client:
        await client.runs.start(
            contract,
            "swebench",
            model="new",
            agent_kwargs={"added": "yes"},
            secrets={"KEY": "SecretName"},
        )

    submitted_contract = captured_body["contract"]
    assert isinstance(submitted_contract, dict)
    assert submitted_contract["name"] == "contract-agent"
    assert submitted_contract["model"] == "new"
    assert submitted_contract["kwargs"] == {"keep": "yes", "added": "yes"}
    assert submitted_contract["secrets"] == {"KEY": "SecretName"}
    assert contract.model == "old"
    assert contract.kwargs == {"keep": "yes"}


async def test_fetch_list_stop_and_s3_results_are_typed() -> None:
    run_id = uuid4()
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/fetch-benchmark":
            return httpx.Response(200, json=fetch_response(run_id))
        if request.url.path == "/fetch-benchmarks":
            return httpx.Response(200, json={"benchmarks": [], "total_count": 0, "next_cursor": None})
        if request.url.path == f"/stop-benchmark/{run_id}":
            return httpx.Response(200, json={"status": "success"})
        if request.url.path == "/retrieve-results":
            if request.url.params["s3"] == "false":
                return httpx.Response(
                    200,
                    json={
                        "benchmark_id": str(run_id),
                        "benchmark_name": "swebench",
                        "started_at": "2026-07-08T12:00:00Z",
                        "finished_at": "2026-07-08T12:01:00Z",
                        "status": "FINISHED",
                        "error_message": None,
                        "benchmark_arguments": {
                            "contract": {"name": "sweagent"},
                            "concurrency": 1,
                        },
                        "tasks_stopped": 0,
                        "final_evaluation": None,
                        "average_task_breakdown": None,
                        "evaluation_results": {},
                        "task_errors": None,
                    },
                )
            return httpx.Response(
                200,
                json={
                    "s3_url": "s3://runs-bucket/results.json",
                    "presigned_url": "https://download.test/results.json",
                    "console_url": "https://console.aws.test/results.json",
                },
            )
        raise AssertionError(f"Unexpected request: {request.url}")

    client = make_client(handler)
    async with client:
        fetched = await client.runs.fetch(run_id)
        listed = await client.runs.list(FetchBenchmarksRequest(limit=25))
        stopped = await client.runs.stop(run_id, force=True)
        inline_results = await client.runs.results(run_id)
        results = await client.runs.results(run_id, task_ids=["task-1"], upload_to_s3=True)

    assert fetched.benchmark_id == run_id
    assert listed.total_count == 0
    assert stopped.status == "success"
    assert inline_results.benchmark_id == run_id
    assert results.s3_url == "s3://runs-bucket/results.json"
    assert paths == [
        "/fetch-benchmark",
        "/fetch-benchmarks",
        f"/stop-benchmark/{run_id}",
        "/retrieve-results",
        "/retrieve-results",
    ]


@pytest.mark.parametrize(("method_name", "retry"), [("resume", "false"), ("retry", "true")])
async def test_resume_and_retry_resolve_run_service_auth(method_name: str, retry: str) -> None:
    run_id = uuid4()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/fetch-benchmark":
            return httpx.Response(200, json=fetch_response(run_id))
        return httpx.Response(200, json={"status": "success"})

    client = make_client(handler)
    async with client:
        method = getattr(client.runs, method_name)
        response = await method(
            run_id,
            concurrency=4,
            task_ids=["task-1"],
            secrets={"KEY": "SecretName"},
            service_headers={"Authorization": "override"},
            from_scratch=True,
        )

    assert response.status == "success"
    request = requests[1]
    assert request.url.params["retry"] == retry
    assert request.url.params["retry_mode"] == "from_scratch"
    assert request.url.params["concurrency"] == "4"
    assert json.loads(request.content) == {
        "task_ids": ["task-1"],
        "service_headers": {"Authorization": "override"},
        "secrets": {"KEY": "SecretName"},
    }


async def test_stream_yields_snapshots_and_stops_on_complete() -> None:
    run_id = uuid4()
    event = json.dumps(fetch_response(run_id))
    timeout: dict[str, float | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        timeout.update(request.extensions["timeout"])
        return httpx.Response(200, text=f"data: {event}\n\nevent: complete\n\n")

    client = make_client(handler)
    async with client:
        snapshots = [snapshot async for snapshot in client.runs.stream(run_id)]

    assert [snapshot.benchmark_id for snapshot in snapshots] == [run_id]
    assert timeout == {"connect": 120, "read": None, "write": 120, "pool": 120}


async def test_stream_converts_error_and_malformed_events() -> None:
    responses = iter(
        [
            httpx.Response(200, text='event: error\ndata: {"error":"run missing"}\n\n'),
            httpx.Response(200, text="data: not-json\n\n"),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    client = make_client(handler)
    async with client:
        with pytest.raises(ValkyrieStreamError, match="run missing"):
            _ = [snapshot async for snapshot in client.runs.stream(uuid4())]
        with pytest.raises(ValkyrieStreamError, match="Invalid Valkyrie run stream event"):
            _ = [snapshot async for snapshot in client.runs.stream(uuid4())]


async def test_api_and_transport_failures_use_sdk_exceptions() -> None:
    def api_error(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "invalid API key"})

    client = make_client(api_error)
    async with client:
        with pytest.raises(ValkyrieAPIError) as exc_info:
            await client.runs.fetch(uuid4())
    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "invalid API key"

    def text_error(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="upstream unavailable")

    client = make_client(text_error)
    async with client:
        with pytest.raises(ValkyrieAPIError, match="upstream unavailable"):
            await client.runs.fetch(uuid4())

    def connection_error(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    client = make_client(connection_error)
    async with client:
        with pytest.raises(ValkyrieTransportError, match="offline"):
            await client.runs.fetch(uuid4())


async def test_start_validates_inputs_before_request() -> None:
    request_count = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(500)

    client = make_client(handler)
    async with client:
        with pytest.raises(ValueError, match="concurrency"):
            await client.runs.start("agent", "swebench", concurrency=0)
        with pytest.raises(ValueError, match="agent must not be blank"):
            await client.runs.start(" ", "swebench")
        with pytest.raises(ValueError, match="benchmark must not be blank"):
            await client.runs.start("agent", " ")
        with pytest.raises(ValueError, match="mutually exclusive"):
            await client.runs.start("agent", "swebench", task_ids=["task"], slice_str=":1")
        with pytest.raises(ValueError, match="concurrency"):
            await client.runs.retry(uuid4(), concurrency=0)

    no_webhook_client = make_client(handler, config=sdk_config(webhook=None))
    async with no_webhook_client:
        with pytest.raises(ValkyrieConfigError, match="webhook_intervals require"):
            await no_webhook_client.runs.start("agent", "swebench", webhook_intervals=[50])

    invalid_interval_client = make_client(handler)
    async with invalid_interval_client:
        with pytest.raises(ValueError, match="divisible by 5"):
            await invalid_interval_client.runs.start("agent", "swebench", webhook_intervals=[23])
        with pytest.raises(ValueError, match="maximum of 3"):
            await invalid_interval_client.runs.start("agent", "swebench", webhook_intervals=[5, 10, 15, 20])

    assert request_count == 0
