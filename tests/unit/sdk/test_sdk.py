"""Tests for the public async Valkyrie SDK.

Run: uv run pytest tests/unit/sdk

Covers config validation, request construction, response parsing, streaming, and SDK errors without live services.
"""

import json
import logging
from pathlib import Path
from typing import Any, assert_type
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from tests.unit.sdk.conftest import ClientFactory, SDKConfigFactory
from valkyrie.sdk.models import AWSResources

from valkyrie.sdk import (
    AgentContractRequest,
    FetchBenchmarksRequest,
    FinalViewResponse,
    S3UploadResultsResponse,
    ValkyrieAPIError,
    ValkyrieClient,
    ValkyrieConfig,
    ValkyrieConfigError,
    ValkyrieRunAcceptedError,
    ValkyrieRunError,
    ValkyrieSDKError,
    ValkyrieStreamError,
    ValkyrieTransportError,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sdk_api"


def load_sdk_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_config_loads_api_key_and_provider_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        "api_key: vals-key\nsandbox_providers:\n  daytona: DaytonaSecret\ndefault_sandbox_provider: daytona\n",
        encoding="utf-8",
    )

    config = ValkyrieConfig.from_yaml(config_path)

    assert config.request_headers() == {"X-Api-Key": "vals-key"}
    assert config.resolve_sandbox_provider() == ("daytona", "DaytonaSecret")


def test_config_environment_selects_tracker_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sdk_config) -> None:
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        """
environment: prod
api_key: vals-key
""".strip(),
        encoding="utf-8",
    )

    client = ValkyrieClient.from_config(config_path)

    assert client.config.tracker_url == "https://benchmark-tracker-prod.vals.ai"
    assert str(client._client.base_url) == "https://benchmark-tracker-prod.vals.ai"
    assert sdk_config().tracker_url == "https://benchmark-tracker.vals.ai"
    with pytest.raises(ValidationError, match="environment"):
        sdk_config(environment="staging")


def test_config_redacts_secrets_and_unwraps_them_for_requests(sdk_config) -> None:
    config = sdk_config()

    rendered_config = f"{config!r}\n{config.model_dump_json(by_alias=True)}"
    for secret in ("vals-key", "benchmark-token"):
        assert secret not in rendered_config

    assert config.request_headers() == {"X-Api-Key": "vals-key"}


def test_config_omits_absent_api_key(sdk_config: SDKConfigFactory) -> None:
    assert sdk_config(api_key=None).request_headers() == {}


@pytest.mark.parametrize(
    ("providers", "default", "selected", "expected"),
    [
        ({"daytona": "DaytonaSecrets", "modal": "ModalSecrets"}, "modal", "daytona", ("daytona", "DaytonaSecrets")),
        ({"daytona": "DaytonaSecrets", "modal": "ModalSecrets"}, "modal", None, ("modal", "ModalSecrets")),
        ({"modal": "ModalSecrets", "daytona": "DaytonaSecrets"}, None, None, ("modal", "ModalSecrets")),
        ({}, None, None, (None, None)),
    ],
)
async def test_start_preserves_provider_selection(
    providers: dict[str, str],
    default: str | None,
    selected: str | None,
    expected: tuple[str | None, str | None],
    sdk_config: SDKConfigFactory,
) -> None:
    config = sdk_config(sandbox_providers=providers, default_sandbox_provider=default)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = json.loads(request.content)
        assert request.url.path == "/start-benchmark"
        assert request.headers["X-Api-Key"] == "vals-key"
        assert payload.get("sandbox_provider") == expected[0]
        assert payload.get("sandbox_provider_secret_name") == expected[1]
        assert "harness_config" not in payload
        assert "properties" not in payload
        return httpx.Response(200, json=load_sdk_fixture("start.json")["response"])

    async with ValkyrieClient(config, transport=httpx.MockTransport(handler)) as client:
        await client.runs.start("agent", "test", provider=selected)
        if providers:
            with pytest.raises(ValkyrieConfigError, match="Unknown sandbox provider"):
                await client.runs.start("agent", "test", provider="unknown")

    assert len(requests) == 1


def test_run_error_is_a_public_sdk_error() -> None:
    assert issubclass(ValkyrieRunError, ValkyrieSDKError)


@pytest.mark.parametrize(
    "field",
    [
        "S3_BUKET",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "DAYTONA_SECRET_NAME",
        "aws",
        "AWS_DEFAULT_REGION",
        "S3_BUCKET",
        "LOG_GROUP",
        "LOG_RETENTION_POLICY",
    ],
)
def test_config_rejects_unsupported_fields(field: str, sdk_config: SDKConfigFactory) -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        sdk_config(**{field: "private-canary"})


@pytest.mark.parametrize("field", ["S3_BUCKET", "s3_bucket", "AWS_DEFAULT_REGION", "aws_default_region"])
def test_config_rejects_client_resource_fields(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        ValkyrieConfig.model_validate({field: "value"})


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
    incomplete_path.write_text("AWS_ACCESS_KEY_ID: key\n", encoding="utf-8")
    with pytest.raises(ValkyrieConfigError, match="Invalid Valkyrie config"):
        ValkyrieClient.from_config(incomplete_path)


@pytest.mark.parametrize(
    "content",
    [
        "AWS_SECRET_ACCESS_KEY: secret-canary\n",
        "aws:\n  AWS_DEFAULT_REGION: us-west-2\n  S3_BUCKET: runs-bucket\n  credentials:\n    AWS_SECRET_ACCESS_KEY: secret-canary\n",
    ],
)
def test_from_yaml_errors_redact_rejected_credentials(tmp_path: Path, content: str) -> None:
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(content, encoding="utf-8")

    with pytest.raises(ValkyrieConfigError) as raised:
        ValkyrieConfig.from_yaml(config_path)

    assert "secret-canary" not in str(raised.value)
    assert config_path.read_text(encoding="utf-8") == content


async def test_start_normalizes_agent_and_builds_configured_payload(make_client) -> None:
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
        )

    assert response.benchmark_id == run_id
    request = requests[0]
    body = json.loads(request.content)
    assert request.url.path == "/start-benchmark"
    assert request.headers["x-api-key"] == "vals-key"
    assert not any(name.startswith("x-harness-") for name in request.headers)
    contract = body["contract"]
    assert contract["name"] == "sweagent"
    assert contract["model"] == "claude-sonnet"
    assert contract["secrets"] == {"ANTHROPIC_API_KEY": "AnthropicSecret"}
    assert contract["kwargs"] == {"temperature": "0"}
    assert body["custom_benchmark_service"] == "https://local.swebench"
    assert body["service_headers"] == {"Authorization": "benchmark-token", "X-Custom": "explicit"}
    assert "harness_config" not in body
    assert body["sandbox_provider"] == "modal"
    assert body["sandbox_provider_secret_name"] == "ModalSecret"


async def test_start_uses_api_key_without_local_resources(make_client, sdk_config) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(uuid4()),
                "concurrency": 1,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
            },
        )

    config = sdk_config(sandbox_providers={}, default_sandbox_provider=None)
    client = make_client(handler, config=config)
    async with client:
        await client.runs.start("sweagent", "swebench", ignore_custom_services=True)

    request = requests[0]

    assert request.headers["x-api-key"] == "vals-key"
    assert not any(name.lower().startswith("x-harness-") for name in request.headers)

    body = json.loads(request.content)

    assert "harness_config" not in body
    assert "sandbox_provider" not in body
    assert "sandbox_provider_secret_name" not in body


async def test_start_with_managed_storage_uses_guarded_route(make_client, sdk_config) -> None:
    run_id = uuid4()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(run_id),
                "concurrency": 1,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://vs-dev-acme-123/benchmarks/run",
                "storage_bucket": "vs-dev-acme-123",
            },
        )

    config = sdk_config()
    client = make_client(handler, config=config)
    async with client:
        response = await client.runs.start(
            "sweagent",
            "swebench",
            managed_s3_bucket="vs-dev-acme-123",
            ignore_custom_services=True,
        )

    assert response.storage_bucket == "vs-dev-acme-123"
    assert len(requests) == 1
    request = requests[0]
    assert request.url.path == "/start-benchmark-with-storage"
    body = json.loads(request.content)
    assert body["managed_s3_bucket"] == "vs-dev-acme-123"
    assert "properties" not in body
    assert "harness_config" not in body


async def test_start_without_managed_storage_uses_ordinary_route(make_client) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(uuid4()),
                "concurrency": 1,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
            },
        )

    async with make_client(handler) as client:
        response = await client.runs.start("sweagent", "swebench")

    assert response.storage_bucket is None
    assert [request.url.path for request in requests] == ["/start-benchmark"]


@pytest.mark.parametrize("returned_bucket", [None, "vs-dev-other-456"])
async def test_start_with_managed_storage_rejects_unconfirmed_bucket(
    make_client,
    sdk_config,
    returned_bucket: str | None,
) -> None:
    run_id = uuid4()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(run_id),
                "concurrency": 1,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
                "storage_bucket": returned_bucket,
            },
        )

    config = sdk_config()
    async with make_client(handler, config=config) as client:
        with pytest.raises(ValkyrieRunError, match=str(run_id)) as error:
            await client.runs.start(
                "sweagent",
                "swebench",
                managed_s3_bucket="vs-dev-acme-123",
            )

    assert error.value.run_id == run_id
    assert not isinstance(error.value, ValkyrieRunAcceptedError)
    assert ValkyrieRunError("invalid input").run_id is None
    assert str(ValkyrieRunError("invalid input")) == "invalid input"


async def test_start_with_managed_storage_rejects_explicit_resources_before_request(make_client: ClientFactory) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("Conflicting resources must fail before HTTP")

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieRunError):
            await client.runs.start(
                "sweagent",
                "swebench",
                managed_s3_bucket="vs-dev-acme-123",
                properties=AWSResources(region="us-east-1", s3_bucket="custom", log_group="logs", log_retention_days=7),
            )


async def test_start_can_omit_optional_run_configuration(make_client, sdk_config) -> None:
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(uuid4()),
                "concurrency": 5,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
            },
        )

    client = make_client(handler, config=sdk_config(benchmark_auth={}))
    async with client:
        await client.runs.start("sweagent", "swebench", ignore_custom_services=True)

    assert captured_body["custom_benchmark_service"] is None
    assert captured_body["service_headers"] == {}
    assert captured_body["concurrency"] == 5
    assert "priority" not in captured_body
    assert "environment" not in captured_body
    assert "properties" not in captured_body


@pytest.mark.parametrize(
    "properties",
    [
        None,
        AWSResources(
            region="us-east-1",
            s3_bucket="custom-bucket",
            log_group="custom-logs",
            log_retention_days=7,
        ),
    ],
)
async def test_start_serializes_explicit_queue_priority(make_client, sdk_config, properties) -> None:
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "benchmark_name": "swebench",
                "agent_name": "sweagent",
                "benchmark_id": str(uuid4()),
                "concurrency": 5,
                "started_at": "2026-07-08T12:00:00Z",
                "task_count": 1,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/run",
            },
        )

    client = make_client(handler, config=sdk_config())
    async with client:
        await client.runs.start("sweagent", "swebench", priority=3, properties=properties)

    assert captured_body["priority"] == 3
    if properties is None:
        assert "properties" not in captured_body
    else:
        assert captured_body["properties"] == properties.model_dump(mode="json")


async def test_start_overlays_a_supplied_contract_without_mutating_it(make_client) -> None:
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


async def test_fetch_list_stop_and_s3_results_are_typed(make_client, fetch_response) -> None:
    run_id = uuid4()
    paths: list[str] = []
    preview_query: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/fetch-benchmark":
            return httpx.Response(200, json=fetch_response(run_id))
        if request.url.path == "/fetch-benchmarks":
            return httpx.Response(200, json={"benchmarks": [], "total_count": 0, "next_cursor": None})
        if request.url.path == f"/stop-benchmark/{run_id}":
            return httpx.Response(200, json={"status": "success"})
        if request.url.path == "/preview-results":
            preview_query.extend(request.url.params.multi_items())
            return httpx.Response(
                200,
                json={
                    "s3_url": "s3://runs-bucket/results.json",
                    "presigned_url": "https://download.test/preview.json",
                    "console_url": "https://console.aws.test/preview.json",
                },
            )
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
                            "environment": "aws",
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
        preview = await client.runs.preview(run_id, task_ids=["task-1"])

    assert fetched.benchmark_id == run_id
    assert listed.total_count == 0
    assert stopped.status == "success"
    assert_type(inline_results, FinalViewResponse)
    assert_type(results, S3UploadResultsResponse)
    assert inline_results.benchmark_id == run_id
    assert results.s3_url == "s3://runs-bucket/results.json"
    assert results.expires_in == 86400
    assert preview.presigned_url == "https://download.test/preview.json"
    assert preview_query == [("benchmark_id", str(run_id)), ("task_ids", "task-1")]
    assert paths == [
        "/fetch-benchmark",
        "/fetch-benchmarks",
        f"/stop-benchmark/{run_id}",
        "/retrieve-results",
        "/retrieve-results",
        "/preview-results",
    ]


@pytest.mark.parametrize(("method_name", "retry"), [("resume", "false"), ("retry", "true")])
async def test_resume_and_retry_resolve_run_service_auth(
    method_name: str, retry: str, make_client, fetch_response
) -> None:
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
            update_agent=True,
            benchmark_url="https://new.example",
        )

    assert response.status == "success"
    request = requests[1]
    assert request.url.params["update_agent"] == "true"
    assert request.url.params["retry"] == retry
    assert request.url.params["retry_mode"] == "from_scratch"
    assert request.url.params["concurrency"] == "4"
    assert json.loads(request.content) == {
        "benchmark_url": "https://new.example",
        "task_ids": ["task-1"],
        "service_headers": {"Authorization": "override"},
        "secrets": {"KEY": "SecretName"},
    }


async def test_resume_without_optional_overrides_uses_empty_payload(make_client, fetch_response, sdk_config) -> None:
    run_id = uuid4()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/fetch-benchmark":
            return httpx.Response(200, json=fetch_response(run_id))
        return httpx.Response(200, json={"status": "success"})

    client = make_client(handler, config=sdk_config(benchmark_auth={}))
    async with client:
        await client.runs.resume(run_id)

    request = requests[1]
    assert request.url.params["update_agent"] == "false"
    assert "concurrency" not in request.url.params
    assert json.loads(request.content) == {"task_ids": [], "service_headers": {}, "secrets": {}}


async def test_resume_request_matches_canonical_wire_fixture(make_client, sdk_config) -> None:
    fixture = load_sdk_fixture("retry_resume.json")
    run_id = uuid4()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/fetch-benchmark":
            response = load_sdk_fixture("fetch.json")["response"]
            response["benchmark_id"] = str(run_id)
            return httpx.Response(200, json=response)
        return httpx.Response(200, json=fixture["response"])

    client = make_client(handler, config=sdk_config(benchmark_auth={}))
    async with client:
        await client.runs.resume(
            run_id,
            concurrency=fixture["query"]["concurrency"],
            task_ids=fixture["body"]["task_ids"],
        )

    request = requests[1]
    assert dict(request.url.params) == {
        "retry": str(fixture["query"]["retry"]).lower(),
        "update_agent": str(fixture["query"]["update_agent"]).lower(),
        "retry_mode": fixture["query"]["retry_mode"],
        "concurrency": str(fixture["query"]["concurrency"]),
    }
    assert json.loads(request.content) == fixture["body"]


async def test_stream_yields_snapshots_and_stops_on_complete(make_client, fetch_response) -> None:
    event = load_sdk_fixture("fetch.json")["sse"]
    run_id = event["data"]["benchmark_id"]
    event_prefix = f"event: {event['event']}\n" if event["event"] else ""
    wire_event = f"{event_prefix}data: {json.dumps(event['data'])}\n\nevent: complete\n\n"
    timeout: dict[str, float | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        timeout.update(request.extensions["timeout"])
        return httpx.Response(200, text=wire_event)

    client = make_client(handler)
    async with client:
        snapshots = [snapshot async for snapshot in client.runs.stream(run_id)]

    assert [str(snapshot.benchmark_id) for snapshot in snapshots] == [run_id]
    assert timeout == {"connect": 120, "read": None, "write": 120, "pool": 120}


async def test_stream_parses_eof_after_ignoring_empty_events(make_client, fetch_response) -> None:
    run_id = uuid4()
    event = json.dumps(fetch_response(run_id))
    responses = iter(
        [
            httpx.Response(
                200,
                text=f"\n: keepalive\n\nevent: message\n\nevent: message\ndata: {event}",
            ),
            httpx.Response(200, text=""),
            httpx.Response(200, text="event: disconnect"),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    client = make_client(handler)
    async with client:
        snapshots = [snapshot async for snapshot in client.runs.stream(run_id)]
        empty_snapshots = [snapshot async for snapshot in client.runs.stream(run_id)]
        disconnected_snapshots = [snapshot async for snapshot in client.runs.stream(run_id)]

    assert [snapshot.benchmark_id for snapshot in snapshots] == [run_id]
    assert empty_snapshots == []
    assert disconnected_snapshots == []


async def test_stream_converts_error_and_malformed_events(make_client) -> None:
    responses = iter(
        [
            httpx.Response(200, text='event: error\ndata: {"error":"run missing"}\n\n'),
            httpx.Response(200, text="event: error\ndata: plain error\n\n"),
            httpx.Response(200, text="data: not-json\n\n"),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    client = make_client(handler)
    async with client:
        with pytest.raises(ValkyrieStreamError, match="run missing"):
            _ = [snapshot async for snapshot in client.runs.stream(uuid4())]
        with pytest.raises(ValkyrieStreamError, match="plain error"):
            _ = [snapshot async for snapshot in client.runs.stream(uuid4())]
        with pytest.raises(ValkyrieStreamError, match="Invalid Valkyrie run stream event"):
            _ = [snapshot async for snapshot in client.runs.stream(uuid4())]


async def test_stream_converts_status_and_transport_failures(
    make_client,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Convert stream failures to SDK errors.

    Test cases:
    - API failures retain their response detail.
    - HTTPX failures are logged before conversion.
    """

    def status_error(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "run missing"})

    client = make_client(status_error)
    async with client:
        with pytest.raises(ValkyrieAPIError, match="run missing"):
            _ = [snapshot async for snapshot in client.runs.stream(uuid4())]

    def transport_error(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("stream interrupted", request=request)

    client = make_client(transport_error)
    with caplog.at_level(logging.WARNING, logger="valkyrie.sdk.errors"):
        async with client:
            with pytest.raises(ValkyrieTransportError, match="stream interrupted"):
                _ = [snapshot async for snapshot in client.runs.stream(uuid4())]

    assert "Valkyrie stream failed: stream interrupted" in caplog.text


async def test_api_and_transport_failures_use_sdk_exceptions(make_client) -> None:
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


async def test_start_validates_inputs_before_request(make_client, sdk_config) -> None:
    request_count = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(500)

    client = make_client(handler)
    async with client:
        with pytest.raises(ValkyrieSDKError, match="concurrency") as exc_info:
            await client.runs.start("agent", "swebench", concurrency=0)
        assert isinstance(exc_info.value, ValkyrieRunError)
        with pytest.raises(ValkyrieSDKError, match="priority must be"):
            await client.runs.start("agent", "swebench", priority=5)
        with pytest.raises(ValkyrieSDKError, match="agent must not be blank") as exc_info:
            await client.runs.start(" ", "swebench")
        assert isinstance(exc_info.value, ValkyrieRunError)
        with pytest.raises(ValkyrieSDKError, match="benchmark must not be blank") as exc_info:
            await client.runs.start("agent", " ")
        assert isinstance(exc_info.value, ValkyrieRunError)
        with pytest.raises(ValkyrieSDKError, match="mutually exclusive") as exc_info:
            await client.runs.start("agent", "swebench", task_ids=["task"], slice_str=":1")
        assert isinstance(exc_info.value, ValkyrieRunError)
        with pytest.raises(ValkyrieSDKError, match="concurrency") as exc_info:
            await client.runs.retry(uuid4(), concurrency=0)
        assert isinstance(exc_info.value, ValkyrieRunError)
    assert request_count == 0


@pytest.mark.parametrize("provider", [None, "modal"])
async def test_tracker_url_only_start_sends_configuration_without_credentials(provider: str | None) -> None:
    config = ValkyrieConfig(tracker_url="http://127.0.0.1:8765")

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if provider is None:
            assert "sandbox_provider" not in payload
        else:
            assert payload["sandbox_provider"] == provider
        assert "execution_secrets" not in payload
        assert "properties" not in payload
        assert "environment" not in payload
        assert request.url.host == "127.0.0.1"
        assert "harness_config" not in payload
        assert "sandbox_provider_secret_name" not in payload
        assert not config.request_headers()
        return httpx.Response(200, json=load_sdk_fixture("start.json")["response"])

    async with ValkyrieClient(config, transport=httpx.MockTransport(handler)) as client:
        await client.runs.start("agent", "test", provider=provider)
