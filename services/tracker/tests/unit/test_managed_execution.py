import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from benchmark_service import SandboxProviderConfig
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchBenchmarkLogSink
from tracker.aws.resolver import ManagedAWSEligibilityError
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    Org,
)
from tracker.exceptions import TrackerServiceError
from tracker.executor.dependencies import get_execution_runtime
from tracker.local.resources import LocalResources
from tracker.types import ManagedExecutionContext, RunExecutionRequest, StartBenchmarkRequest
from tracker.utils import process_benchmark, start_benchmark_request_to_benchmark
from tracker.utils.run_orchestration import (
    _parse_queued_execution,  # pyright: ignore[reportPrivateUsage]
)


_TASK_IDS = ["task-1", "task-2"]
_EXPECTED_BUCKET_OWNER = "123456789012"


@pytest.fixture
def aws_runtime() -> AWSRuntime:
    return AWSRuntime(
        AWSResources(
            region="us-east-1",
            s3_bucket="test-bucket",
            log_group="test-log-group",
            log_retention_days=30,
        ),
        AWSClientProvider("us-east-1"),
        expected_bucket_owner=_EXPECTED_BUCKET_OWNER,
    )


def _managed_request(contract: AgentContractRequest) -> RunExecutionRequest:
    return RunExecutionRequest(
        contract=contract,
        benchmark_name="test-benchmark",
        task_ids=_TASK_IDS,
        sandbox_provider="daytona",
        sandbox_provider_secret_name="sandbox-provider-secret",
    )


def _execution_context(
    request: RunExecutionRequest,
    benchmark_id: UUID,
) -> dict[str, Any]:
    return ManagedExecutionContext(
        version=2,
        benchmark_id=benchmark_id,
        verified_task_ids=_TASK_IDS,
        start_benchmark_request=request,
    ).model_dump(mode="json")


def _persist_benchmark(
    session: Session,
    request: StartBenchmarkRequest,
) -> Benchmark:
    starter = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"))
    benchmark = start_benchmark_request_to_benchmark(request, starter)
    session.add(benchmark)
    session.commit()
    return benchmark


def test_persisted_request_reconstruction_requires_a_provider_secret(
    contract: AgentContractRequest,
    database_session: Session,
) -> None:
    managed_benchmark = _persist_benchmark(database_session, _managed_request(contract))

    managed_benchmark.arguments = managed_benchmark.arguments.model_copy(update={"sandbox_provider_secret_name": None})
    with pytest.raises(ValueError, match="Managed runs require a sandbox provider secret name"):
        managed_benchmark.managed_start_benchmark_request()


def test_benchmark_creation_rejects_aws_runs_without_a_provider_secret(
    contract: AgentContractRequest,
    database_session: Session,
) -> None:
    invalid_managed_request = _managed_request(contract).model_copy(
        update={"sandbox_provider": None, "sandbox_provider_secret_name": None}
    )
    with pytest.raises(ValueError, match="Managed runs require a sandbox provider and provider secret name"):
        _persist_benchmark(database_session, invalid_managed_request)


def test_taskiq_adapter_requires_execution_context(contract: AgentContractRequest) -> None:
    request = RunExecutionRequest(
        contract=contract,
        benchmark_name="test-benchmark",
        task_ids=_TASK_IDS,
    )
    benchmark_id = uuid4()

    with pytest.raises(ValueError, match="benchmark request requires an execution context"):
        _parse_queued_execution(
            request.model_dump(mode="json"),
            str(benchmark_id),
            _TASK_IDS,
            None,
        )


def test_execution_payload_parser_accepts_v2_envelope_only(contract: AgentContractRequest) -> None:
    request = _managed_request(contract)
    benchmark_id = uuid4()

    execution = _parse_queued_execution(
        None,
        None,
        None,
        _execution_context(request, benchmark_id),
    )

    assert execution.request == RunExecutionRequest.model_validate(request.model_dump(mode="python"))
    assert execution.benchmark_id == benchmark_id
    assert execution.verified_task_ids == _TASK_IDS


async def test_taskiq_adapter_preserves_queued_local_jobs(
    contract: AgentContractRequest, database_session: Session, tmp_path: Path
) -> None:
    request = RunExecutionRequest(
        environment="local",
        properties=LocalResources(data_root=tmp_path),
        sandbox_provider="docker",
        contract=contract,
        benchmark_name="test-benchmark",
        task_ids=_TASK_IDS,
    )
    benchmark = _persist_benchmark(database_session, request)
    payload = {
        **request.model_dump(mode="json", exclude={"resolved_dataset_version"}),
        "harness_config": None,
        "webhook_secret_name": None,
        "webhook_intervals": None,
    }

    execution = _parse_queued_execution(payload, str(benchmark.id), _TASK_IDS, None)
    runtime = await get_execution_runtime(
        execution.request,
        benchmark,
        Org(id=TEST_ORG_ID, name="default"),
        context_version=execution.context_version,
    )

    assert execution.request == request
    assert execution.benchmark_id == benchmark.id
    assert execution.verified_task_ids == _TASK_IDS
    assert runtime.sandbox_provider == "docker"


def test_taskiq_adapter_rejects_mixed_and_invalid_managed_inputs(contract: AgentContractRequest) -> None:
    request = _managed_request(contract)
    benchmark_id = uuid4()
    context = _execution_context(request, benchmark_id)

    with pytest.raises(ValueError, match="must contain only an execution context"):
        _parse_queued_execution({}, None, None, context)

    invalid_version = {**context, "version": 1}
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_queued_execution(None, None, None, invalid_version)

    request_without_provider = request.model_copy(update={"sandbox_provider": "", "sandbox_provider_secret_name": None})
    context_without_provider = {
        **context,
        "start_benchmark_request": request_without_provider.model_dump(mode="json"),
    }
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_queued_execution(None, None, None, context_without_provider)


async def test_queued_execution_parse_failure_marks_run_error(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request)
    invalid_context = {**_execution_context(request, benchmark.id), "version": 1}

    await process_benchmark(execution_context_json=invalid_context, **executor_authority_kwargs(benchmark))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Queued managed execution context is invalid" in (benchmark.error_message or "")


async def test_access_key_execution_for_managed_row_marks_run_error(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    managed_request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, managed_request)

    await process_benchmark(
        start_benchmark_request_json=managed_request.model_dump(mode="json"),
        benchmark_id_str=str(benchmark.id),
        verified_task_ids=_TASK_IDS,
        **executor_authority_kwargs(benchmark),
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "benchmark request requires an execution context" in (benchmark.error_message or "")


async def test_ineligible_managed_execution_marks_run_error(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request)
    spans: list[tuple[str, dict[str, Any]]] = []

    def reject_managed_runtime(_org_id: UUID, properties: AWSResources | None = None) -> AWSRuntime:
        raise ManagedAWSEligibilityError("Managed AWS access is not available for this organization")

    def record_span(name: str, **attributes: Any) -> MagicMock:
        spans.append((name, attributes))
        return MagicMock()

    monkeypatch.setattr("tracker.aws.services.deployment_aws_runtime", reject_managed_runtime)
    monkeypatch.setattr("tracker.utils.run_orchestration.observability_span", record_span)

    await process_benchmark(
        execution_context_json=_execution_context(request, benchmark.id),
        **executor_authority_kwargs(benchmark),
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Managed AWS access is not available for this organization" in (benchmark.error_message or "")
    finalized_span = next(attributes for name, attributes in spans if name == "run.finalized")
    assert finalized_span["benchmark_id"] == str(benchmark.id)
    assert finalized_span["status"] == "ERROR"


@pytest.mark.parametrize("version", [2, 3])
async def test_managed_execution_completes_with_the_deployment_runtime(
    version: int,
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract.model_copy(update={"secrets": {"MODEL_API_KEY": "model-secret"}})).model_copy(
        update={"lambda_function": "post-run-handler", "properties": aws_runtime.resources}
    )
    benchmark = _persist_benchmark(database_session, request)
    calls: list[str] = []
    spans: list[tuple[str, dict[str, Any]]] = []
    provider_config = cast(SandboxProviderConfig, MagicMock(create_provider=MagicMock(return_value=AsyncMock())))

    def deployment_runtime(_org_id: UUID, properties: AWSResources | None = None) -> AWSRuntime:
        return aws_runtime

    async def create_log_group(_self: object, _benchmark_id: str, *, retention_days: int) -> None:
        assert retention_days == aws_runtime.resources.log_retention_days
        calls.append("logs")

    async def fetch_provider(runtime: RuntimeServices) -> SandboxProviderConfig:
        assert runtime.secrets is not aws_runtime.clients
        calls.append("provider-secret")
        return provider_config

    async def resolve_agent_secrets(_secrets: object, secret_store: object) -> dict[str, str]:
        assert secret_store is not aws_runtime.clients
        calls.append("agent-secrets")
        return {"MODEL_API_KEY": "resolved"}

    async def dry_run(clients: object, _function_name: str) -> None:
        assert clients is aws_runtime.clients
        calls.append("lambda-dry-run")

    async def upload_results(_store: object, key: str, content: bytes) -> None:
        assert key == f"benchmarks/{benchmark.id}/{benchmark.name}.json"
        result = json.loads(content)
        assert result["benchmark_id"] == str(benchmark.id)
        assert result["status"] == "FINISHED"
        calls.append("s3-final-upload")

    async def invoke_post_run(clients: object, _function_name: str, _payload: object, **_kwargs: Any) -> dict[str, Any]:
        assert clients is aws_runtime.clients
        assert isinstance(_payload, dict)
        assert _payload["bucket"] == aws_runtime.resources.s3_bucket
        calls.append("lambda-post-run")
        return {}

    def record_span(name: str, **attributes: Any) -> MagicMock:
        spans.append((name, attributes))
        return MagicMock()

    monkeypatch.setattr("tracker.aws.services.deployment_aws_runtime", deployment_runtime)
    monkeypatch.setattr(CloudWatchBenchmarkLogSink, "create_benchmark", create_log_group)
    monkeypatch.setattr("tracker.runtime.services.RuntimeServices.get_sandbox_provider_config", fetch_provider)
    monkeypatch.setattr("tracker.aws.services.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.aws.services.dry_run_lambda", dry_run)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.put_bytes", upload_results)
    monkeypatch.setattr("tracker.aws.services.invoke_lambda", invoke_post_run)
    monkeypatch.setattr("tracker.utils.run_orchestration.observability_span", record_span)

    execution_context = _execution_context(request, benchmark.id)
    execution_context["version"] = version
    # A resume may change stored inputs while this job is queued; the queued job must still run.
    benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": 20})
    database_session.add(benchmark)
    database_session.commit()

    await process_benchmark(execution_context_json=execution_context, **executor_authority_kwargs(benchmark))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert calls[:4] == ["logs", "agent-secrets", "lambda-dry-run", "provider-secret"]
    assert calls.count("agent-secrets") >= 2
    assert calls[-2:] == ["s3-final-upload", "lambda-post-run"]
    finalized_span = next(attributes for name, attributes in spans if name == "run.finalized")
    assert finalized_span["status"] == "FINISHED"


async def test_managed_execution_preflight_checks_aws_dependencies_in_order(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _managed_request(contract.model_copy(update={"secrets": {"AGENT_TOKEN": "agent-secret"}})).model_copy(
        update={"lambda_function": "result-handler"}
    )

    benchmark_id = uuid4()
    calls: list[str] = []
    provider_config = cast(SandboxProviderConfig, MagicMock(create_provider=MagicMock(return_value=AsyncMock())))

    async def create_log_group(*_args: Any, **_kwargs: Any) -> str:
        calls.append("logs")
        return "benchmark-log-group"

    async def fetch_provider(*_args: Any, **_kwargs: Any) -> SandboxProviderConfig:
        calls.append("sandbox_provider_secret")
        return provider_config

    async def resolve_agent_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        calls.append("agent_secrets")
        return {"AGENT_TOKEN": "resolved"}

    async def dry_run(*_args: Any, **_kwargs: Any) -> None:
        calls.append("lambda")

    monkeypatch.setattr(CloudWatchBenchmarkLogSink, "create_benchmark", create_log_group)
    monkeypatch.setattr("tracker.runtime.services.RuntimeServices.get_sandbox_provider_config", fetch_provider)
    monkeypatch.setattr("tracker.aws.services.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.aws.services.dry_run_lambda", dry_run)

    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider=request.sandbox_provider,
        sandbox_provider_secret_name=request.sandbox_provider_secret_name,
    )
    await runtime.prepare_execution(request, benchmark_id)
    result = await runtime.get_sandbox_provider_config()

    assert result is provider_config
    assert calls == ["logs", "agent_secrets", "lambda", "sandbox_provider_secret"]


async def test_managed_preflight_failure_happens_before_sandbox(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request)
    create_sandbox = AsyncMock()

    def deployment_runtime(_org_id: UUID, properties: AWSResources | None = None) -> AWSRuntime:
        return aws_runtime

    async def fail_log_preflight(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("managed log preflight failed")

    monkeypatch.setattr("tracker.aws.services.deployment_aws_runtime", deployment_runtime)
    monkeypatch.setattr(CloudWatchBenchmarkLogSink, "create_benchmark", fail_log_preflight)
    monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)

    await process_benchmark(
        execution_context_json=_execution_context(request, benchmark.id),
        **executor_authority_kwargs(benchmark),
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "managed log preflight failed" in (benchmark.error_message or "")
    create_sandbox.assert_not_awaited()


@pytest.mark.parametrize(
    ("version", "saved_bucket", "queued_bucket", "error"),
    [
        (3, "saved-bucket", "other-bucket", "Queued AWS resources differ from the saved run"),
        (3, "saved-bucket", None, "Queued AWS resources differ from the saved run"),
        (3, None, "queued-bucket", "Managed execution has no saved AWS resources"),
    ],
)
async def test_managed_execution_rejects_invalid_saved_storage_before_runtime(
    version: int,
    saved_bucket: str | None,
    queued_bucket: str | None,
    error: str,
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    saved = replace(aws_runtime.resources, s3_bucket=saved_bucket) if saved_bucket else None
    queued = replace(aws_runtime.resources, s3_bucket=queued_bucket) if queued_bucket else None
    request = _managed_request(contract).model_copy(update={"properties": saved})
    benchmark = _persist_benchmark(database_session, request)
    context = _execution_context(request.model_copy(update={"properties": queued}), benchmark.id)
    context["version"] = version
    runtime = AsyncMock()
    monkeypatch.setattr("tracker.executor.dependencies.CloudRuntimeFactory.create_execution_runtime", runtime)

    await process_benchmark(execution_context_json=context, **executor_authority_kwargs(benchmark))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert error in (benchmark.error_message or "")
    runtime.assert_not_awaited()


async def test_runtime_factory_rejects_queued_resources_before_aws(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _managed_request(contract).model_copy(
        update={"properties": replace(aws_runtime.resources, log_group="untrusted-log-group")}
    )
    deployment = MagicMock()
    monkeypatch.setattr("tracker.aws.services.deployment_aws_runtime", deployment)

    with pytest.raises(TrackerServiceError, match="Queued AWS resources differ from the saved run"):
        await CloudRuntimeFactory.create_execution_runtime(
            request,
            Org(id=TEST_ORG_ID, name="default", storage_key="test-bucket"),
            uuid4(),
            properties=aws_runtime.resources,
        )

    deployment.assert_not_called()
