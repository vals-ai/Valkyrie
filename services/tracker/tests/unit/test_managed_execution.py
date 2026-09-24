"""Tests for managed and access-key executor inputs.

Run: uv run pytest tests/unit/test_managed_execution.py
"""

from dataclasses import replace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from benchmark_service import SandboxProviderConfig
from fastapi import HTTPException
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchBenchmarkLogSink
from tracker.aws.managed_storage import ManagedStorageError
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import AgentContractRequest, Benchmark, Org
from tracker.exceptions import TrackerServiceError
from tracker.types import HarnessConfig, ManagedExecutionContext, StartBenchmarkRequest
from tracker.utils import start_benchmark_request_to_benchmark
from tracker.utils.run_orchestration import (
    parse_queued_execution,
)


_TASK_IDS = ["task-1", "task-2"]
_EXPECTED_BUCKET_OWNER = "123456789012"


@pytest.fixture
def aws_runtime(harness_config: HarnessConfig) -> AWSRuntime:
    resources = AWSRuntime.from_harness_config(harness_config).resources
    return AWSRuntime(
        resources,
        DefaultChainAWSClientProvider(resources.region),
        expected_bucket_owner=_EXPECTED_BUCKET_OWNER,
    )


def _access_key_request(contract: AgentContractRequest, harness_config: HarnessConfig) -> StartBenchmarkRequest:
    return StartBenchmarkRequest(
        contract=contract,
        benchmark_name="test-benchmark",
        task_ids=_TASK_IDS,
        harness_config=harness_config,
    )


def _managed_request(contract: AgentContractRequest) -> StartBenchmarkRequest:
    return StartBenchmarkRequest(
        contract=contract,
        benchmark_name="test-benchmark",
        task_ids=_TASK_IDS,
        sandbox_provider="daytona",
        sandbox_provider_secret_name="sandbox-provider-secret",
    )


def _execution_context(
    request: StartBenchmarkRequest,
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
    *,
    aws_managed: bool,
) -> Benchmark:
    starter = RequestIdentity(
        org=Org(id=TEST_ORG_ID, name="default"),
        access_key_id=None,
        email=None,
        name=None,
    )
    benchmark = start_benchmark_request_to_benchmark(
        request,
        starter,
        aws_managed=aws_managed,
    )
    session.add(benchmark)
    session.commit()
    return benchmark


def test_persisted_request_reconstruction_rejects_invalid_aws_modes(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
) -> None:
    access_key_benchmark = _persist_benchmark(
        database_session,
        _access_key_request(contract, harness_config),
        aws_managed=False,
    )
    managed_benchmark = _persist_benchmark(
        database_session,
        _managed_request(contract),
        aws_managed=True,
    )

    with pytest.raises(ValueError, match="Managed runs cannot create access-key"):
        managed_benchmark.access_key_start_benchmark_request(harness_config)
    with pytest.raises(ValueError, match="Access-key runs cannot create managed"):
        access_key_benchmark.managed_start_benchmark_request()

    managed_benchmark.arguments = managed_benchmark.arguments.model_copy(update={"sandbox_provider_secret_name": None})
    with pytest.raises(ValueError, match="Managed runs require a sandbox provider secret name"):
        managed_benchmark.managed_start_benchmark_request()


def test_benchmark_creation_rejects_inconsistent_managed_inputs(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
) -> None:
    with pytest.raises(ValueError, match="AWS mode does not match"):
        _persist_benchmark(
            database_session,
            _access_key_request(contract, harness_config),
            aws_managed=True,
        )
    with pytest.raises(ValueError, match="AWS mode does not match"):
        _persist_benchmark(
            database_session,
            _managed_request(contract),
            aws_managed=False,
        )

    invalid_managed_request = _managed_request(contract).model_copy(
        update={"sandbox_provider": None, "sandbox_provider_secret_name": None}
    )
    with pytest.raises(ValueError, match="Managed runs require a sandbox provider and provider secret name"):
        _persist_benchmark(database_session, invalid_managed_request, aws_managed=True)


def test_taskiq_adapter_accepts_exact_access_key_shape(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
) -> None:
    request = _access_key_request(contract, harness_config)
    benchmark_id = uuid4()

    execution = parse_queued_execution(
        request.model_dump(mode="json"),
        str(benchmark_id),
        _TASK_IDS,
        None,
    )

    assert execution.request == request
    assert execution.benchmark_id == benchmark_id
    assert execution.verified_task_ids == _TASK_IDS
    assert execution.aws_managed is False


def test_taskiq_adapter_accepts_v2_envelope_only(contract: AgentContractRequest) -> None:
    request = _managed_request(contract)
    benchmark_id = uuid4()

    execution = parse_queued_execution(
        None,
        None,
        None,
        _execution_context(request, benchmark_id),
    )

    assert execution.request == request
    assert execution.benchmark_id == benchmark_id
    assert execution.verified_task_ids == _TASK_IDS
    assert execution.aws_managed is True


def test_taskiq_adapter_rejects_mixed_and_invalid_managed_inputs(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
) -> None:
    request = _managed_request(contract)
    benchmark_id = uuid4()
    context = _execution_context(request, benchmark_id)

    with pytest.raises(ValueError, match="mixes access-key and managed"):
        parse_queued_execution({}, None, None, context)

    invalid_version = {**context, "version": 1}
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        parse_queued_execution(None, None, None, invalid_version)

    request_with_credentials = request.model_copy(update={"harness_config": harness_config})
    context_with_credentials = {
        **context,
        "start_benchmark_request": request_with_credentials.model_dump(mode="json"),
    }
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        parse_queued_execution(None, None, None, context_with_credentials)

    with pytest.raises(ValueError, match="incomplete"):
        parse_queued_execution(
            _access_key_request(contract, harness_config).model_dump(mode="json"),
            None,
            _TASK_IDS,
            None,
        )

    with pytest.raises(ValueError, match="access-key benchmark request has no AWS configuration"):
        parse_queued_execution(
            request.model_dump(mode="json"),
            str(benchmark_id),
            _TASK_IDS,
            None,
        )

    request_without_provider = request.model_copy(update={"sandbox_provider": "", "sandbox_provider_secret_name": None})
    context_without_provider = {
        **context,
        "start_benchmark_request": request_without_provider.model_dump(mode="json"),
    }
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        parse_queued_execution(None, None, None, context_without_provider)


@pytest.mark.parametrize("aws_managed", [False, True])
async def test_managed_execution_preflight_checks_aws_dependencies_in_order(
    aws_managed: bool,
    harness_config: HarnessConfig,
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _managed_request(contract.model_copy(update={"secrets": {"AGENT_TOKEN": "agent-secret"}})).model_copy(
        update={
            "webhook_secret_name": "webhook-secret",
            "webhook_intervals": [10],
            "lambda_function": "result-handler",
        }
    )
    if not aws_managed:
        aws_runtime = AWSRuntime.from_harness_config(harness_config)
        request = request.model_copy(update={"harness_config": harness_config})

    benchmark_id = uuid4()
    calls: list[str] = []
    provider_config = cast(SandboxProviderConfig, MagicMock(create_provider=MagicMock(return_value=AsyncMock())))

    def create_log_group(*_args: Any, **_kwargs: Any) -> str:
        calls.append("logs")
        return "benchmark-log-group"

    async def fetch_provider(*_args: Any, **_kwargs: Any) -> SandboxProviderConfig:
        calls.append("sandbox_provider_secret")
        return provider_config

    def resolve_agent_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        calls.append("agent_secrets")
        return {"AGENT_TOKEN": "resolved"}

    def get_webhook_secret(_store: object, _name: str) -> dict[str, str]:
        calls.append("webhook_secret")
        return {"url": "https://example.com"}

    def dry_run(*_args: Any, **_kwargs: Any) -> None:
        calls.append("lambda")

    monkeypatch.setattr(CloudWatchBenchmarkLogSink, "create_benchmark", create_log_group)
    monkeypatch.setattr("tracker.runtime.services.RuntimeServices._load_sandbox_provider_config", fetch_provider)
    monkeypatch.setattr("tracker.aws.services.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.aws.secrets.SecretsManagerStore.get", get_webhook_secret)
    monkeypatch.setattr("tracker.aws.services.dry_run_lambda", dry_run)

    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider=request.sandbox_provider,
        sandbox_provider_secret_name=request.sandbox_provider_secret_reference,
    )
    runtime.prepare_execution(request, benchmark_id)
    result = await runtime.get_sandbox_provider_config()

    assert result is provider_config
    expected_preflight = ["agent_secrets", "webhook_secret", "lambda"] if aws_managed else []
    assert calls == ["logs", *expected_preflight, "sandbox_provider_secret"]


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
            request, TEST_ORG_ID, uuid4(), properties=aws_runtime.resources
        )

    deployment.assert_not_called()


@pytest.mark.parametrize("denied", [False, True])
async def test_owner_execution_validates_saved_location_before_preparation(
    denied: bool,
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resources = replace(aws_runtime.resources, s3_bucket="vs-dev-owner-42")
    owner_runtime = aws_runtime.with_resources(resources)
    request = _managed_request(contract).model_copy(update={"properties": resources})
    deployment = MagicMock(return_value=owner_runtime)
    denial = ManagedStorageError("Managed storage bucket is not authorized", status_code=403)
    validation = AsyncMock(side_effect=denial if denied else None)
    runtime = MagicMock(spec=RuntimeServices)
    compose = MagicMock(return_value=runtime)
    monkeypatch.setattr("tracker.aws.services.deployment_aws_runtime", deployment)
    monkeypatch.setattr("tracker.aws.services.validate_saved_managed_storage_runtime", validation)
    monkeypatch.setattr(CloudRuntimeFactory, "create_runtime", compose)

    if denied:
        with pytest.raises(TrackerServiceError) as error:
            await CloudRuntimeFactory.create_execution_runtime(request, TEST_ORG_ID, uuid4(), properties=resources)

        assert not isinstance(error.value, HTTPException)
        assert str(error.value) == "Managed storage bucket is not authorized"
        assert error.value.__cause__ is denial
        compose.assert_not_called()
    else:
        await CloudRuntimeFactory.create_execution_runtime(request, TEST_ORG_ID, uuid4(), properties=resources)
        assert compose.call_args.args[0] is owner_runtime
        runtime.prepare_execution.assert_called_once()

    deployment.assert_called_once_with(TEST_ORG_ID, resources)
    validation.assert_awaited_once_with(owner_runtime, org_id=TEST_ORG_ID)


def test_access_key_dispatch_rejects_managed_override(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
) -> None:
    request = _access_key_request(contract, harness_config).model_copy(update={"managed_s3_bucket": "vs-dev-owner-42"})
    with pytest.raises(ValueError, match="admission-only storage override"):
        parse_queued_execution(request.model_dump(mode="json"), str(uuid4()), _TASK_IDS, None)


async def test_v2_null_resources_reject_owner_deployment_fallback(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_runtime = aws_runtime.with_resources(replace(aws_runtime.resources, s3_bucket="vs-dev-owner-42"))
    monkeypatch.setattr("tracker.aws.services.deployment_aws_runtime", MagicMock(return_value=owner_runtime))
    validation = AsyncMock()
    monkeypatch.setattr("tracker.aws.services.validate_saved_managed_storage_runtime", validation)

    with pytest.raises(TrackerServiceError, match="Protocol 2 cannot execute owner storage"):
        await CloudRuntimeFactory.create_execution_runtime(
            _managed_request(contract), TEST_ORG_ID, uuid4(), context_version=2
        )

    validation.assert_not_awaited()
