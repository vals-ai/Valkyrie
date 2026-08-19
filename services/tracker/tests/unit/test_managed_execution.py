from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from benchmark_service import SandboxProviderConfig
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.resolver import ManagedAWSEligibilityError
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkStatus, Org
from tracker.types import HarnessConfig, ManagedExecutionContext, StartBenchmarkRequest
from tracker.utils import process_benchmark, start_benchmark_request_to_benchmark
from tracker.utils.run_orchestration import (
    _preflight_managed_aws,  # pyright: ignore[reportPrivateUsage]
    _parse_queued_execution,  # pyright: ignore[reportPrivateUsage]
)


_TASK_IDS = ["task-1", "task-2"]


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

    execution = _parse_queued_execution(
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

    execution = _parse_queued_execution(
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
        _parse_queued_execution({}, None, None, context)

    invalid_version = {**context, "version": 1}
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_queued_execution(None, None, None, invalid_version)

    request_with_credentials = request.model_copy(update={"harness_config": harness_config})
    context_with_credentials = {
        **context,
        "start_benchmark_request": request_with_credentials.model_dump(mode="json"),
    }
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_queued_execution(None, None, None, context_with_credentials)

    with pytest.raises(ValueError, match="incomplete"):
        _parse_queued_execution(
            _access_key_request(contract, harness_config).model_dump(mode="json"),
            None,
            _TASK_IDS,
            None,
        )

    with pytest.raises(ValueError, match="access-key benchmark request has no AWS configuration"):
        _parse_queued_execution(
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
        _parse_queued_execution(None, None, None, context_without_provider)


async def test_queued_execution_parse_failure_marks_run_error(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)
    invalid_context = {**_execution_context(request, benchmark.id), "version": 1}

    await process_benchmark(execution_context_json=invalid_context, **executor_authority_kwargs(benchmark))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Queued managed execution context is invalid" in (benchmark.error_message or "")


async def test_managed_execution_for_access_key_row_marks_run_error(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    access_key_request = _access_key_request(contract, harness_config)
    benchmark = _persist_benchmark(database_session, access_key_request, aws_managed=False)
    context = _execution_context(_managed_request(contract), benchmark.id)

    await process_benchmark(execution_context_json=context, **executor_authority_kwargs(benchmark))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Queued managed execution does not match the stored access-key run mode" in (benchmark.error_message or "")


async def test_access_key_execution_for_managed_row_marks_run_error(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    managed_request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, managed_request, aws_managed=True)
    access_key_request = _access_key_request(contract, harness_config)

    await process_benchmark(
        start_benchmark_request_json=access_key_request.model_dump(mode="json"),
        benchmark_id_str=str(benchmark.id),
        verified_task_ids=_TASK_IDS,
        **executor_authority_kwargs(benchmark),
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Queued access-key execution does not match the stored managed run mode" in (benchmark.error_message or "")


async def test_ineligible_managed_execution_marks_run_error(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)

    def reject_managed_runtime(_org_id: UUID) -> AWSRuntime:
        raise ManagedAWSEligibilityError("Managed AWS access is not available for this organization")

    monkeypatch.setattr("tracker.utils.run_orchestration.deployment_aws_runtime", reject_managed_runtime)

    await process_benchmark(
        execution_context_json=_execution_context(request, benchmark.id),
        **executor_authority_kwargs(benchmark),
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Managed AWS access is not available for this organization" in (benchmark.error_message or "")


async def test_managed_execution_completes_with_the_deployment_runtime(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract.model_copy(update={"secrets": {"MODEL_API_KEY": "model-secret"}})).model_copy(
        update={"lambda_function": "post-run-handler"}
    )
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)
    calls: list[str] = []
    provider_config = cast(SandboxProviderConfig, MagicMock())

    def deployment_runtime(_org_id: UUID) -> AWSRuntime:
        return aws_runtime

    def create_log_group(_benchmark_id: str, runtime: AWSRuntime) -> str:
        assert runtime is aws_runtime
        calls.append("logs")
        return "benchmark-log-group"

    def fetch_provider(_name: str, clients: object, _provider: str) -> SandboxProviderConfig:
        assert clients is aws_runtime.clients
        calls.append("provider-secret")
        return provider_config

    def resolve_agent_secrets(_secrets: object, clients: object) -> dict[str, str]:
        assert clients is aws_runtime.clients
        calls.append("agent-secrets")
        return {"MODEL_API_KEY": "resolved"}

    def dry_run(clients: object, _function_name: str) -> None:
        assert clients is aws_runtime.clients
        calls.append("lambda-dry-run")

    async def upload_results(_benchmark: Benchmark, _final_view: object, runtime: AWSRuntime) -> None:
        assert runtime is aws_runtime
        calls.append("s3-final-upload")

    def invoke_post_run(clients: object, _function_name: str, _payload: object, **_kwargs: Any) -> dict[str, Any]:
        assert clients is aws_runtime.clients
        calls.append("lambda-post-run")
        return {}

    monkeypatch.setattr("tracker.utils.run_orchestration.deployment_aws_runtime", deployment_runtime)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_benchmark_log_group", create_log_group)
    monkeypatch.setattr("tracker.utils.run_orchestration.fetch_sandbox_provider_config", fetch_provider)
    monkeypatch.setattr("tracker.utils.run_orchestration.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.utils.run_orchestration.dry_run_lambda", dry_run)
    monkeypatch.setattr("tracker.utils.run_orchestration.upload_final_view", upload_results)
    monkeypatch.setattr("tracker.utils.run_orchestration.invoke_lambda", invoke_post_run)

    execution_context = _execution_context(request, benchmark.id)
    # A resume may change stored inputs while this job is queued; the queued job must still run.
    benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": 20})
    database_session.add(benchmark)
    database_session.commit()

    await process_benchmark(execution_context_json=execution_context, **executor_authority_kwargs(benchmark))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert calls[:4] == ["logs", "provider-secret", "agent-secrets", "lambda-dry-run"]
    assert calls.count("agent-secrets") >= 2
    assert calls[-2:] == ["s3-final-upload", "lambda-post-run"]


def test_managed_execution_preflight_checks_aws_dependencies_in_order(
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
    execution = _parse_queued_execution(None, None, None, _execution_context(request, uuid4()))
    calls: list[str] = []
    provider_config = cast(SandboxProviderConfig, MagicMock())

    def create_log_group(*_args: Any, **_kwargs: Any) -> str:
        calls.append("logs")
        return "benchmark-log-group"

    def fetch_provider(*_args: Any, **_kwargs: Any) -> SandboxProviderConfig:
        calls.append("sandbox_provider_secret")
        return provider_config

    def resolve_agent_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        calls.append("agent_secrets")
        return {"AGENT_TOKEN": "resolved"}

    def fetch_webhook_secret(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        calls.append("webhook_secret")
        return {"url": "https://example.com"}

    def dry_run(*_args: Any, **_kwargs: Any) -> None:
        calls.append("lambda")

    monkeypatch.setattr("tracker.utils.run_orchestration.create_benchmark_log_group", create_log_group)
    monkeypatch.setattr("tracker.utils.run_orchestration.fetch_sandbox_provider_config", fetch_provider)
    monkeypatch.setattr("tracker.utils.run_orchestration.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.utils.run_orchestration.fetch_aws_secret", fetch_webhook_secret)
    monkeypatch.setattr("tracker.utils.run_orchestration.dry_run_lambda", dry_run)

    result = _preflight_managed_aws(execution, aws_runtime)

    assert result is provider_config
    assert calls == ["logs", "sandbox_provider_secret", "agent_secrets", "webhook_secret", "lambda"]


async def test_managed_preflight_failure_happens_before_sandbox(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
    executor_authority_kwargs: Any,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)
    create_sandbox = AsyncMock()

    def deployment_runtime(_org_id: UUID) -> AWSRuntime:
        return aws_runtime

    def fail_log_preflight(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("managed log preflight failed")

    monkeypatch.setattr("tracker.utils.run_orchestration.deployment_aws_runtime", deployment_runtime)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_benchmark_log_group", fail_log_preflight)
    monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)

    await process_benchmark(
        execution_context_json=_execution_context(request, benchmark.id),
        **executor_authority_kwargs(benchmark),
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "managed log preflight failed" in (benchmark.error_message or "")
    create_sandbox.assert_not_awaited()
