from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from benchmark_service import SandboxProviderConfig
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.auth import SelfHostedIdentity
from tracker.aws.resolver import ManagedAWSEligibilityError
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkStatus, Org
from tracker.types import (
    HarnessConfig,
    ManagedExecutionContextV2,
    ManagedExecutionContextV3,
    StartBenchmarkRequest,
)
from tracker.utils import process_benchmark, start_benchmark_request_to_benchmark
from tracker.utils.run_orchestration import (
    _managed_worker_preflight,  # pyright: ignore[reportPrivateUsage]
    _parse_worker_execution,  # pyright: ignore[reportPrivateUsage]
)


_TASK_IDS = ["task-1", "task-2"]


@pytest.fixture(autouse=True)
def mock_managed_benchmark_service_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    def managed_headers(_org: object, _clients: object) -> dict[str, str]:
        return {"X-Descope-Api-Key": "managed-service-key"}

    monkeypatch.setattr(
        "tracker.utils.run_orchestration.managed_benchmark_service_headers",
        managed_headers,
    )


def _legacy_request(contract: AgentContractRequest, harness_config: HarnessConfig) -> StartBenchmarkRequest:
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


def _execution_context_v2(
    request: StartBenchmarkRequest,
    benchmark_id: UUID,
) -> dict[str, Any]:
    return ManagedExecutionContextV2(
        version=2,
        benchmark_id=benchmark_id,
        verified_task_ids=_TASK_IDS,
        start_benchmark_request=request,
    ).model_dump(mode="json")


def _execution_context_v3(
    request: StartBenchmarkRequest,
    benchmark_id: UUID,
) -> dict[str, Any]:
    return ManagedExecutionContextV3(
        version=3,
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
    starter = SelfHostedIdentity(org=Org(id=TEST_ORG_ID, name="default"))
    benchmark = start_benchmark_request_to_benchmark(
        request,
        starter,
        aws_managed=aws_managed,
    )
    session.add(benchmark)
    session.commit()
    return benchmark


def test_taskiq_adapter_accepts_exact_legacy_shape(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
) -> None:
    request = _legacy_request(contract, harness_config)
    benchmark_id = uuid4()

    execution = _parse_worker_execution(
        request.model_dump(mode="json"),
        str(benchmark_id),
        _TASK_IDS,
        None,
    )

    assert execution.request == request
    assert execution.benchmark_id == benchmark_id
    assert execution.verified_task_ids == _TASK_IDS
    assert execution.kind == "legacy"


def test_taskiq_adapter_accepts_v2_envelope_only(contract: AgentContractRequest) -> None:
    request = _managed_request(contract).model_copy(
        update={"service_headers": {"X-Descope-Api-Key": "embedded-service-key"}}
    )
    benchmark_id = uuid4()

    execution = _parse_worker_execution(
        None,
        None,
        None,
        _execution_context_v2(request, benchmark_id),
    )

    assert execution.request == request
    assert execution.benchmark_id == benchmark_id
    assert execution.verified_task_ids == _TASK_IDS
    assert execution.kind == "managed_v2"


def test_taskiq_adapter_accepts_v3_envelope_only(contract: AgentContractRequest) -> None:
    request = _managed_request(contract)
    benchmark_id = uuid4()

    execution = _parse_worker_execution(None, None, None, _execution_context_v3(request, benchmark_id))

    assert execution.request == request
    assert execution.benchmark_id == benchmark_id
    assert execution.verified_task_ids == _TASK_IDS
    assert execution.kind == "managed_v3"


def test_managed_resume_drops_legacy_caller_integrations(
    contract: AgentContractRequest,
    database_session: Session,
) -> None:
    request = _managed_request(contract).model_copy(
        update={
            "lambda_function": "legacy-function",
            "webhook_secret_name": "legacy-webhook",
            "webhook_intervals": [10],
        }
    )
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)

    resumed = benchmark.managed_start_benchmark_request()

    assert resumed.lambda_function is None
    assert resumed.webhook_secret_name is None
    assert resumed.webhook_intervals is None


def test_taskiq_adapter_rejects_mixed_and_invalid_managed_inputs(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
) -> None:
    request = _managed_request(contract)
    benchmark_id = uuid4()
    context = _execution_context_v3(request, benchmark_id)

    with pytest.raises(ValueError, match="mixes legacy and managed"):
        _parse_worker_execution({}, None, None, context)

    invalid_version = {**context, "version": 1}
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_worker_execution(None, None, None, invalid_version)

    request_with_credentials = request.model_copy(update={"harness_config": harness_config})
    context_with_credentials = {
        **context,
        "start_benchmark_request": request_with_credentials.model_dump(mode="json"),
    }
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_worker_execution(None, None, None, context_with_credentials)

    with pytest.raises(ValueError, match="incomplete"):
        _parse_worker_execution(
            _legacy_request(contract, harness_config).model_dump(mode="json"),
            None,
            _TASK_IDS,
            None,
        )

    with pytest.raises(ValueError, match="legacy benchmark request has no AWS configuration"):
        _parse_worker_execution(
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
        _parse_worker_execution(None, None, None, context_without_provider)

    context_with_service_key = {
        **context,
        "start_benchmark_request": request.model_copy(
            update={"service_headers": {"X-Descope-Api-Key": "must-not-enter-redis"}}
        ).model_dump(mode="json"),
    }
    with pytest.raises(ValueError, match="managed execution context is invalid"):
        _parse_worker_execution(None, None, None, context_with_service_key)

    for update in (
        {"lambda_function": "caller-function"},
        {"webhook_secret_name": "caller-secret"},
        {"webhook_intervals": [10]},
    ):
        context_with_integration = {
            **context,
            "start_benchmark_request": request.model_copy(update=update).model_dump(mode="json"),
        }
        with pytest.raises(ValueError, match="managed execution context is invalid"):
            _parse_worker_execution(None, None, None, context_with_integration)


async def test_managed_worker_input_for_legacy_row_marks_run_error(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
    process_benchmark_env: None,
) -> None:
    legacy_request = _legacy_request(contract, harness_config)
    benchmark = _persist_benchmark(database_session, legacy_request, aws_managed=False)
    context = _execution_context_v3(_managed_request(contract), benchmark.id)

    await process_benchmark(execution_context_json=context)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Managed worker input does not match the stored run mode" in (benchmark.error_message or "")


async def test_legacy_worker_input_for_managed_row_marks_run_error(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
    process_benchmark_env: None,
) -> None:
    managed_request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, managed_request, aws_managed=True)
    legacy_request = _legacy_request(contract, harness_config)

    await process_benchmark(
        start_benchmark_request_json=legacy_request.model_dump(mode="json"),
        benchmark_id_str=str(benchmark.id),
        verified_task_ids=_TASK_IDS,
    )

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Legacy worker input does not match the stored run mode" in (benchmark.error_message or "")


async def test_ineligible_managed_worker_marks_run_error(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)

    def reject_managed_runtime(_tenant_id: str) -> AWSRuntime:
        raise ManagedAWSEligibilityError("Managed AWS access is not available for this organization")

    monkeypatch.setattr("tracker.utils.run_orchestration.deployment_aws_runtime", reject_managed_runtime)

    await process_benchmark(execution_context_json=_execution_context_v3(request, benchmark.id))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Managed AWS access is not available for this organization" in (benchmark.error_message or "")


async def test_managed_worker_completes_with_the_deployment_runtime(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
) -> None:
    request = _managed_request(contract.model_copy(update={"secrets": {"MODEL_API_KEY": "model-secret"}}))
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)
    calls: list[str] = []
    provider_config = cast(SandboxProviderConfig, MagicMock())

    def deployment_runtime(tenant_id: str) -> AWSRuntime:
        assert tenant_id == "default"
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

    async def copy_agent(_benchmark_id: str, _agent_name: str, runtime: AWSRuntime) -> None:
        assert runtime is aws_runtime
        calls.append("s3-copy")

    async def upload_results(_benchmark: Benchmark, _final_view: object, runtime: AWSRuntime) -> None:
        assert runtime is aws_runtime
        calls.append("s3-final-upload")

    monkeypatch.setattr("tracker.utils.run_orchestration.deployment_aws_runtime", deployment_runtime)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_benchmark_log_group", create_log_group)
    monkeypatch.setattr("tracker.utils.run_orchestration.fetch_sandbox_provider_config", fetch_provider)
    monkeypatch.setattr("tracker.utils.run_orchestration.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", resolve_agent_secrets)
    monkeypatch.setattr("tracker.utils.run_orchestration.copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr("tracker.utils.run_orchestration.upload_final_view", upload_results)

    execution_context = _execution_context_v3(request, benchmark.id)
    # A resume may change stored inputs while this job is queued; the queued job must still run.
    benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": 20})
    database_session.add(benchmark)
    database_session.commit()

    await process_benchmark(execution_context_json=execution_context)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert calls[:4] == ["logs", "provider-secret", "agent-secrets", "s3-copy"]
    assert calls.count("agent-secrets") >= 2
    assert calls[-1] == "s3-final-upload"


def test_managed_v2_worker_preflight_checks_legacy_aws_dependencies_in_order(
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
    execution = _parse_worker_execution(None, None, None, _execution_context_v2(request, uuid4()))
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

    result = _managed_worker_preflight(execution, aws_runtime)

    assert result is provider_config
    assert calls == ["logs", "sandbox_provider_secret", "agent_secrets", "webhook_secret", "lambda"]


async def test_managed_preflight_failure_happens_before_copy_or_sandbox(
    contract: AgentContractRequest,
    aws_runtime: AWSRuntime,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    process_benchmark_env: None,
) -> None:
    request = _managed_request(contract)
    benchmark = _persist_benchmark(database_session, request, aws_managed=True)
    copy_agent = AsyncMock()
    create_sandbox = AsyncMock()

    def deployment_runtime(_tenant_id: str) -> AWSRuntime:
        return aws_runtime

    def fail_log_preflight(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("managed log preflight failed")

    monkeypatch.setattr("tracker.utils.run_orchestration.deployment_aws_runtime", deployment_runtime)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_benchmark_log_group", fail_log_preflight)
    monkeypatch.setattr("tracker.utils.run_orchestration.copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)

    await process_benchmark(execution_context_json=_execution_context_v3(request, benchmark.id))

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "managed log preflight failed" in (benchmark.error_message or "")
    copy_agent.assert_not_awaited()
    create_sandbox.assert_not_awaited()
