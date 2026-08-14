import json
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from main import app
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tests.conftest import TEST_ORG_ID
from tracker import config
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorRelease,
    Task,
    TaskStatus,
)
from tracker.executor.release_control import promote_release
from tracker.types import HarnessConfig, StartBenchmarkRequest


client = TestClient(app)

_DISPATCH_TASK_KWARGS = {
    "executor_dispatch_id",
    "executor_release_id",
    "executor_artifact_uri",
    "executor_artifact_digest",
    "executor_protocol_version",
}
_ACCESS_KEY_TASK_KWARGS = {
    "start_benchmark_request_json",
    "benchmark_id_str",
    "verified_task_ids",
} | _DISPATCH_TASK_KWARGS
_MANAGED_TASK_KWARGS = {"execution_context_json"} | _DISPATCH_TASK_KWARGS
_CALLER_AWS_HEADERS = {
    "x-harness-aws-access-key-id": "caller-access-key",
    "x-harness-aws-secret-access-key": "caller-secret-key",
    "x-harness-aws-default-region": "caller-region",
    "x-harness-aws-session-token": "caller-session-token",
    "x-harness-aws-profile": "caller-profile",
    "x-harness-s3-bucket": "caller-bucket",
}


def _configure_managed_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(TEST_ORG_ID))
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30")
    monkeypatch.setattr(config, "AWS_MANAGED_SUBMISSIONS_ENABLED", True)


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


def _promote_test_release(
    session: Session,
    *,
    release_id: str = "producer-test-release",
    protocol_version: str = SUPPORTED_PROTOCOL_VERSION,
) -> None:
    release = ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version=protocol_version,
        readiness_verified=True,
    )
    session.add(release)
    session.commit()
    promote_release(session, release.id)
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
        sandbox_provider="daytona",
        sandbox_provider_secret_name="provider-secret",
    )


def test_managed_start_and_resume_emit_credential_free_v2(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_task_payloads(monkeypatch)

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def agent_exists(*_args: Any, **_kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr("main.s3_object_exists", agent_exists)
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))
    reset_to_in_progress = AsyncMock(return_value=["task-2"])
    monkeypatch.setattr("main.reset_to_in_progress_status", reset_to_in_progress)

    response = client.post("/start-benchmark", json=_start_request(contract, None).model_dump(mode="json"))

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.aws_managed is True
    assert len(payloads) == 1
    assert set(payloads[0]) == _MANAGED_TASK_KWARGS
    start_context = payloads[0]["execution_context_json"]
    assert start_context["version"] == 2
    assert start_context["start_benchmark_request"]["harness_config"] is None
    _assert_no_aws_authority(start_context)

    _stop_benchmark(benchmark, database_session)
    payloads.clear()
    monkeypatch.setattr(config, "AWS_MANAGED_SUBMISSIONS_ENABLED", False)

    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        headers=_CALLER_AWS_HEADERS,
        json={"secrets": {"MODEL_API_KEY": "resume-model-secret"}},
    )

    assert response.status_code == 200
    assert len(payloads) == 1
    assert set(payloads[0]) == _MANAGED_TASK_KWARGS
    resume_context = payloads[0]["execution_context_json"]
    assert resume_context["version"] == 2
    assert resume_context["benchmark_id"] == str(benchmark.id)
    assert resume_context["verified_task_ids"] == ["task-2"]
    assert resume_context["start_benchmark_request"]["harness_config"] is None
    assert resume_context["start_benchmark_request"]["contract"]["secrets"] == {"MODEL_API_KEY": "resume-model-secret"}
    _assert_no_aws_authority(resume_context)

    _stop_benchmark(benchmark, database_session)
    payloads.clear()
    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"secrets": {"aws_secret_access_key": "credential"}},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.STOPPED
    assert reset_to_in_progress.await_count == 1
    assert payloads == []


def test_managed_start_rejects_aws_authority_before_persistence(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    payloads = _capture_task_payloads(monkeypatch)

    async def agent_exists(*_args: Any, **_kwargs: Any) -> bool:
        raise AssertionError("invalid managed requests must be rejected before checking S3")

    monkeypatch.setattr("main.s3_object_exists", agent_exists)
    request = _start_request(contract, None).model_copy(
        update={"service_headers": {"AWS_SECRET_ACCESS_KEY": "credential"}}
    )

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


def test_managed_start_rejects_aws_authority_from_resolved_contract(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_task_payloads(monkeypatch)
    request = _start_request(contract.model_copy(update={"install_cmd": "", "run_cmd": ""}), None)
    resolved_contract = contract.model_copy(update={"secrets": {"aws_profile": "credential"}})
    monkeypatch.setattr("main._resolve_contract_from_s3", AsyncMock(return_value=resolved_contract))

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


def test_managed_start_requires_a_compatible_executor_release(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session, protocol_version="1")

    response = client.post("/start-benchmark", json=_start_request(contract, None).model_dump(mode="json"))

    assert response.status_code == 503
    assert response.json()["detail"] == "Activate an executor release that supports managed runs"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []


def test_managed_resume_rolls_back_when_the_active_release_is_incompatible(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_task_payloads(monkeypatch)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "verify_task_ids",
        AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"])),
    )
    monkeypatch.setattr("main.s3_object_exists", AsyncMock(return_value=True))

    response = client.post("/start-benchmark", json=_start_request(contract, None).model_dump(mode="json"))
    assert response.status_code == 200
    benchmark_id = UUID(response.json()["benchmark_id"])
    benchmark = database_session.get(Benchmark, benchmark_id)
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    task.status = TaskStatus.ERROR
    database_session.add(benchmark)
    database_session.add(task)
    database_session.commit()
    _promote_test_release(database_session, release_id="legacy-release", protocol_version="1")
    payloads.clear()

    async def mutate_recovery_state(**_kwargs: Any) -> list[str]:
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        task.status = TaskStatus.PENDING
        database_session.add(benchmark)
        database_session.add(task)
        return [task.task_id]

    monkeypatch.setattr("main.reset_to_in_progress_status", mutate_recovery_state)

    response = client.post(f"/retry-or-resume-benchmark/{benchmark_id}")

    assert response.status_code == 503
    assert response.json()["detail"] == "Activate an executor release that supports managed runs"
    database_session.expire_all()
    persisted_benchmark = database_session.get(Benchmark, benchmark_id)
    persisted_task = database_session.get(Task, task.id)
    assert persisted_benchmark is not None
    assert persisted_task is not None
    assert persisted_benchmark.status == BenchmarkStatus.STOPPED
    assert persisted_task.status == TaskStatus.ERROR
    assert payloads == []


def test_managed_resume_payload_failure_rolls_back_recovery_state(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_task_payloads(monkeypatch)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "verify_task_ids",
        AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"])),
    )
    monkeypatch.setattr("main.s3_object_exists", AsyncMock(return_value=True))
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))

    response = client.post("/start-benchmark", json=_start_request(contract, None).model_dump(mode="json"))

    assert response.status_code == 200
    benchmark_id = UUID(response.json()["benchmark_id"])
    benchmark = database_session.get(Benchmark, benchmark_id)
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    task.status = TaskStatus.ERROR
    database_session.add(benchmark)
    database_session.add(task)
    database_session.commit()
    original_dispatch_ids = {
        dispatch.id
        for dispatch in database_session.exec(
            select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_id)
        ).all()
    }
    payloads.clear()

    async def mutate_recovery_state(**_kwargs: Any) -> list[str]:
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        task.status = TaskStatus.PENDING
        database_session.add(benchmark)
        database_session.add(task)
        return [task.task_id]

    def fail_payload_build(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("payload validation failed")

    monkeypatch.setattr("main.reset_to_in_progress_status", mutate_recovery_state)
    monkeypatch.setattr("main._process_benchmark_kwargs", fail_payload_build)

    response = TestClient(app, raise_server_exceptions=False).post(f"/retry-or-resume-benchmark/{benchmark_id}")

    assert response.status_code == 500
    database_session.expire_all()
    persisted_benchmark = database_session.get(Benchmark, benchmark_id)
    persisted_task = database_session.get(Task, task.id)
    assert persisted_benchmark is not None
    assert persisted_task is not None
    assert persisted_benchmark.status == BenchmarkStatus.STOPPED
    assert persisted_task.status == TaskStatus.ERROR
    assert {
        dispatch.id
        for dispatch in database_session.exec(
            select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_id)
        ).all()
    } == original_dispatch_ids
    assert payloads == []


def test_access_key_start_and_resume_keep_v1_task_kwargs(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    harness_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _promote_test_release(database_session, protocol_version="1")
    payloads = _capture_task_payloads(monkeypatch)

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def reset_to_in_progress(*_args: Any, **_kwargs: Any) -> list[str]:
        return ["task-1"]

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))
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
    assert set(payloads[0]) == _ACCESS_KEY_TASK_KWARGS
    assert payloads[0]["start_benchmark_request_json"]["harness_config"] is not None

    _stop_benchmark(benchmark, database_session)
    payloads.clear()

    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        headers=harness_headers,
    )

    assert response.status_code == 200
    assert len(payloads) == 1
    assert set(payloads[0]) == _ACCESS_KEY_TASK_KWARGS
    assert payloads[0]["start_benchmark_request_json"]["harness_config"] is not None
