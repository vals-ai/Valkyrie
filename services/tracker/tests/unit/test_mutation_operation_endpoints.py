"""Endpoint tests for bearer-managed mutation receipts."""

from typing import Any, Literal
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi.testclient import TestClient
import pytest
from sqlmodel import Session, select

from main import app
from tests.unit.utils.task_execution_support import MockKicker
from tests.utils import TEST_ORG_ID
from tracker import config
from tracker.api.mutation_operations import claim_mutation, mark_mutation_uncertain, mutation_fingerprint
from tracker.auth import AccessKeyIdentity, BearerIdentity, SelfHostedIdentity, get_current_org, get_current_starter
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    MutationOperation,
    MutationOperationKind,
    Org,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest


client = TestClient(app)
TEST_ORG = Org(id=TEST_ORG_ID, name="default")


@pytest.fixture
def managed_bearer(monkeypatch: pytest.MonkeyPatch) -> BearerIdentity:
    identity = BearerIdentity(
        org=TEST_ORG,
        principal_id="U2abc",
        email="alice@vals.ai",
    )
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: TEST_ORG)
    monkeypatch.setattr(config, "AWS_MANAGED_TENANT_IDS", TEST_ORG.name)
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

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    def managed_service_headers(*_args: Any) -> dict[str, str]:
        return {"X-Descope-Api-Key": "managed-service-key"}

    monkeypatch.setattr("main.s3_object_exists", agent_exists)
    monkeypatch.setattr("main._resolve_contract_from_s3", resolve_contract)
    monkeypatch.setattr("main.ensure_managed_benchmark_service_headers", managed_service_headers)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    return identity


def test_bearer_start_requires_idempotency_key(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
) -> None:
    response = client.post("/start-benchmark", json=_start_body(contract))

    assert response.status_code == 400
    assert response.json() == {"detail": "Idempotency-Key is required"}
    assert database_session.exec(select(Benchmark)).all() == []
    assert mock_kicker.queued_calls == []


def test_bearer_analyze_requires_idempotency_key_before_preflight(
    managed_bearer: BearerIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invoke = Mock()
    monkeypatch.setattr("main.invoke_analyzer", invoke)

    response = client.post(
        f"/analyze-benchmark/{uuid4()}",
        json={"lambda_function": "docent-analyzer"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Idempotency-Key is required"}
    invoke.assert_not_called()


def test_bearer_analyze_replays_one_lambda_invocation(
    managed_bearer: BearerIdentity,
    example_benchmark_object: Benchmark,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    example_benchmark_object.status = BenchmarkStatus.FINISHED
    example_benchmark_object.aws_managed = True
    database_session.add(example_benchmark_object)
    database_session.commit()
    invoke = Mock(return_value={"reading_plan_url": "https://docent.example/reading-plan"})
    monkeypatch.setattr("main.invoke_analyzer", invoke)
    operation_id = uuid4()
    headers = {"Idempotency-Key": str(operation_id)}
    body = {"lambda_function": "docent-analyzer", "no_cache": True}

    first = client.post(
        f"/analyze-benchmark/{example_benchmark_object.id}",
        headers=headers,
        json=body,
    )
    replay = client.post(
        f"/analyze-benchmark/{example_benchmark_object.id}",
        headers=headers,
        json=body,
    )
    operation = client.get(f"/operations/{operation_id}")

    assert first.status_code == replay.status_code == 200
    assert (
        first.json()
        == replay.json()
        == {
            "status": "done",
            "reading_plan_url": "https://docent.example/reading-plan",
        }
    )
    assert operation.json() == {
        "state": "succeeded",
        "operation_id": str(operation_id),
        "kind": "analyze_benchmark",
        "response": first.json(),
    }
    invoke.assert_called_once()


def test_bearer_analyze_replays_preflight_failure(
    managed_bearer: BearerIdentity,
    example_benchmark_object: Benchmark,
    database_session: Session,
) -> None:
    example_benchmark_object.status = BenchmarkStatus.FINISHED
    example_benchmark_object.aws_managed = True
    database_session.add(example_benchmark_object)
    database_session.commit()
    operation_id = uuid4()
    headers = {"Idempotency-Key": str(operation_id)}

    first = client.post(
        f"/analyze-benchmark/{example_benchmark_object.id}",
        headers=headers,
        json={},
    )
    replay = client.post(
        f"/analyze-benchmark/{example_benchmark_object.id}",
        headers=headers,
        json={},
    )

    assert first.status_code == replay.status_code == 400
    assert first.json() == replay.json()
    assert "No ingest_lambda provided" in first.json()["detail"]


def test_bearer_analyze_uncertain_never_invokes_twice(
    managed_bearer: BearerIdentity,
    example_benchmark_object: Benchmark,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    example_benchmark_object.status = BenchmarkStatus.FINISHED
    example_benchmark_object.aws_managed = True
    database_session.add(example_benchmark_object)
    database_session.commit()
    invoke = Mock(side_effect=RuntimeError("lambda response lost"))
    monkeypatch.setattr("main.invoke_analyzer", invoke)
    operation_id = uuid4()
    headers = {"Idempotency-Key": str(operation_id)}
    body = {"lambda_function": "docent-analyzer", "no_cache": True}

    first = client.post(
        f"/analyze-benchmark/{example_benchmark_object.id}",
        headers=headers,
        json=body,
    )
    replay = client.post(
        f"/analyze-benchmark/{example_benchmark_object.id}",
        headers=headers,
        json=body,
    )

    assert first.status_code == replay.status_code == 200
    assert (
        first.json()
        == replay.json()
        == {
            "state": "uncertain",
            "operation_id": str(operation_id),
        }
    )
    invoke.assert_called_once()


def test_bearer_start_replays_one_run_and_one_enqueue(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
) -> None:
    headers = {"Idempotency-Key": str(uuid4())}
    body = _start_body(contract)

    first = client.post("/start-benchmark", headers=headers, json=body)
    replay = client.post("/start-benchmark", headers=headers, json=body)

    assert first.status_code == replay.status_code == 200
    assert replay.json()["benchmark_id"] == first.json()["benchmark_id"]
    assert len(database_session.exec(select(Benchmark)).all()) == 1
    assert len(mock_kicker.queued_calls) == 1


def test_bearer_start_replay_skips_preflight(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    mock_kicker: MockKicker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_exists = AsyncMock(return_value=True)
    verify_task_ids = AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"]))
    monkeypatch.setattr("main.s3_object_exists", agent_exists)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    headers = {"Idempotency-Key": str(uuid4())}
    body = _start_body(contract)

    first = client.post("/start-benchmark", headers=headers, json=body)
    replay = client.post("/start-benchmark", headers=headers, json=body)

    assert first.status_code == replay.status_code == 200
    assert agent_exists.await_count == 1
    assert verify_task_ids.await_count == 1
    assert len(mock_kicker.queued_calls) == 1


def test_bearer_start_rejects_reused_key_for_changed_body(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
) -> None:
    headers = {"Idempotency-Key": str(uuid4())}

    first = client.post("/start-benchmark", headers=headers, json=_start_body(contract))
    conflict = client.post(
        "/start-benchmark",
        headers=headers,
        json={**_start_body(contract), "concurrency": 2},
    )

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "Operation ID was already used for a different request"}
    assert len(database_session.exec(select(Benchmark)).all()) == 1
    assert len(mock_kicker.queued_calls) == 1


def test_bearer_retry_replays_one_reset_and_one_enqueue(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_id = _start_and_stop(contract, database_session)
    mock_kicker.queued_calls.clear()
    reset = AsyncMock(return_value=["task-1"])
    monkeypatch.setattr("main.reset_to_in_progress_status", reset)
    headers = {"Idempotency-Key": str(uuid4())}
    url = f"/retry-or-resume-benchmark/{benchmark_id}?retry=true"
    body = {"task_ids": ["task-1"]}

    first = client.post(url, headers=headers, json=body)
    replay = client.post(url, headers=headers, json=body)

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json() == {"status": "success"}
    assert reset.await_count == 1
    assert len(mock_kicker.queued_calls) == 1


@pytest.mark.parametrize(
    ("uncertain", "expected_state"),
    [
        (False, "processing"),
        (True, "uncertain"),
    ],
)
def test_bearer_retry_incomplete_receipt_never_mutates(
    uncertain: bool,
    expected_state: str,
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_id = _start_and_stop(contract, database_session)
    mock_kicker.queued_calls.clear()
    reset = AsyncMock(return_value=["task-1"])
    monkeypatch.setattr("main.reset_to_in_progress_status", reset)
    operation_id = uuid4()
    request = _retry_operation_request(benchmark_id)
    claim_mutation(
        database_session,
        TEST_ORG,
        operation_id,
        MutationOperationKind.RETRY_OR_RESUME_BENCHMARK,
        mutation_fingerprint(MutationOperationKind.RETRY_OR_RESUME_BENCHMARK, request),
    )
    if uncertain:
        mark_mutation_uncertain(database_session, TEST_ORG, operation_id)

    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark_id}?retry=true",
        headers={"Idempotency-Key": str(operation_id)},
        json={"task_ids": ["task-1"]},
    )

    assert response.status_code == 200
    assert response.json() == {
        "state": expected_state,
        "operation_id": str(operation_id),
    }
    assert reset.await_count == 0
    assert mock_kicker.queued_calls == []


def test_successful_operation_lookup_is_org_scoped(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_id = uuid4()
    start = client.post(
        "/start-benchmark",
        headers={"Idempotency-Key": str(operation_id)},
        json=_start_body(contract),
    )

    operation = client.get(f"/operations/{operation_id}")

    assert start.status_code == operation.status_code == 200
    assert operation.json() == {
        "state": "succeeded",
        "operation_id": str(operation_id),
        "kind": "start_benchmark",
        "response": start.json(),
    }

    other_org = Org(id=uuid4(), name="other")
    database_session.add(other_org)
    database_session.commit()
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: other_org)

    assert client.get(f"/operations/{operation_id}").status_code == 404


def test_bearer_start_replays_deterministic_failure(
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_exists = AsyncMock(return_value=False)
    monkeypatch.setattr("main.s3_object_exists", agent_exists)
    operation_id = uuid4()
    headers = {"Idempotency-Key": str(operation_id)}

    first = client.post("/start-benchmark", headers=headers, json=_start_body(contract))
    replay = client.post("/start-benchmark", headers=headers, json=_start_body(contract))
    operation = client.get(f"/operations/{operation_id}")

    assert first.status_code == replay.status_code == 404
    assert (
        first.json()
        == replay.json()
        == {"detail": f"Agent '{contract.name}' is not available in the deployment bucket."}
    )
    assert operation.json() == {
        "state": "failed",
        "operation_id": str(operation_id),
        "kind": "start_benchmark",
        "status_code": 404,
        "detail": f"Agent '{contract.name}' is not available in the deployment bucket.",
    }
    assert agent_exists.await_count == 1
    assert database_session.exec(select(Benchmark)).all() == []
    assert mock_kicker.queued_calls == []


@pytest.mark.parametrize("failure", ["enqueue", "complete"])
def test_bearer_start_ambiguous_failure_is_uncertain_and_never_reexecutes(
    failure: str,
    managed_bearer: BearerIdentity,
    contract: AgentContractRequest,
    database_session: Session,
    mock_kicker: MockKicker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    enqueue: AsyncMock | None = None
    if failure == "enqueue":
        enqueue = AsyncMock(side_effect=RuntimeError("broker unavailable"))
        monkeypatch.setattr(mock_kicker, "kiq", enqueue)
    else:

        def fail_completion(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("receipt unavailable")

        monkeypatch.setattr("main.complete_mutation", fail_completion)
    operation_id = uuid4()
    headers = {"Idempotency-Key": str(operation_id)}

    first = client.post("/start-benchmark", headers=headers, json=_start_body(contract))
    replay = client.post("/start-benchmark", headers=headers, json=_start_body(contract))

    assert first.status_code == replay.status_code == 200
    assert (
        first.json()
        == replay.json()
        == {
            "state": "uncertain",
            "operation_id": str(operation_id),
        }
    )
    assert len(database_session.exec(select(Benchmark)).all()) == 1
    queued_count = 0 if failure == "enqueue" else 1
    assert len(mock_kicker.queued_calls) == queued_count
    if enqueue is not None:
        assert enqueue.await_count == 1


@pytest.mark.parametrize("identity_kind", ["access_key", "self_hosted"])
def test_non_bearer_operation_key_stores_no_receipt(
    identity_kind: Literal["access_key", "self_hosted"],
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    database_session: Session,
    mock_kicker: MockKicker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    match identity_kind:
        case "access_key":
            identity = AccessKeyIdentity(org=TEST_ORG, principal_id="access-key", email="alice@vals.ai")
        case "self_hosted":
            identity = SelfHostedIdentity(org=TEST_ORG)
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_starter,
        lambda: identity,
    )
    request = StartBenchmarkRequest(
        contract=contract,
        benchmark_name="receipt-test",
        task_ids=["task-1"],
        harness_config=harness_config,
        service_headers={"Authorization": "private"},
    )

    response = client.post(
        "/start-benchmark",
        headers={"Idempotency-Key": str(uuid4())},
        json=request.model_dump(mode="json"),
    )

    assert response.status_code == 200
    assert database_session.exec(select(MutationOperation)).all() == []
    assert len(mock_kicker.queued_calls) == 1


def _start_body(contract: AgentContractRequest) -> dict[str, Any]:
    selection = contract.model_copy(update={"install_cmd": "", "run_cmd": ""})
    return StartBenchmarkRequest(
        contract=selection,
        benchmark_name="receipt-test",
        task_ids=["task-1"],
    ).model_dump(mode="json")


def _start_and_stop(contract: AgentContractRequest, session: Session) -> UUID:
    response = client.post(
        "/start-benchmark",
        headers={"Idempotency-Key": str(uuid4())},
        json=_start_body(contract),
    )
    assert response.status_code == 200
    benchmark = session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    session.add(benchmark)
    session.commit()
    return benchmark.id


def _retry_operation_request(benchmark_id: UUID) -> dict[str, Any]:
    return {
        "benchmark_id": str(benchmark_id),
        "concurrency": None,
        "retry": True,
        "retry_mode": "auto",
        "secrets": {},
        "service_headers": {},
        "task_ids": ["task-1"],
    }
