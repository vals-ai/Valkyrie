import asyncio
import json
import math
import time
from asyncio import Semaphore
from contextlib import asynccontextmanager
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from benchmark_service import ImageSource, Resources
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse
from sqlmodel import Session, select

import tracker.utils.task_execution as utils_module
from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkStatus,
    EvaluationResult,
    ModelGatewayPolicyConfig,
    ModelGatewayTaskCapabilityPolicy,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import SandboxSetupError
from tracker.model_gateway import (
    CapabilityEvalResumeState,
    CapabilityMintRequest,
    CapabilityMintResponse,
    CapabilityUsageSummary,
    ModelGatewayError,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import fetch_sandbox_provider_config, process_task, start_benchmark_request_to_benchmark

_TEST_ORG = Org(id=TEST_ORG_ID, name="default")
_TEST_STARTER = RequestIdentity(
    org=_TEST_ORG,
    access_key_id=None,
    email=None,
    name=None,
)


def _task_capability_contract(contract: AgentContractRequest) -> AgentContractRequest:
    model = "openai/gpt-5.5"
    return contract.model_copy(
        update={
            "model": model,
            "secrets": {"INSTALL_SECRET": "install-secret-name"},
            "model_gateway_policy": ModelGatewayTaskCapabilityPolicy(
                kind="task_capability",
                model=model,
                config=ModelGatewayPolicyConfig(client_scope="shared", max_tokens=8192),
                max_queries=800,
                max_sessions=4,
            ),
        }
    )


def _create_task_env(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    run_starter: RequestIdentity | None = None,
) -> tuple[StartBenchmarkRequest, Task, UUID]:
    """Create a benchmark request, benchmark row, and task row for process_task tests."""
    start_benchmark_request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=1,
        task_ids=["task_0"],
        harness_config=harness_config,
    )

    benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, run_starter or _TEST_STARTER)
    benchmark_row.status = BenchmarkStatus.IN_PROGRESS
    database_session.add(benchmark_row)
    database_session.commit()

    task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
    database_session.add(task_row)
    database_session.commit()

    return start_benchmark_request, task_row, benchmark_row.id


async def _run_process_task(
    start_benchmark_request: StartBenchmarkRequest,
    task_row: Task,
    benchmark_id: UUID,
    harness_config: HarnessConfig,
) -> dict[str, dict[str, Any] | None]:
    return await process_task(
        task_row=task_row,
        start_benchmark_request=start_benchmark_request,
        benchmark_service=start_benchmark_request.benchmark_service,
        benchmark_id=benchmark_id,
        task_id="task_0",
        harness_config=harness_config,
        org=_TEST_ORG,
        sandbox_provider_config=fetch_sandbox_provider_config(
            harness_config.sandbox_provider_secret_name,
            harness_config.aws,
            start_benchmark_request.sandbox_provider,
        ),
        creation_semaphore=Semaphore(1),
    )


async def test_process_task_injects_tracker_owned_attribution_env(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = contract.model_copy(update={"secrets": {"UNRELATED_SECRET": "secret-name"}})
    run_starter = RequestIdentity(
        org=_TEST_ORG,
        access_key_id="access-key-id",
        email="starter@example.com",
        name="Starter User",
    )
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
        run_starter,
    )
    start_benchmark_request = start_benchmark_request.model_copy(
        update={
            "benchmark_name": "transient-benchmark-name",
            "contract": contract.model_copy(update={"name": "transient-agent-name"}),
        }
    )
    captured_sandbox_env_vars: list[dict[str, str]] = []
    captured_agent_env_vars: list[dict[str, str]] = []

    resolved_env_vars = {
        "UNRELATED_SECRET": "secret-value",
        "MODEL_GATEWAY_URL": "https://gateway.example.test",
        "MODEL_GATEWAY_API_KEY": "gateway-key",
    }

    def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return resolved_env_vars

    @asynccontextmanager
    async def _capture_create_sandbox(*_args: Any, env_vars: dict[str, str], **_kwargs: Any):
        captured_sandbox_env_vars.append(env_vars)
        yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

    async def _capture_run_agent(*_args: Any, agent_env_vars: dict[str, str], **_kwargs: Any) -> tuple[None, float]:
        captured_agent_env_vars.append(dict(agent_env_vars))
        return None, 0.0

    monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_secrets)
    monkeypatch.setattr(utils_module, "create_sandbox", _capture_create_sandbox)
    monkeypatch.setattr(utils_module, "run_agent", _capture_run_agent)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": {"status": "success", "score": 1.0}}
    assert len(captured_sandbox_env_vars) == 1
    env_vars = captured_sandbox_env_vars[0]
    assert set(env_vars) == {
        "RUN_ID",
        "TASK_ID",
        "IDENTITY",
        "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS",
    }
    assert env_vars["RUN_ID"] == str(benchmark_id)
    assert "QUESTION_ID" not in env_vars
    assert env_vars["TASK_ID"] == "task_0"
    assert json.loads(env_vars["IDENTITY"]) == {
        "benchmark_name": "swebench",
        "agent_name": contract.name,
        "email": "starter@example.com",
    }
    assert env_vars["DAYTONA_SANDBOX_OTEL_EXTRA_LABELS"] == (
        f"benchmark_id={benchmark_id},task_id=task_0,environment={utils_module.ENVIRONMENT}"
    )
    assert captured_agent_env_vars == [
        {
            "UNRELATED_SECRET": "secret-value",
            "MODEL_GATEWAY_URL": "https://gateway.example.test",
            "MODEL_GATEWAY_API_KEY": "gateway-key",
        }
    ]
    assert resolved_env_vars == {}


async def test_process_task_omits_identity_email_when_unavailable(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    captured_sandbox_env_vars: list[dict[str, str]] = []
    captured_agent_env_vars: list[dict[str, str]] = []

    @asynccontextmanager
    async def _capture_create_sandbox(*_args: Any, env_vars: dict[str, str], **_kwargs: Any):
        captured_sandbox_env_vars.append(env_vars)
        yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

    def _mock_resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    async def _capture_run_agent(*_args: Any, agent_env_vars: dict[str, str], **_kwargs: Any) -> tuple[None, float]:
        captured_agent_env_vars.append(agent_env_vars)
        return None, 0.0

    monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_no_secrets)
    monkeypatch.setattr(utils_module, "create_sandbox", _capture_create_sandbox)
    monkeypatch.setattr(utils_module, "run_agent", _capture_run_agent)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": {"status": "success", "score": 1.0}}
    assert len(captured_sandbox_env_vars) == 1
    env_vars = captured_sandbox_env_vars[0]
    assert json.loads(env_vars["IDENTITY"]) == {
        "benchmark_name": "swebench",
        "agent_name": contract.name,
    }
    assert "MODEL_GATEWAY_URL" not in env_vars
    assert "MODEL_GATEWAY_API_KEY" not in env_vars
    assert captured_agent_env_vars == [{}]


async def test_process_task_clears_agent_secrets_before_error_capture(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = contract.model_copy(update={"secrets": {"AGENT_SECRET": "secret-name"}})
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    resolved_env_vars = {"AGENT_SECRET": "sentry-secret-sentinel"}
    captured_states: list[dict[str, str]] = []

    @asynccontextmanager
    async def _capture_create_sandbox(*_args: Any, **_kwargs: Any):
        yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

    async def _fail_run_agent(*_args: Any, agent_env_vars: dict[str, str], **_kwargs: Any) -> tuple[None, float]:
        assert agent_env_vars == resolved_env_vars
        raise RuntimeError("agent failed")

    def _capture_exception(exc: BaseException) -> None:
        captured_states.append(dict(resolved_env_vars))
        assert "sentry-secret-sentinel" not in str(exc)

    def _resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return resolved_env_vars

    monkeypatch.setattr(utils_module, "resolve_secrets", _resolve_secrets)
    monkeypatch.setattr(utils_module, "create_sandbox", _capture_create_sandbox)
    monkeypatch.setattr(utils_module, "run_agent", _fail_run_agent)
    monkeypatch.setattr(utils_module.sentry_sdk, "capture_exception", _capture_exception)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": None}
    assert captured_states == [{}]
    assert resolved_env_vars == {}


async def test_process_task_does_not_resolve_agent_secrets_before_sandbox_exists(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = contract.model_copy(update={"secrets": {"AGENT_SECRET": "secret-name"}})
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    resolved = False

    def _unexpected_resolve(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        nonlocal resolved
        resolved = True
        return {"AGENT_SECRET": "secret-value"}

    @asynccontextmanager
    async def _fail_create_sandbox(*_args: Any, **_kwargs: Any):
        raise SandboxSetupError("sandbox creation failed")
        yield SimpleNamespace(id="unreachable", name="unreachable")

    monkeypatch.setattr(utils_module, "resolve_secrets", _unexpected_resolve)
    monkeypatch.setattr(utils_module, "create_sandbox", _fail_create_sandbox)

    with pytest.raises(SandboxSetupError, match="sandbox creation failed"):
        await _run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            harness_config,
        )

    assert not resolved


async def test_process_task_runs_task_capability_lifecycle_before_upload_and_evaluation(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    events: list[str] = []
    mint_requests: list[CapabilityMintRequest] = []
    install_env_copies: list[dict[str, str]] = []
    install_env_refs: list[dict[str, str]] = []
    runtime_env_copies: list[dict[str, str]] = []
    runtime_env_refs: list[dict[str, str]] = []
    usage_uploads: list[tuple[bytes, str]] = []
    persisted_resume_states: list[dict[str, Any] | None] = []
    resolved_install_env = {"INSTALL_SECRET": "install-secret-value"}
    usage = CapabilityUsageSummary(
        capability_id="cap_task",
        state="revoked",
        drained=True,
        session_count=4,
        query_count=12,
        completed_queries=12,
        total_input_tokens=1_000,
        total_output_tokens=500,
        cost_usd=Decimal("0.42"),
    )

    async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            source=ImageSource(image="test-image:latest"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/testbed",
            agent_timeout=120.0,
            resources=Resources(vcpu=2, memory=4, disk=5),
        )

    def resolve_install_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        events.append("resolve")
        return resolved_install_env

    async def install_agent(*_args: Any, **_kwargs: Any) -> None:
        agent_env_vars = cast(dict[str, str], _args[3])
        events.append("install")
        install_env_copies.append(dict(agent_env_vars))
        install_env_refs.append(agent_env_vars)

    async def execute_agent(
        *_args: Any,
        **_kwargs: Any,
    ) -> tuple[None, float]:
        runtime_env = cast(dict[str, str], _args[6])
        events.append("execute")
        runtime_env_copies.append(dict(runtime_env))
        runtime_env_refs.append(runtime_env)
        return None, 1.5

    async def upload_outputs(*_args: Any, **_kwargs: Any) -> None:
        events.append("upload")

    async def upload_usage(content: bytes, key: str, *_args: Any, **_kwargs: Any) -> None:
        events.append("usage upload")
        usage_uploads.append((content, key))

    async def evaluate(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        events.append("evaluate")
        _kwargs["on_eval_resume_state"]({"job_id": "eval-job"})
        database_session.refresh(task_row)
        persisted_resume_states.append(task_row.eval_resume_state)
        return {"status": "success", "score": 1.0}

    class FakeGateway:
        gateway_url = "https://gateway.example.test"

        async def __aenter__(self) -> "FakeGateway":
            return self

        async def __aexit__(self, *_args: object) -> None:
            events.append("close")

        async def mint(self, request: CapabilityMintRequest) -> CapabilityMintResponse:
            events.append("mint")
            mint_requests.append(request)
            return CapabilityMintResponse(
                capability_id="cap_task",
                token="mgc_task-token",
                state="active",
                expires_at=request.expires_at,
            )

        async def finalize(self, capability_id: str) -> CapabilityUsageSummary:
            assert capability_id == "cap_task"
            events.append("finalize")
            return usage

    gateway = FakeGateway()

    class FakeGatewayFactory:
        @classmethod
        def from_environment(cls) -> FakeGateway:
            return gateway

    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate)
    monkeypatch.setattr(utils_module, "resolve_secrets", resolve_install_secrets)
    monkeypatch.setattr(utils_module, "install_agent", install_agent)
    monkeypatch.setattr(utils_module, "execute_agent", execute_agent)
    monkeypatch.setattr(utils_module, "upload_to_s3", upload_usage)
    monkeypatch.setattr(utils_module, "upload_agent_outputs", upload_outputs)
    monkeypatch.setattr(utils_module, "ModelGatewayAdminClient", FakeGatewayFactory)

    before = time.time()
    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)
    after = time.time()

    assert events == [
        "resolve",
        "install",
        "mint",
        "execute",
        "finalize",
        "usage upload",
        "close",
        "upload",
        "evaluate",
    ]
    assert install_env_copies == [{"INSTALL_SECRET": "install-secret-value"}]
    assert install_env_refs == [{}]
    assert resolved_install_env == {}
    assert runtime_env_copies == [
        {
            "MODEL_GATEWAY_URL": "https://gateway.example.test",
            "MODEL_GATEWAY_API_KEY": "mgc_task-token",
        }
    ]
    assert runtime_env_refs == [{}]
    usage_content = (json.dumps(usage.model_dump(mode="json"), sort_keys=True) + "\n").encode()
    assert usage_uploads == [
        (
            usage_content,
            f"benchmarks/{benchmark_id}/task_0/model_gateway_usage/cap_task.json",
        ),
    ]
    assert persisted_resume_states == [
        CapabilityEvalResumeState(
            kind="model_gateway_eval_resume",
            capability_id="cap_task",
            benchmark_state={"job_id": "eval-job"},
        ).model_dump(mode="json")
    ]

    assert len(mint_requests) == 1
    mint_request = mint_requests[0]
    assert mint_request.run_id == str(benchmark_id)
    assert mint_request.task_id == "task_0"
    assert mint_request.model == "openai/gpt-5.5"
    assert mint_request.config == {"client_scope": "shared", "max_tokens": 8192}
    assert mint_request.sandbox_id == "mock-sandbox-id"
    assert mint_request.identity == {
        "org_id": str(TEST_ORG_ID),
        "benchmark_name": "swebench",
        "agent_name": contract.name,
    }
    assert math.floor(before) + 420 <= mint_request.expires_at <= math.floor(after) + 420
    assert mint_request.max_queries == 800
    assert mint_request.max_sessions == 4

    assert result == {
        "task_0": {
            "status": "success",
            "score": 1.0,
            "_valkyrie_model_gateway_usage": usage.model_dump(mode="json"),
        }
    }


async def test_process_task_resume_restores_task_capability_usage(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    task_row.status = TaskStatus.EVALUATING
    task_row.eval_resume_state = CapabilityEvalResumeState(
        kind="model_gateway_eval_resume",
        capability_id="cap_task",
        benchmark_state={"job_id": "eval-job"},
    ).model_dump(mode="json")
    database_session.add(task_row)
    database_session.commit()
    task_row.started_at += timedelta(seconds=1)
    database_session.add(task_row)
    database_session.commit()

    usage = CapabilityUsageSummary(
        capability_id="cap_task",
        state="revoked",
        drained=True,
        session_count=4,
        query_count=12,
        completed_queries=12,
        total_input_tokens=1_000,
        total_output_tokens=500,
        cost_usd=Decimal("0.42"),
    )
    artifacts = {
        f"benchmarks/{benchmark_id}/task_0/model_gateway_usage/cap_task.json": (
            json.dumps(usage.model_dump(mode="json"), sort_keys=True) + "\n"
        ).encode(),
    }
    downloaded: list[str] = []

    async def download_usage(key: str, *_args: Any, **_kwargs: Any) -> bytes:
        downloaded.append(key)
        return artifacts[key]

    async def resume_evaluation(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        assert _kwargs["eval_resume_state"] == {"job_id": "eval-job"}
        return {"status": "success", "score": 1.0}

    @asynccontextmanager
    async def unexpected_sandbox(*_args: Any, **_kwargs: Any):
        raise AssertionError("eval resume should not create a sandbox")
        yield

    monkeypatch.setattr(utils_module, "download_from_s3", download_usage)
    monkeypatch.setattr(utils_module, "create_sandbox", unexpected_sandbox)
    monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", resume_evaluation)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    expected = {
        "status": "success",
        "score": 1.0,
        "_valkyrie_model_gateway_usage": usage.model_dump(mode="json"),
    }
    evaluation = database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).one()
    assert result == {"task_0": expected}
    assert evaluation.result == expected
    assert downloaded == list(artifacts)


async def test_process_task_preserves_out_of_order_usage_from_two_attempts(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    capability_ids = iter(["cap_attempt_1", "cap_attempt_2"])
    sandbox_ids = iter(["sandbox-attempt-1", "sandbox-attempt-2"])
    uploads: list[tuple[bytes, str]] = []
    first_attempt_upload_started = asyncio.Event()
    release_first_attempt_upload = asyncio.Event()

    async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            source=ImageSource(image="test-image:latest"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/testbed",
            agent_timeout=120.0,
            resources=Resources(vcpu=2, memory=4, disk=5),
        )

    async def no_op(*_args: Any, **_kwargs: Any) -> None:
        pass

    async def execute_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
        return None, 1.0

    def resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    async def capture_upload(content: bytes, key: str, *_args: Any, **_kwargs: Any) -> None:
        if json.loads(content)["capability_id"] == "cap_attempt_1":
            first_attempt_upload_started.set()
            await release_first_attempt_upload.wait()
        uploads.append((content, key))

    @asynccontextmanager
    async def create_sandbox(*_args: Any, **_kwargs: Any):
        sandbox_id = next(sandbox_ids)
        yield SimpleNamespace(id=sandbox_id, name=sandbox_id)

    class FakeGateway:
        gateway_url = "https://gateway.example.test"

        async def __aenter__(self) -> "FakeGateway":
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

        async def mint(self, request: CapabilityMintRequest) -> CapabilityMintResponse:
            return CapabilityMintResponse(
                capability_id=next(capability_ids),
                token="mgc_task-token",
                state="active",
                expires_at=request.expires_at,
            )

        async def finalize(self, capability_id: str) -> CapabilityUsageSummary:
            return CapabilityUsageSummary(
                capability_id=capability_id,
                state="revoked",
                drained=True,
                session_count=1,
                query_count=1,
                completed_queries=1,
                total_input_tokens=100,
                total_output_tokens=50,
                cost_usd=Decimal("0.10"),
            )

    gateway = FakeGateway()

    class FakeGatewayFactory:
        @classmethod
        def from_environment(cls) -> FakeGateway:
            return gateway

    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
    monkeypatch.setattr(utils_module, "resolve_secrets", resolve_no_secrets)
    monkeypatch.setattr(utils_module, "create_sandbox", create_sandbox)
    monkeypatch.setattr(utils_module, "install_agent", no_op)
    monkeypatch.setattr(utils_module, "execute_agent", execute_agent)
    monkeypatch.setattr(utils_module, "upload_to_s3", capture_upload)
    monkeypatch.setattr(utils_module, "upload_agent_outputs", no_op)
    monkeypatch.setattr(utils_module, "ModelGatewayAdminClient", FakeGatewayFactory)

    first_task = asyncio.create_task(_run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config))
    await first_attempt_upload_started.wait()

    database_session.refresh(task_row)
    task_row.status = TaskStatus.PENDING
    task_row.started_at += timedelta(seconds=1)
    task_row.finished_at = None
    task_row.eval_resume_state = None
    database_session.add(task_row)
    database_session.commit()

    second_result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)
    release_first_attempt_upload.set()
    first_result = await first_task

    prefix = f"benchmarks/{benchmark_id}/task_0/model_gateway_usage"
    assert [key for _, key in uploads] == [
        f"{prefix}/cap_attempt_2.json",
        f"{prefix}/cap_attempt_1.json",
    ]
    assert [json.loads(content)["capability_id"] for content, _ in uploads] == [
        "cap_attempt_2",
        "cap_attempt_1",
    ]
    assert first_result == {"task_0": None}
    assert second_result["task_0"] is not None


async def test_process_task_cancellation_during_finalization_persists_usage_before_exit(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    finalization_started = asyncio.Event()
    release_finalization = asyncio.Event()
    events: list[str] = []
    upload_keys: list[str] = []

    async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            source=ImageSource(image="test-image:latest"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/testbed",
            agent_timeout=120.0,
            resources=Resources(vcpu=2, memory=4, disk=5),
        )

    async def install_agent(*_args: Any, **_kwargs: Any) -> None:
        events.append("install")

    async def execute_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
        events.append("execute")
        return None, 1.0

    def resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    async def upload_usage(_content: bytes, key: str, *_args: Any, **_kwargs: Any) -> None:
        events.append("usage upload")
        upload_keys.append(key)

    async def unexpected(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("cancelled task must not upload outputs or evaluate")

    class FakeGateway:
        gateway_url = "https://gateway.example.test"

        async def __aenter__(self) -> "FakeGateway":
            return self

        async def __aexit__(self, *_args: object) -> None:
            events.append("close")

        async def mint(self, request: CapabilityMintRequest) -> CapabilityMintResponse:
            events.append("mint")
            return CapabilityMintResponse(
                capability_id="cap_cancelled",
                token="mgc_task-token",
                state="active",
                expires_at=request.expires_at,
            )

        async def finalize(self, capability_id: str) -> CapabilityUsageSummary:
            assert capability_id == "cap_cancelled"
            events.append("finalize")
            finalization_started.set()
            await release_finalization.wait()
            return CapabilityUsageSummary(
                capability_id=capability_id,
                state="revoked",
                drained=True,
                session_count=1,
                query_count=1,
                completed_queries=1,
                total_input_tokens=100,
                total_output_tokens=50,
                cost_usd=Decimal("0.10"),
            )

    gateway = FakeGateway()

    class FakeGatewayFactory:
        @classmethod
        def from_environment(cls) -> FakeGateway:
            return gateway

    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", unexpected)
    monkeypatch.setattr(utils_module, "resolve_secrets", resolve_no_secrets)
    monkeypatch.setattr(utils_module, "install_agent", install_agent)
    monkeypatch.setattr(utils_module, "execute_agent", execute_agent)
    monkeypatch.setattr(utils_module, "upload_to_s3", upload_usage)
    monkeypatch.setattr(utils_module, "upload_agent_outputs", unexpected)
    monkeypatch.setattr(utils_module, "ModelGatewayAdminClient", FakeGatewayFactory)

    task = asyncio.create_task(_run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config))
    await finalization_started.wait()
    task.cancel()
    release_finalization.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    prefix = f"benchmarks/{benchmark_id}/task_0/model_gateway_usage"
    assert upload_keys == [f"{prefix}/cap_cancelled.json"]
    assert events == ["install", "mint", "execute", "finalize", "usage upload", "close"]


async def test_process_task_finalizes_capability_after_agent_error_and_blocks_outputs(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    events: list[str] = []

    async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            source=ImageSource(image="test-image:latest"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/testbed",
            agent_timeout=120.0,
            resources=Resources(vcpu=2, memory=4, disk=5),
        )

    async def install_agent(*_args: Any, **_kwargs: Any) -> None:
        events.append("install")

    async def execute_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
        events.append("execute")
        raise RuntimeError("agent failed")

    async def unexpected_upload(*_args: Any, **_kwargs: Any) -> None:
        events.append("unexpected upload")

    async def upload_usage(*_args: Any, **_kwargs: Any) -> None:
        events.append("usage upload")

    async def unexpected_evaluate(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        events.append("unexpected evaluate")
        return {}

    class FakeGateway:
        gateway_url = "https://gateway.example.test"

        async def __aenter__(self) -> "FakeGateway":
            return self

        async def __aexit__(self, *_args: object) -> None:
            events.append("close")

        async def mint(self, request: CapabilityMintRequest) -> CapabilityMintResponse:
            events.append("mint")
            return CapabilityMintResponse(
                capability_id="cap_task",
                token="mgc_task-token",
                state="active",
                expires_at=request.expires_at,
            )

        async def finalize(self, capability_id: str) -> CapabilityUsageSummary:
            assert capability_id == "cap_task"
            events.append("finalize")
            return CapabilityUsageSummary(
                capability_id="cap_task",
                state="revoked",
                drained=True,
                session_count=0,
                query_count=0,
                completed_queries=0,
                total_input_tokens=0,
                total_output_tokens=0,
                cost_usd=Decimal("0"),
            )

    gateway = FakeGateway()

    class FakeGatewayFactory:
        @classmethod
        def from_environment(cls) -> FakeGateway:
            return gateway

    def resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", unexpected_evaluate)
    monkeypatch.setattr(utils_module, "resolve_secrets", resolve_no_secrets)
    monkeypatch.setattr(utils_module, "install_agent", install_agent)
    monkeypatch.setattr(utils_module, "execute_agent", execute_agent)
    monkeypatch.setattr(utils_module, "upload_to_s3", upload_usage)
    monkeypatch.setattr(utils_module, "upload_agent_outputs", unexpected_upload)
    monkeypatch.setattr(utils_module, "ModelGatewayAdminClient", FakeGatewayFactory)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": None}
    assert events == ["install", "mint", "execute", "finalize", "usage upload", "close"]


async def test_process_task_blocks_outputs_when_capability_does_not_drain(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    events: list[str] = []

    async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            source=ImageSource(image="test-image:latest"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/testbed",
            agent_timeout=120.0,
            resources=Resources(vcpu=2, memory=4, disk=5),
        )

    async def install_agent(*_args: Any, **_kwargs: Any) -> None:
        events.append("install")

    async def execute_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
        events.append("execute")
        return None, 1.0

    async def unexpected(*_args: Any, **_kwargs: Any) -> None:
        events.append("unexpected")

    class FakeGateway:
        gateway_url = "https://gateway.example.test"

        async def __aenter__(self) -> "FakeGateway":
            return self

        async def __aexit__(self, *_args: object) -> None:
            events.append("close")

        async def mint(self, request: CapabilityMintRequest) -> CapabilityMintResponse:
            events.append("mint")
            return CapabilityMintResponse(
                capability_id="cap_task",
                token="mgc_task-token",
                state="active",
                expires_at=request.expires_at,
            )

        async def finalize(self, capability_id: str) -> CapabilityUsageSummary:
            assert capability_id == "cap_task"
            events.append("finalize")
            raise ModelGatewayError("Task capability did not drain before revocation")

    gateway = FakeGateway()

    class FakeGatewayFactory:
        @classmethod
        def from_environment(cls) -> FakeGateway:
            return gateway

    def resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", unexpected)
    monkeypatch.setattr(utils_module, "resolve_secrets", resolve_no_secrets)
    monkeypatch.setattr(utils_module, "install_agent", install_agent)
    monkeypatch.setattr(utils_module, "execute_agent", execute_agent)
    monkeypatch.setattr(utils_module, "upload_agent_outputs", unexpected)
    monkeypatch.setattr(utils_module, "ModelGatewayAdminClient", FakeGatewayFactory)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": None}
    assert events == ["install", "mint", "execute", "finalize", "close"]


async def test_process_task_rejects_capability_without_agent_timeout_before_install(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = _task_capability_contract(contract)
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    installed = False

    async def install_agent(*_args: Any, **_kwargs: Any) -> None:
        nonlocal installed
        installed = True

    monkeypatch.setattr(utils_module, "install_agent", install_agent)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": None}
    assert not installed


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("BAD-NAME", "Invalid agent secret environment variable names: BAD-NAME"),
        ("RUN_ID", "Agent secret environment variables use reserved names: RUN_ID"),
        ("TERM", "Agent secret environment variables use reserved names: TERM"),
        (
            "VALKYRIE_AGENT_SECRET_SCOPE",
            "Agent secret environment variables use reserved names: VALKYRIE_AGENT_SECRET_SCOPE",
        ),
    ],
)
def test_agent_secret_environment_names_fail_closed(name: str, message: str) -> None:
    validate_agent_env_vars = getattr(utils_module, "_validate_agent_env_vars")

    with pytest.raises(SandboxSetupError, match=message):
        validate_agent_env_vars({name: "secret-value"})
