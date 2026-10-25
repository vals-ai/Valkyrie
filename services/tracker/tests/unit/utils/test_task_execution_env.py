"""Unit tests for task execution environment assembly.

Run: uv run pytest tests/unit/utils/test_task_execution_env.py
"""

import asyncio
import json
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from hashlib import sha256
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from benchmark_service import (
    CreditedGeneration,
    DaytonaProviderConfig,
    SandboxCreateRequest,
    SandboxSource,
    TargetedSnapshotSource,
)
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

import tracker.utils.task_execution as utils_module
from tests.unit.utils.task_execution_support import (
    TEST_ORG,
    bind_task_to_dispatch,
    create_task_environment,
    finalize_agent_generation,
    make_retrieve_task_response,
    run_process_task,
)
from tracker.auth import RequestIdentity
from tracker.runtime.services import RuntimeServices
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.generation_plan import GenerationPlan
from tracker.external_service_gateway import ExternalServiceGatewayClient, GenerationAccountingSummary
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.sandbox import SandboxCreationState, create_sandbox
from tracker.scheduler.admission import SandboxQueueContext
from tracker.scheduler.store import reserved_queue_pool_id
import tracker.runtime.model_gateway as model_gateway_module


def _install_gateway(
    monkeypatch: pytest.MonkeyPatch,
    minted: list[dict[str, Any]],
    mint_requests: list[httpx.Request] | None = None,
    on_revoke: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Answer the tracker's run-token mint and revoke calls in process."""

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/service-auth":
            if mint_requests is not None:
                mint_requests.append(request)
            minted.append(json.loads(request.content))
            return httpx.Response(
                200, json={"token": "mgwt_scoped", "lease_id": "lease-1", "expires_at": 1_800_000_000.0}
            )
        assert request.url.path == "/service-auth/revoke", request.url.path
        if on_revoke is not None:
            await on_revoke()
        return httpx.Response(200, json={"revoked": 1})

    monkeypatch.setattr(model_gateway_module, "_client", httpx.AsyncClient(transport=httpx.MockTransport(handle)))


@asynccontextmanager
async def _capture_sandbox_environment(
    captured_env_vars: list[dict[str, str]],
    *_args: Any,
    env_vars: dict[str, str],
    **_kwargs: Any,
) -> AsyncGenerator[SimpleNamespace, None]:
    captured_env_vars.append(env_vars)
    yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")


async def _complete_base_only_agent(
    *_args: Any,
    deadline_controller: Any,
    on_external_service_finalized: Callable[[GenerationAccountingSummary], Awaitable[None]],
    **_kwargs: Any,
) -> tuple[None, float]:
    await finalize_agent_generation(
        deadline_controller=deadline_controller,
        on_external_service_finalized=on_external_service_finalized,
    )
    return None, 1.0


class TestQueuedTaskSource:
    """Source propagation through queued task execution."""

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize("cancel_preparing", [False, True], ids=["complete", "cancel"])
    async def test_reserved_backlog_preparation_is_bounded_and_releases_slots(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        cancel_preparing: bool,
    ) -> None:
        request, first, benchmark_id, authority = create_task_environment(contract, database_session)
        tasks = [first]
        for index in range(1, 15):
            task = Task(org_id=first.org_id, benchmark=benchmark_id, task_id=f"task_{index}")
            database_session.add(task)
            tasks.append(task)
        database_session.commit()
        task_engine = database_session.get_bind()
        assert isinstance(task_engine, Engine)
        context = SandboxQueueContext(
            provider=Mock(),
            pool_id=reserved_queue_pool_id("pool_test"),
            engine=task_engine,
            capacity_cache=Mock(),
        )
        started = 0
        active = 0
        peak = 0
        ten_preparing = asyncio.Event()
        finish_preparing = asyncio.Event()

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            nonlocal started, active, peak
            started += 1
            active += 1
            peak = max(peak, active)
            if started == 10:
                ten_preparing.set()
            try:
                await finish_preparing.wait()
                return make_retrieve_task_response()
            finally:
                active -= 1

        async def publish_candidate(*, preparation_stack: AsyncExitStack, **_kwargs: Any) -> None:
            await preparation_stack.aclose()

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module, "enter_queued_sandbox", publish_candidate)
        running = [
            asyncio.create_task(
                run_process_task(
                    request,
                    task,
                    benchmark_id,
                    runtime_services,
                    authority,
                    queue_context=context,
                )
            )
            for task in tasks
        ]
        try:
            await asyncio.wait_for(ten_preparing.wait(), timeout=5)
            assert started == 10
            assert active == 10
            if cancel_preparing:
                for runner in running:
                    runner.cancel()
                await asyncio.gather(*running, return_exceptions=True)
                finish_preparing.set()
                # Cancellation must return all slots, including cancellation while acquiring one.
                await asyncio.wait_for(
                    asyncio.gather(
                        *(
                            run_process_task(
                                request,
                                task,
                                benchmark_id,
                                runtime_services,
                                authority,
                                queue_context=context,
                            )
                            for task in tasks
                        )
                    ),
                    timeout=5,
                )
            else:
                finish_preparing.set()
                await asyncio.wait_for(asyncio.gather(*running), timeout=5)
            assert started == (25 if cancel_preparing else 15)
            assert active == 0
            assert peak == 10
        finally:
            for runner in running:
                runner.cancel()
            await asyncio.gather(*running, return_exceptions=True)

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_targeted_source_is_shared_by_admission_and_creation(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        source = TargetedSnapshotSource(snapshot="snapshot", target="us-west-3")
        task_response = make_retrieve_task_response().model_copy(update={"source": source})
        admission_sources: list[SandboxSource] = []
        creation_sources: list[SandboxSource] = []
        admission_states: list[SandboxCreationState] = []
        lifecycle: list[tuple[str, bool]] = []
        provider = Mock()
        provider_context_entries: list[DaytonaProviderConfig] = []
        mock_sandbox = SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name", state="started", labels={})

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        async def provider_create(request: SandboxCreateRequest) -> SimpleNamespace:
            creation_sources.append(request.source)
            lifecycle.append(("create", admission_states[0].cleanup_pending))
            return mock_sandbox

        async def provider_delete(_sandbox_id: str) -> None:
            lifecycle.append(("delete", admission_states[0].cleanup_pending))

        provider.create_sandbox = AsyncMock(side_effect=provider_create)
        provider.delete_sandbox = AsyncMock(side_effect=provider_delete)

        @asynccontextmanager
        async def provider_context(config: DaytonaProviderConfig) -> AsyncGenerator[Any, None]:
            provider_context_entries.append(config)
            yield provider

        async def enter_queue(
            *,
            stack: Any,
            task_row_id: Any,
            source: SandboxSource,
            create: Callable[[], Any],
            creation_state: SandboxCreationState,
            **_kwargs: Any,
        ) -> Any:
            admission_sources.append(source)
            admission_states.append(creation_state)
            lifecycle.append(("admission", creation_state.cleanup_pending))
            sandbox = await stack.enter_async_context(create())
            with Session(task_engine) as task_session:
                queued_task = task_session.get(Task, task_row_id)
                assert queued_task is not None
                queued_task.status = TaskStatus.IN_PROGRESS
                task_session.add(queued_task)
                task_session.commit()
            return sandbox

        task_engine = database_session.get_bind()
        assert isinstance(task_engine, Engine)
        queue_context = SandboxQueueContext(
            provider=provider, pool_id="pool_test", engine=task_engine, capacity_cache=Mock()
        )
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(DaytonaProviderConfig, "create_provider", provider_context)
        monkeypatch.setattr(utils_module, "create_sandbox", create_sandbox)
        monkeypatch.setattr(utils_module, "enter_queued_sandbox", enter_queue)

        result = await run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            runtime_services,
            authority,
            queue_context=queue_context,
        )

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert admission_sources == [source]
        assert creation_sources == [source]
        assert admission_sources[0] is creation_sources[0] is source
        assert len(provider_context_entries) == 1
        assert lifecycle == [("admission", False), ("create", True), ("delete", True)]
        assert len(admission_states) == 1
        assert not admission_states[0].cleanup_pending


class TestProcessTaskEnvironment:
    """Tracker-owned environment variables passed to agent tasks."""

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_injects_tracker_owned_attribution_env(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "kwargs": {"variant": "xhigh"},
                "secrets": {"UNRELATED_SECRET": "secret-name"},
                "inference_settings_attested": True,
            }
        )
        run_starter = RequestIdentity(
            org=TEST_ORG,
            access_key_id="access-key-id",
            email="starter@example.com",
            name="Starter User",
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            run_starter,
        )
        start_benchmark_request = start_benchmark_request.model_copy(
            update={
                "benchmark_name": "transient-benchmark-name",
                "contract": contract.model_copy(update={"name": "transient-agent-name"}),
            }
        )
        captured_env_vars: list[dict[str, str]] = []
        minted: list[dict[str, Any]] = []
        _install_gateway(monkeypatch, minted)

        async def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {
                "RUN_ID": "secret-run-id",
                "TASK_ID": "secret-task-id",
                "VALKYRIE_AGENT_MODEL": "secret-model",
                "VALKYRIE_AGENT_VARIANT": "secret-variant",
                "IDENTITY": '{"source":"secret"}',
                "UNRELATED_SECRET": "secret-value",
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
            }

        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_env_vars) == 1
        env_vars = captured_env_vars[0]
        assert env_vars["RUN_ID"] == str(benchmark_id)
        assert "QUESTION_ID" not in env_vars
        assert env_vars["TASK_ID"] == "task_0"
        assert env_vars["VALKYRIE_AGENT_MODEL"] == "provider/model"
        assert env_vars["VALKYRIE_AGENT_VARIANT"] == "xhigh"
        assert json.loads(env_vars["IDENTITY"]) == {
            "benchmark_name": "swebench",
            "agent_name": contract.name,
            "email": "starter@example.com",
        }
        assert env_vars["UNRELATED_SECRET"] == "secret-value"
        assert env_vars["MODEL_GATEWAY_URL"] == "https://gateway.example.test"
        assert env_vars["MODEL_GATEWAY_API_KEY"] == "mgwt_scoped"
        assert minted == [
            {
                "run_id": str(benchmark_id),
                "task_id": "task_0",
                "attempt_id": env_vars["ATTEMPT_ID"],
                "allowed_models": ["provider/model"],
                "identity": {
                    "benchmark_name": "swebench",
                    "agent_name": contract.name,
                    "email": "starter@example.com",
                },
                "ttl_seconds": 7 * 24 * 60 * 60,
            }
        ]

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize("allowance", [None, 10.0], ids=["unbounded-generation", "credited-generation"])
    async def test_eligible_task_mints_native_bearer_and_persists_terminal_counter(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        allowance: float | None,
    ) -> None:
        credit_cap_seconds = 5.0
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "kwargs": {"variant": "xhigh"},
                "secrets": {
                    "MODEL_GATEWAY_URL": "upstream-url-secret",
                    "MODEL_GATEWAY_API_KEY": "upstream-key-secret",
                    "UNRELATED_SECRET": "unrelated-secret",
                },
                "inference_settings_attested": True,
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        # A pending retry carries the original wall deadline into native mint.
        original_deadline = datetime.now(UTC) + timedelta(seconds=5)
        task_row.credited_wall_deadline_at = original_deadline
        database_session.add(task_row)
        database_session.commit()
        task_response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": allowance,
                "credited_generation": (
                    CreditedGeneration(allowance_seconds=allowance, stage_protocol=None)
                    if allowance is not None
                    else None
                ),
            }
        )
        captured_env_vars: list[dict[str, str]] = []
        resolved_references: list[dict[str, str]] = []
        mint_requests: list[httpx.Request] = []
        minted: list[dict[str, Any]] = []
        _install_gateway(monkeypatch, minted, mint_requests)

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        async def resolve_secrets(references: dict[str, str], *_args: Any, **_kwargs: Any) -> dict[str, str]:
            resolved_references.append(references)
            return {
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
                "UNRELATED_SECRET": "unrelated-value",
            }

        accounting_id = sha256(b"mgwt_scoped").hexdigest()
        counter_requests: list[httpx.Request] = []
        counter_values = [(2_000, 2), (7_000, 3)]

        def read_counter(request: httpx.Request) -> httpx.Response:
            counter_requests.append(request)
            overhead_ms, revision = counter_values.pop(0)
            return httpx.Response(
                200,
                json={
                    "accounting_id": accounting_id,
                    "cumulative_neutral_overhead_ms": overhead_ms,
                    "revision": revision,
                },
            )

        ssp_http = httpx.AsyncClient(transport=httpx.MockTransport(read_counter))

        async def capture_run_agent(
            *_args: Any,
            deadline_controller: Any,
            on_external_service_finalized: Callable[[GenerationAccountingSummary], Awaitable[None]],
            **_kwargs: Any,
        ) -> tuple[None, float]:
            deadline_controller.begin_generation(now=100.0)
            await deadline_controller.refresh()
            assert deadline_controller.effective_allowance_seconds() == (
                allowance + 2.0 if allowance is not None else None
            )
            deadline_controller.end_generation(now=104.0)
            await finalize_agent_generation(
                deadline_controller=deadline_controller,
                on_external_service_finalized=on_external_service_finalized,
            )
            assert deadline_controller.effective_allowance_seconds() == (
                allowance + credit_cap_seconds if allowance is not None else None
            )
            return None, 1.0

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", resolve_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )
        monkeypatch.setattr(utils_module, "run_agent", capture_run_agent)
        monkeypatch.setattr(
            utils_module,
            "ExternalServiceGatewayClient",
            partial(ExternalServiceGatewayClient, client=ssp_http),
        )
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", credit_cap_seconds)

        try:
            result = await run_process_task(
                start_benchmark_request,
                task_row,
                benchmark_id,
                runtime_services,
                authority,
            )
        finally:
            await ssp_http.aclose()

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert resolved_references == [contract.secrets]
        assert captured_env_vars[0]["MODEL_GATEWAY_URL"] == "https://gateway.example.test"
        assert captured_env_vars[0]["MODEL_GATEWAY_API_KEY"] == "mgwt_scoped"
        assert [str(request.url) for request in counter_requests] == [
            f"http://local-gateway/counters/{accounting_id}",
            f"http://local-gateway/counters/{accounting_id}",
        ]
        assert all(request.headers["X-SSP-Control-Token"] == "tracker-control" for request in counter_requests)
        assert counter_values == []
        assert len(mint_requests) == 1
        assert str(mint_requests[0].url) == "https://gateway.example.test/service-auth"
        assert mint_requests[0].headers["Authorization"] == "Bearer gateway-key"
        assert "X-SSP-Session-ID" not in mint_requests[0].headers
        assert "X-SSP-Control-Token" not in mint_requests[0].headers
        assert minted[0]["allowed_models"] == ["provider/model"]
        assert minted[0]["ttl_seconds"] == 7 * 24 * 60 * 60
        assert captured_env_vars[0]["UNRELATED_SECRET"] == "unrelated-value"
        assert captured_env_vars[0]["VALKYRIE_AGENT_VARIANT"] == "xhigh"
        database_session.expire_all()
        persisted_task = database_session.get(Task, task_row.id)
        assert persisted_task is not None
        assert persisted_task.credited_wall_deadline_at is not None
        assert persisted_task.credited_wall_deadline_at.replace(tzinfo=UTC) == original_deadline
        assert persisted_task.task_breakdown is not None
        breakdown = database_session.get(TaskBreakdown, persisted_task.task_breakdown)
        assert breakdown is not None
        assert getattr(breakdown, "accounting_id") == accounting_id
        assert getattr(breakdown, "external_service_overhead_seconds") == 7.0
        assert getattr(breakdown, "base_generation_allowance_seconds") == allowance
        assert getattr(breakdown, "external_service_credit_applied_seconds") == credit_cap_seconds
        assert getattr(breakdown, "external_service_credit_revision") == 3
        assert getattr(breakdown, "generation_elapsed_seconds") == 4.0

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize("handoff", ["stop", "retry"])
    async def test_stopped_or_retried_attempt_rejects_prior_finalized_accounting(
        self, contract: AgentContractRequest, database_session: Session, handoff: str
    ) -> None:
        _, task_row, _, authority = create_task_environment(contract, database_session)
        bind_task_to_dispatch(database_session, task_row, authority)
        task_row.status = TaskStatus.IN_PROGRESS
        database_session.add(task_row)
        database_session.commit()
        expected_started_at = task_row.started_at
        assert expected_started_at is not None
        bind = database_session.get_bind()
        assert isinstance(bind, Engine)
        summary = GenerationAccountingSummary(
            accounting_id=sha256(b"prior-attempt-bearer").hexdigest(),
            base_generation_allowance_seconds=10.0,
            cumulative_time_credit_cap_seconds=5.0,
            external_service_overhead_seconds=2.0,
            external_service_credit_applied_seconds=2.0,
            external_service_credit_revision=3,
            generation_elapsed_seconds=3.0,
        )

        with Session(bind=bind) as handoff_session:
            current_task = handoff_session.get(Task, task_row.id)
            assert current_task is not None
            if handoff == "stop":
                current_task.status = TaskStatus.STOPPED
            else:
                current_task.started_at = expected_started_at + timedelta(seconds=1)
            handoff_session.add(current_task)
            handoff_session.commit()

        with pytest.raises(utils_module.ExecutionAuthorityRevoked):
            utils_module._persist_generation_accounting_summary(
                summary,
                task_breakdown=TaskBreakdown(),
                task_row_id=task_row.id,
                org=TEST_ORG,
                authority=authority,
                expected_started_at=expected_started_at,
                open_task_session=lambda: Session(bind=bind),
            )

        with Session(bind=bind) as verify_session:
            current_task = verify_session.get(Task, task_row.id)
            assert current_task is not None
            if handoff == "stop":
                assert current_task.status == TaskStatus.STOPPED
                assert current_task.started_at == expected_started_at
            else:
                assert current_task.status == TaskStatus.IN_PROGRESS
                assert current_task.started_at == expected_started_at + timedelta(seconds=1)
            assert current_task.task_breakdown is None
            assert verify_session.exec(select(TaskBreakdown)).all() == []

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_accounting_persistence_failure_blocks_evaluation(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "inference_settings_attested": True,
                "secrets": {"MODEL_GATEWAY_URL": "gateway-url", "MODEL_GATEWAY_API_KEY": "gateway-key"},
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        task_response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": 10.0,
                "credited_generation": CreditedGeneration(allowance_seconds=10.0, stage_protocol=None),
            }
        )
        persistence_error = RuntimeError("summary persistence failed")
        evaluate_instance = AsyncMock()

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        async def resolve_native_gateway(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {"MODEL_GATEWAY_URL": "https://gateway.example.test", "MODEL_GATEWAY_API_KEY": "gateway-key"}

        async def fail_during_agent(
            *_args: Any,
            on_external_service_finalized: Callable[[GenerationAccountingSummary], Awaitable[None]],
            **_kwargs: Any,
        ) -> tuple[None, float]:
            await on_external_service_finalized(
                GenerationAccountingSummary(
                    accounting_id=sha256(b"mgwt_scoped").hexdigest(),
                    base_generation_allowance_seconds=10.0,
                    cumulative_time_credit_cap_seconds=5.0,
                    external_service_overhead_seconds=1.0,
                    external_service_credit_applied_seconds=1.0,
                    external_service_credit_revision=1,
                    generation_elapsed_seconds=2.0,
                )
            )
            raise AssertionError("unreachable")

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        _install_gateway(monkeypatch, [])
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate_instance)
        monkeypatch.setattr(
            "tracker.utils.task_execution.resolve_secrets",
            resolve_native_gateway,
        )
        monkeypatch.setattr(utils_module, "run_agent", fail_during_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)
        monkeypatch.setattr(
            utils_module,
            "_persist_generation_accounting_summary",
            Mock(side_effect=persistence_error),
        )

        result = await run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            runtime_services,
            authority,
        )

        assert result == {"task_0": None}
        evaluate_instance.assert_not_awaited()

    @pytest.mark.parametrize(
        ("gateway_url", "control_token", "attested", "resolved", "allowance"),
        [
            (
                None,
                "tracker-control",
                True,
                {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "key"},
                10.0,
            ),
            (
                "http://local-gateway",
                None,
                True,
                {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "key"},
                10.0,
            ),
            (
                "http://local-gateway",
                "tracker-control",
                False,
                {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "key"},
                None,
            ),
            ("http://local-gateway", "tracker-control", True, {"MODEL_GATEWAY_URL": "https://gateway.test"}, 10.0),
            ("http://local-gateway", "tracker-control", True, {"MODEL_GATEWAY_API_KEY": "key"}, None),
        ],
    )
    async def test_ineligible_tasks_keep_their_local_controller_without_an_ssp_counter(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
        gateway_url: str | None,
        control_token: str | None,
        attested: bool,
        resolved: dict[str, str],
        allowance: float | None,
    ) -> None:
        selected = contract.model_copy(update={"model": "provider/model", "inference_settings_attested": attested})
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", gateway_url)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", control_token)

        controller = await utils_module._create_generation_deadline_controller(
            selected,
            GenerationPlan(allowance_seconds=allowance, staged=False, episode=None),
            resolved,
            {**resolved, "MODEL_GATEWAY_API_KEY": "scoped-token"},
        )

        assert controller.base_allowance_seconds == allowance
        assert controller.client is None

    @pytest.mark.parametrize("allowance", [None, 10.0], ids=["unbounded-generation", "timed-generation"])
    @pytest.mark.parametrize("persisted_retry", [False, True], ids=["new-attempt", "live-retry"])
    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_base_only_task_keeps_its_whole_task_wall_without_model_or_ssp_session(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        persisted_retry: bool,
        allowance: float | None,
    ) -> None:
        contract = contract.model_copy(update={"model": None, "inference_settings_attested": False})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        original_deadline = datetime.now(UTC) + timedelta(seconds=5) if persisted_retry else None
        if original_deadline is not None:
            task_row.credited_wall_deadline_at = original_deadline
            database_session.add(task_row)
            database_session.commit()
        task_response = make_retrieve_task_response().model_copy(update={"agent_timeout": allowance})
        observed: list[tuple[float | None, Any, str | None]] = []

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        async def capture_run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            observed.append(
                (kwargs["plan"].allowance_seconds, kwargs["deadline_controller"], kwargs["generation_gateway_url"])
            )
            return await _complete_base_only_agent(**kwargs)

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module, "run_agent", capture_run_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", None)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(observed) == 1
        selected_allowance, deadline, generation_gateway_url = observed[0]
        assert selected_allowance == allowance
        assert deadline.base_allowance_seconds == allowance
        assert deadline.client is None
        assert generation_gateway_url is None
        database_session.refresh(task_row)
        assert task_row.credited_wall_deadline_at is not None
        if original_deadline is not None:
            assert task_row.credited_wall_deadline_at.replace(tzinfo=UTC) == original_deadline
        else:
            remaining = (task_row.credited_wall_deadline_at.replace(tzinfo=UTC) - datetime.now(UTC)).total_seconds()
            assert 165 * 3600 < remaining <= 166 * 3600

    @pytest.mark.parametrize("native_key", [False, True], ids=["without-token", "with-token"])
    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_task_wall_expires_during_agent_execution_with_or_without_gateway_key(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        native_key: bool,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model" if native_key else None,
                "inference_settings_attested": native_key,
                "secrets": {"MODEL_GATEWAY_URL": "gateway-url", "MODEL_GATEWAY_API_KEY": "gateway-key"}
                if native_key
                else {},
            }
        )
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        captured_env_vars: list[dict[str, str]] = []
        minted: list[dict[str, Any]] = []
        revoked = asyncio.Event()

        async def revoke() -> None:
            revoked.set()

        async def resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            if not native_key:
                return {}
            return {"MODEL_GATEWAY_URL": "https://gateway.example.test", "MODEL_GATEWAY_API_KEY": "gateway-key"}

        async def wait_for_wall(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            await asyncio.Event().wait()
            pytest.fail("the wall deadline must interrupt agent execution")

        _install_gateway(monkeypatch, minted, on_revoke=revoke)
        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", resolve_secrets)
        monkeypatch.setattr(utils_module, "create_sandbox", partial(_capture_sandbox_environment, captured_env_vars))
        monkeypatch.setattr(utils_module, "run_agent", wait_for_wall)
        monkeypatch.setattr(utils_module, "TASK_WALL_SECONDS", 0.5)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)

        result = await asyncio.wait_for(
            run_process_task(request, task_row, benchmark_id, runtime_services, authority), timeout=5
        )

        assert result == {"task_0": None}
        assert len(captured_env_vars) == 1
        if native_key:
            assert captured_env_vars[0]["MODEL_GATEWAY_API_KEY"] == "mgwt_scoped"
        else:
            assert "MODEL_GATEWAY_API_KEY" not in captured_env_vars[0]
        assert len(minted) == int(native_key)
        assert revoked.is_set() is native_key
        with Session(database_session.get_bind()) as session:
            errors = session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
            assert len(errors) == 1
            assert errors[0].error_type == "CreditedTaskWallTimeExceeded"
            assert errors[0].cause_code == "credited_task_wall_time_exceeded"

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_sandbox_creation_error_survives_revoke_past_wall_deadline(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "inference_settings_attested": True,
                "secrets": {"MODEL_GATEWAY_URL": "gateway-url", "MODEL_GATEWAY_API_KEY": "gateway-key"},
            }
        )
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        minted: list[dict[str, Any]] = []
        revoke_complete = asyncio.Event()
        creation_failed = asyncio.Event()
        wall_timeouts: list[asyncio.Timeout] = []
        original_timeout_at = asyncio.timeout_at

        def record_timeout_at(when: float | None) -> asyncio.Timeout:
            timeout = original_timeout_at(when)
            wall_timeouts.append(timeout)
            return timeout

        async def slow_revoke() -> None:
            release = asyncio.Event()
            asyncio.get_running_loop().call_soon(release.set)
            await release.wait()
            revoke_complete.set()

        async def resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {"MODEL_GATEWAY_URL": "https://gateway.example.test", "MODEL_GATEWAY_API_KEY": "gateway-key"}

        @asynccontextmanager
        async def failed_creation(*_args: Any, **_kwargs: Any) -> AsyncGenerator[SimpleNamespace, None]:
            creation_failed.set()
            assert wall_timeouts and not wall_timeouts[-1].expired()
            wall_timeouts[-1].reschedule(asyncio.get_running_loop().time())
            raise RuntimeError("sandbox creation failed")
            yield SimpleNamespace(id="unreachable")

        _install_gateway(monkeypatch, minted, on_revoke=slow_revoke)
        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", resolve_secrets)
        monkeypatch.setattr(utils_module, "create_sandbox", failed_creation)
        monkeypatch.setattr(asyncio, "timeout_at", record_timeout_at)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)

        result = await asyncio.wait_for(
            run_process_task(request, task_row, benchmark_id, runtime_services, authority), timeout=5
        )

        assert result == {"task_0": None}
        assert creation_failed.is_set()
        assert revoke_complete.is_set()
        assert len(minted) == 1
        with Session(database_session.get_bind()) as session:
            errors = session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
            assert len(errors) == 1
            assert errors[0].error_type == "RuntimeError"
            assert errors[0].error_message == "RuntimeError: sandbox creation failed"

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_credited_wall_timer_does_not_interrupt_sandbox_teardown(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"model": None, "inference_settings_attested": False})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": 10.0,
                "credited_generation": CreditedGeneration(allowance_seconds=10.0, stage_protocol=None),
            }
        )
        teardown_complete = asyncio.Event()

        @asynccontextmanager
        async def delayed_teardown(*_args: Any, **_kwargs: Any) -> AsyncGenerator[SimpleNamespace, None]:
            try:
                yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")
            finally:
                await asyncio.sleep(0.6)
                teardown_complete.set()

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", AsyncMock(return_value=response))
        monkeypatch.setattr(utils_module, "create_sandbox", delayed_teardown)

        async def complete_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            await _complete_base_only_agent(**kwargs)
            return None, 0.0

        monkeypatch.setattr(utils_module, "run_agent", complete_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)
        monkeypatch.setattr(utils_module, "TASK_WALL_SECONDS", 0.5)

        result = await asyncio.wait_for(
            run_process_task(request, task_row, benchmark_id, runtime_services, authority), timeout=3
        )

        assert teardown_complete.is_set()
        assert result == {"task_0": {"status": "success", "score": 1.0}}
        database_session.refresh(task_row)
        assert task_row.credited_wall_deadline_at is not None
        assert datetime.now(UTC) > task_row.credited_wall_deadline_at.replace(tzinfo=UTC)

    async def test_distinct_scoped_tokens_have_distinct_accounting_ids(
        self, contract: AgentContractRequest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        selected = contract.model_copy(update={"model": "provider/model", "inference_settings_attested": True})
        resolved = {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "static"}
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)

        first = await utils_module._create_generation_deadline_controller(
            selected,
            GenerationPlan(allowance_seconds=None, staged=False, episode=None),
            resolved,
            {**resolved, "MODEL_GATEWAY_API_KEY": "scoped-one"},
        )
        second = await utils_module._create_generation_deadline_controller(
            selected,
            GenerationPlan(allowance_seconds=10.0, staged=False, episode=None),
            resolved,
            {**resolved, "MODEL_GATEWAY_API_KEY": "scoped-two"},
        )

        assert first.snapshot.accounting_id == sha256(b"scoped-one").hexdigest()
        assert second.snapshot.accounting_id == sha256(b"scoped-two").hexdigest()
        assert first.snapshot.accounting_id != second.snapshot.accounting_id

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize(
        ("attested", "model"),
        [(False, "provider/model"), (True, None), (True, ""), (True, "   ")],
    )
    async def test_ineligible_model_runs_base_only_without_ssp_accounting(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        attested: bool,
        model: str | None,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": model,
                "inference_settings_attested": attested,
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        task_response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": 10.0,
                "credited_generation": CreditedGeneration(allowance_seconds=10.0, stage_protocol=None),
            }
        )
        captured_env: list[dict[str, str]] = []

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module, "create_sandbox", partial(_capture_sandbox_environment, captured_env))
        monkeypatch.setattr(utils_module, "run_agent", _complete_base_only_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_env) == 1
        database_session.expire_all()
        assert database_session.get(Task, task_row.id).status == TaskStatus.FINISHED

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_missing_native_gateway_credentials_runs_base_only(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "inference_settings_attested": True,
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        task_response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": 10.0,
                "credited_generation": CreditedGeneration(allowance_seconds=10.0, stage_protocol=None),
            }
        )
        captured_env: list[dict[str, str]] = []

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module, "create_sandbox", partial(_capture_sandbox_environment, captured_env))
        monkeypatch.setattr(utils_module, "run_agent", _complete_base_only_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)
        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", AsyncMock(return_value={}))

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_env) == 1
        assert "MODEL_GATEWAY_API_KEY" not in captured_env[0]
        database_session.expire_all()
        assert database_session.get(Task, task_row.id).status == TaskStatus.FINISHED

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_injects_attempt_id_that_changes_on_rerun(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """One execution carries one ATTEMPT_ID; a whole-task rerun carries another."""
        run_starter = RequestIdentity(
            org=TEST_ORG,
            access_key_id="access-key-id",
            email="starter@example.com",
            name="Starter User",
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            run_starter,
        )
        captured_env_vars: list[dict[str, str]] = []
        _install_gateway(monkeypatch, [])
        monkeypatch.setattr(
            "tracker.utils.task_execution.resolve_secrets",
            AsyncMock(return_value={"ATTEMPT_ID": "secret-attempt-id"}),
        )
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)
        first_attempt_id = captured_env_vars[0]["ATTEMPT_ID"]
        assert first_attempt_id == utils_module.task_attempt_id(task_row.id, task_row.started_at)

        # A whole-task rerun advances started_at the way run_control does.
        database_session.refresh(task_row)
        task_row.status = TaskStatus.PENDING
        task_row.started_at = task_row.started_at + timedelta(microseconds=1)
        database_session.add(task_row)
        database_session.commit()
        database_session.refresh(task_row)

        await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert len(captured_env_vars) == 2
        second_attempt_id = captured_env_vars[1]["ATTEMPT_ID"]
        assert second_attempt_id == utils_module.task_attempt_id(task_row.id, task_row.started_at)
        assert second_attempt_id != first_attempt_id

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_withholds_unattested_inference_settings(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """A caller-supplied contract must not reach setup as trusted settings."""
        contract = contract.model_copy(
            update={
                "model": "caller/model",
                "kwargs": {"variant": "caller-variant"},
                "install_cmd": "echo install",
                "run_cmd": "echo run",
            }
        )
        assert contract.inference_settings_attested is False
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        captured_env_vars: list[dict[str, str]] = []

        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", AsyncMock(return_value={}))
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert len(captured_env_vars) == 1
        env_vars = captured_env_vars[0]
        assert "VALKYRIE_AGENT_MODEL" not in env_vars
        assert "VALKYRIE_AGENT_VARIANT" not in env_vars

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_does_not_scope_credentials_to_an_injected_model(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """A contract names its own secrets' variables, so it can put anything
        under VALKYRIE_AGENT_MODEL. Unattested, that must not mint a credential."""
        contract = contract.model_copy(
            update={
                "model": "caller/model",
                "secrets": {"VALKYRIE_AGENT_MODEL": "secret-name"},
                "install_cmd": "echo install",
                "run_cmd": "echo run",
            }
        )
        assert contract.inference_settings_attested is False
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        captured_env_vars: list[dict[str, str]] = []
        minted: list[dict[str, Any]] = []
        _install_gateway(monkeypatch, minted)

        async def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {
                "VALKYRIE_AGENT_MODEL": "anthropic/claude-4-opus",
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
            }

        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert minted == []
        assert captured_env_vars[0]["MODEL_GATEWAY_API_KEY"] == "gateway-key"

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_omits_identity_email_when_unavailable(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"inference_settings_attested": True})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        captured_env_vars: list[dict[str, str]] = []

        async def _mock_resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {}

        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", _mock_resolve_no_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_env_vars) == 1
        env_vars = captured_env_vars[0]
        assert env_vars["VALKYRIE_AGENT_MODEL"] == ""
        assert env_vars["VALKYRIE_AGENT_VARIANT"] == ""
        assert json.loads(env_vars["IDENTITY"]) == {
            "benchmark_name": "swebench",
            "agent_name": contract.name,
        }
        assert "MODEL_GATEWAY_URL" not in env_vars
        assert "MODEL_GATEWAY_API_KEY" not in env_vars

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_forwards_native_secret_references_without_resolving_values(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"secrets": {"LEGACY_API_KEY": "aws-secret"}})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        captured: dict[str, dict[str, str]] = {}
        resolved_inputs: list[dict[str, str]] = []

        async def _mock_resolve_secrets(secrets: dict[str, str], *_args: Any, **_kwargs: Any) -> dict[str, str]:
            resolved_inputs.append(secrets)
            return {"LEGACY_API_KEY": "legacy-value"}

        @asynccontextmanager
        async def _capture_sandbox(*_args: Any, **kwargs: Any) -> AsyncGenerator[SimpleNamespace, None]:
            captured["env_vars"] = kwargs["env_vars"]
            captured["sandbox_secrets"] = kwargs["sandbox_secrets"]
            yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> Any:
            response = make_retrieve_task_response()
            response.sandbox_secrets = {"TAVILY_API_KEY": "daytona-tavily"}
            return response

        monkeypatch.setattr("tracker.utils.task_execution.resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(utils_module, "create_sandbox", _capture_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert resolved_inputs == [{"LEGACY_API_KEY": "aws-secret"}]
        assert captured["sandbox_secrets"] == {"TAVILY_API_KEY": "daytona-tavily"}
        assert captured["env_vars"]["LEGACY_API_KEY"] == "legacy-value"
        assert "TAVILY_API_KEY" not in captured["env_vars"]

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_stopped_task_output_is_fenced_while_sibling_keeps_dispatch_active(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        benchmark = database_session.get(Benchmark, benchmark_id)
        assert benchmark is not None
        benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": 2})
        sibling = Task(
            org_id=task_row.org_id,
            benchmark=benchmark_id,
            task_id="task_sibling",
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(sibling)
        database_session.commit()
        output_authority_checks: list[bool] = []

        async def stop_before_output(
            *_args: Any,
            execution_is_current: Callable[[], bool],
            **_kwargs: Any,
        ) -> tuple[None, float]:
            selected = database_session.get(Task, task_row.id)
            assert selected is not None
            selected.status = TaskStatus.STOPPED
            database_session.add(selected)
            database_session.commit()

            dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
            assert dispatch is not None
            assert dispatch.status == ExecutorDispatchStatus.RUNNING
            persisted_sibling = database_session.get(Task, sibling.id)
            assert persisted_sibling is not None
            assert persisted_sibling.status == TaskStatus.IN_PROGRESS

            output_authority_checks.append(execution_is_current())
            return None, 0.0

        monkeypatch.setattr(utils_module, "run_agent", stop_before_output)

        result = await run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            runtime_services,
            authority,
        )

        assert output_authority_checks == [False]
        assert result == {task_row.task_id: None}


@pytest.mark.usefixtures("process_benchmark_env")
async def test_task_waits_for_final_log_write(
    contract: AgentContractRequest,
    database_session: Session,
    runtime_services: RuntimeServices,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task completion must not race a buffered write still running in a thread."""
    request, task, benchmark_id, authority = create_task_environment(contract, database_session)
    loop = asyncio.get_running_loop()
    writing = asyncio.Event()
    release_write = threading.Event()
    written: list[str] = []

    async def run_agent(*args: Any, **kwargs: Any) -> tuple[None, float]:
        cast(Callable[[str], None], args[4])("final agent message")
        await _complete_base_only_agent(**kwargs)
        return None, 0.0

    def write(_self: object, _stream: str, message: str) -> None:
        loop.call_soon_threadsafe(writing.set)
        if not release_write.wait(timeout=5):
            raise TimeoutError("test did not release the log write")
        written.append(message)

    monkeypatch.setattr(utils_module, "run_agent", run_agent)
    monkeypatch.setattr("tracker.aws.cloudwatch_logs.CloudWatchBenchmarkLogSink.write", write)
    execution = asyncio.create_task(run_process_task(request, task, benchmark_id, runtime_services, authority))
    try:
        await asyncio.wait_for(writing.wait(), timeout=2)
        assert not execution.done()
    finally:
        release_write.set()
        await asyncio.wait_for(execution, timeout=2)
    assert any("final agent message" in message for message in written)


def test_task_log_writes_leave_default_executor_available_across_loops() -> None:
    with ThreadPoolExecutor(max_workers=1) as default_executor:

        async def first_loop() -> None:
            loop = asyncio.get_running_loop()
            loop.set_default_executor(default_executor)
            started = asyncio.Event()
            release_write = threading.Event()
            written: list[str] = []

            def held_write(_stream: str, message: str) -> None:
                loop.call_soon_threadsafe(started.set)
                if not release_write.wait(timeout=5):
                    raise TimeoutError("test did not release the log write")
                written.append(message)

            sink = Mock()
            sink.write.side_effect = held_write
            logs = TaskLogBuffer(sink, "first-stream")
            try:
                for index in range(20):
                    logs.write(f"{index}\n")
                await asyncio.wait_for(started.wait(), timeout=2)
                closing = asyncio.create_task(logs.close())
                assert await asyncio.wait_for(asyncio.to_thread(lambda: "unrelated"), timeout=2) == "unrelated"
                assert not closing.done()
            finally:
                release_write.set()
                await asyncio.wait_for(logs.close(), timeout=2)
            assert written == ["".join(f"{index}\n" for index in range(20))]

        asyncio.run(first_loop())

    async def second_loop() -> None:
        sink = Mock()
        logs = TaskLogBuffer(sink, "second-stream")
        logs.write("second loop")
        await logs.close()
        sink.write.assert_called_once_with("second-stream", "second loop")

    asyncio.run(second_loop())
