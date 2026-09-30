"""Unit tests for task execution environment assembly.

Run: uv run pytest tests/unit/utils/test_task_execution_env.py
"""

import asyncio
import json
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from benchmark_service import SandboxSource, TargetedSnapshotSource
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

import tracker.utils.task_execution as utils_module
from tests.unit.utils.task_execution_support import (
    TEST_ORG,
    bind_task_to_dispatch,
    create_task_environment,
    make_retrieve_task_response,
    run_process_task,
)
from tracker.auth import RequestIdentity
from tracker.runtime.services import RuntimeServices
from tracker.external_service_gateway import (
    AccountingSessionSnapshot,
    ExternalServiceAccountingSummary,
    AccountingSessionState,
)
from tracker.database.models import (
    AgentContractRequest,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.scheduler.admission import SandboxQueueContext
import tracker.runtime.model_gateway as model_gateway_module
from tracker.types import HarnessConfig


def _install_gateway(
    monkeypatch: pytest.MonkeyPatch,
    minted: list[dict[str, Any]],
    mint_requests: list[httpx.Request] | None = None,
) -> None:
    """Answer the tracker's run-token mint and revoke calls in process."""

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/service-auth":
            if mint_requests is not None:
                mint_requests.append(request)
            minted.append(json.loads(request.content))
            return httpx.Response(
                200, json={"token": "mgwt_scoped", "lease_id": "lease-1", "expires_at": 1_800_000_000.0}
            )
        assert request.url.path == "/service-auth/revoke", request.url.path
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


class TestQueuedTaskSource:
    """Source propagation through queued task execution."""

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_targeted_source_is_shared_by_admission_and_creation(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        source = TargetedSnapshotSource(snapshot="snapshot", target="us-west-3")
        task_response = make_retrieve_task_response().model_copy(update={"source": source})
        admission_sources: list[SandboxSource] = []
        creation_sources: list[SandboxSource] = []

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        @asynccontextmanager
        async def capture_sandbox(
            *_args: Any,
            source: SandboxSource,
            **_kwargs: Any,
        ) -> AsyncGenerator[SimpleNamespace, None]:
            creation_sources.append(source)
            yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

        async def enter_queue(
            *,
            stack: Any,
            task_row_id: Any,
            source: SandboxSource,
            create: Callable[[], Any],
            **_kwargs: Any,
        ) -> Any:
            admission_sources.append(source)
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
        queue_context = SandboxQueueContext(provider=Mock(), pool_id="pool_test", engine=task_engine)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module, "create_sandbox", capture_sandbox)
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


class TestProcessTaskEnvironment:
    """Tracker-owned environment variables passed to agent tasks."""

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_injects_tracker_owned_attribution_env(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
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
            harness_config,
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

        def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
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

        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", _mock_resolve_secrets)
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
    @pytest.mark.parametrize("credit_cap_seconds", [5.0, 7205.25])
    async def test_controlled_task_routes_model_gateway_through_accounting_session(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
        credit_cap_seconds: float,
    ) -> None:
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
            harness_config,
        )
        # A pending retry carries the original wall deadline into native mint.
        original_deadline = datetime.now(UTC) + timedelta(seconds=5)
        task_row.credited_wall_deadline_at = original_deadline
        database_session.add(task_row)
        database_session.commit()
        task_response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": 10.0,
                "credited_generation": True,
            }
        )
        captured_env_vars: list[dict[str, str]] = []
        resolved_references: list[dict[str, str]] = []
        mint_requests: list[httpx.Request] = []
        minted: list[dict[str, Any]] = []
        _install_gateway(monkeypatch, minted, mint_requests)
        captured_deadlines: list[Any] = []

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        def resolve_secrets(references: dict[str, str], *_args: Any, **_kwargs: Any) -> dict[str, str]:
            resolved_references.append(references)
            return {
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
                "UNRELATED_SECRET": "unrelated-value",
            }

        async def capture_run_agent(
            *_args: Any,
            external_service_deadline: Any,
            on_external_service_sealed: Callable[[ExternalServiceAccountingSummary], Awaitable[None]],
            **_kwargs: Any,
        ) -> tuple[None, float]:
            captured_deadlines.append(external_service_deadline)
            await on_external_service_sealed(
                ExternalServiceAccountingSummary(
                    accounting_session_id="session-1",
                    base_generation_allowance_seconds=10.0,
                    cumulative_time_credit_cap_seconds=credit_cap_seconds,
                    external_service_overhead_seconds=2.0,
                    external_service_credit_applied_seconds=2.0,
                    effective_generation_allowance_seconds=12.0,
                    external_service_credit_revision=3,
                )
            )
            return None, 1.0

        create_session = AsyncMock(
            return_value=AccountingSessionSnapshot(
                session_id="session-1",
                state=AccountingSessionState.OPEN,
                cumulative_neutral_overhead_ms=0,
                revision=0,
                accounting_epoch=0,
                generation_active=False,
                interval_index=0,
            )
        )
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", resolve_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )
        monkeypatch.setattr(utils_module, "run_agent", capture_run_agent)
        monkeypatch.setattr(
            utils_module.ExternalServiceGatewayClient,
            "create_session",
            create_session,
        )
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", credit_cap_seconds)

        result = await run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            runtime_services,
            authority,
        )

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert resolved_references == [contract.secrets]
        assert captured_env_vars[0]["MODEL_GATEWAY_URL"] == "http://local-gateway"
        assert captured_env_vars[0]["MODEL_GATEWAY_API_KEY"] == "mgwt_scoped"
        assert captured_env_vars[0]["MODEL_GATEWAY_API_KEY"] != "session-1"
        assert len(mint_requests) == 1
        assert str(mint_requests[0].url) == "http://local-gateway/service-auth"
        assert mint_requests[0].headers["Authorization"] == "Bearer gateway-key"
        assert mint_requests[0].headers["X-SSP-Session-ID"] == "session-1"
        assert mint_requests[0].headers["X-SSP-Control-Token"] == "tracker-control"
        assert minted[0]["allowed_models"] == ["provider/model"]
        assert minted[0]["ttl_seconds"] == 7 * 24 * 60 * 60
        assert captured_env_vars[0]["UNRELATED_SECRET"] == "unrelated-value"
        assert captured_env_vars[0]["VALKYRIE_AGENT_VARIANT"] == "xhigh"
        assert captured_deadlines[0].base_allowance_seconds == 10.0
        assert captured_deadlines[0].credit_cap_seconds == credit_cap_seconds
        database_session.expire_all()
        persisted_task = database_session.get(Task, task_row.id)
        assert persisted_task is not None
        assert persisted_task.credited_wall_deadline_at is not None
        assert persisted_task.credited_wall_deadline_at.replace(tzinfo=UTC) == original_deadline
        assert persisted_task.task_breakdown is not None
        breakdown = database_session.get(TaskBreakdown, persisted_task.task_breakdown)
        assert breakdown is not None
        assert getattr(breakdown, "accounting_session_id") == "session-1"
        assert getattr(breakdown, "external_service_overhead_seconds") == 2.0
        assert getattr(breakdown, "external_service_credit_revision") == 3

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize("handoff", ["stop", "retry"])
    async def test_stopped_or_retried_attempt_rejects_prior_sealed_accounting(
        self, contract: AgentContractRequest, database_session: Session, harness_config: HarnessConfig, handoff: str
    ) -> None:
        _, task_row, _, authority = create_task_environment(contract, database_session, harness_config)
        bind_task_to_dispatch(database_session, task_row, authority)
        task_row.status = TaskStatus.IN_PROGRESS
        database_session.add(task_row)
        database_session.commit()
        expected_started_at = task_row.started_at
        assert expected_started_at is not None
        bind = database_session.get_bind()
        assert isinstance(bind, Engine)
        summary = ExternalServiceAccountingSummary(
            accounting_session_id="sealed-prior-attempt",
            base_generation_allowance_seconds=10.0,
            cumulative_time_credit_cap_seconds=5.0,
            external_service_overhead_seconds=2.0,
            external_service_credit_applied_seconds=2.0,
            effective_generation_allowance_seconds=12.0,
            external_service_credit_revision=3,
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
            utils_module._persist_external_service_summary(
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
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "inference_settings_attested": True,
                "secrets": {"MODEL_GATEWAY_URL": "gateway-url", "MODEL_GATEWAY_API_KEY": "gateway-key"},
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )
        task_response = make_retrieve_task_response().model_copy(
            update={
                "agent_timeout": 10.0,
                "credited_generation": True,
            }
        )
        persistence_error = RuntimeError("summary persistence failed")
        evaluate_instance = AsyncMock()

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        def resolve_native_gateway(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {"MODEL_GATEWAY_URL": "https://gateway.example.test", "MODEL_GATEWAY_API_KEY": "gateway-key"}

        async def fail_during_agent(
            *_args: Any,
            on_external_service_sealed: Callable[[ExternalServiceAccountingSummary], Awaitable[None]],
            **_kwargs: Any,
        ) -> tuple[None, float]:
            await on_external_service_sealed(
                ExternalServiceAccountingSummary(
                    accounting_session_id="session-1",
                    base_generation_allowance_seconds=10.0,
                    cumulative_time_credit_cap_seconds=5.0,
                    external_service_overhead_seconds=1.0,
                    external_service_credit_applied_seconds=1.0,
                    effective_generation_allowance_seconds=11.0,
                    external_service_credit_revision=1,
                )
            )
            raise AssertionError("unreachable")

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        _install_gateway(monkeypatch, [])
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate_instance)
        monkeypatch.setattr(
            "tracker.runtime.services.resolve_secrets",
            resolve_native_gateway,
        )
        monkeypatch.setattr(utils_module, "run_agent", fail_during_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)
        monkeypatch.setattr(
            utils_module.ExternalServiceGatewayClient,
            "create_session",
            AsyncMock(
                return_value=AccountingSessionSnapshot(
                    session_id="session-1",
                    state=AccountingSessionState.OPEN,
                    cumulative_neutral_overhead_ms=0,
                    revision=0,
                    accounting_epoch=0,
                    generation_active=False,
                    interval_index=0,
                )
            ),
        )
        monkeypatch.setattr(
            utils_module,
            "_persist_external_service_summary",
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
        ("gateway_url", "task_enabled", "timeout", "base_only"),
        [
            (None, True, 10.0, True),
            ("http://local-gateway", False, 10.0, False),
        ],
    )
    async def test_base_only_opt_in_and_ineligible_tasks_never_create_ssp_sessions(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
        gateway_url: str | None,
        task_enabled: bool,
        timeout: float | None,
        base_only: bool,
    ) -> None:
        create_session = AsyncMock()
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", gateway_url)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)
        monkeypatch.setattr(utils_module.ExternalServiceGatewayClient, "create_session", create_session)

        deadline = await utils_module._create_external_service_deadline(  # pyright: ignore[reportPrivateUsage]
            contract, task_enabled, timeout
        )

        if base_only:
            assert deadline is not None
            assert deadline.client is None
            assert deadline.credit_cap_seconds == 0
        else:
            assert deadline is None
        create_session.assert_not_awaited()

    @pytest.mark.parametrize("persisted_retry", [False, True], ids=["new-attempt", "live-retry"])
    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_base_only_task_runs_without_model_or_ssp_session(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
        persisted_retry: bool,
    ) -> None:
        contract = contract.model_copy(update={"model": None, "inference_settings_attested": False})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)
        original_deadline = datetime.now(UTC) + timedelta(seconds=5) if persisted_retry else None
        if original_deadline is not None:
            task_row.credited_wall_deadline_at = original_deadline
            database_session.add(task_row)
            database_session.commit()
        task_response = make_retrieve_task_response().model_copy(
            update={"agent_timeout": 10.0, "credited_generation": True}
        )
        create_session = AsyncMock()
        observed: list[tuple[bool, Any]] = []

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        async def capture_run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            observed.append((kwargs["task_credited_generation"], kwargs["external_service_deadline"]))
            return None, 1.0

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module, "run_agent", capture_run_agent)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", None)
        monkeypatch.setattr(utils_module.ExternalServiceGatewayClient, "create_session", create_session)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(observed) == 1
        opted_in, deadline = observed[0]
        assert opted_in is True
        assert deadline is not None
        assert deadline.base_allowance_seconds == 10.0
        assert deadline.client is None
        create_session.assert_not_awaited()
        database_session.refresh(task_row)
        assert task_row.credited_wall_deadline_at is not None
        if original_deadline is not None:
            assert task_row.credited_wall_deadline_at.replace(tzinfo=UTC) == original_deadline

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_credited_wall_timer_does_not_interrupt_sandbox_teardown(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"model": None, "inference_settings_attested": False})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)
        response = make_retrieve_task_response().model_copy(update={"agent_timeout": 10.0, "credited_generation": True})
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
        monkeypatch.setattr(utils_module, "run_agent", AsyncMock(return_value=(None, 0.0)))
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)
        monkeypatch.setattr(utils_module, "CONTROLLED_TASK_WALL_SECONDS", 0.5)

        result = await asyncio.wait_for(
            run_process_task(request, task_row, benchmark_id, runtime_services, authority), timeout=3
        )

        assert teardown_complete.is_set()
        assert result == {"task_0": {"status": "success", "score": 1.0}}
        database_session.refresh(task_row)
        assert task_row.credited_wall_deadline_at is not None
        assert datetime.now(UTC) > task_row.credited_wall_deadline_at.replace(tzinfo=UTC)

    async def test_two_eligible_tasks_get_distinct_accounting_sessions(
        self,
        contract: AgentContractRequest,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        selected_contract = contract.model_copy(
            update={
                "model": "provider/model",
                "inference_settings_attested": True,
            }
        )
        requested_session_ids: list[str] = []

        async def create_session(
            _client: Any,
            *,
            session_id: str,
        ) -> AccountingSessionSnapshot:
            requested_session_ids.append(session_id)
            return AccountingSessionSnapshot(
                session_id=session_id,
                state=AccountingSessionState.OPEN,
                cumulative_neutral_overhead_ms=0,
                revision=0,
                accounting_epoch=0,
                generation_active=False,
                interval_index=0,
            )

        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)
        monkeypatch.setattr(
            utils_module.ExternalServiceGatewayClient,
            "create_session",
            create_session,
        )

        first = await utils_module._create_external_service_deadline(  # pyright: ignore[reportPrivateUsage]
            selected_contract, True, 10.0
        )
        second = await utils_module._create_external_service_deadline(  # pyright: ignore[reportPrivateUsage]
            selected_contract, True, 10.0
        )

        assert first is not None
        assert second is not None
        assert requested_session_ids[0] != requested_session_ids[1]

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize(
        ("attested", "model"),
        [(False, "provider/model"), (True, None), (True, ""), (True, "   ")],
    )
    async def test_ssp_credit_rejects_unattested_or_empty_model_before_session_or_sandbox(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
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
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )
        task_response = make_retrieve_task_response().model_copy(
            update={"agent_timeout": 10.0, "credited_generation": True}
        )
        create_session = AsyncMock()
        create_sandbox = Mock()

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module.ExternalServiceGatewayClient, "create_session", create_session)
        monkeypatch.setattr(utils_module, "create_sandbox", create_sandbox)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        create_session.assert_not_awaited()
        create_sandbox.assert_not_called()
        database_session.expire_all()
        assert database_session.get(Task, task_row.id).status == TaskStatus.ERROR
        errors = database_session.exec(select(utils_module.ErrorResult)).all()
        assert len(errors) == 1

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_controlled_task_without_native_gateway_credentials_never_starts_sandbox(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "inference_settings_attested": True,
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )
        task_response = make_retrieve_task_response().model_copy(
            update={"agent_timeout": 10.0, "credited_generation": True}
        )
        create_sandbox = Mock()
        create_session = AsyncMock(
            return_value=AccountingSessionSnapshot(
                session_id="session-1",
                state=AccountingSessionState.OPEN,
                cumulative_neutral_overhead_ms=0,
                revision=0,
                accounting_epoch=0,
                generation_active=False,
                interval_index=0,
            )
        )

        async def retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_response

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(utils_module.ExternalServiceGatewayClient, "create_session", create_session)
        monkeypatch.setattr(utils_module, "create_sandbox", create_sandbox)
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_URL", "http://local-gateway")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN", "tracker-control")
        monkeypatch.setattr(utils_module, "EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS", 5.0)
        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", lambda *_args, **_kwargs: {})

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        create_session.assert_awaited_once()
        create_sandbox.assert_not_called()
        errors = database_session.exec(select(utils_module.ErrorResult)).all()
        assert len(errors) == 1

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_withholds_unattested_inference_settings(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
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
            harness_config,
        )
        captured_env_vars: list[dict[str, str]] = []

        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", lambda *_args, **_kwargs: {})
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
        harness_config: HarnessConfig,
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
            harness_config,
        )
        captured_env_vars: list[dict[str, str]] = []
        minted: list[dict[str, Any]] = []
        _install_gateway(monkeypatch, minted)

        def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {
                "VALKYRIE_AGENT_MODEL": "anthropic/claude-4-opus",
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
            }

        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", _mock_resolve_secrets)
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
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"inference_settings_attested": True})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        captured_env_vars: list[dict[str, str]] = []

        def _mock_resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {}

        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", _mock_resolve_no_secrets)
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
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"secrets": {"LEGACY_API_KEY": "aws-secret"}})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        captured: dict[str, dict[str, str]] = {}
        resolved_inputs: list[dict[str, str]] = []

        def _mock_resolve_secrets(secrets: dict[str, str], *_args: Any, **_kwargs: Any) -> dict[str, str]:
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

        monkeypatch.setattr("tracker.runtime.services.resolve_secrets", _mock_resolve_secrets)
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
        harness_config: HarnessConfig,
        runtime_services: RuntimeServices,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
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
    harness_config: HarnessConfig,
    runtime_services: RuntimeServices,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task completion must not race a buffered write still running in a thread."""
    request, task, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)
    loop = asyncio.get_running_loop()
    writing = asyncio.Event()
    release_write = threading.Event()
    written: list[str] = []

    async def run_agent(*args: Any, **_kwargs: Any) -> tuple[None, float]:
        cast(Callable[[str], None], args[4])("final agent message")
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
