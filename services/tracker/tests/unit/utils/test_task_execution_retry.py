"""Unit tests for task execution retries and timing signals.

Run: uv run pytest tests/unit/utils/test_task_execution_retry.py
"""

import asyncio
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Never, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from benchmark_service import (
    CreditedGeneration,
    ComposeSource,
    DockerProviderConfig,
    ImageSource,
    SandboxNotFoundError,
    SandboxRecoveryPolicy,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceStreamClosedError
from benchmark_service.sandbox.local.docker import DockerSandbox
from benchmark_service.schemas import (
    AgentInstallOrder,
    BenchmarkEgressPlan,
    RetrieveTaskResponse,
    SetupTaskResponse,
    VolumeMount,
)
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, desc, select

from tests.unit.utils.task_execution_support import (
    TEST_ORG,
    bind_task_to_dispatch,
    create_task_environment,
    finalize_agent_generation,
    install_sqlite_evaluation_lock,
    make_retrieve_task_response,
    run_process_task,
)
from tracker.scheduler.admission import SandboxQueueContext
from tracker.runtime.services import RuntimeServices
from tracker.database.models import (
    AgentContractRequest,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    FailureCategory,
    FailurePhase,
    TaskStatus,
)
from tracker.egress import EgressPolicy
from tracker.exceptions import (
    AgentRunFailedError,
    ControlledGenerationTerminationUnconfirmedError,
    DependencySetupExhaustedError,
    GenerationTerminationUnconfirmedError,
    SandboxError,
    SandboxSetupError,
)
from tracker.sandbox import DependencySetupMode, apply_egress_policy, create_sandbox
from tracker.utils import TrackedTask
from tracker.utils import task_execution as task_execution_module


class TestTaskExecutionRetry:
    """Task execution retries, callbacks, and transition spans."""

    @pytest.mark.parametrize("preview_failure", [None, "publish_native_output", "clear_native_output"])
    @pytest.mark.parametrize("stop", [False, True])
    @pytest.mark.parametrize(
        ("output_path", "expected_path"),
        [
            ("/agent/claude_code.log", "/agent/claude_code.log"),
            ("logs/claude_code.log", "/workspace/task/logs/claude_code.log"),
        ],
    )
    async def test_native_output_does_not_change_cancellation(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        stop: bool,
        output_path: str,
        expected_path: str,
        preview_failure: str | None,
    ) -> None:
        if preview_failure is not None:
            monkeypatch.setattr(
                task_execution_module, preview_failure, Mock(side_effect=RuntimeError("preview database failure"))
            )
        contract = contract.model_copy(update={"final_output": output_path})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        source = ComposeSource(
            outer=ImageSource(image="original-image"), service="agent", compose_command="docker compose -f /run.yml"
        )
        teardown_seen = False

        @asynccontextmanager
        async def create(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal teardown_seen
            sandbox = AsyncMock()
            sandbox.id = "actual-sandbox"
            sandbox.name = "actual-sandbox"
            try:
                yield sandbox
            finally:
                database_session.refresh(task_row)
                assert (task_row.active_native_output is None) == (preview_failure != "clear_native_output")
                teardown_seen = True

        async def retrieve(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response().model_copy(update={"source": source, "cwd": "/workspace/task"})

        async def install(*_args: Any, **_kwargs: Any) -> None:
            database_session.refresh(task_row)
            descriptor = task_row.active_native_output
            if preview_failure == "publish_native_output":
                assert descriptor is None
            else:
                assert descriptor is not None
                assert descriptor.runtime_source == source
                assert descriptor.sandbox_id == "actual-sandbox"
                assert descriptor.output_path == expected_path
            if stop:
                task_row.status = TaskStatus.STOPPED
                dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
                assert dispatch is not None
                dispatch.status = ExecutorDispatchStatus.FINISHED
                database_session.add(dispatch)
                database_session.add(task_row)
                database_session.commit()
            raise asyncio.CancelledError

        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module, "create_sandbox", create)
        monkeypatch.setattr(task_execution_module, "install_agent_dependencies", install)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve)

        with pytest.raises(asyncio.CancelledError):
            await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        assert teardown_seen
        database_session.refresh(task_row)
        assert task_row.status == (TaskStatus.STOPPED if stop else TaskStatus.IN_PROGRESS)
        assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all() == []

    @pytest.mark.parametrize("preview_failure", [None, "publish_native_output", "clear_native_output"])
    @pytest.mark.parametrize(
        "error,second_error,expected_dependency_modes,expected_status",
        [
            (
                DependencySetupExhaustedError("dependency setup exhausted"),
                None,
                [DependencySetupMode.IN_PLACE_RETRIES, DependencySetupMode.FINAL_FRESH_SANDBOX],
                TaskStatus.FINISHED,
            ),
            (
                DependencySetupExhaustedError("dependency setup exhausted"),
                AgentRunFailedError("fresh sandbox setup failed"),
                [DependencySetupMode.IN_PLACE_RETRIES, DependencySetupMode.FINAL_FRESH_SANDBOX],
                TaskStatus.ERROR,
            ),
            (
                DependencySetupExhaustedError("dependency setup exhausted"),
                DependencySetupExhaustedError("dependency setup exhausted again"),
                [DependencySetupMode.IN_PLACE_RETRIES, DependencySetupMode.FINAL_FRESH_SANDBOX],
                TaskStatus.ERROR,
            ),
        ],
    )
    async def test_process_task_retries_on_dependency_setup_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        error: SandboxSetupError,
        second_error: Exception | None,
        expected_dependency_modes: list[DependencySetupMode],
        expected_status: TaskStatus,
        preview_failure: str | None,
    ) -> None:
        """Setup failures retry even when preview bookkeeping fails."""
        if preview_failure is not None:
            monkeypatch.setattr(
                task_execution_module, preview_failure, Mock(side_effect=RuntimeError("preview database failure"))
            )
        contract = contract.model_copy(update={"final_output": "/agent/native-output"})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )

        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)

        sandbox_entry_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-{sandbox_entry_count}"
            mock_sandbox.name = f"mock-sandbox-{sandbox_entry_count}"
            try:
                yield mock_sandbox
            finally:
                database_session.refresh(task_row)
                assert (task_row.active_native_output is None) == (preview_failure != "clear_native_output")

        generations: list[str] = []
        call_count = 0
        dependency_modes: list[DependencySetupMode] = []

        async def _install_dependencies(*args: Any, **_kwargs: Any) -> None:
            nonlocal call_count
            dependency_modes.append(args[3])
            database_session.refresh(task_row)
            descriptor = task_row.active_native_output
            if preview_failure == "publish_native_output":
                assert descriptor is None
            else:
                assert descriptor is not None
                assert descriptor.sandbox_id == f"mock-sandbox-{sandbox_entry_count}"
                assert descriptor.output_path == "/agent/native-output"
                assert descriptor.matches_attempt(task_row.started_at)
                generations.append(descriptor.generation)
            call_count += 1
            if call_count == 1:
                raise error
            if second_error:
                raise second_error

        async def _mock_run_agent(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            database_session.refresh(task_row)
            assert task_row.status is TaskStatus.EVALUATING
            assert (task_row.active_native_output is None) == (preview_failure == "publish_native_output")
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.install_agent_dependencies", _install_dependencies)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        # Run through TrackedTask like the executor does, so the error that escapes once
        # retries run out is recorded by the production handler.
        tracked = TrackedTask(
            run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority),
            TEST_ORG,
            authority,
            task_row.started_at,
        )
        result = await tracked.run(None, task_row)

        expected_result = None if expected_status is TaskStatus.ERROR else {"status": "success", "score": 1.0}
        assert result == {"task_0": expected_result}
        assert sandbox_entry_count == 2
        assert call_count == 2
        assert dependency_modes == expected_dependency_modes
        assert len(set(generations)) == (0 if preview_failure == "publish_native_output" else 2)

        database_session.refresh(task_row)
        assert task_row.status == expected_status

        error_results = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
        retry_results = [result for result in error_results if result.retry_scheduled]
        terminal_results = [result for result in error_results if not result.retry_scheduled]

        assert len(retry_results) == 1
        retry_result = retry_results[0]
        assert retry_result.error_message == f"{type(error).__name__}: {error}"
        assert retry_result.producer == "sandbox_provider"
        assert retry_result.operation == "setup"
        assert retry_result.error_type == type(error).__name__
        assert retry_result.cause_code is None
        assert retry_result.failed_attempt_number == 1
        assert retry_result.phase == FailurePhase.SETUP
        assert retry_result.transient is True

        if expected_status is TaskStatus.ERROR:
            assert second_error is not None
            assert len(terminal_results) == 1
            terminal_result = terminal_results[0]
            assert str(second_error) in terminal_result.error_message
            if isinstance(second_error, SandboxSetupError):
                assert terminal_result.producer == "sandbox_provider"
                assert terminal_result.operation == "setup"
            else:
                assert terminal_result.producer == "tracker"
                assert terminal_result.operation == "process_task"
            assert terminal_result.phase == FailurePhase.SETUP
            assert terminal_result.transient == getattr(second_error, "transient")
            assert terminal_result.error_type == type(second_error).__name__
            assert terminal_result.cause_code is None
            assert terminal_result.failed_attempt_number is None
        else:
            assert terminal_results == []

    async def test_native_output_publication_preserves_authority_revocation(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        contract = contract.model_copy(update={"final_output": "/agent/native-output"})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)

        @asynccontextmanager
        async def create(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            sandbox = AsyncMock()
            sandbox.id = sandbox.name = "revoked-sandbox"
            dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
            assert dispatch is not None
            dispatch.status = ExecutorDispatchStatus.FINISHED
            database_session.add(dispatch)
            database_session.commit()
            yield sandbox

        install = AsyncMock(side_effect=AssertionError("Revoked attempts must not execute"))
        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module, "create_sandbox", create)
        monkeypatch.setattr(task_execution_module, "install_agent_dependencies", install)
        monkeypatch.setattr(
            BenchmarkServiceClient, "retrieve_task", AsyncMock(return_value=make_retrieve_task_response())
        )

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        assert result == {task_row.task_id: None}
        install.assert_not_awaited()
        assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all() == []

    @pytest.mark.usefixtures("process_benchmark_env")
    @pytest.mark.parametrize(
        ("stage", "policy"),
        [(None, "*")]
        + [
            (stage, policy)
            for stage in ("install", "setup_task", "run", "evaluation")
            for policy in ([], ["example.com"])
        ]
        + [("legacy_agent", ["example.com"])],
    )
    async def test_docker_generation_requires_an_unrestricted_plan_before_creation(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        stage: str | None,
        policy: EgressPolicy,
    ) -> None:
        task_data = make_retrieve_task_response().model_copy(update={"source": ImageSource(image="python:3.12-slim")})
        if stage == "install":
            contract = contract.model_copy(update={"install_egress": policy})
        elif stage == "legacy_agent":
            contract = contract.model_copy(update={"egress_allowlist": policy})
        elif stage is not None:
            task_data = task_data.model_copy(update={"egress": task_data.egress.model_copy(update={stage: policy})})
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        # Keep Docker's real unsupported egress methods; only container/provider I/O is replaced.
        sandbox = DockerSandbox(
            cast(Any, Mock()),
            cast(
                Any,
                SimpleNamespace(id="docker-task", labels={}, state="running", created=datetime(2026, 1, 1, tzinfo=UTC)),
            ),
        )
        provider = AsyncMock()
        provider.__aenter__.return_value = provider
        provider.create_sandbox.return_value = sandbox

        def provider_factory(_config: DockerProviderConfig) -> Any:
            return provider

        monkeypatch.setattr(DockerProviderConfig, "create_provider", provider_factory)
        monkeypatch.setattr(
            runtime_services, "get_sandbox_provider_config", AsyncMock(return_value=DockerProviderConfig())
        )
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", AsyncMock(return_value=task_data))
        monkeypatch.setattr(task_execution_module, "create_sandbox", create_sandbox)
        monkeypatch.setattr(task_execution_module, "apply_egress_policy", apply_egress_policy)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        if stage is None:
            assert result == {"task_0": {"status": "success", "score": 1.0}}
            assert provider.create_sandbox.call_args.args[0].network_block_all is False
        else:
            assert result == {"task_0": None}
            assert not provider.create_sandbox.called
            error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
            assert "requires unrestricted egress for every stage" in error.error_message

    @pytest.mark.parametrize(
        ("failing_policy", "expected_status", "expected_sandboxes", "expected_phase"),
        [
            pytest.param(["https://setup.example.com"], TaskStatus.FINISHED, 2, FailurePhase.SETUP, id="setup"),
            pytest.param(
                ["https://grading.example.com"], TaskStatus.ERROR, 1, FailurePhase.EVALUATION, id="evaluation"
            ),
        ],
    )
    async def test_transient_sandbox_error_recreates_sandbox_only_before_the_agent_runs(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        failing_policy: list[str],
        expected_status: TaskStatus,
        expected_sandboxes: int,
        expected_phase: FailurePhase,
    ) -> None:
        """The same transient provider failure is a free fresh-sandbox retry before generation and terminal after it."""
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        task_data = make_retrieve_task_response().model_copy(
            update={
                "egress": BenchmarkEgressPlan(
                    setup_task=["https://setup.example.com"],
                    run=["https://run.example.com"],
                    evaluation=["https://grading.example.com"],
                ),
            }
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        sandbox_count = 0
        failed = False

        @asynccontextmanager
        async def create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[Mock, None]:
            nonlocal sandbox_count
            sandbox_count += 1
            sandbox = Mock()
            sandbox.id = sandbox.name = f"sandbox-{sandbox_count}"
            yield sandbox

        async def apply(_sandbox: Any, policy: EgressPolicy) -> None:
            nonlocal failed
            if policy == failing_policy and not failed:
                failed = True
                raise SandboxError("network settings conflict", transient=True)

        async def noop(*_args: Any, **_kwargs: Any) -> None:
            pass

        def unwrapped(sandbox: Any, _source: Any) -> Any:
            return sandbox

        async def run(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def retrieve(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_data

        async def setup(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            return SetupTaskResponse(status="ok")

        async def evaluate(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.upload_agent_artifacts", noop)
        monkeypatch.setattr("tracker.utils.task_execution.runtime_sandbox", unwrapped)
        monkeypatch.setattr("tracker.utils.task_execution.apply_egress_policy", apply)
        monkeypatch.setattr("tracker.utils.task_execution.install_agent_dependencies", noop)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", run)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", setup)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        database_session.refresh(task_row)
        assert task_row.status == expected_status
        assert sandbox_count == expected_sandboxes
        error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
        assert error.phase == expected_phase
        assert error.transient is True
        assert error.category == FailureCategory.INFRASTRUCTURE
        if expected_status is TaskStatus.FINISHED:
            assert result == {"task_0": {"status": "success", "score": 1.0}}
            assert error.retry_scheduled
            assert error.error_type == "SandboxSetupError"
        else:
            assert result == {"task_0": None}
            assert not error.retry_scheduled
            assert error.error_type == "SandboxError"

    @pytest.mark.parametrize(
        ("compose_runtime", "agent_install_order", "setup_before_install"),
        [
            pytest.param(False, "before_setup", False, id="image-before-setup"),
            pytest.param(False, "after_setup", True, id="image-after-setup"),
            pytest.param(True, "before_setup", True, id="compose-before-setup-override"),
            pytest.param(True, "after_setup", True, id="compose-after-setup"),
        ],
    )
    async def test_process_task_applies_stage_policies_in_lifecycle_order(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        compose_runtime: bool,
        agent_install_order: AgentInstallOrder,
        setup_before_install: bool,
    ) -> None:
        """Benchmarks choose install order; Compose always bootstraps first."""
        contract = contract.model_copy(
            update={
                "install_egress": ["https://packages.example.com"],
                "egress_allowlist": ["https://agent-runtime.example.com"],
            }
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        source = (
            ComposeSource(
                outer=ImageSource(image="docker:28-dind"),
                compose_command="docker compose -f /harbor/compose.yaml",
            )
            if compose_runtime
            else ImageSource(image="test-image:latest")
        )
        task_data = make_retrieve_task_response().model_copy(
            update={
                "source": source,
                "agent_install_order": agent_install_order,
                "egress": BenchmarkEgressPlan(
                    setup_task=[],
                    run=["https://benchmark-runtime.example.com"],
                    evaluation=["https://grading.example.com"],
                ),
            }
        )
        events: list[str] = []
        raw_sandbox = Mock(id="raw-sandbox", name="raw-sandbox")
        agent_sandbox = Mock(id="agent-sandbox", name="agent-sandbox")
        expected_agent_sandbox = agent_sandbox if compose_runtime else raw_sandbox

        @asynccontextmanager
        async def create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[Mock, None]:
            try:
                yield raw_sandbox
            finally:
                events.append("teardown")

        async def upload(*_args: Any, **_kwargs: Any) -> None:
            events.append("upload")

        def wrap(sandbox: Any, observed_source: Any) -> Mock:
            assert sandbox is raw_sandbox
            assert observed_source is source
            events.append("wrap")
            return expected_agent_sandbox

        async def apply(sandbox: Any, policy: EgressPolicy) -> None:
            target = "agent" if sandbox is agent_sandbox else "raw"
            events.append(f"policy:{target}:{policy}")

        async def install(sandbox: Any, *_args: Any, **_kwargs: Any) -> None:
            assert sandbox is expected_agent_sandbox
            events.append("install")

        async def retrieve(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return task_data

        async def setup(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            events.append("setup")
            return SetupTaskResponse(status="ok")

        async def run(
            sandbox: Any, *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            assert sandbox is expected_agent_sandbox
            events.append("run")
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def evaluate(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            events.append("evaluate")
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.upload_agent_artifacts", upload)
        monkeypatch.setattr("tracker.utils.task_execution.runtime_sandbox", wrap)
        monkeypatch.setattr("tracker.utils.task_execution.apply_egress_policy", apply)
        monkeypatch.setattr("tracker.utils.task_execution.install_agent_dependencies", install)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", run)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", setup)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        expected_agent_target = "agent" if compose_runtime else "raw"
        install_events = [
            f"policy:{expected_agent_target}:['https://packages.example.com']",
            "install",
        ]
        setup_events = ["policy:raw:[]", "setup"]
        assert events == [
            "upload",
            "wrap",
            *(setup_events if setup_before_install else install_events),
            *(install_events if setup_before_install else setup_events),
            (
                f"policy:{expected_agent_target}:"
                "['https://benchmark-runtime.example.com', 'https://agent-runtime.example.com']"
            ),
            "run",
            "policy:raw:['https://grading.example.com']",
            "evaluate",
            "teardown",
        ]

    @pytest.mark.parametrize(
        ("service_error", "cause_code"),
        [
            pytest.param(
                "docker daemon is not ready inside the sandbox: nohup: failed to run command 'dockerd': No such file or directory",
                "docker_not_ready",
                id="vcb",
            ),
            pytest.param(
                "Docker daemon did not become ready inside the compose sandbox (dockerd exited, 1 restart(s))",
                "docker_not_ready",
                id="terminal_bench",
            ),
            pytest.param(
                "Command failed: docker compose -f /app/docker-compose.yaml up -d (exit 1)",
                None,
                id="unclassified-compose",
            ),
            pytest.param(
                "cleanroom start failed (exit 1): cleanroom has unexpected network interfaces: ip6_vti0 ip_vti0 lo",
                None,
                id="unclassified-cleanroom",
            ),
        ],
    )
    async def test_process_task_retries_when_service_reports_broken_sandbox(
        self,
        service_error: str,
        cause_code: str | None,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """A service error during setup retries on a fresh sandbox instead of failing terminally.

        Test cases:
        - setup_task raises a broken-sandbox signature, or an error that does not say whether
          it is transient, on attempt 1
        - process_task retries with a new sandbox and the task finishes
        - The recorded failure is marked retry_scheduled as a sandbox setup failure, with the
          signature's cause code when there is one
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())

        sandbox_entry_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-{sandbox_entry_count}"
            mock_sandbox.name = f"mock-sandbox-{sandbox_entry_count}"
            yield mock_sandbox

        setup_call_count = 0

        async def _fails_first_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            nonlocal setup_call_count
            setup_call_count += 1
            if setup_call_count == 1:
                raise BenchmarkServiceError(service_error)
            return SetupTaskResponse(status="ok")

        async def _mock_run_agent(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _fails_first_setup_task)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert sandbox_entry_count == 2
        assert setup_call_count == 2

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.FINISHED

        error_results = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
        retry_results = [result for result in error_results if result.retry_scheduled]
        assert len(retry_results) == 1
        assert retry_results[0].error_message == (
            f"SandboxSetupError: Sandbox error: {service_error} (caused by BenchmarkServiceError: {service_error})"
        )
        assert retry_results[0].producer == "sandbox_provider"
        assert retry_results[0].operation == "setup"
        assert retry_results[0].error_type == "SandboxSetupError"
        assert retry_results[0].cause_code == cause_code
        assert retry_results[0].failed_attempt_number == 1
        assert [result for result in error_results if not result.retry_scheduled] == []

    @pytest.mark.parametrize(
        ("service_error", "cause_code"),
        [
            pytest.param(
                "docker daemon is not ready inside the sandbox: dockerd exited", "docker_not_ready", id="docker"
            ),
            pytest.param("Command failed: docker compose up -d (exit 1)", None, id="unclassified-compose"),
        ],
    )
    async def test_exhausted_setup_retry_ends_in_error_with_cause_code_on_both_rows(
        self,
        service_error: str,
        cause_code: str | None,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """A setup failure on both sandboxes ends in ERROR, with the same cause_code on the retry and final rows.

        Test cases:
        - setup_task reports Docker not ready, or an unclassified compose failure, on every call
        - The task gets one fresh sandbox, then ends in ERROR through TrackedTask, like the executor runs it
        - Both ErrorResult rows carry the failure's cause_code
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())

        sandbox_entry_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-{sandbox_entry_count}"
            mock_sandbox.name = mock_sandbox.id
            yield mock_sandbox

        async def _setup_always_fails(*_args: Any, **_kwargs: Any) -> Never:
            raise BenchmarkServiceError(service_error)

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _setup_always_fails)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        tracked = TrackedTask(
            run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority),
            TEST_ORG,
            authority,
            task_row.started_at,
        )
        result = await tracked.run(None, task_row)

        assert result == {"task_0": None}
        assert sandbox_entry_count == 2
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_results = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
        assert sorted((row.retry_scheduled, row.cause_code) for row in error_results) == [
            (False, cause_code),
            (True, cause_code),
        ]

    @pytest.mark.parametrize(
        ("gateway_resolves", "expected_status", "expected_sandboxes", "expected_rows"),
        [
            pytest.param([False], TaskStatus.FINISHED, 2, [(True, "sandbox_dns_resolution")], id="dns-broken-once"),
            pytest.param([True], TaskStatus.ERROR, 1, [(False, None)], id="dns-fine"),
            pytest.param(
                [False, False],
                TaskStatus.ERROR,
                2,
                [(False, "sandbox_dns_resolution"), (True, "sandbox_dns_resolution")],
                id="dns-broken-on-both-runs",
            ),
        ],
    )
    async def test_agent_failure_reruns_when_the_sandbox_cannot_resolve_the_gateway(
        self,
        gateway_resolves: list[bool],
        expected_status: TaskStatus,
        expected_sandboxes: int,
        expected_rows: list[tuple[bool, str | None]],
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """A failed agent run gets one fresh sandbox only when the tracker sees the sandbox's DNS is broken.

        Test cases:
        - The agent fails and the sandbox cannot resolve the gateway: one rerun, the task finishes,
          and the retry row reads sandbox_dns_resolution
        - The agent fails but the gateway resolves: no rerun, the task ends in ERROR
        - DNS is broken on both runs: one rerun only, and both rows read sandbox_dns_resolution
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())

        sandbox_entry_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-{sandbox_entry_count}"
            mock_sandbox.name = mock_sandbox.id
            yield mock_sandbox

        agent_calls = 0

        async def _run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            nonlocal agent_calls
            agent_calls += 1
            if agent_calls <= len(gateway_resolves):
                raise AgentRunFailedError("Agent command failed with exit code 1")
            await finalize_agent_generation(
                deadline_controller=kwargs["deadline_controller"],
                on_external_service_finalized=kwargs["on_external_service_finalized"],
            )
            return None, 0.0

        probed_hosts: list[str] = []

        async def _sandbox_resolves(_sandbox: Any, host: str) -> bool:
            probed_hosts.append(host)
            return gateway_resolves[len(probed_hosts) - 1]

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            return SetupTaskResponse(status="ok")

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _run_agent)
        monkeypatch.setattr("tracker.utils.task_execution.sandbox_resolves", _sandbox_resolves)
        monkeypatch.setattr(
            "tracker.utils.task_execution.resolve_secrets",
            AsyncMock(return_value={"MODEL_GATEWAY_URL": "https://gateway.example.com/v1"}),
        )
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        tracked = TrackedTask(
            run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority),
            TEST_ORG,
            authority,
            task_row.started_at,
        )
        await tracked.run(None, task_row)

        database_session.refresh(task_row)
        error_results = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
        assert task_row.status == expected_status
        assert probed_hosts == ["gateway.example.com"] * len(gateway_resolves)
        assert sandbox_entry_count == expected_sandboxes
        assert sorted((row.retry_scheduled, row.cause_code) for row in error_results) == expected_rows

    async def test_dns_check_probes_the_external_gateway_when_the_agent_was_given_it(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        """With a generation budget the agent is pointed at the external gateway, so that host is probed."""
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())
        monkeypatch.setattr(
            task_execution_module, "EXTERNAL_SERVICE_GATEWAY_URL", "https://external-gateway.example.com"
        )
        create_controller = task_execution_module._create_generation_deadline_controller  # pyright: ignore[reportPrivateUsage]

        class _BudgetedController:
            snapshot = object()

            def __init__(self, inner: Any) -> None:
                self._inner = inner

            def __getattr__(self, name: str) -> Any:
                return getattr(self._inner, name)

        async def _budgeted_controller(*args: Any, **kwargs: Any) -> Any:
            return _BudgetedController(await create_controller(*args, **kwargs))

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox"
            mock_sandbox.name = mock_sandbox.id
            yield mock_sandbox

        async def _run_agent(*_args: Any, **_kwargs: Any) -> Never:
            raise AgentRunFailedError("Agent command failed with exit code 1")

        probed_hosts: list[str] = []

        async def _sandbox_resolves(_sandbox: Any, host: str) -> bool:
            probed_hosts.append(host)
            return True

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            return SetupTaskResponse(status="ok")

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        monkeypatch.setattr(task_execution_module, "_create_generation_deadline_controller", _budgeted_controller)
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _run_agent)
        monkeypatch.setattr("tracker.utils.task_execution.sandbox_resolves", _sandbox_resolves)
        monkeypatch.setattr(
            "tracker.utils.task_execution.resolve_secrets",
            AsyncMock(return_value={"MODEL_GATEWAY_URL": "https://gateway.example.com/v1"}),
        )
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        tracked = TrackedTask(
            run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority),
            TEST_ORG,
            authority,
            task_row.started_at,
        )
        await tracked.run(None, task_row)

        assert probed_hosts == ["external-gateway.example.com"]

    @pytest.mark.parametrize(
        ("failing_call", "service_error", "status_code", "expected_phase"),
        [
            pytest.param(
                "setup_task",
                "Task programbench-0042 not found",
                404,
                FailurePhase.SETUP,
                id="rejected-request",
            ),
            pytest.param(
                "evaluate_instance",
                "docker daemon is not ready inside the sandbox: dockerd exited",
                None,
                FailurePhase.EVALUATION,
                id="broken-sandbox-after-agent",
            ),
        ],
    )
    async def test_process_task_terminates_on_service_errors_it_does_not_retry(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        failing_call: str,
        service_error: str,
        status_code: int | None,
        expected_phase: FailurePhase,
    ) -> None:
        """A service error stays terminal when the service rejected the request or the agent already ran.

        Test cases:
        - setup_task raises a BenchmarkServiceError carrying a 4xx status
        - evaluate_instance reports a broken sandbox, which would replay the agent on a fresh one
        - No retry is scheduled and the task ends in ERROR, in the phase it failed
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())

        sandbox_entry_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-{sandbox_entry_count}"
            mock_sandbox.name = mock_sandbox.id
            yield mock_sandbox

        async def _fail(*_args: Any, **_kwargs: Any) -> Never:
            raise BenchmarkServiceError(service_error, status_code=status_code)

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            return SetupTaskResponse(status="ok")

        async def _mock_run_agent(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, failing_call, _fail)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        assert sandbox_entry_count == 1

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
        assert not error.retry_scheduled
        assert error.phase == expected_phase

    @pytest.mark.parametrize("failure_site", ["queued_admission", "service_setup"])
    async def test_process_task_releases_queued_task_when_pending_transition_refused(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        failure_site: str,
    ) -> None:
        """A queued task whose PENDING transition is refused ends without a retry.

        Both retry-eligible failures must return a queued task to PENDING before
        re-raising so the retry can be re-admitted; a refused transition means
        the task is owned elsewhere and the attempt stops.

        Test cases:
        - the setup error surfaces during queued admission (SandboxSetupError)
        - the setup error is reported by the benchmark service (BenchmarkServiceError)
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())

        mock_sandbox = AsyncMock()
        mock_sandbox.id = "mock-sandbox-queued"
        mock_sandbox.name = mock_sandbox.id

        async def _mock_enter_queued_sandbox(*_args: Any, **_kwargs: Any) -> AsyncMock:
            if failure_site == "queued_admission":
                raise SandboxSetupError("provisioner rejected the sandbox")
            return mock_sandbox

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
            raise BenchmarkServiceError(
                "docker daemon is not ready inside the sandbox: "
                "nohup: failed to run command 'dockerd': No such file or directory"
            )

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        transition_statuses: list[TaskStatus] = []

        def _refuse_pending_transition(*args: Any, **_kwargs: Any) -> bool:
            transition_statuses.append(args[3])
            return False

        task_engine = database_session.get_bind()
        assert isinstance(task_engine, Engine)
        queue_context = SandboxQueueContext(
            provider=Mock(),
            capacity_cache=Mock(),
            pool_id="pool_test",
            engine=task_engine,
        )
        monkeypatch.setattr(task_execution_module, "enter_queued_sandbox", _mock_enter_queued_sandbox)
        monkeypatch.setattr(task_execution_module, "commit_task_status_transition", _refuse_pending_transition)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        result = await run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            runtime_services,
            authority,
            queue_context=queue_context,
        )

        assert result == {"task_0": None}
        assert transition_statuses == [TaskStatus.PENDING]
        error_results = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
        assert [result for result in error_results if result.retry_scheduled] == []

    async def test_revoked_dispatch_does_not_begin_the_second_automatic_attempt(
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
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())

        sandbox_entry_count = 0
        retrieve_task_call_count = 0

        @asynccontextmanager
        async def _fail_sandbox_setup(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            raise SandboxSetupError("sandbox setup failed")
            yield AsyncMock()

        def _revoke_authority(*_args: object) -> None:
            with Session(bind=database_session.bind) as session:
                dispatch = session.get(ExecutorDispatch, authority.dispatch_id)
                assert dispatch is not None
                dispatch.status = ExecutorDispatchStatus.FAILED
                session.add(dispatch)
                session.commit()

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            nonlocal retrieve_task_call_count
            retrieve_task_call_count += 1
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _fail_sandbox_setup)
        monkeypatch.setattr(task_execution_module, "_observe_task_retry", _revoke_authority)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        assert sandbox_entry_count == 1
        assert retrieve_task_call_count == 1
        assert database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).all() == []

    @pytest.mark.parametrize(
        "failure_stage,agent_timeout,max_attempts,failures,expected_attempts,expected_status",
        [
            ("setup", None, 3, 1, 2, TaskStatus.FINISHED),
            ("setup", None, 3, 2, 3, TaskStatus.FINISHED),
            ("setup", None, 2, 2, 2, TaskStatus.ERROR),
            ("setup", None, None, 1, 1, TaskStatus.ERROR),
            ("agent", None, None, 1, 1, TaskStatus.ERROR),
            ("agent", None, 3, 1, 1, TaskStatus.ERROR),
            ("agent", 10.0, 2, 1, 1, TaskStatus.ERROR),
            ("agent_setup", None, 3, 1, 1, TaskStatus.ERROR),
        ],
    )
    async def test_sandbox_recovery_is_bounded_before_agent_start_and_terminal_after(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        max_attempts: int | None,
        failures: int,
        expected_attempts: int,
        expected_status: TaskStatus,
        failure_stage: str,
        agent_timeout: float | None,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("benchmark_service.client.time.time", lambda: 1_234.5)
        monkeypatch.setattr("benchmark_service.client.uuid4", lambda: UUID(int=1))

        sandbox_envs: list[dict[str, str]] = []

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            sandbox_envs.append(kwargs["env_vars"])
            sandbox = AsyncMock()
            sandbox.id = f"mock-sandbox-{len(sandbox_envs)}"
            sandbox.name = sandbox.id
            yield sandbox

        run_count = 0
        setup_count = 0

        async def _mock_install_dependencies(*_args: Any, **_kwargs: Any) -> None:
            nonlocal setup_count
            setup_count += 1
            if failure_stage == "setup" and setup_count <= failures:
                raise SandboxNotFoundError("sandbox was preempted")

        async def _mock_run_agent(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            nonlocal run_count
            run_count += 1
            if failure_stage == "agent":
                raise SandboxNotFoundError("sandbox was preempted")
            if failure_stage == "agent_setup":
                raise SandboxSetupError("sandbox setup failed after agent start")
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            response = make_retrieve_task_response(problem_path="/tmp/problem.txt").model_copy(
                update={"agent_timeout": agent_timeout}
            )
            if max_attempts is not None:
                response.sandbox_recovery = SandboxRecoveryPolicy(max_sandbox_attempts=max_attempts)
            return response

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module, "create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(task_execution_module, "install_agent_dependencies", _mock_install_dependencies)
        monkeypatch.setattr(task_execution_module, "run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        expected_result = None if expected_status is TaskStatus.ERROR else {"status": "success", "score": 1.0}
        assert result == {"task_0": expected_result}
        assert len(sandbox_envs) == expected_attempts
        assert all(env["RUN_ID"] == str(benchmark_id) for env in sandbox_envs)
        # Replacement sandboxes continue the same execution, so they keep its ATTEMPT_ID.
        assert {env["ATTEMPT_ID"] for env in sandbox_envs} == {
            task_execution_module.task_attempt_id(task_row.id, task_row.started_at)
        }
        assert "VALKYRIE_SANDBOX_OUTAGE_ID" not in sandbox_envs[0]
        for env in sandbox_envs[1:]:
            assert env["VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH"] == "1234.5"
            assert env["VALKYRIE_SANDBOX_OUTAGE_ID"] == (f"{benchmark_id}:task_0:1:00000000000000000000000000000001")

        database_session.refresh(task_row)
        assert task_row.status == expected_status
        if failure_stage in ("agent", "agent_setup"):
            error = database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).one()
            assert error.retry_scheduled is False
            assert error.operation == "sandbox_recovery"
            if failure_stage == "agent_setup":
                assert error.producer == "sandbox_provider"
                assert error.error_type == "SandboxSetupError"

    @pytest.mark.parametrize("status", [TaskStatus.PENDING, TaskStatus.EVALUATING])
    async def test_expired_credited_wall_deadline_stops_before_task_retrieval(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        status: TaskStatus,
    ) -> None:
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        if status == TaskStatus.EVALUATING:
            bind_task_to_dispatch(database_session, task_row, authority)
            task_row.status = status
            task_row.eval_resume_state = {"artifact_prefix": "s3://bucket/run"}
        task_row.credited_wall_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
        database_session.add(task_row)
        database_session.commit()
        retrieve_task = AsyncMock()
        resume_evaluation = AsyncMock()
        create_sandbox = Mock()
        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", resume_evaluation, raising=False)
        monkeypatch.setattr(task_execution_module, "create_sandbox", create_sandbox)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        retrieve_task.assert_not_awaited()
        create_sandbox.assert_not_called()
        resume_evaluation.assert_not_awaited()
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error = database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).one()
        assert error.cause_code == "credited_task_wall_time_exceeded"
        assert error.retry_scheduled is False

    async def test_evaluating_resume_cannot_outlive_durable_credited_wall_deadline(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        bind_task_to_dispatch(database_session, task_row, authority)
        task_row.status = TaskStatus.EVALUATING
        task_row.eval_resume_state = {"artifact_prefix": "s3://bucket/run"}
        task_row.credited_wall_deadline_at = datetime.now(UTC) + timedelta(seconds=0.5)
        database_session.add(task_row)
        database_session.commit()
        install_sqlite_evaluation_lock(database_session, monkeypatch)
        started = asyncio.Event()

        async def stalled_resume(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", stalled_resume, raising=False)

        result = await asyncio.wait_for(
            run_process_task(request, task_row, benchmark_id, runtime_services, authority), timeout=3
        )

        assert started.is_set()
        assert result == {"task_0": None}
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error = database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).one()
        assert error.cause_code == "credited_task_wall_time_exceeded"
        assert error.retry_scheduled is False

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_interrupted_evaluation_stream_recovery_shares_the_task_wall_deadline(
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
        saved_state = {"artifact_prefix": "s3://bucket/run"}
        resume_started = asyncio.Event()

        async def disconnected_stream(*_args: Any, on_eval_resume_state: Any, **_kwargs: Any) -> dict[str, Any]:
            on_eval_resume_state(saved_state)
            raise BenchmarkServiceStreamClosedError(close_code=1011, close_reason="keepalive timeout", idle_s=30.0)

        async def stalled_resume(*_args: Any, eval_resume_state: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
            assert eval_resume_state == saved_state
            resume_started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", AsyncMock(return_value=response))
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", disconnected_stream)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", stalled_resume, raising=False)

        async def _mock_run_agent(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **_kwargs: Any
        ) -> tuple[None, float]:
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        monkeypatch.setattr(task_execution_module, "run_agent", _mock_run_agent)
        monkeypatch.setattr(task_execution_module, "EXTERNAL_SERVICE_GATEWAY_URL", None)
        monkeypatch.setattr(task_execution_module, "TASK_WALL_SECONDS", 0.5)

        result = await asyncio.wait_for(
            run_process_task(request, task_row, benchmark_id, runtime_services, authority), timeout=3
        )

        assert resume_started.is_set()
        assert result == {"task_0": None}
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.credited_wall_deadline_at is not None
        error = database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).one()
        assert error.cause_code == "credited_task_wall_time_exceeded"
        assert error.retry_scheduled is False

    @pytest.mark.parametrize(
        ("failure", "expected_operation", "expected_cause_code"),
        [
            (
                GenerationTerminationUnconfirmedError("deadline termination unconfirmed"),
                "generation_termination",
                "deadline_expired_termination_unconfirmed",
            ),
            (
                ControlledGenerationTerminationUnconfirmedError("transport termination unconfirmed"),
                "sandbox",
                None,
            ),
        ],
        ids=["deadline", "transport"],
    )
    async def test_unconfirmed_controlled_termination_is_terminal_without_retry(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
        failure: ControlledGenerationTerminationUnconfirmedError,
        expected_operation: str,
        expected_cause_code: str | None,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        sandbox_entries = 0
        observed_task_opt_in: list[float | None] = []

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_entries
            sandbox_entries += 1
            sandbox = AsyncMock()
            sandbox.id = f"mock-sandbox-{sandbox_entries}"
            sandbox.name = sandbox.id
            yield sandbox

        async def _mock_run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            observed_task_opt_in.append(kwargs["plan"].allowance_seconds)
            raise failure

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            response = make_retrieve_task_response(problem_path="/tmp/problem.txt").model_copy(
                update={
                    "agent_timeout": 10.0,
                    "credited_generation": CreditedGeneration(allowance_seconds=10.0, stage_protocol=None),
                }
            )
            response.sandbox_recovery = SandboxRecoveryPolicy(max_sandbox_attempts=3)
            return response

        evaluate_instance = AsyncMock(return_value={"status": "success", "score": 1.0})
        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module, "create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(task_execution_module, "run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        assert sandbox_entries == 1
        assert observed_task_opt_in == [10.0]
        evaluate_instance.assert_not_awaited()
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error = database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).one()
        assert error.producer == (
            "tracker" if isinstance(failure, GenerationTerminationUnconfirmedError) else "sandbox_provider"
        )
        assert error.operation == expected_operation
        assert error.cause_code == expected_cause_code
        assert error.retry_scheduled is False

    @pytest.mark.parametrize("resume_origin", ["direct", "stream"])
    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_resumed_result_rolls_back_when_task_was_stopped(
        self,
        resume_origin: str,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        runtime_services: RuntimeServices,
    ) -> None:
        request, task_row, benchmark_id, authority = create_task_environment(contract, database_session)
        saved_state = {"artifact_prefix": "s3://bucket/run"}
        if resume_origin == "direct":
            task_row.status = TaskStatus.EVALUATING
            task_row.eval_resume_state = saved_state
            database_session.add(task_row)
            database_session.commit()
            bind_task_to_dispatch(database_session, task_row, authority)
            install_sqlite_evaluation_lock(database_session, monkeypatch)
        else:

            async def _stream_closed(*_args: Any, on_eval_resume_state: Any, **_kwargs: Any) -> dict[str, Any]:
                on_eval_resume_state(saved_state)
                raise BenchmarkServiceStreamClosedError(close_code=1011, close_reason="keepalive timeout", idle_s=30.0)

            monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _stream_closed)

        async def _resume(*_args: Any, eval_resume_state: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
            assert eval_resume_state == saved_state
            with Session(bind=database_session.bind) as session:
                stopped_task = session.get(Task, task_row.id)
                assert stopped_task is not None
                stopped_task.status = TaskStatus.STOPPED
                session.add(stopped_task)
                session.commit()
            return {"score": 1.0}

        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _resume, raising=False)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.STOPPED
        assert database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).all() == []

    async def test_eval_resume_loads_recovery_policy_before_handling_sandbox_loss(
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
        task_row.status = TaskStatus.EVALUATING
        task_row.eval_resume_state = {"artifact_prefix": "s3://bucket/run"}
        database_session.add(task_row)
        database_session.commit()
        bind_task_to_dispatch(database_session, task_row, authority)
        install_sqlite_evaluation_lock(database_session, monkeypatch)

        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("benchmark_service.client.time.time", lambda: 1_234.5)

        retrieve_count = 0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            nonlocal retrieve_count
            retrieve_count += 1
            response = make_retrieve_task_response(problem_path="/tmp/problem.txt")
            response.sandbox_recovery = SandboxRecoveryPolicy(max_sandbox_attempts=2)
            return response

        resume_count = 0

        async def _mock_resume_evaluation(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            nonlocal resume_count
            resume_count += 1
            if resume_count == 1:
                raise SandboxNotFoundError("grading sandbox was preempted")
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _mock_resume_evaluation, raising=False)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert retrieve_count == 1
        assert resume_count == 2
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.FINISHED

    async def test_eval_resume_policy_lookup_failure_preserves_sandbox_loss(
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
        task_row.status = TaskStatus.EVALUATING
        task_row.eval_resume_state = {"artifact_prefix": "s3://bucket/run"}
        database_session.add(task_row)
        database_session.commit()
        bind_task_to_dispatch(database_session, task_row, authority)
        install_sqlite_evaluation_lock(database_session, monkeypatch)

        async def _failed_policy_lookup(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            raise BenchmarkServiceError("policy lookup failed")

        async def _lost_grading_sandbox(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise SandboxNotFoundError("grading sandbox was preempted")

        capture_exception = Mock()
        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module.sentry_sdk, "capture_exception", capture_exception)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _failed_policy_lookup)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _lost_grading_sandbox, raising=False)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        assert result == {"task_0": None}
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_message, phase = database_session.exec(
            select(ErrorResult.error_message, ErrorResult.phase)
            .where(ErrorResult.task == task_row.id)
            .order_by(desc(ErrorResult.created_at))
        ).one()
        assert error_message == "SandboxNotFoundError: grading sandbox was preempted"
        assert phase == FailurePhase.EVALUATION
        captured_error = capture_exception.call_args.args[0]
        assert isinstance(captured_error, SandboxNotFoundError)

    async def test_process_task_spans_timed_status_transitions(
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

        create_sandbox_kwargs: dict[str, Any] = {}

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            create_sandbox_kwargs.update(kwargs)
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox-id"
            mock_sandbox.name = "mock-sandbox-name"
            yield mock_sandbox

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            response = make_retrieve_task_response(problem_path="/tmp/problem.txt")
            response.volumes = [
                VolumeMount(
                    name="shared-fixtures",
                    mount_path="/fixtures",
                    read_only=True,
                    subpath="{run_id}",
                )
            ]
            return response

        async def _mock_upload_agent_artifacts(*_args: Any, **_kwargs: Any) -> None:
            return None

        run_agent_kwargs: dict[str, Any] = {}

        async def _mock_run_agent(
            *_args: Any, deadline_controller: Any, on_external_service_finalized: Any, **kwargs: Any
        ) -> tuple[None, float]:
            run_agent_kwargs.update(kwargs)
            await finalize_agent_generation(
                deadline_controller=deadline_controller, on_external_service_finalized=on_external_service_finalized
            )
            return None, 0.0

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        log_records: list[dict[str, Any]] = []
        span_records: list[dict[str, Any]] = []

        def _mock_logger_info(
            message: str,
            *_args: object,
            extra: dict[str, Any] | None = None,
            **_kwargs: Any,
        ) -> None:
            log_records.append({"message": message, **(extra or {})})

        @contextmanager
        def _mock_span(message: str, **attributes: Any) -> Generator[None, None, None]:
            record = {"message": message, **attributes}
            span_records.append(record)
            record["entered"] = True
            try:
                yield
            finally:
                record["exited"] = True

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.TaskLogBuffer.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.upload_agent_artifacts", _mock_upload_agent_artifacts)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _mock_run_agent)
        monkeypatch.setattr("tracker.utils.task_execution.logger.info", _mock_logger_info)
        monkeypatch.setattr("tracker.observability.tracing.logfire.span", _mock_span)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        await run_process_task(start_benchmark_request, task_row, benchmark_id, runtime_services, authority)

        transition_records = [record for record in span_records if record["message"] == "task.status_transition"]
        lifecycle_records = [
            record for record in span_records if record["message"] in {"task.started", "task.completed"}
        ]

        assert [(record["from_status"], record["to_status"]) for record in transition_records] == [
            (TaskStatus.PENDING.value, TaskStatus.BUILDING.value),
            (TaskStatus.BUILDING.value, TaskStatus.IN_PROGRESS.value),
            (TaskStatus.IN_PROGRESS.value, TaskStatus.EVALUATING.value),
            (TaskStatus.EVALUATING.value, TaskStatus.FINISHED.value),
        ]
        assert all(record["task_id"] == "task_0" for record in transition_records)
        assert all(record["benchmark_id"] == str(benchmark_id) for record in transition_records)
        assert all(record["entered"] and record["exited"] for record in transition_records)
        assert [record["message"] for record in lifecycle_records] == ["task.started", "task.completed"]
        assert all(record["task_id"] == "task_0" for record in lifecycle_records)
        assert all(record["benchmark_id"] == str(benchmark_id) for record in lifecycle_records)
        assert not any(record["message"].startswith("task.status_transition") for record in log_records)
        assert run_agent_kwargs["benchmark_id"] == str(benchmark_id)
        assert create_sandbox_kwargs["labels"]["run-id"] == str(benchmark_id)
        assert create_sandbox_kwargs["volumes"] == [
            VolumeMount(
                name="shared-fixtures",
                mount_path="/fixtures",
                read_only=True,
                subpath="{run_id}",
            )
        ]

        event_names = [record["message"] for record in log_records]
        stream_record = next(record for record in log_records if record["message"] == "Task output stream selected")
        assert stream_record["benchmark_id"] == str(benchmark_id)
        assert stream_record["task_id"] == "task_0"
        assert str(benchmark_id) in stream_record["cloudwatch_log_url"]
        assert "agent.run.complete" in event_names
        assert "task.evaluation.start" in event_names

        agent_run_record = next(record for record in log_records if record["message"] == "agent.run.complete")
        assert agent_run_record["task_id"] == "task_0"
        assert agent_run_record["benchmark_id"] == str(benchmark_id)
        assert agent_run_record["exit_reason"] is None

        evaluation_start_record = next(record for record in log_records if record["message"] == "task.evaluation.start")
        assert evaluation_start_record["task_id"] == "task_0"
        assert evaluation_start_record["benchmark_id"] == str(benchmark_id)
        assert evaluation_start_record["sandbox_id"] == "mock-sandbox-id"


def test_task_retry_telemetry_carries_failure_category(monkeypatch: pytest.MonkeyPatch) -> None:
    increments: list[tuple[str, dict[str, str]]] = []
    spans: list[dict[str, Any]] = []
    log_records: list[dict[str, Any]] = []

    def fake_incr(name: str, _value: float = 1, tags: dict[str, str] | None = None) -> None:
        increments.append((name, tags or {}))

    @contextmanager
    def fake_span(name: str, **attributes: Any) -> Generator[None, None, None]:
        spans.append({"name": name, **attributes})
        yield

    def fake_warning(message: str, *_args: object, extra: dict[str, Any] | None = None, **_kwargs: Any) -> None:
        log_records.append({"message": message, **(extra or {})})

    monkeypatch.setattr(task_execution_module, "incr", fake_incr)
    monkeypatch.setattr(task_execution_module, "observability_span", fake_span)
    monkeypatch.setattr(task_execution_module.logger, "warning", fake_warning)

    task_execution_module._observe_task_retry(  # pyright: ignore[reportPrivateUsage]
        Mock(number=2), SandboxSetupError("sandbox setup failed")
    )

    assert increments == [
        ("valkyrie.task.retry", {"error_class": "SandboxSetupError", "failure_category": "infrastructure"})
    ]
    assert spans == [
        {"name": "task.retry", "attempt": 2, "error_class": "SandboxSetupError", "failure_category": "infrastructure"}
    ]
    assert log_records == [
        {
            "message": "retry.before_sleep",
            "metric": "valkyrie.task",
            "fn": "_process_task_attempt",
            "attempt": 2,
            "idle_for": task_execution_module._SANDBOX_RETRY_DELAY_SECONDS,  # pyright: ignore[reportPrivateUsage]
            "error_class": "SandboxSetupError",
            "failure_category": "infrastructure",
        }
    ]
