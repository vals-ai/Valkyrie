"""Unit tests for task execution retries and timing signals.

Run: uv run pytest tests/unit/utils/test_task_execution_retry.py
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from benchmark_service import SandboxNotFoundError, SandboxRecoveryPolicy
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import RetrieveTaskResponse, VolumeMount
from sqlmodel import Session, col, desc, select

from tests.unit.utils.task_execution_support import (
    create_task_environment,
    make_retrieve_task_response,
    run_process_task,
)
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import (
    AgentContractRequest,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    TaskStatus,
)
from tracker.exceptions import AgentRunFailedError, DependencySetupExhaustedError, SandboxSetupError
from tracker.sandbox import DependencySetupMode
from tracker.types import HarnessConfig
from tracker.utils import task_execution as task_execution_module


class TestTaskExecutionRetry:
    """Task execution retries, callbacks, and transition spans."""

    @pytest.mark.parametrize(
        "fail_target,error,second_error,expected_dependency_modes,expected_status",
        [
            (
                "tracker.utils.task_execution.run_agent",
                SandboxSetupError("Failed to create command stream"),
                None,
                [DependencySetupMode.IN_PLACE_RETRIES, DependencySetupMode.IN_PLACE_RETRIES],
                TaskStatus.FINISHED,
            ),
            (
                "tracker.utils.task_execution.upload_agent_artifacts",
                SandboxSetupError("Command failed with exit code 35"),
                None,
                [DependencySetupMode.IN_PLACE_RETRIES],
                TaskStatus.FINISHED,
            ),
            (
                "tracker.utils.task_execution.run_agent",
                DependencySetupExhaustedError("dependency setup exhausted"),
                None,
                [DependencySetupMode.IN_PLACE_RETRIES, DependencySetupMode.FINAL_FRESH_SANDBOX],
                TaskStatus.FINISHED,
            ),
            (
                "tracker.utils.task_execution.run_agent",
                DependencySetupExhaustedError("dependency setup exhausted"),
                AgentRunFailedError("fresh sandbox setup failed"),
                [DependencySetupMode.IN_PLACE_RETRIES, DependencySetupMode.FINAL_FRESH_SANDBOX],
                TaskStatus.ERROR,
            ),
        ],
    )
    async def test_process_task_retries_on_sandbox_setup_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
        fail_target: str,
        error: SandboxSetupError,
        second_error: Exception | None,
        expected_dependency_modes: list[DependencySetupMode],
        expected_status: TaskStatus,
    ) -> None:
        """When a SandboxSetupError subclass is raised during sandbox setup or agent execution,
        process_task should delete the sandbox, create a fresh one, and complete successfully.

        Test Cases:
            - SandboxSetupError is raised on the first attempt
            - process_task retries with a new sandbox
            - Task ends in FINISHED state after the retry succeeds
            - The sandbox context manager is entered twice (one per attempt)
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
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
            yield mock_sandbox

        call_count = 0
        dependency_modes: list[DependencySetupMode] = []

        async def _fails_first_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            nonlocal call_count
            call_count += 1
            dependency_modes.append(_kwargs["dependency_setup_mode"])
            if call_count == 1:
                raise error
            if second_error:
                raise second_error
            return None, 0.0

        async def _fails_first_other(*_args: Any, **_kwargs: Any) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise error

        async def _mock_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            dependency_modes.append(_kwargs["dependency_setup_mode"])
            return None, 0.0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        is_run_agent_target = fail_target == "tracker.utils.task_execution.run_agent"
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(fail_target, _fails_first_run_agent if is_run_agent_target else _fails_first_other)
        if not is_run_agent_target:
            monkeypatch.setattr("tracker.utils.task_execution.run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        expected_result = None if expected_status is TaskStatus.ERROR else {"status": "success", "score": 1.0}
        assert result == {"task_0": expected_result}
        assert sandbox_entry_count == 2
        assert call_count == 2
        assert dependency_modes == expected_dependency_modes

        database_session.refresh(task_row)
        assert task_row.status == expected_status

        error_results = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all()
        retry_results = [result for result in error_results if result.retry_scheduled]
        terminal_results = [result for result in error_results if not result.retry_scheduled]

        assert len(retry_results) == 1
        retry_result = retry_results[0]
        assert retry_result.error_message == str(error)
        assert retry_result.producer == "sandbox_provider"
        assert retry_result.operation == "setup"
        assert retry_result.error_type == type(error).__name__
        assert retry_result.cause_code is None
        assert retry_result.failed_attempt_number == 1

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
            assert terminal_result.error_type == type(second_error).__name__
            assert terminal_result.cause_code is None
            assert terminal_result.failed_attempt_number is None
        else:
            assert terminal_results == []

    async def test_revoked_dispatch_does_not_begin_the_second_automatic_attempt(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        monkeypatch.setattr(task_execution_module, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.buffer_logs", Mock())

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

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}
        assert sandbox_entry_count == 1
        assert retrieve_task_call_count == 1
        assert database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).all() == []

    @pytest.mark.parametrize(
        "max_attempts,failures,expected_attempts,expected_status",
        [
            (3, 1, 2, TaskStatus.FINISHED),
            (3, 2, 3, TaskStatus.FINISHED),
            (2, 2, 2, TaskStatus.ERROR),
            (None, 1, 1, TaskStatus.ERROR),
        ],
    )
    async def test_lost_sandbox_recovery_is_opt_in_and_bounded(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
        max_attempts: int | None,
        failures: int,
        expected_attempts: int,
        expected_status: TaskStatus,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
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

        async def _mock_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            nonlocal run_count
            run_count += 1
            if run_count <= failures:
                raise SandboxNotFoundError("sandbox was preempted")
            return None, 0.0

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            response = make_retrieve_task_response(problem_path="/tmp/problem.txt")
            if max_attempts is not None:
                response.sandbox_recovery = SandboxRecoveryPolicy(max_sandbox_attempts=max_attempts)
            return response

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module, "create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(task_execution_module, "run_agent", _mock_run_agent)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        expected_result = None if expected_status is TaskStatus.ERROR else {"status": "success", "score": 1.0}
        assert result == {"task_0": expected_result}
        assert len(sandbox_envs) == expected_attempts
        assert all(env["RUN_ID"] == str(benchmark_id) for env in sandbox_envs)
        assert "VALKYRIE_SANDBOX_OUTAGE_ID" not in sandbox_envs[0]
        for lost_attempt, env in enumerate(sandbox_envs[1:], start=1):
            assert env["VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH"] == "1234.5"
            assert env["VALKYRIE_SANDBOX_OUTAGE_ID"] == (
                f"{benchmark_id}:task_0:{lost_attempt}:00000000000000000000000000000001"
            )

        database_session.refresh(task_row)
        assert task_row.status == expected_status

    async def test_eval_resume_loads_recovery_policy_before_handling_sandbox_loss(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        task_row.status = TaskStatus.EVALUATING
        task_row.eval_resume_state = {"artifact_prefix": "s3://bucket/run"}
        database_session.add(task_row)
        database_session.commit()

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
        monkeypatch.setattr(task_execution_module, "buffer_logs", Mock())
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _mock_resume_evaluation, raising=False)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

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
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        task_row.status = TaskStatus.EVALUATING
        task_row.eval_resume_state = {"artifact_prefix": "s3://bucket/run"}
        database_session.add(task_row)
        database_session.commit()

        async def _failed_policy_lookup(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            raise BenchmarkServiceError("policy lookup failed")

        async def _lost_grading_sandbox(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise SandboxNotFoundError("grading sandbox was preempted")

        capture_exception = Mock()
        monkeypatch.setattr(task_execution_module, "engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr(task_execution_module, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module.sentry_sdk, "capture_exception", capture_exception)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _failed_policy_lookup)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _lost_grading_sandbox, raising=False)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}
        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_message = database_session.exec(
            select(ErrorResult.error_message)
            .where(ErrorResult.task == task_row.id)
            .order_by(desc(ErrorResult.created_at))
        ).one()
        assert error_message == "grading sandbox was preempted"
        captured_error = capture_exception.call_args.args[0]
        assert isinstance(captured_error, SandboxNotFoundError)

    async def test_process_task_spans_timed_status_transitions(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
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

        async def _mock_run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            run_agent_kwargs.update(kwargs)
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
        monkeypatch.setattr("tracker.utils.task_execution.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.task_execution.upload_agent_artifacts", _mock_upload_agent_artifacts)
        monkeypatch.setattr("tracker.utils.task_execution.run_agent", _mock_run_agent)
        monkeypatch.setattr("tracker.utils.task_execution.logger.info", _mock_logger_info)
        monkeypatch.setattr("tracker.observability.tracing.logfire.span", _mock_span)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

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
