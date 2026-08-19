"""Unit tests for task execution retries and timing signals.

Run: uv run pytest tests/unit/utils/test_task_execution_retry.py
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse, VolumeMount
from sqlmodel import Session, col, select
from tenacity import wait_none

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

    def test_process_task_retry_decorator_uses_observability_retry_callback(self) -> None:
        retryable_process_task = getattr(task_execution_module, "_process_task_attempt")
        before_sleep = retryable_process_task.retry.before_sleep

        assert before_sleep is not None
        assert callable(before_sleep)

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

        retryable_process_task = getattr(task_execution_module, "_process_task_attempt")
        monkeypatch.setattr(retryable_process_task.retry, "wait", wait_none())

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
        retryable_process_task = getattr(task_execution_module, "_process_task_attempt")
        monkeypatch.setattr(retryable_process_task.retry, "wait", wait_none())
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

        def _revoke_authority(_retry_state: object) -> None:
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
        monkeypatch.setattr(task_execution_module, "_process_task_retry_observer", _revoke_authority)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}
        assert sandbox_entry_count == 1
        assert retrieve_task_call_count == 1
        assert database_session.exec(select(ErrorResult).where(col(ErrorResult.task) == task_row.id)).all() == []

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
        monkeypatch.setattr("tracker.utils.task_execution.logfire.span", _mock_span)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        transition_records = [record for record in span_records if record["message"] == "task.status_transition"]

        assert [(record["from_status"], record["to_status"]) for record in transition_records] == [
            (TaskStatus.PENDING.value, TaskStatus.BUILDING.value),
            (TaskStatus.BUILDING.value, TaskStatus.IN_PROGRESS.value),
            (TaskStatus.IN_PROGRESS.value, TaskStatus.EVALUATING.value),
            (TaskStatus.EVALUATING.value, TaskStatus.FINISHED.value),
        ]
        assert all(record["task_id"] == "task_0" for record in transition_records)
        assert all(record["benchmark_id"] == str(benchmark_id) for record in transition_records)
        assert all(record["entered"] and record["exited"] for record in transition_records)
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
