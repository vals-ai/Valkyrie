"""Unit tests for task execution retries and timing signals.

Run: uv run pytest tests/unit/utils/test_task_execution_retry.py
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse
from sqlmodel import Session
from tenacity import wait_none

from tests.unit.utils.task_execution_support import (
    create_task_environment,
    make_retrieve_task_response,
    run_process_task,
)
from tracker.database.models import AgentContractRequest, TaskStatus
from tracker.exceptions import SandboxSetupError
from tracker.types import HarnessConfig
from tracker.utils import process_task


class TestTaskExecutionRetry:
    """Task execution retries, callbacks, and transition spans."""

    @pytest.mark.parametrize(
        "fail_target,error",
        [
            ("tracker.utils.task_execution.run_agent", SandboxSetupError("Failed to create command stream")),
            (
                "tracker.utils.task_execution.upload_agent_artifacts",
                SandboxSetupError("Command failed with exit code 35"),
            ),
        ],
    )
    async def test_process_task_retries_on_sandbox_setup_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        fail_target: str,
        error: SandboxSetupError,
    ) -> None:
        """When a SandboxSetupError subclass is raised during sandbox setup or agent execution,
        process_task should delete the sandbox, create a fresh one, and complete successfully.

        Test Cases:
            - SandboxSetupError is raised on the first attempt
            - process_task retries with a new sandbox
            - Task ends in FINISHED state after the retry succeeds
            - The sandbox context manager is entered twice (one per attempt)
        """
        start_benchmark_request, task_row, benchmark_id = create_task_environment(
            contract,
            database_session,
            harness_config,
        )

        monkeypatch.setattr(cast(Any, process_task).retry, "wait", wait_none())

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

        async def _fails_first_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise error
            return None, 0.0

        async def _fails_first_other(*_args: Any, **_kwargs: Any) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise error

        async def _mock_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
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

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert sandbox_entry_count == 2
        assert call_count == 2

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.FINISHED

    async def test_process_task_spans_timed_status_transitions(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id = create_task_environment(
            contract,
            database_session,
            harness_config,
        )

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox-id"
            mock_sandbox.name = "mock-sandbox-name"
            yield mock_sandbox

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return make_retrieve_task_response(problem_path="/tmp/problem.txt")

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

        await run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

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
