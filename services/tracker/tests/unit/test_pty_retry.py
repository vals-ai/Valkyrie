from asyncio import Semaphore
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import Resources, RetrieveTaskResponse
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import AgentContractRequest, BenchmarkStatus, Org, Task, TaskStatus
from tracker.exceptions import PtyCreationError
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_task, start_benchmark_request_to_benchmark


class TestPtyRetry:
    _test_org = Org(id=TEST_ORG_ID, name="default")

    async def test_process_task_retries_on_pty_creation_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """
        When run_agent raises PtyCreationError on the first attempt, process_task should
        delete the sandbox, create a fresh one, and complete successfully on the retry.

        Test Cases:
            - run_agent raises PtyCreationError on the first attempt
            - process_task retries with a new sandbox
            - Task ends in FINISHED state after the retry succeeds
            - The sandbox context manager is entered twice (one per attempt)
        """
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_org)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        sandbox_entry_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any):
            nonlocal sandbox_entry_count
            sandbox_entry_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-{sandbox_entry_count}"
            mock_sandbox.name = f"mock-sandbox-{sandbox_entry_count}"
            yield mock_sandbox

        run_agent_call_count = 0

        async def _run_agent_fails_first(*_args: Any, **_kwargs: Any) -> None:
            nonlocal run_agent_call_count
            run_agent_call_count += 1
            if run_agent_call_count == 1:
                raise PtyCreationError("Failed to create PTY session after 5 attempts: connection refused")

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse(
                docker_image="test-image:latest",
                problem_path="/tmp/problem.txt",
                cwd="/testbed",
                resources=Resources(vcpu=2, memory=4, disk=5),
            )

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"status": "success", "score": 1.0}

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr("tracker.utils.run_agent", _run_agent_fails_first)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await process_task(
            task_row=task_row,
            start_benchmark_request=start_benchmark_request,
            benchmark_service=start_benchmark_request.benchmark_service,
            benchmark_id=benchmark_row.id,
            task_id="task_0",
            harness_config=harness_config,
            org=self._test_org,
            creation_semaphore=Semaphore(1),
        )

        assert result == {"task_0": {"status": "success", "score": 1.0}}

        # Two sandbox entries: first attempt (PTY failed) and retry
        assert sandbox_entry_count == 2
        assert run_agent_call_count == 2

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.FINISHED
