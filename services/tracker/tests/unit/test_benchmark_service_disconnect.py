import time
from asyncio import Semaphore
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import Resources, RetrieveTaskResponse
from sqlmodel import Session
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from websockets.http11 import Response

from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import AgentContractRequest, BenchmarkStatus, Org, Task, TaskStatus
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark, process_task, start_benchmark_request_to_benchmark


class TestBenchmarkServiceDisconnect:
    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

    async def test_connection_closed_after_messages_produces_elapsed_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any):
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox-id"
            mock_sandbox.name = "mock-sandbox-name"
            yield mock_sandbox

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse(
                docker_image="test-image:latest",
                problem_path="/tmp/problem.txt",
                cwd="/testbed",
                resources=Resources(vcpu=2, memory=4, disk=5),
            )

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            # Simulate the benchmark service disconnecting 10s after last_log_time was reset.
            monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
            raise ConnectionClosedError(None, None)

        real_monotonic = time.monotonic

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.create_sandbox", _mock_create_sandbox)
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

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert (
            "Benchmark service has not sent a message, causing the connection to disconnect" in task_row.error_message
        )
        assert "last message received" in task_row.error_message
        assert "10s ago" in task_row.error_message

    async def test_validation_error_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-5D: ValidationError from retrieve_task is caught with field names."""
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse.model_validate(
                {"source": {"type": "image"}, "resources": {"vcpu_gb": 4, "memory_gb": 4, "disk_gb": 10}}
            )

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

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

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert "Missing or invalid fields" in task_row.error_message
        assert "docker_image" in task_row.error_message

    async def test_invalid_status_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-5A: InvalidStatus from WebSocket rejection is caught with HTTP status."""
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any):
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox-id"
            mock_sandbox.name = "mock-sandbox-name"
            yield mock_sandbox

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse(
                docker_image="test-image:latest",
                problem_path="/tmp/problem.txt",
                cwd="/testbed",
                resources=Resources(vcpu=2, memory=4, disk=5),
            )

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> Any:
            raise InvalidStatus(Response(404, "Not Found", Headers()))

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)

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

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert "rejected the WebSocket connection" in task_row.error_message
        assert "404" in task_row.error_message

    async def test_benchmark_service_error_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-59: BenchmarkServiceError from setup_task is caught and stored."""
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any):
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox-id"
            mock_sandbox.name = "mock-sandbox-name"
            yield mock_sandbox

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse(
                docker_image="test-image:latest",
                problem_path="/tmp/problem.txt",
                cwd="/testbed",
                resources=Resources(vcpu=2, memory=4, disk=5),
            )

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> Any:
            raise BenchmarkServiceError(
                "ProgramBench task container failed to start: task_cleanroom: Pulling from programbench/test"
            )

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)

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

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert "ProgramBench task container failed to start" in task_row.error_message

    async def test_benchmark_service_error_in_process_benchmark(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-1Z: BenchmarkServiceError from final_score is caught at the benchmark level."""
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        html_error = (
            "Final score failed with status code 404, response: <!DOCTYPE html><html><body>404 Not Found</body></html>"
        )

        async def _mock_final_score(*_args: Any, **_kwargs: Any) -> Any:
            raise BenchmarkServiceError(html_error)

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)

        await process_benchmark(
            start_benchmark_request_json=start_benchmark_request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=["task_0"],
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.ERROR
        assert benchmark_row.error_message is not None
        assert "Final score failed with status code 404" in benchmark_row.error_message
