import time
from asyncio import Semaphore
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from benchmark_service import SnapshotSource
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import RetrieveTaskResponse
from sqlmodel import Session
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from websockets.http11 import Response

import tracker.utils as utils_module
from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import AgentContractRequest, BenchmarkStatus, Org, Task, TaskStatus
from tracker.exceptions import OutputArtifactError
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import fetch_benchmark_row, process_benchmark, process_task, start_benchmark_request_to_benchmark


class TestBenchmarkServiceDisconnect:
    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

    def _create_task_env(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> tuple[StartBenchmarkRequest, Task, UUID]:
        """Create a benchmark request, benchmark row, and task row for process_task tests."""
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

        return start_benchmark_request, task_row, benchmark_row.id

    async def _run_process_task(
        self,
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
            org=self._test_org,
            creation_semaphore=Semaphore(1),
        )

    async def test_connection_closed_after_messages_produces_elapsed_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
            raise ConnectionClosedError(None, None)

        real_monotonic = time.monotonic
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await self._run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

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
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-5D: ValidationError from retrieve_task is caught with field names."""
        start_benchmark_request, task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )

        async def _mock_retrieve_task_invalid(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse.model_validate(
                {"source": {"type": "image"}, "resources": {"vcpu_gb": 4, "memory": 4, "disk": 10}}
            )

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task_invalid)

        result = await self._run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert "Missing or invalid fields" in task_row.error_message
        assert "source.image.image" in task_row.error_message
        assert "resources.vcpu" in task_row.error_message

    async def test_legacy_snapshot_docker_image_uses_snapshot_source(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )
        sandbox_sources: list[Any] = []

        async def _mock_retrieve_task_legacy_snapshot(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse.model_validate(
                {
                    "docker_image": "snapshot:code-migration-eval-container-v8",
                    "problem_path": "/tmp/problem_statement.txt",
                    "cwd": "/testbed",
                    "resources": {"vcpu": 2, "memory": 4, "disk": 5},
                }
            )

        @asynccontextmanager
        async def _capture_create_sandbox(*_args: Any, source: Any, **_kwargs: Any):
            sandbox_sources.append(source)
            yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task_legacy_snapshot)
        monkeypatch.setattr(utils_module, "create_sandbox", _capture_create_sandbox)

        result = await self._run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(sandbox_sources) == 1
        assert isinstance(sandbox_sources[0], SnapshotSource)
        assert sandbox_sources[0].snapshot == "code-migration-eval-container-v8"

    async def test_invalid_status_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-5A: InvalidStatus from WebSocket rejection is caught with HTTP status."""
        start_benchmark_request, task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> Any:
            raise InvalidStatus(Response(404, "Not Found", Headers()))

        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)

        result = await self._run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert "rejected the WebSocket connection" in task_row.error_message
        assert "404" in task_row.error_message

    async def test_output_artifact_error_marks_task_error_without_generic_exception(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )

        async def _mock_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            raise OutputArtifactError("Required output artifact missing: /logs/result.json")

        monkeypatch.setattr(utils_module, "run_agent", _mock_run_agent)

        result = await self._run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert task_row.error_message is not None
        assert "Output artifact error" in task_row.error_message
        assert "Required output artifact missing" in task_row.error_message

    async def test_benchmark_service_error_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """VALKYRIE-59: BenchmarkServiceError from setup_task is caught and stored."""
        start_benchmark_request, task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> Any:
            raise BenchmarkServiceError(
                "ProgramBench task container failed to start: task_cleanroom: Pulling from programbench/test"
            )

        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)

        result = await self._run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

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
        start_benchmark_request, _task_row, benchmark_id = self._create_task_env(
            contract, database_session, harness_config
        )

        html_error = (
            "Final score failed with status code 404, response: <!DOCTYPE html><html><body>404 Not Found</body></html>"
        )

        async def _mock_final_score(*_args: Any, **_kwargs: Any) -> Any:
            raise BenchmarkServiceError(html_error)

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)

        await process_benchmark(
            start_benchmark_request_json=start_benchmark_request.model_dump(),
            benchmark_id_str=str(benchmark_id),
            verified_task_ids=["task_0"],
        )

        with Session(bind=database_session.bind) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, self._test_org)
            assert benchmark_row.status == BenchmarkStatus.ERROR
            assert benchmark_row.error_message is not None
            assert "Final score failed with status code 404" in benchmark_row.error_message
