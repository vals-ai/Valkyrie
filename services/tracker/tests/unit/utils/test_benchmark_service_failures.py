"""Unit tests for benchmark-service failure handling.

Run: uv run pytest tests/unit/utils/test_benchmark_service_failures.py
"""

import asyncio
import time
from typing import Any, Never

import httpx
import pytest
from benchmark_service import ExecResult
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import RetrieveTaskResponse
from sqlmodel import Session, desc, select
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from websockets.frames import Close
from websockets.http11 import Response

import tracker.sandbox as sandbox_module
import tracker.utils.run_orchestration as run_orchestration_module
import tracker.utils.task_execution as utils_module
from tests.unit.utils.task_execution_support import TEST_ORG, create_task_environment, run_process_task
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, BenchmarkStatus, ErrorResult, Task, TaskStatus
from tracker.types import HarnessConfig
from tracker.utils import (
    fetch_benchmark_row,
    process_benchmark,
)


class TestBenchmarkServiceFailures:
    """Benchmark service disconnect, validation, and task error handling."""

    def _latest_task_error_result(self, database_session: Session, task_row: Task) -> ErrorResult:
        return database_session.exec(
            select(ErrorResult)
            .where(ErrorResult.task == task_row.id)
            .where(ErrorResult.org_id == task_row.org_id)
            .order_by(desc(ErrorResult.created_at))
        ).one()

    def _latest_task_error(self, database_session: Session, task_row: Task) -> str:
        return self._latest_task_error_result(database_session, task_row).error_message

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_connection_closed_after_messages_produces_elapsed_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
            raise ConnectionClosedError(None, None)

        real_monotonic = time.monotonic
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_result = self._latest_task_error_result(database_session, task_row)
        assert "Benchmark service WebSocket disconnected: no close frame received or sent" in error_result.error_message
        assert "last application message received" in error_result.error_message
        assert "10s ago" in error_result.error_message
        assert error_result.producer == "benchmark_service"
        assert error_result.operation == "websocket"
        assert error_result.error_type == "ConnectionClosedError"
        assert error_result.cause_code == "websocket_connection_closed"
        assert error_result.retry_scheduled is False
        assert error_result.failed_attempt_number is None

    @pytest.mark.parametrize(
        ("code", "reason"),
        [
            (1008, "Unauthorized"),
            (1011, "keepalive ping timeout"),
        ],
    )
    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_connection_closed_preserves_remote_close_details(
        self,
        code: int,
        reason: str,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )

        async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise ConnectionClosedError(Close(code, reason), Close(code, reason), True)

        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_message = self._latest_task_error(database_session, task_row)
        assert f"received {code}" in error_message
        assert reason in error_message
        assert "last application message received" in error_message

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_validation_error_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        """VALKYRIE-5D: ValidationError from retrieve_task is caught with field names."""
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )

        async def _mock_retrieve_task_invalid(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return RetrieveTaskResponse.model_validate(
                {"source": {"type": "image"}, "resources": {"vcpu_gb": 4, "memory": 4, "disk": 10}}
            )

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task_invalid)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_message = self._latest_task_error(database_session, task_row)
        assert "Missing or invalid fields" in error_message
        assert "source.image.image" in error_message
        assert "resources.vcpu" in error_message

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_invalid_status_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        """VALKYRIE-5A: InvalidStatus from WebSocket rejection is caught with HTTP status."""
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> Never:
            raise InvalidStatus(Response(404, "Not Found", Headers()))

        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_message = self._latest_task_error(database_session, task_row)
        assert "rejected the WebSocket connection" in error_message
        assert "404" in error_message

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_required_missing_output_artifact_persists_compatible_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        contract.output_artifacts = ["artifacts/missing.json"]
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )
        expected_error = "Output artifact error: Required output artifact missing: /tmp/valkyrie/artifacts/missing.json"
        expected_log = f"[ERROR] {expected_error}"
        logged_messages: list[str] = []
        expected_log_written = asyncio.Event()
        event_loop = asyncio.get_running_loop()

        async def _mock_install_agent_dependencies(*_args: Any, **_kwargs: Any) -> None:
            return None

        async def _mock_stream_command_output(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
            return None, 0.0

        async def _mock_exec(_sandbox: Any, command: str) -> ExecResult:
            if command == "mkdir -p /testbed":
                return ExecResult(exit_code=0, output="")
            if command == "test -f /tmp/valkyrie/artifacts/missing.json":
                return ExecResult(exit_code=1, output="")
            raise AssertionError(f"unexpected command: {command}")

        def _mock_write_benchmark_log_event(_stream_key: str, message: str, *_args: Any, **_kwargs: Any) -> None:
            logged_messages.append(message)
            if expected_log in message:
                event_loop.call_soon_threadsafe(expected_log_written.set)

        monkeypatch.setattr(utils_module, "run_agent", sandbox_module.run_agent)
        monkeypatch.setattr(sandbox_module, "install_agent_dependencies", _mock_install_agent_dependencies)
        monkeypatch.setattr(
            sandbox_module,
            "_stream_command_output_with_egress_allowlist",
            _mock_stream_command_output,
        )
        monkeypatch.setattr(sandbox_module, "_exec", _mock_exec)
        monkeypatch.setattr(utils_module, "write_benchmark_log_event", _mock_write_benchmark_log_event)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)
        # Log writes are dispatched to an executor and never awaited, so the flush carrying the
        # error can land after run_process_task returns. Wait for that flush, not just the first.
        await asyncio.wait_for(expected_log_written.wait(), timeout=10)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_result = self._latest_task_error_result(database_session, task_row)
        assert error_result.error_message == expected_error
        assert error_result.producer == "output_artifact"
        assert error_result.operation == "upload_output_artifacts"
        assert error_result.error_type == "OutputArtifactError"
        assert error_result.cause_code is None
        assert error_result.retry_scheduled is False
        assert error_result.failed_attempt_number is None
        assert any(expected_log in message for message in logged_messages)

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_benchmark_service_error_produces_human_readable_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        """VALKYRIE-59: BenchmarkServiceError from setup_task is caught and stored."""
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )

        async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> Never:
            raise BenchmarkServiceError(
                "ProgramBench task container failed to start: task_cleanroom: Pulling from programbench/test"
            )

        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_message = self._latest_task_error(database_session, task_row)
        assert "ProgramBench task container failed to start" in error_message

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_empty_network_error_stores_visible_message(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        """Network exceptions with empty strings must still produce visible task errors.

        Test cases:
        - Empty-string httpx.ConnectTimeout stores its exception type in the DB.
        - The task log path receives the same visible exception type.
        """
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )
        logged_messages: list[str] = []

        async def _mock_retrieve_task_timeout(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            raise httpx.ConnectTimeout("")

        def _mock_write_benchmark_log_event(_stream_key: str, message: str, *_args: Any, **_kwargs: Any) -> None:
            logged_messages.append(message)

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task_timeout)
        monkeypatch.setattr(utils_module, "write_benchmark_log_event", _mock_write_benchmark_log_event)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": None}

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        assert self._latest_task_error(database_session, task_row) == "ConnectTimeout"
        assert any("[ERROR] ConnectTimeout" in message for message in logged_messages)

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_benchmark_blocks_external_internal_custom_destination(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request, _task_row, benchmark_id, authority = create_task_environment(
            contract, database_session, harness_config
        )
        start_benchmark_request = start_benchmark_request.model_copy(
            update={"custom_benchmark_service": "http://service.internal:8001"}
        )
        monkeypatch.setattr(run_orchestration_module, "AUTH_REQUIRED", True)
        monkeypatch.setattr(
            run_orchestration_module,
            "fetch_sandbox_provider_config",
            lambda *_args, **_kwargs: pytest.fail("sandbox config resolved before destination validation"),
        )

        await process_benchmark(
            start_benchmark_request_json=start_benchmark_request.model_dump(),
            benchmark_id_str=str(benchmark_id),
            verified_task_ids=["task_0"],
            executor_dispatch_id=str(authority.dispatch_id),
        )

        with Session(bind=database_session.bind) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, TEST_ORG)
            assert benchmark_row.status == BenchmarkStatus.ERROR
            assert benchmark_row.error_message is not None
            assert "Custom benchmark destination is not allowed" in benchmark_row.error_message

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_benchmark_service_error_in_process_benchmark(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        executor_authority_kwargs: Any,
    ) -> None:
        """VALKYRIE-1Z: BenchmarkServiceError from final_score is caught at the benchmark level."""
        start_benchmark_request, _task_row, benchmark_id, _authority = create_task_environment(
            contract, database_session, harness_config
        )

        html_error = (
            "Final score failed with status code 404, response: <!DOCTYPE html><html><body>404 Not Found</body></html>"
        )

        async def _mock_final_score(*_args: Any, **_kwargs: Any) -> Never:
            raise BenchmarkServiceError(html_error)

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
        benchmark_row = fetch_benchmark_row(benchmark_id, database_session, TEST_ORG)
        authority_kwargs = executor_authority_kwargs(benchmark_row)

        await process_benchmark(
            start_benchmark_request_json=start_benchmark_request.model_dump(),
            benchmark_id_str=str(benchmark_id),
            verified_task_ids=["task_0"],
            **authority_kwargs,
        )

        with Session(bind=database_session.bind) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, TEST_ORG)
            assert benchmark_row.status == BenchmarkStatus.ERROR
            assert benchmark_row.error_message is not None
            assert "Final score failed with status code 404" in benchmark_row.error_message
