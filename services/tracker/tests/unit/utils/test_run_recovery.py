"""Tests for stopping, resuming, and retrying benchmark runs.

Run: uv run pytest tests/unit/utils/test_run_recovery.py

Covers task state transitions, sandbox cleanup, and run-control API behavior.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from benchmark_service import SandboxQuery
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.sandbox import DaytonaProviderConfig
from benchmark_service.schemas import FinalScoreResponse, RetrieveTaskResponse, VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select

import main as main_module
from main import app
import tracker.utils as tracker_utils
from tests.factories import make_benchmark, make_error_result, make_evaluation_result
from tests.unit.utils.task_execution_support import MockKicker, make_retrieve_task_response
from tests.utils import TEST_ORG_ID
from tracker import config
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
    FinalEvaluation,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.executor.release_control import ReleaseControlError, promote_release
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import run_control as run_control_module
from tracker.utils import (
    ResizableLimiter,
    TaskMonitor,
    TrackedTask,
    TrackedTaskStatus,
    force_stop_sandboxes,
    initiate_stop_benchmark,
    process_benchmark,
    process_task,
    reset_to_in_progress_status,
    start_benchmark_request_to_benchmark,
    stop_sandbox,
    update_benchmark_concurrency,
)
from tracker.utils.task_execution import handle_early_exit

UTC = ZoneInfo("UTC")
_NEVER_RELEASED = 1_000_000
_ORIGINAL_ATTEMPT_AT = datetime(2026, 7, 8)
_RESUMED_ATTEMPT_AT = datetime(2026, 7, 9)
client = TestClient(app)


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest, database_session: Session) -> Benchmark:
    """Build recovery benchmarks with a persisted executor release identity."""
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="s3://artifacts/test-release.pex",
        artifact_digest="digest-test-release",
        protocol_version="1",
        readiness_verified=True,
    )
    database_session.add(release)
    database_session.commit()
    promote_release(database_session, release.id)
    database_session.commit()

    benchmark = make_benchmark(contract=contract, concurrency=5)
    benchmark.executor_release_id = release.id
    benchmark.current_execution_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version
    return benchmark


def _created_at(day: int) -> datetime:
    return datetime(2026, 6, day, tzinfo=UTC)


@asynccontextmanager
async def _counted_sandbox(counter: list[int]) -> AsyncGenerator[AsyncMock, None]:
    counter[0] += 1
    mock_sandbox = AsyncMock()
    mock_sandbox.id = f"mock-sandbox-id-{counter[0]}"
    yield mock_sandbox


class MockSubsetSandboxProvider:
    """Expose task-labeled sandboxes and record deletions."""

    def __init__(self, task_ids: list[str]) -> None:
        self.sandboxes_by_task: dict[str, Mock] = {}
        self.deleted_sandbox_ids: list[str] = []
        for task_id in task_ids:
            sandbox = Mock()
            sandbox.id = f"sandbox-{task_id}"
            sandbox.name = task_id
            self.sandboxes_by_task[task_id] = sandbox

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Mock, None]:
        task_id = query.labels.get("Task")
        if task_id is not None and task_id in self.sandboxes_by_task:
            yield self.sandboxes_by_task[task_id]

    async def delete_sandbox(self, sandbox_id: str) -> None:
        self.deleted_sandbox_ids.append(sandbox_id)


class MockReleasingSandboxProvider:
    """Expose one benchmark sandbox until the executor's own teardown removes it."""

    sandbox_id = "sandbox-task_0"

    def __init__(self, release_after_polls: int) -> None:
        self.list_calls = 0
        self.deleted_sandbox_ids: list[str] = []
        self._release_after_polls = release_after_polls

    async def list_sandboxes(self, _query: SandboxQuery) -> AsyncGenerator[Mock, None]:
        self.list_calls += 1
        if self.list_calls > self._release_after_polls:
            return
        sandbox = Mock()
        sandbox.id = self.sandbox_id
        sandbox.name = "task_0"
        yield sandbox

    async def delete_sandbox(self, sandbox_id: str) -> None:
        self.deleted_sandbox_ids.append(sandbox_id)


class TestRunRecovery:
    """Benchmark stop, retry, resume, and stale-worker recovery."""

    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_benchmark_uses_persisted_and_refreshed_concurrency(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        task_ids = ["task_0", "task_1"]
        request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=7,
            task_ids=task_ids,
            harness_config=harness_config,
        )
        benchmark_row = start_benchmark_request_to_benchmark(request, self._test_starter)
        benchmark_row.arguments = benchmark_row.arguments.model_copy(update={"concurrency": 1})
        database_session.add(benchmark_row)
        database_session.commit()
        admitted_task_ids: list[str] = []
        first_admitted = asyncio.Event()
        release_first = asyncio.Event()
        second_admitted = asyncio.Event()
        sandbox_count = [0]

        def unique_sandbox(*_args: Any, **_kwargs: Any) -> AbstractAsyncContextManager[AsyncMock]:
            return _counted_sandbox(sandbox_count)

        async def controlled_retrieve_task(*_args: Any, task_id: str, **_kwargs: Any) -> RetrieveTaskResponse:
            admitted_task_ids.append(task_id)
            if len(admitted_task_ids) == 1:
                first_admitted.set()
                await release_first.wait()
            else:
                second_admitted.set()
            return make_retrieve_task_response()

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", controlled_retrieve_task)
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", unique_sandbox)
        authority_kwargs = executor_authority_kwargs(benchmark_row)

        process_future = asyncio.create_task(
            process_benchmark(
                start_benchmark_request_json=request.model_dump(),
                benchmark_id_str=str(benchmark_row.id),
                verified_task_ids=task_ids,
                **authority_kwargs,
            )
        )
        try:
            await asyncio.wait_for(first_admitted.wait(), timeout=2)
            benchmark_row.arguments = benchmark_row.arguments.model_copy(update={"concurrency": 2})
            database_session.add(benchmark_row)
            database_session.commit()
            await asyncio.wait_for(second_admitted.wait(), timeout=2)
        finally:
            release_first.set()
            await asyncio.wait_for(process_future, timeout=5)

        assert admitted_task_ids == task_ids

    async def test_stop_selected_tasks_scopes_graceful_and_force(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_headers: dict[str, str],
        executor_authority: Any,
    ) -> None:
        """Apply selected stops and reject invalid selections without affecting other work.

        Test cases:
        - Graceful stop changes only the selected tasks.
        - Force stop deletes only the selected task sandbox.
        - Empty and out-of-run selections are rejected.
        - Unselected work and benchmark status remain unchanged.
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        foreign_benchmark = Benchmark(
            id=UUID("123e4567-e89b-12d3-a456-426614174001"),
            org_id=TEST_ORG_ID,
            name="other-benchmark",
            arguments=benchmark_row.arguments,
        )
        foreign_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_not_in_run",
            benchmark=foreign_benchmark.id,
            status=TaskStatus.PENDING,
        )
        database_session.add(benchmark_row)
        database_session.add(foreign_benchmark)
        database_session.add(foreign_task)
        database_session.add_all(
            [
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="task_pending",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.PENDING,
                ),
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="task_evaluating",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.EVALUATING,
                ),
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="task_force_selected",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.IN_PROGRESS,
                ),
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="task_unselected",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.IN_PROGRESS,
                ),
            ]
        )
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)
        provider = MockSubsetSandboxProvider(["task_force_selected", "task_unselected"])

        async def reject_task_verification(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            raise AssertionError("stopping existing tasks must not call the benchmark service")

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", reject_task_verification)

        def get_sandbox_provider(*_args: object, **_kwargs: object) -> MockSubsetSandboxProvider:
            return provider

        monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", get_sandbox_provider)

        graceful_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=false",
            json={"task_ids": ["task_pending", "task_evaluating"]},
            headers=harness_headers,
        )

        force_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=true",
            json={"task_ids": ["task_force_selected"]},
            headers=harness_headers,
        )

        empty_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}",
            json={"task_ids": []},
            headers=harness_headers,
        )

        missing_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}",
            json={"task_ids": ["task_not_in_run"]},
            headers=harness_headers,
        )

        assert graceful_response.status_code == 200, graceful_response.text
        assert force_response.status_code == 200, force_response.text
        assert empty_response.status_code == 400
        assert missing_response.status_code == 400
        assert "not part of run" in missing_response.text

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert task_statuses == {
            "task_pending": TaskStatus.STOPPED,
            "task_evaluating": TaskStatus.STOPPED,
            "task_force_selected": TaskStatus.STOPPED,
            "task_unselected": TaskStatus.IN_PROGRESS,
        }
        assert provider.deleted_sandbox_ids == ["sandbox-task_force_selected"]

        database_session.refresh(foreign_task)
        assert foreign_task.status == TaskStatus.PENDING

        database_session.refresh(benchmark_row)
        dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert dispatch is not None
        assert dispatch.status == ExecutorDispatchStatus.RUNNING

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_stale_worker_cannot_continue_after_force_stop_resume(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        harness_headers: dict[str, str],
        executor_authority: Any,
    ) -> None:
        """Keep an old worker from continuing after force stop and immediate resume.

        Test cases:
        - Force stop occurs while task retrieval is in flight.
        - The task is immediately resumed with a new attempt token.
        - A stale early-exit write cannot stop the resumed attempt.
        - The stale worker cannot enter BUILDING or create a sandbox.
        """
        start_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=["task_selected"],
            harness_config=harness_config,
        )
        benchmark_row = start_benchmark_request_to_benchmark(start_request, self._test_starter)
        benchmark_row.executor_release_id = "test-release"
        benchmark_row.executor_artifact_uri = "s3://artifacts/test-release.pex"
        benchmark_row.executor_artifact_digest = "digest-test-release"
        benchmark_row.executor_protocol_version = "1"
        selected_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_selected",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
            started_at=_ORIGINAL_ATTEMPT_AT,
        )
        database_session.add(
            ExecutorRelease(
                id="test-release",
                artifact_uri="s3://artifacts/test-release.pex",
                artifact_digest="digest-test-release",
                protocol_version="1",
                readiness_verified=True,
            )
        )
        database_session.add(benchmark_row)
        database_session.add(selected_task)
        database_session.commit()
        promote_release(database_session, "test-release")
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)
        database_session.expire_all()
        selected_task = database_session.exec(
            select(Task).where(Task.benchmark == benchmark_row.id).where(Task.task_id == "task_selected")
        ).one()
        original_started_at = selected_task.started_at

        retrieval_started = asyncio.Event()
        continue_retrieval = asyncio.Event()

        async def delayed_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            retrieval_started.set()
            await continue_retrieval.wait()

            return make_retrieve_task_response()

        create_sandbox = Mock(side_effect=AssertionError("stopped task attempted to create a sandbox"))

        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", delayed_retrieve_task)
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)

        def get_sandbox_provider(*_args: object, **_kwargs: object) -> MockSubsetSandboxProvider:
            return MockSubsetSandboxProvider([])

        def resumed_attempt_time(_timezone: object) -> datetime:
            return _RESUMED_ATTEMPT_AT

        monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", get_sandbox_provider)
        monkeypatch.setattr(
            "tracker.utils.run_control.datetime",
            SimpleNamespace(now=resumed_attempt_time),
        )

        benchmark_service = benchmark_row.benchmark_service()
        process_future = asyncio.create_task(
            process_task(
                selected_task,
                start_request,
                benchmark_service,
                benchmark_row.id,
                selected_task.task_id,
                AWSRuntime.from_harness_config(harness_config),
                self._test_org,
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="key",
                    DAYTONA_API_URL="url",
                    DAYTONA_TARGET="target",
                ),
                creation_semaphore=asyncio.Semaphore(1),
                authority=authority,
            )
        )

        await asyncio.wait_for(retrieval_started.wait(), timeout=2)
        stop_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=true",
            json={"task_ids": [selected_task.task_id]},
            headers=harness_headers,
        )
        resume_response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={"task_ids": [selected_task.task_id]},
            headers=harness_headers,
        )
        continue_retrieval.set()

        assert stop_response.status_code == 200, stop_response.text
        assert resume_response.status_code == 200, resume_response.text

        try:
            result = await asyncio.wait_for(process_future, timeout=5)
        finally:
            await benchmark_service.close()

        database_session.refresh(selected_task)
        database_session.refresh(benchmark_row)
        assert result == {selected_task.task_id: None}
        assert selected_task.status == TaskStatus.PENDING
        assert original_started_at == _ORIGINAL_ATTEMPT_AT
        assert selected_task.started_at == _RESUMED_ATTEMPT_AT
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        create_sandbox.assert_not_called()

        selected_task.status = TaskStatus.IN_PROGRESS
        selected_task.started_at = _ORIGINAL_ATTEMPT_AT
        benchmark_row.status = BenchmarkStatus.STOPPING
        database_session.add_all([selected_task, benchmark_row])
        database_session.commit()
        with Session(bind=database_session.get_bind()) as stale_session:
            stale_task = stale_session.get(Task, selected_task.id)
            assert stale_task is not None
            benchmark_row.status = BenchmarkStatus.STOPPED
            database_session.add(benchmark_row)
            database_session.commit()
            late_resume_response = client.post(
                f"/retry-or-resume-benchmark/{benchmark_row.id}",
                json={"task_ids": [selected_task.task_id]},
            )
            handle_early_exit(stale_task, stale_session, authority)

        assert late_resume_response.status_code == 200, late_resume_response.text

        database_session.refresh(selected_task)
        assert selected_task.status == TaskStatus.PENDING
        assert selected_task.started_at == _RESUMED_ATTEMPT_AT

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_stop_and_resume(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        """Tests stop and resume when some tasks have already completed.

        Test Cases:
            - Start benchmark with 5 tasks
            - 2 tasks are completed (finished), 3 are still pending
            - Stop benchmark - 3 tasks are stopped (pending -> stopped)
            - Resume benchmark - only the 3 tasks that are stopped should be resumed
            - Retry or resume benchmark - all 5 tasks should have evaluation results after completion
            - The finalization lambda receives the persisted benchmark name
        """
        task_ids: list[str] = [
            "astropy__astropy-12907",
            "astropy__astropy-13033",
            "django__django-11066",
            "django__django-12325",
            "django__django-12858",
        ]
        captured_lambda_payloads: list[dict[str, Any]] = []

        def _capture_lambda_payload(_client: Any, _function_name: str, payload: dict[str, Any]) -> None:
            captured_lambda_payloads.append(payload)

        monkeypatch.setattr("tracker.utils.run_orchestration.invoke_lambda", _capture_lambda_payload)

        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=2,
            task_ids=task_ids,
            lambda_function="vals-format-lambda",
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        database_session.add(benchmark_row)
        database_session.commit()

        # Create tasks - 2 tasks are finished, 3 tasks are pending
        finished_task_ids = task_ids[:2]
        pending_task_ids = task_ids[2:]

        for task_id in finished_task_ids:
            task_row = Task(org_id=TEST_ORG_ID, task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            database_session.add(task_row)
            database_session.flush()
            database_session.add(
                EvaluationResult(
                    org_id=TEST_ORG_ID,
                    task=task_row.id,
                    instance_id=f"existing-{task_id}",
                    result={"status": "success", "score": 1.0},
                )
            )

        for task_id in pending_task_ids:
            task_row = Task(org_id=TEST_ORG_ID, task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.PENDING)
            database_session.add(task_row)

        database_session.commit()

        # Stop benchmark - only tasks that are pending become stopped
        await initiate_stop_benchmark(benchmark_row, database_session, force=False, org=self._test_org)

        # Verify: 2 tasks are finished, 3 tasks are stopped
        finished_count = len(
            database_session.exec(
                select(Task).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.FINISHED)
            ).all()
        )
        stopped_count = len(
            database_session.exec(
                select(Task).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.STOPPED)
            ).all()
        )
        assert finished_count == 2
        assert stopped_count == 3

        # Set benchmark to stopped (simulating first run completion of all tasks)
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=start_benchmark_request.benchmark_service,
            retry=False,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=[],
            org=self._test_org,
        )
        database_session.commit()
        # Only 3 tasks should be verified for resume (the 3 tasks that are stopped)
        assert len(verified_task_ids) == 3
        assert set(verified_task_ids) == set(pending_task_ids)

        # Run process_benchmark to complete the remaining tasks (the 3 tasks that are pending)
        authority_kwargs = executor_authority_kwargs(benchmark_row)
        await process_benchmark(
            start_benchmark_request_json=benchmark_row.start_benchmark_request(harness_config).model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verified_task_ids,
            **authority_kwargs,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message
        assert len(captured_lambda_payloads) == 1
        assert captured_lambda_payloads[0]["benchmark_name"] == "swebench"

    @pytest.mark.parametrize(
        ("retry_mode", "eval_resume_state", "expected_status", "expected_state"),
        [
            (
                RetryMode.AUTO,
                {"artifact_prefix": "s3://bucket/run"},
                TaskStatus.EVALUATING,
                {"artifact_prefix": "s3://bucket/run"},
            ),
            (RetryMode.AUTO, None, TaskStatus.PENDING, None),
            (RetryMode.FROM_SCRATCH, {"artifact_prefix": "s3://bucket/run"}, TaskStatus.PENDING, None),
        ],
    )
    async def test_reset_handles_eval_resume_state(
        self,
        retry_mode: RetryMode,
        eval_resume_state: dict[str, str] | None,
        expected_status: TaskStatus,
        expected_state: dict[str, str] | None,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
            eval_resume_state=eval_resume_state,
        )
        database_session.add(benchmark_row)
        database_session.add(task_row)
        database_session.commit()

        async def _mock_request_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[task_row.task_id])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=benchmark_row.benchmark_service(),
            retry=False,
            retry_mode=retry_mode,
            rerun_task_ids=[],
            org=self._test_org,
        )
        database_session.commit()

        database_session.refresh(task_row)
        assert verified_task_ids == [task_row.task_id]
        assert task_row.status == expected_status
        assert task_row.eval_resume_state == expected_state

    async def test_retry_preserves_previous_task_history_for_export(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_headers: dict[str, str],
    ) -> None:
        """Retried tasks should keep prior attempts visible in exported results.

        Test cases:
        - Previous error messages are saved before retry clears the task row.
        - Previous evaluation results are saved and history exports newest first.
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)

        task_error = Task(
            org_id=TEST_ORG_ID,
            task_id="task_error",
            benchmark=benchmark_row.id,
            status=TaskStatus.ERROR,
        )
        task_result = Task(
            org_id=TEST_ORG_ID,
            task_id="task_result",
            benchmark=benchmark_row.id,
            status=TaskStatus.FINISHED,
        )
        database_session.add_all([task_error, task_result])
        database_session.flush()
        for result_row in (
            make_evaluation_result(task_error, "older-task-error-result", {"score": 0.25}, _created_at(1)),
            make_error_result(task_error, "retry failed before", _created_at(2)),
            make_evaluation_result(task_result, "previous-task-result", {"score": 0.5}, _created_at(1)),
        ):
            database_session.add(result_row)
        database_session.commit()

        async def _mock_request_verify_task_ids(
            *_args: Any, task_ids: list[str], **_kwargs: Any
        ) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=task_ids)

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=benchmark_row.benchmark_service(),
            retry=True,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=["task_error", "task_result"],
            org=self._test_org,
        )

        assert verified_task_ids == ["task_error", "task_result"]

        for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all():
            task.status = TaskStatus.FINISHED
            database_session.add(task)
            database_session.add(
                make_evaluation_result(task, f"current-{task.task_id}", {"score": 1.0}, _created_at(3))
            )
        database_session.commit()

        response = client.get(
            "/retrieve-results",
            params={"benchmark_id": str(benchmark_row.id)},
            headers=harness_headers,
        )

        assert response.status_code == 200
        evaluation_results = response.json()["evaluation_results"]
        error_history = evaluation_results["task_error"]["history"]
        result_history = evaluation_results["task_result"]["history"]
        assert evaluation_results["task_error"]["attempts"] == 3
        assert evaluation_results["task_result"]["attempts"] == 2
        assert [entry.get("error_message") for entry in error_history] == ["retry failed before", None]
        assert error_history[0]["created_at"] > error_history[1]["created_at"]
        assert error_history[1]["result"] == {"score": 0.25}
        assert len(result_history) == 1
        assert result_history[0]["result"] == {"score": 0.5}

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_reset_lazily_creates_rows_for_unregistered_task_ids(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
    ) -> None:
        """rerun_task_ids that don't have a row yet become fresh PENDING rows."""
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.STOPPED),
        )
        database_session.commit()

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=benchmark_row.benchmark_service(),
            retry=False,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=["task_1", "task_2"],
            org=self._test_org,
        )

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }

        assert set(verified_task_ids) == {"task_0", "task_1", "task_2"}
        assert task_statuses == {
            "task_0": TaskStatus.PENDING,
            "task_1": TaskStatus.PENDING,
            "task_2": TaskStatus.PENDING,
        }

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_error_retry_with_task_ids_only_resets_requested_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
    ) -> None:
        """Explicit retry task ids on errored runs must not pull in every ERROR task.

        Test cases:
            - Terminal ERROR retry with --task-ids resets only the requested error task.
            - Other ERROR rows stay ERROR for a later full retry.
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.ERROR
        database_session.add(benchmark_row)
        database_session.add_all(
            [
                Task(org_id=TEST_ORG_ID, task_id="task_requested", benchmark=benchmark_row.id, status=TaskStatus.ERROR),
                Task(org_id=TEST_ORG_ID, task_id="task_other", benchmark=benchmark_row.id, status=TaskStatus.ERROR),
            ]
        )
        database_session.commit()

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=benchmark_row.benchmark_service(),
            retry=True,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=["task_requested"],
            org=self._test_org,
        )

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert verified_task_ids == ["task_requested"]
        assert task_statuses == {"task_requested": TaskStatus.PENDING, "task_other": TaskStatus.ERROR}

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_resume_runs_lazily_added_task_and_recomputes_final_score(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        executor_authority_kwargs: Any,
    ) -> None:
        """Resuming a finished run must clear its old score before recomputing all tasks.

        Test cases:
        - Reset removes the stale final evaluation while new work is pending.
        - Finalization scores the existing and newly completed tasks together.
        """
        existing_task_ids = ["task_0", "task_1"]
        new_task_id = "task_2"

        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=1,
            task_ids=existing_task_ids,
            harness_config=harness_config,
        )
        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_starter)
        benchmark_row.status = BenchmarkStatus.FINISHED
        benchmark_row.finished_at = datetime.now(ZoneInfo("UTC"))
        database_session.add(benchmark_row)
        database_session.flush()
        database_session.add(FinalEvaluation(org_id=TEST_ORG_ID, benchmark=benchmark_row.id, final_score=2.0))

        # Seed pre-existing FINISHED tasks with stored EvaluationResults
        for task_id in existing_task_ids:
            task = Task(
                org_id=TEST_ORG_ID,
                task_id=task_id,
                benchmark=benchmark_row.id,
                status=TaskStatus.FINISHED,
                finished_at=datetime.now(ZoneInfo("UTC")),
            )
            database_session.add(task)
            database_session.flush()
            database_session.add(
                EvaluationResult(org_id=TEST_ORG_ID, task=task.id, result={"resolved": True, "score": 1.0})
            )
        database_session.commit()

        # Override final_score to capture what it sees and weight by count
        final_score_calls: list[set[str]] = []

        async def _capturing_final_score(
            *_args: Any, evaluation_results: dict[str, Any], **_kwargs: Any
        ) -> FinalScoreResponse:
            tasks_evaluated = list(evaluation_results.keys())
            final_score_calls.append(set(tasks_evaluated))
            return FinalScoreResponse(
                tasks_evaluated=tasks_evaluated,
                final_score=len(tasks_evaluated),
                metadata={"resolved_tasks": tasks_evaluated, "unresolved_tasks": []},
            )

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _capturing_final_score)

        # Resume with a new task id — should be lazily created as PENDING
        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=start_benchmark_request.benchmark_service,
            retry=False,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=[new_task_id],
            org=self._test_org,
        )
        database_session.commit()
        assert verified_task_ids == [new_task_id]

        stale_final_evaluation = database_session.exec(
            select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark_row.id)
        ).first()
        assert stale_final_evaluation is None

        # Run the worker — the new task should make it through evaluation
        authority_kwargs = executor_authority_kwargs(benchmark_row)
        await process_benchmark(
            start_benchmark_request_json=benchmark_row.start_benchmark_request(harness_config).model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verified_task_ids,
            **authority_kwargs,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert task_statuses[new_task_id] == TaskStatus.FINISHED

        # final_score was called with the merged set: pre-existing + newly run task
        assert final_score_calls == [{"task_0", "task_1", new_task_id}]
        assert benchmark_row.final_evaluation is not None
        assert benchmark_row.final_evaluation.final_score == 3.0

    async def test_process_task_resumes_evaluation_without_sandbox(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        executor_authority: Any,
    ) -> None:
        request = StartBenchmarkRequest(
            benchmark_name="vcb",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )
        benchmark_row = start_benchmark_request_to_benchmark(request, self._test_starter)
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark_row.id,
            status=TaskStatus.EVALUATING,
            eval_resume_state={"artifact_prefix": "s3://bucket/run"},
        )
        database_session.add(task_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        create_sandbox = Mock(side_effect=AssertionError("eval resume should not create a sandbox"))

        sandbox_provider_config = DaytonaProviderConfig(
            DAYTONA_API_KEY="key",
            DAYTONA_API_URL="url",
            DAYTONA_TARGET="target",
        )

        async def _mock_resume_evaluation(
            _self: BenchmarkServiceClient,
            task_id: str,
            *_args: Any,
            eval_resume_state: dict[str, Any],
            on_eval_resume_state: Any,
            sandbox_provider: DaytonaProviderConfig,
            **_kwargs: Any,
        ) -> dict[str, Any]:
            assert task_id == "task_0"
            assert eval_resume_state == {"artifact_prefix": "s3://bucket/run"}
            assert sandbox_provider is sandbox_provider_config
            on_eval_resume_state({"artifact_prefix": "s3://bucket/run", "job_id": "job-1"})
            return {"score": 1.0}

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _mock_resume_evaluation, raising=False)

        benchmark_service = request.benchmark_service
        try:
            result = await process_task(
                task_row,
                request,
                benchmark_service,
                benchmark_row.id,
                task_row.task_id,
                AWSRuntime.from_harness_config(harness_config),
                self._test_org,
                sandbox_provider_config=sandbox_provider_config,
                creation_semaphore=asyncio.Semaphore(1),
                authority=authority,
            )
        finally:
            await benchmark_service.close()

        database_session.refresh(task_row)
        evaluation = database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).one()
        assert result == {"task_0": {"score": 1.0}}
        create_sandbox.assert_not_called()
        assert task_row.status == TaskStatus.FINISHED
        assert task_row.eval_resume_state == {"artifact_prefix": "s3://bucket/run", "job_id": "job-1"}
        assert evaluation.instance_id is None

    async def test_process_task_keeps_stopped_eval_resume_task_stopped(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        executor_authority: Any,
    ) -> None:
        request = StartBenchmarkRequest(
            benchmark_name="vcb",
            contract=contract,
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )
        benchmark_row = start_benchmark_request_to_benchmark(request, self._test_starter)
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark_row.id,
            status=TaskStatus.EVALUATING,
            eval_resume_state={"artifact_prefix": "s3://bucket/run"},
        )
        database_session.add(task_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        async def _mock_resume_evaluation(
            _self: BenchmarkServiceClient,
            task_id: str,
            *_args: Any,
            eval_resume_state: dict[str, Any],
            **_kwargs: Any,
        ) -> dict[str, Any]:
            assert task_id == "task_0"
            assert eval_resume_state == {"artifact_prefix": "s3://bucket/run"}
            task = database_session.get(Task, task_row.id)
            assert task is not None
            task.status = TaskStatus.STOPPED
            database_session.add(task)
            database_session.commit()
            raise RuntimeError("evaluation interrupted")

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.buffer_logs", Mock())
        monkeypatch.setattr(BenchmarkServiceClient, "resume_evaluation", _mock_resume_evaluation, raising=False)

        benchmark_service = request.benchmark_service
        try:
            result = await process_task(
                task_row,
                request,
                benchmark_service,
                benchmark_row.id,
                task_row.task_id,
                AWSRuntime.from_harness_config(harness_config),
                self._test_org,
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="key",
                    DAYTONA_API_URL="url",
                    DAYTONA_TARGET="target",
                ),
                creation_semaphore=asyncio.Semaphore(1),
                authority=authority,
            )
        finally:
            await benchmark_service.close()

        database_session.refresh(task_row)
        evaluations = database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).all()
        assert result == {"task_0": None}
        assert task_row.status == TaskStatus.STOPPED
        assert evaluations == []

    async def test_retry_or_resume_blocks_external_persisted_internal_destination(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.custom_benchmark_service = "https://benchmarks-dev.vals.ai"
        database_session.add(benchmark_row)
        database_session.commit()
        monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={"task_ids": [], "service_headers": {}},
        )

        assert response.status_code == 403
        assert response.json() == {"detail": "Custom benchmark destination is not allowed"}

    async def test_retry_or_resume_forwards_tracker_api_key_to_benchmark_service(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_headers: dict[str, str],
        mock_kicker: MockKicker,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.arguments = benchmark_row.arguments.model_copy(
            update={"sandbox_provider": "modal", "sandbox_provider_secret_name": "ModalSecrets"}
        )
        database_session.add(benchmark_row)
        database_session.commit()

        observed_headers: dict[str, str] = {}

        async def _mock_reset_to_in_progress_status(
            *_args: Any, benchmark_service: BenchmarkServiceClient, **_kwargs: Any
        ) -> list[str]:
            observed_headers.update(getattr(benchmark_service, "_headers"))
            return ["task_0"]

        monkeypatch.setattr("main.reset_to_in_progress_status", _mock_reset_to_in_progress_status)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?concurrency=20",
            json={"task_ids": [], "service_headers": {}},
            headers={**harness_headers, "X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"

        admitted_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert admitted_request["concurrency"] == 20
        assert admitted_request["sandbox_provider"] == "modal"
        assert admitted_request["harness_config"]["sandbox_provider_secret_name"] == "ModalSecrets"
        assert admitted_request["service_headers"]["X-Descope-Api-Key"] == "tracker-api-key"
        database_session.refresh(benchmark_row)
        assert benchmark_row.arguments.concurrency == 20

    async def test_retry_or_resume_does_not_forward_tracker_key_to_custom_service(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.custom_benchmark_service = "https://team.example"
        database_session.add(benchmark_row)
        database_session.commit()

        observed_headers: dict[str, str] = {}

        async def _mock_reset_to_in_progress_status(
            *_args: Any,
            benchmark_service: BenchmarkServiceClient,
            **_kwargs: Any,
        ) -> list[str]:
            observed_headers.update(getattr(benchmark_service, "_headers"))
            return ["task_0"]

        monkeypatch.setattr("main.reset_to_in_progress_status", _mock_reset_to_in_progress_status)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={"task_ids": [], "service_headers": {}},
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert "X-Descope-Api-Key" not in observed_headers
        admitted_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert "X-Descope-Api-Key" not in admitted_request["service_headers"]

    async def test_force_stop_uses_stored_provider_secret(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_headers: dict[str, str],
    ) -> None:
        """Force stop should use the provider secret stored with the run.

        Test cases:
        - A modal run is force-stopped with its stored provider and secret.
        - The current harness config secret is not used for the stored run.
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.arguments = benchmark_row.arguments.model_copy(
            update={"sandbox_provider": "modal", "sandbox_provider_secret_name": "ModalSecrets"}
        )
        database_session.add_all(
            [
                benchmark_row,
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="evaluating-task",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.EVALUATING,
                ),
            ]
        )
        database_session.commit()

        captured: dict[str, object] = {}

        async def _mock_force_stop_sandboxes(
            _benchmark_row: Benchmark,
            sandbox_provider_secret_name: str,
            _aws: Any,
            _org: Org,
            *,
            sandbox_provider: str,
            task_ids: list[str] | None = None,
        ) -> None:
            captured["sandbox_provider_secret_name"] = sandbox_provider_secret_name
            captured["sandbox_provider"] = sandbox_provider
            captured["task_ids"] = task_ids

        monkeypatch.setattr("main.force_stop_sandboxes", _mock_force_stop_sandboxes)

        response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=true",
            headers=harness_headers,
        )

        assert response.status_code == 200
        assert captured == {
            "sandbox_provider_secret_name": "ModalSecrets",
            "sandbox_provider": "modal",
            "task_ids": None,
        }

    async def test_force_stop_missing_provider_secret_does_not_mutate_run(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """Reject an invalid managed force stop without changing run state."""
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.aws_managed = True
        benchmark_row.arguments = benchmark_row.arguments.model_copy(update={"sandbox_provider_secret_name": ""})
        pending_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
        )
        database_session.add_all([benchmark_row, pending_task])
        database_session.commit()

        monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(TEST_ORG_ID))
        monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region")
        monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket")
        monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group")
        monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30")
        force_stop = AsyncMock()
        monkeypatch.setattr("main.force_stop_sandboxes", force_stop)

        response = client.post(f"/stop-benchmark/{benchmark_row.id}?force=true")

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "sandbox provider secret name" in detail
        assert "x-harness-sandbox-provider-secret-name" not in detail
        database_session.expire_all()
        stored_benchmark = database_session.get(Benchmark, benchmark_row.id)
        stored_task = database_session.get(Task, pending_task.id)
        assert stored_benchmark is not None
        assert stored_benchmark.status == BenchmarkStatus.IN_PROGRESS
        assert stored_task is not None
        assert stored_task.status == TaskStatus.PENDING
        force_stop.assert_not_awaited()

    async def test_retry_or_resume_applies_secrets_to_stored_contract(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        """Resume secrets should update the contract used by resumed tasks.

        Test cases:
        - Existing env var mappings are replaced by resume overrides.
        - New env var mappings are added to the stored contract before enqueue.
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.arguments.contract.secrets = {
            "ANTHROPIC_API_KEY": "old-secret",
            "OPENAI_API_KEY": "openai-secret",
        }
        database_session.add(benchmark_row)
        database_session.commit()

        async def _mock_reset_to_in_progress_status(*_args: Any, **_kwargs: Any) -> list[str]:
            return ["task_0"]

        monkeypatch.setattr("main.reset_to_in_progress_status", _mock_reset_to_in_progress_status)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={
                "task_ids": [],
                "service_headers": {},
                "secrets": {
                    "ANTHROPIC_API_KEY": "new-secret",
                    "GEMINI_API_KEY": "gemini-secret",
                },
            },
        )

        assert response.status_code == 200

        admitted_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert admitted_request["contract"]["secrets"] == {
            "ANTHROPIC_API_KEY": "new-secret",
            "OPENAI_API_KEY": "openai-secret",
            "GEMINI_API_KEY": "gemini-secret",
        }
        database_session.refresh(benchmark_row)
        assert benchmark_row.arguments.contract.secrets == admitted_request["contract"]["secrets"]

    async def test_retry_or_resume_replaces_benchmark_url(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: MockKicker,
    ) -> None:
        """A retry URL override should be validated, normalized, and persisted.

        Test cases:
        - Invalid overrides fail before task state changes.
        - Task verification uses the normalized replacement benchmark URL.
        - The queued request and stored run retain the replacement URL.
        - An active resume stores URL and secret overrides without queueing duplicate work.
        - An active retry with no retryable tasks still stores all request overrides.
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.custom_benchmark_service = "https://old.example"
        benchmark_row.arguments.contract.secrets = {"EXISTING_API_KEY": "existing-secret"}
        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
        )
        database_session.add_all([benchmark_row, task_row])
        database_session.commit()
        verified_urls: list[str] = []
        verified_headers: list[dict[str, str]] = []
        create_benchmark_service_client = tracker_utils.create_benchmark_service_client

        def _create_benchmark_service_client(
            url: str,
            service_headers: dict[str, str] | None = None,
        ) -> BenchmarkServiceClient:
            verified_urls.append(url)
            return create_benchmark_service_client(url, service_headers)

        async def _verify_task_ids(
            benchmark_service: BenchmarkServiceClient,
            *,
            task_ids: list[str],
            slice_str: str | None,
            dataset: str | None,
        ) -> VerifyTaskIdsResponse:
            verified_headers.append(dict(getattr(benchmark_service, "_headers")))

            return VerifyTaskIdsResponse(task_ids=task_ids)

        monkeypatch.setattr(tracker_utils, "create_benchmark_service_client", _create_benchmark_service_client)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_task_ids)

        invalid_response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={
                "task_ids": [],
                "service_headers": {},
                "benchmark_url": "",
            },
        )

        assert invalid_response.status_code == 400
        assert invalid_response.json() == {"detail": "Invalid benchmark service URL"}
        assert mock_kicker.queued_calls == []
        database_session.refresh(benchmark_row)
        database_session.refresh(task_row)
        assert benchmark_row.custom_benchmark_service == "https://old.example"
        assert task_row.status == TaskStatus.STOPPED

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={
                "task_ids": [],
                "service_headers": {},
                "benchmark_url": "https://new.example/",
            },
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert verified_urls == ["https://new.example"]
        assert "X-Descope-Api-Key" not in verified_headers[0]

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["custom_benchmark_service"] == "https://new.example"
        assert "X-Descope-Api-Key" not in queued_request["service_headers"]

        database_session.refresh(benchmark_row)
        assert benchmark_row.custom_benchmark_service == "https://new.example"

        active_response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={
                "task_ids": [],
                "service_headers": {},
                "secrets": {"ACTIVE_API_KEY": "active-secret"},
                "benchmark_url": "https://active.example/",
            },
        )

        assert active_response.status_code == 200
        assert len(mock_kicker.queued_calls) == 1
        database_session.refresh(benchmark_row)
        assert benchmark_row.custom_benchmark_service == "https://active.example"
        assert benchmark_row.arguments.contract.secrets == {
            "EXISTING_API_KEY": "existing-secret",
            "ACTIVE_API_KEY": "active-secret",
        }

        active_retry_response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true&concurrency=10",
            json={
                "task_ids": [],
                "service_headers": {},
                "secrets": {"ACTIVE_API_KEY": "active-retry-secret"},
                "benchmark_url": "https://active-retry.example/",
            },
        )

        assert active_retry_response.status_code == 200
        assert len(mock_kicker.queued_calls) == 1
        database_session.refresh(benchmark_row)
        assert benchmark_row.custom_benchmark_service == "https://active-retry.example"
        assert benchmark_row.arguments.concurrency == 10
        assert benchmark_row.arguments.contract.secrets == {
            "EXISTING_API_KEY": "existing-secret",
            "ACTIVE_API_KEY": "active-retry-secret",
        }

    async def test_retry_or_resume_secret_merge_preserves_concurrent_concurrency_update(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.arguments.contract.secrets = {"ANTHROPIC_API_KEY": "old-secret"}
        database_session.add(benchmark_row)
        database_session.commit()

        async def _concurrent_reset(*_args: Any, **_kwargs: Any) -> list[str]:
            with Session(bind=database_session.get_bind()) as control_session:
                persisted = control_session.get(Benchmark, benchmark_row.id)
                assert persisted is not None
                persisted.status = BenchmarkStatus.IN_PROGRESS
                control_session.add(persisted)
                control_session.commit()
                update_benchmark_concurrency(benchmark_row.id, 9, control_session, self._test_org)
            return ["task_0"]

        monkeypatch.setattr("main.reset_to_in_progress_status", _concurrent_reset)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={
                "task_ids": [],
                "service_headers": {},
                "secrets": {"ANTHROPIC_API_KEY": "new-secret"},
            },
        )

        assert response.status_code == 200
        admitted_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert admitted_request["concurrency"] == 9
        assert admitted_request["contract"]["secrets"] == {"ANTHROPIC_API_KEY": "new-secret"}

        database_session.expire_all()
        persisted = database_session.get(Benchmark, benchmark_row.id)
        assert persisted is not None
        assert persisted.arguments.concurrency == 9
        assert persisted.arguments.contract.secrets == {"ANTHROPIC_API_KEY": "new-secret"}

    async def test_running_retry_noops_without_error_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.PENDING)
        )
        database_session.commit()

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        assert mock_kicker.queued_calls == []

    async def test_running_resume_noops(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_error", benchmark=benchmark_row.id, status=TaskStatus.ERROR)
        )
        database_session.commit()

        async def _unexpected_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            raise AssertionError("running resume should not verify retry tasks")

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _unexpected_verify_task_ids)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}")

        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        assert mock_kicker.queued_calls == []

        task_row = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).one()
        assert task_row.status == TaskStatus.ERROR

    async def test_running_resume_updates_concurrency_without_enqueuing_work(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?concurrency=8")

        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        assert mock_kicker.queued_calls == []
        database_session.expire_all()
        persisted = database_session.get(Benchmark, benchmark_row.id)
        assert persisted is not None
        assert persisted.arguments.concurrency == 8

    async def test_running_retry_only_resets_error_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add_all(
            [
                Task(org_id=TEST_ORG_ID, task_id="task_error", benchmark=benchmark_row.id, status=TaskStatus.ERROR),
                Task(org_id=TEST_ORG_ID, task_id="task_pending", benchmark=benchmark_row.id, status=TaskStatus.PENDING),
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="task_finished",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.FINISHED,
                ),
            ]
        )
        database_session.commit()
        database_session.add_all(
            [
                ExecutorRelease(
                    id="new-release",
                    artifact_uri="s3://artifacts/new-release.pex",
                    artifact_digest="digest-new-release",
                    protocol_version="1",
                    readiness_verified=True,
                ),
                ExecutorRelease(
                    id="latest-release",
                    artifact_uri="s3://artifacts/latest-release.pex",
                    artifact_digest="digest-latest-release",
                    protocol_version="1",
                    readiness_verified=True,
                ),
            ]
        )
        database_session.commit()
        promote_release(database_session, "new-release")
        database_session.commit()

        async def _mock_request_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            promote_release(database_session, "latest-release")
            database_session.commit()
            return VerifyTaskIdsResponse(task_ids=["task_error"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        admitted_payload = mock_kicker.queued_calls[0]
        assert admitted_payload["verified_task_ids"] == ["task_error"]
        dispatch_id = UUID(admitted_payload["executor_dispatch_id"])
        dispatch = database_session.get(ExecutorDispatch, dispatch_id)
        assert dispatch is not None
        assert dispatch.benchmark_id == benchmark_row.id
        assert dispatch.kind == ExecutorDispatchKind.RETRY
        assert dispatch.status == ExecutorDispatchStatus.QUEUED
        retried_task = database_session.exec(
            select(Task).where(Task.benchmark == benchmark_row.id).where(Task.task_id == "task_error")
        ).one()
        assert retried_task.started_at <= dispatch.created_at
        assert dispatch.executor_release_id == "test-release"
        assert benchmark_row.executor_release_id == "test-release"
        assert benchmark_row.current_execution_release_id == "test-release"
        assert benchmark_row.executor_artifact_digest == "digest-test-release"

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert task_statuses == {
            "task_error": TaskStatus.PENDING,
            "task_pending": TaskStatus.PENDING,
            "task_finished": TaskStatus.FINISHED,
        }

    async def test_running_retry_rejects_missing_current_owner_without_mutation(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.current_execution_release_id = None
        task = Task(org_id=TEST_ORG_ID, task_id="task_error", benchmark=benchmark_row.id, status=TaskStatus.ERROR)
        database_session.add(benchmark_row)
        database_session.add(task)
        database_session.commit()

        async def _verify_error_task(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[task.task_id])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_error_task)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 409
        assert "no current executor release" in response.json()["detail"]
        with Session(bind=database_session.get_bind()) as fresh_session:
            persisted_task = fresh_session.get(Task, task.id)
            assert persisted_task is not None
            assert persisted_task.status == TaskStatus.ERROR
            assert fresh_session.exec(select(ExecutorDispatch)).all() == []

    async def test_release_resolution_failure_rolls_back_retry_mutation(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        task = Task(org_id=TEST_ORG_ID, task_id="task_error", benchmark=benchmark_row.id, status=TaskStatus.ERROR)
        old_evaluation = FinalEvaluation(org_id=TEST_ORG_ID, benchmark=benchmark_row.id, final_score=0.5)
        database_session.add(benchmark_row)
        database_session.add(task)
        database_session.add(old_evaluation)
        database_session.commit()
        old_evaluation_id = old_evaluation.id

        async def _verify_error_task(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[task.task_id])

        def _fail_release_resolution(*_args: Any, **_kwargs: Any) -> ExecutorRelease:
            raise ReleaseControlError("current release unavailable")

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_error_task)
        monkeypatch.setattr(
            "tracker.executor.dispatch_control.resolve_current_execution_release",
            _fail_release_resolution,
        )

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 409
        with Session(bind=database_session.get_bind()) as fresh_session:
            persisted_benchmark = fresh_session.get(Benchmark, benchmark_row.id)
            persisted_task = fresh_session.get(Task, task.id)
            assert persisted_benchmark is not None
            assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
            assert persisted_benchmark.current_execution_release_id == "test-release"
            assert persisted_task is not None
            assert persisted_task.status == TaskStatus.ERROR
            assert fresh_session.get(FinalEvaluation, old_evaluation_id) is not None
            assert persisted_benchmark.final_evaluation is not None
            assert persisted_benchmark.final_evaluation.id == old_evaluation_id
            assert fresh_session.exec(select(ExecutorDispatch)).all() == []

    @pytest.mark.parametrize(
        ("retry", "task_status", "dispatch_kind"),
        [
            (False, TaskStatus.STOPPED, ExecutorDispatchKind.RESUME),
            (True, TaskStatus.ERROR, ExecutorDispatchKind.RETRY),
        ],
    )
    async def test_recovery_removes_prior_final_evaluation_before_admitting_dispatch(
        self,
        retry: bool,
        task_status: TaskStatus,
        dispatch_kind: ExecutorDispatchKind,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        task = Task(org_id=TEST_ORG_ID, task_id="recoverable-task", benchmark=benchmark_row.id, status=task_status)
        old_evaluation = FinalEvaluation(org_id=TEST_ORG_ID, benchmark=benchmark_row.id, final_score=0.5)
        database_session.add(benchmark_row)
        database_session.add(task)
        database_session.add(old_evaluation)
        database_session.commit()
        old_evaluation_id = old_evaluation.id

        async def _verify_recoverable_task(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[task.task_id])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_recoverable_task)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true"
            if retry
            else f"/retry-or-resume-benchmark/{benchmark_row.id}"
        )

        assert response.status_code == 200

        dispatch_id = UUID(mock_kicker.queued_calls[0]["executor_dispatch_id"])

        with Session(bind=database_session.get_bind()) as fresh_session:
            persisted_benchmark = fresh_session.get(Benchmark, benchmark_row.id)
            dispatch = fresh_session.get(ExecutorDispatch, dispatch_id)

            assert persisted_benchmark is not None
            assert persisted_benchmark.final_evaluation is None
            assert fresh_session.get(FinalEvaluation, old_evaluation_id) is None
            assert dispatch is not None
            assert dispatch.status == ExecutorDispatchStatus.QUEUED
            assert dispatch.kind == dispatch_kind

    async def test_terminal_resume_without_active_release_returns_503_without_mutation(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_stopped",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
            started_at=_ORIGINAL_ATTEMPT_AT,
            finished_at=_RESUMED_ATTEMPT_AT,
        )
        database_session.add(benchmark_row)
        database_session.add(task)
        database_session.commit()
        release = database_session.get(ExecutorRelease, "test-release")
        assert release is not None
        release.status = ExecutorReleaseStatus.DRAINING
        database_session.add(release)
        database_session.commit()

        async def _verify_stopped_task(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[task.task_id])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_stopped_task)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}")

        assert response.status_code == 503
        assert response.json()["detail"] == "No active executor release is configured"
        with Session(bind=database_session.get_bind()) as fresh_session:
            persisted_benchmark = fresh_session.get(Benchmark, benchmark_row.id)
            persisted_task = fresh_session.get(Task, task.id)
            assert persisted_benchmark is not None
            assert persisted_benchmark.status == BenchmarkStatus.STOPPED
            assert persisted_benchmark.current_execution_release_id == "test-release"
            assert persisted_task is not None
            assert persisted_task.status == TaskStatus.STOPPED
            assert persisted_task.started_at == _ORIGINAL_ATTEMPT_AT
            assert persisted_task.finished_at == _RESUMED_ATTEMPT_AT
            assert fresh_session.exec(select(ExecutorDispatch)).all() == []

    async def test_terminal_resume_hands_current_execution_release_to_active(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_stopped", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
        )
        release = ExecutorRelease(
            id="recovery-release",
            artifact_uri="s3://artifacts/recovery-release.pex",
            artifact_digest="digest-recovery-release",
            protocol_version="1",
            readiness_verified=True,
        )
        database_session.add(release)
        database_session.commit()
        promote_release(database_session, release.id)
        database_session.commit()

        async def _verify_stopped_task(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_stopped"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_stopped_task)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}")

        assert response.status_code == 200
        database_session.refresh(benchmark_row)
        assert benchmark_row.executor_release_id == "test-release"
        assert benchmark_row.current_execution_release_id == release.id
        first_payload = mock_kicker.queued_calls[0]
        dispatch = database_session.get(ExecutorDispatch, UUID(first_payload["executor_dispatch_id"]))
        assert dispatch is not None
        assert dispatch.kind == ExecutorDispatchKind.RESUME
        assert dispatch.executor_release_id == release.id

        latest_release = ExecutorRelease(
            id="latest-release",
            artifact_uri="s3://artifacts/latest-release.pex",
            artifact_digest="digest-latest-release",
            protocol_version="1",
            readiness_verified=True,
        )
        database_session.add(latest_release)
        database_session.commit()
        promote_release(database_session, latest_release.id)
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        task = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).one()
        task.status = TaskStatus.ERROR
        database_session.add(benchmark_row)
        database_session.add(task)
        database_session.commit()

        retry_response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert retry_response.status_code == 200
        database_session.refresh(benchmark_row)
        assert benchmark_row.current_execution_release_id == release.id
        second_dispatch = database_session.get(
            ExecutorDispatch,
            UUID(mock_kicker.queued_calls[1]["executor_dispatch_id"]),
        )
        assert second_dispatch is not None
        assert second_dispatch.executor_release_id == release.id

        benchmark_row.status = BenchmarkStatus.STOPPED
        task.status = TaskStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.add(task)
        database_session.commit()

        second_resume = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}")

        assert second_resume.status_code == 200
        database_session.refresh(benchmark_row)
        assert benchmark_row.executor_release_id == "test-release"
        assert benchmark_row.current_execution_release_id == latest_release.id
        dispatches = [
            database_session.get(ExecutorDispatch, UUID(payload["executor_dispatch_id"]))
            for payload in mock_kicker.queued_calls
        ]
        assert all(dispatch is not None for dispatch in dispatches)
        assert [dispatch.executor_release_id for dispatch in dispatches if dispatch is not None] == [
            release.id,
            release.id,
            latest_release.id,
        ]

    async def test_retry_succeeds_after_durable_intent_without_broker_access(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_error", benchmark=benchmark_row.id, status=TaskStatus.ERROR)
        )
        database_session.add(
            ExecutorRelease(
                id="new-release",
                artifact_uri="s3://artifacts/new-release.pex",
                artifact_digest="digest-new-release",
                protocol_version="1",
                readiness_verified=True,
            )
        )
        database_session.commit()
        promote_release(database_session, "new-release")
        database_session.commit()

        async def _verify_error_task(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_error"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_error_task)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        dispatches = database_session.exec(select(ExecutorDispatch)).all()
        assert len(dispatches) == 1
        assert dispatches[0].kind == ExecutorDispatchKind.RETRY
        assert dispatches[0].status == ExecutorDispatchStatus.QUEUED
        assert dispatches[0].executor_release_id == "test-release"
        assert mock_kicker.queued_calls[0]["verified_task_ids"] == ["task_error"]

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_running_retry_repairs_error_and_later_finalizes_same_run(
        self,
        contract: AgentContractRequest,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        mock_kicker: Any,
        executor_authority_kwargs: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.arguments.task_ids = ["task_retry", "task_original"]
        database_session.add(benchmark_row)
        database_session.add_all(
            [
                Task(org_id=TEST_ORG_ID, task_id="task_retry", benchmark=benchmark_row.id, status=TaskStatus.ERROR),
                Task(
                    org_id=TEST_ORG_ID, task_id="task_original", benchmark=benchmark_row.id, status=TaskStatus.PENDING
                ),
            ]
        )
        database_session.commit()
        database_session.add(
            ExecutorRelease(
                id="new-release",
                artifact_uri="s3://artifacts/new-release.pex",
                artifact_digest="digest-new-release",
                protocol_version="1",
                readiness_verified=True,
            )
        )
        database_session.commit()
        promote_release(database_session, "new-release")
        database_session.commit()

        async def _mock_verify_task_ids(*_args: Any, task_ids: list[str], **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=task_ids)

        final_score_inputs: list[dict[str, Any]] = []
        sandbox_count = [0]

        def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AbstractAsyncContextManager[AsyncMock]:
            return _counted_sandbox(sandbox_count)

        async def _mock_final_score(
            *_args: Any, evaluation_results: dict[str, Any], **_kwargs: Any
        ) -> FinalScoreResponse:
            final_score_inputs.append(evaluation_results)
            return FinalScoreResponse(
                tasks_evaluated=list(evaluation_results),
                final_score=1.0,
                metadata={"tasks_evaluated": list(evaluation_results)},
            )

        request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=2,
            task_ids=["task_retry", "task_original"],
            harness_config=harness_config,
        )

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)

        original_authority_kwargs = executor_authority_kwargs(benchmark_row)
        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200

        queued_task_ids = mock_kicker.queued_calls[0]["verified_task_ids"]
        assert queued_task_ids == ["task_retry"]
        database_session.refresh(benchmark_row)
        retry_dispatch = database_session.exec(
            select(ExecutorDispatch)
            .where(ExecutorDispatch.benchmark_id == benchmark_row.id)
            .where(ExecutorDispatch.kind == ExecutorDispatchKind.RETRY)
        ).one()
        authority_kwargs = executor_authority_kwargs(benchmark_row, retry_dispatch.id)

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=queued_task_ids,
            **authority_kwargs,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert benchmark_row.final_evaluation is None
        assert final_score_inputs == []

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=["task_original"],
            **original_authority_kwargs,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message
        assert final_score_inputs
        assert set(final_score_inputs[-1]) == {"task_retry", "task_original"}
        assert benchmark_row.final_evaluation is not None

    async def test_running_retry_enqueue_failure_keeps_original_execution_active(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        retry_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_retry",
            benchmark=benchmark_row.id,
            status=TaskStatus.ERROR,
        )
        original_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_original",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
        )
        database_session.add(benchmark_row)
        database_session.add(retry_task)
        database_session.add(original_task)
        database_session.commit()
        executor_authority_kwargs(benchmark_row)

        async def _mock_verify_task_ids(*_args: Any, task_ids: list[str], **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=task_ids)

        class FailingKicker:
            def with_labels(self, **_labels: str) -> "FailingKicker":
                return self

            async def kiq(self, **_kwargs: Any) -> None:
                raise RuntimeError("redis unavailable")

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)
        monkeypatch.setattr("main.process_benchmark.kicker", lambda: FailingKicker())

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 503
        database_session.refresh(benchmark_row)
        database_session.refresh(retry_task)
        database_session.refresh(original_task)
        dispatches = database_session.exec(
            select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_row.id)
        ).all()
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert retry_task.status == TaskStatus.ERROR
        assert original_task.status == TaskStatus.PENDING
        assert {dispatch.status for dispatch in dispatches} == {
            ExecutorDispatchStatus.FAILED,
            ExecutorDispatchStatus.RUNNING,
        }

    async def test_task_monitor_cancels_waiting_stopped_task(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        executor_authority: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
        database_session.add(task_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)

        tracked_task = TrackedTask(asyncio.sleep(0), org=self._test_org, authority=authority)
        setattr(tracked_task, "_status", TrackedTaskStatus.WAITING)

        cancel_mock = Mock()

        def _cancel(*_args: Any, **_kwargs: Any) -> None:
            setattr(tracked_task, "_status", TrackedTaskStatus.DONE)

        cancel_mock.side_effect = _cancel
        setattr(tracked_task, "_task", Mock(cancel=cancel_mock, done=lambda: False))

        monitor = TaskMonitor(
            benchmark_row.id,
            {task_row.task_id: tracked_task},
            org=self._test_org,
            limiter=ResizableLimiter(limit=1),
            authority=authority,
        )
        setattr(monitor, "_TRACK_INTERVAL", 0)

        await monitor.track_tasks()

        cancel_mock.assert_called_once()
        assert getattr(monitor, "_task_tracking") == {}
        getattr(tracked_task, "_coro").close()

    async def test_task_monitor_cancels_task_after_whole_run_recovery_revokes_dispatch(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()
        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
        database_session.add(task_row)
        database_session.commit()

        authority_kwargs = executor_authority_kwargs(benchmark_row)
        dispatch_id = UUID(str(authority_kwargs["executor_dispatch_id"]))
        authority = ExecutionAuthority(
            benchmark_id=benchmark_row.id,
            dispatch_id=dispatch_id,
        )
        dispatch = database_session.get(ExecutorDispatch, dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = datetime.now(UTC)
        database_session.add(dispatch)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        tracked_task = TrackedTask(asyncio.sleep(0), org=self._test_org, authority=authority)
        setattr(tracked_task, "_status", TrackedTaskStatus.WAITING)
        cancel_mock = Mock()

        def _cancel(*_args: Any, **_kwargs: Any) -> None:
            setattr(tracked_task, "_status", TrackedTaskStatus.DONE)

        cancel_mock.side_effect = _cancel
        setattr(tracked_task, "_task", Mock(cancel=cancel_mock, done=lambda: False))
        monitor = TaskMonitor(
            benchmark_row.id,
            {task_row.task_id: tracked_task},
            org=self._test_org,
            limiter=ResizableLimiter(limit=1),
            authority=authority,
        )
        setattr(monitor, "_TRACK_INTERVAL", 0)

        await monitor.track_tasks()

        cancel_mock.assert_called_once()
        getattr(tracked_task, "_coro").close()

    async def test_graceful_whole_run_stop_preserves_running_dispatch_until_finalization(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        pending_task = Task(
            org_id=TEST_ORG_ID,
            task_id="pending-task",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
        )
        running_task = Task(
            org_id=TEST_ORG_ID,
            task_id="running-task",
            benchmark=benchmark_row.id,
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add_all([pending_task, running_task])
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        await initiate_stop_benchmark(benchmark_row, database_session, force=False, org=self._test_org)

        database_session.refresh(benchmark_row)
        database_session.refresh(pending_task)
        database_session.refresh(running_task)
        dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
        assert benchmark_row.status == BenchmarkStatus.STOPPING
        assert pending_task.status == TaskStatus.STOPPED
        assert running_task.status == TaskStatus.IN_PROGRESS
        assert dispatch is not None
        assert dispatch.status == ExecutorDispatchStatus.RUNNING
        assert lock_execution_authority(database_session, authority).id == benchmark_row.id
        database_session.rollback()

    async def test_force_stop_finalizes_database_immediately(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
    ) -> None:
        """Force stop finalizes Valkyrie's run state without waiting for provider teardown."""
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add_all(
            [
                Task(org_id=TEST_ORG_ID, task_id="pending", benchmark=benchmark_row.id, status=TaskStatus.PENDING),
                Task(
                    org_id=TEST_ORG_ID,
                    task_id="running",
                    benchmark=benchmark_row.id,
                    status=TaskStatus.IN_PROGRESS,
                ),
                Task(org_id=TEST_ORG_ID, task_id="finished", benchmark=benchmark_row.id, status=TaskStatus.FINISHED),
            ]
        )
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        await initiate_stop_benchmark(benchmark_row, database_session, force=True, org=self._test_org)

        database_session.refresh(benchmark_row)
        dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
        statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert benchmark_row.status == BenchmarkStatus.STOPPED
        assert statuses == {
            "pending": TaskStatus.STOPPED,
            "running": TaskStatus.STOPPED,
            "finished": TaskStatus.FINISHED,
        }
        assert dispatch is not None
        assert dispatch.status == ExecutorDispatchStatus.FAILED

    async def test_force_stop_sends_provider_signal_without_waiting(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """Provider signaling deletes immediately and does not own the database transition."""
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()
        provider = MockReleasingSandboxProvider(_NEVER_RELEASED)
        monkeypatch.setattr(
            run_control_module,
            "fetch_sandbox_provider_config",
            lambda *_args, **_kwargs: DaytonaProviderConfig(
                DAYTONA_API_KEY="key", DAYTONA_API_URL="url", DAYTONA_TARGET="target"
            ),
        )
        monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", lambda *_args, **_kwargs: provider)

        await force_stop_sandboxes(
            benchmark_row,
            harness_config.sandbox_provider_secret_name,
            AWSRuntime.from_harness_config(harness_config),
            self._test_org,
        )

        assert provider.list_calls == 1
        assert provider.deleted_sandbox_ids == [MockReleasingSandboxProvider.sandbox_id]
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

    async def test_force_stop_provider_failure_does_not_change_database_result(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """A provider failure is logged after the database force stop has already committed."""
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()
        provider = MockReleasingSandboxProvider(_NEVER_RELEASED)
        monkeypatch.setattr(
            run_control_module,
            "fetch_sandbox_provider_config",
            lambda *_args, **_kwargs: DaytonaProviderConfig(
                DAYTONA_API_KEY="key", DAYTONA_API_URL="url", DAYTONA_TARGET="target"
            ),
        )
        monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", lambda *_args, **_kwargs: provider)
        monkeypatch.setattr(run_control_module, "delete_sandbox", AsyncMock(side_effect=RuntimeError("unavailable")))

        await force_stop_sandboxes(
            benchmark_row,
            harness_config.sandbox_provider_secret_name,
            AWSRuntime.from_harness_config(harness_config),
            self._test_org,
        )

        assert provider.list_calls == 1
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

    async def test_stop_sandbox_audits_forced_stop_deletion(self, monkeypatch: MonkeyPatch) -> None:
        """Forced stop identifies itself and its org on every sandbox deletion."""
        delete_mock = AsyncMock()
        monkeypatch.setattr("tracker.utils.run_control.delete_sandbox", delete_mock)

        sandbox = Mock()
        provider = Mock()

        result = await stop_sandbox(sandbox, provider, self._test_org)

        assert result is None
        delete_mock.assert_awaited_once_with(sandbox, provider, initiated_by="force_stop", org_id=str(TEST_ORG_ID))
