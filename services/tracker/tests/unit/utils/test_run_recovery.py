"""Tests for stopping, resuming, and retrying benchmark runs.

Run: uv run pytest tests/unit/utils/test_run_recovery.py

Covers task state transitions, sandbox cleanup, and run-control API behavior.
"""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
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

from main import app
from tests.factories import make_error_result, make_evaluation_result
from tests.unit.utils.task_execution_support import MockKicker, make_retrieve_task_response
from tests.utils import TEST_ORG_ID, async_iterator
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import (
    TaskMonitor,
    TrackedTask,
    TrackedTaskStatus,
    force_stop_sandboxes,
    handle_early_exit,
    initiate_stop_benchmark,
    process_benchmark,
    process_task,
    reset_to_in_progress_status,
    start_benchmark_request_to_benchmark,
)

UTC = ZoneInfo("UTC")
_ORIGINAL_ATTEMPT_AT = datetime(2026, 7, 8)
_RESUMED_ATTEMPT_AT = datetime(2026, 7, 9)
client = TestClient(app)


def _created_at(day: int) -> datetime:
    return datetime(2026, 6, day, tzinfo=UTC)


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


class TestRunRecovery:
    """Benchmark stop, retry, resume, and stale-worker recovery."""

    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

    async def test_stop_selected_tasks_scopes_graceful_and_force(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
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
        )

        force_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=true",
            json={"task_ids": ["task_force_selected"]},
        )

        empty_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}",
            json={"task_ids": []},
        )

        missing_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}",
            json={"task_ids": ["task_not_in_run"]},
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
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_stale_worker_cannot_continue_after_force_stop_resume(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
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
        selected_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_selected",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
            started_at=_ORIGINAL_ATTEMPT_AT,
        )
        database_session.add(benchmark_row)
        database_session.add(selected_task)
        database_session.commit()
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
                harness_config,
                self._test_org,
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="key",
                    DAYTONA_API_URL="url",
                    DAYTONA_TARGET="target",
                ),
                creation_semaphore=asyncio.Semaphore(1),
            )
        )

        await asyncio.wait_for(retrieval_started.wait(), timeout=2)
        stop_response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=true",
            json={"task_ids": [selected_task.task_id]},
        )
        resume_response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={"task_ids": [selected_task.task_id]},
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
            handle_early_exit(stale_task, stale_session)

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

        monkeypatch.setattr("tracker.utils.run_orchestration.lambda_client", Mock(return_value=object()))
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
        # Only 3 tasks should be verified for resume (the 3 tasks that are stopped)
        assert len(verified_task_ids) == 3
        assert set(verified_task_ids) == set(pending_task_ids)

        # Run process_benchmark to complete the remaining tasks (the 3 tasks that are pending)
        await process_benchmark(
            start_benchmark_request_json=benchmark_row.start_benchmark_request(harness_config).model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verified_task_ids,
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

        database_session.refresh(task_row)
        assert verified_task_ids == [task_row.task_id]
        assert task_row.status == expected_status
        assert task_row.eval_resume_state == expected_state

    async def test_retry_preserves_previous_task_history_for_export(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
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

        response = client.get("/retrieve-results", params={"benchmark_id": str(benchmark_row.id)})

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
                final_score=float(len(tasks_evaluated)),
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
        assert verified_task_ids == [new_task_id]

        stale_final_evaluation = database_session.exec(
            select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark_row.id)
        ).first()
        assert stale_final_evaluation is None

        # Run the worker — the new task should make it through evaluation
        await process_benchmark(
            start_benchmark_request_json=benchmark_row.start_benchmark_request(harness_config).model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verified_task_ids,
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
                harness_config,
                self._test_org,
                sandbox_provider_config=sandbox_provider_config,
                creation_semaphore=asyncio.Semaphore(1),
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
                harness_config,
                self._test_org,
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="key",
                    DAYTONA_API_URL="url",
                    DAYTONA_TARGET="target",
                ),
                creation_semaphore=asyncio.Semaphore(1),
            )
        finally:
            await benchmark_service.close()

        database_session.refresh(task_row)
        evaluations = database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).all()
        assert result == {"task_0": None}
        assert task_row.status == TaskStatus.STOPPED
        assert evaluations == []

    async def test_retry_or_resume_forwards_tracker_api_key_to_benchmark_service(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
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
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["concurrency"] == 20
        assert queued_request["sandbox_provider"] == "modal"
        assert queued_request["harness_config"]["sandbox_provider_secret_name"] == "ModalSecrets"
        assert queued_request["service_headers"]["X-Descope-Api-Key"] == "tracker-api-key"
        database_session.refresh(benchmark_row)
        assert benchmark_row.arguments.concurrency == 20

    async def test_force_stop_uses_stored_provider_secret(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
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
        database_session.add(benchmark_row)
        database_session.commit()

        captured: dict[str, object] = {}

        async def _mock_force_stop_sandboxes(
            _benchmark_row: Benchmark,
            _session: Session,
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

        response = client.post(f"/stop-benchmark/{benchmark_row.id}?force=true")

        assert response.status_code == 200
        assert captured == {
            "sandbox_provider_secret_name": "ModalSecrets",
            "sandbox_provider": "modal",
            "task_ids": None,
        }

    async def test_retry_or_resume_applies_secrets_to_stored_contract(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: MockKicker,
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

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["contract"]["secrets"] == {
            "ANTHROPIC_API_KEY": "new-secret",
            "OPENAI_API_KEY": "openai-secret",
            "GEMINI_API_KEY": "gemini-secret",
        }
        database_session.refresh(benchmark_row)
        assert benchmark_row.arguments.contract.secrets == queued_request["contract"]["secrets"]

    async def test_running_retry_noops_without_error_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.PENDING)
        )
        database_session.commit()

        def _unexpected_kicker() -> None:
            raise AssertionError("running retry without error tasks should not enqueue work")

        monkeypatch.setattr("main.process_benchmark.kicker", _unexpected_kicker)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        assert response.json() == {"status": "success"}

    async def test_running_resume_noops(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
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

        def _unexpected_kicker() -> None:
            raise AssertionError("running resume should not enqueue work")

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _unexpected_verify_task_ids)
        monkeypatch.setattr("main.process_benchmark.kicker", _unexpected_kicker)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}")

        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        task_row = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).one()
        assert task_row.status == TaskStatus.ERROR

    async def test_running_retry_only_resets_error_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        mock_kicker: MockKicker,
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

        async def _mock_request_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_error"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        assert mock_kicker.queued_calls[0]["verified_task_ids"] == ["task_error"]

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert task_statuses == {
            "task_error": TaskStatus.PENDING,
            "task_pending": TaskStatus.PENDING,
            "task_finished": TaskStatus.FINISHED,
        }

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_running_retry_repairs_error_and_later_finalizes_same_run(
        self,
        contract: AgentContractRequest,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        mock_kicker: MockKicker,
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

        async def _mock_verify_task_ids(*_args: Any, task_ids: list[str], **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=task_ids)

        final_score_inputs: list[dict[str, Any]] = []
        sandbox_count = 0

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
            nonlocal sandbox_count
            sandbox_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-id-{sandbox_count}"
            yield mock_sandbox

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

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200

        queued_task_ids = mock_kicker.queued_calls[0]["verified_task_ids"]
        assert queued_task_ids == ["task_retry"]

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=queued_task_ids,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert benchmark_row.final_evaluation is None
        assert final_score_inputs == []

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=["task_original"],
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message
        assert final_score_inputs
        assert set(final_score_inputs[-1]) == {"task_retry", "task_original"}
        assert benchmark_row.final_evaluation is not None

    async def test_task_monitor_cancels_waiting_stopped_task(
        self, example_benchmark_object: Benchmark, database_session: Session, monkeypatch: MonkeyPatch
    ) -> None:
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
        database_session.add(task_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)

        tracked_task = TrackedTask(asyncio.sleep(0), org=self._test_org)
        setattr(tracked_task, "_status", TrackedTaskStatus.WAITING)

        cancel_mock = Mock()

        def _cancel(*_args: Any, **_kwargs: Any) -> None:
            setattr(tracked_task, "_status", TrackedTaskStatus.DONE)

        cancel_mock.side_effect = _cancel
        setattr(tracked_task, "_task", Mock(cancel=cancel_mock, done=lambda: False))

        monitor = TaskMonitor(benchmark_row.id, {task_row.task_id: tracked_task}, org=self._test_org)
        setattr(monitor, "_TRACK_INTERVAL", 0)

        await monitor.track_tasks()

        cancel_mock.assert_called_once()
        assert getattr(monitor, "_task_tracking") == {}
        getattr(tracked_task, "_coro").close()

    async def test_force_stop_edge_case(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """Reproduces the bug where force stop is called after the worker has already
        finished processing all tasks. The benchmark gets set to STOPPING but nothing
        transitions it to STOPPED because the worker's process_benchmark has already exited.

        Test Case:
        - All tasks are in a finished state
        - Benchmark status is set to stopping
        - Force stopping results in the benchmark status being STOPPED
        """
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        # All tasks are already in a finished state
        tasks = [
            Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.FINISHED),
            Task(org_id=TEST_ORG_ID, task_id="task_1", benchmark=benchmark_row.id, status=TaskStatus.FINISHED),
            Task(org_id=TEST_ORG_ID, task_id="task_2", benchmark=benchmark_row.id, status=TaskStatus.ERROR),
            Task(org_id=TEST_ORG_ID, task_id="task_3", benchmark=benchmark_row.id, status=TaskStatus.FINISHED),
        ]
        database_session.add_all(tasks)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)

        # Set benchmark status to STOPPING
        await initiate_stop_benchmark(benchmark_row, database_session, force=True, org=self._test_org)
        assert benchmark_row.status == BenchmarkStatus.STOPPING

        def _empty_list_sandboxes(*_args: Any, **_kwargs: Any) -> AsyncIterator[None]:
            return async_iterator(())

        mock_provider = Mock()
        mock_provider.list_sandboxes = _empty_list_sandboxes

        def _provider_config(*_args: Any, **_kwargs: Any) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="key",
                DAYTONA_API_URL="url",
                DAYTONA_TARGET="target",
            )

        def _sandbox_provider(*_args: Any, **_kwargs: Any) -> Mock:
            return mock_provider

        monkeypatch.setattr(
            "tracker.utils.resources.fetch_sandbox_provider_config",
            _provider_config,
        )
        monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", _sandbox_provider)

        # Force stopping the sandboxes results in the benchmark row being stopped
        await force_stop_sandboxes(
            benchmark_row,
            database_session,
            harness_config.sandbox_provider_secret_name,
            harness_config.aws,
            self._test_org,
            sandbox_provider="daytona",
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.STOPPED
