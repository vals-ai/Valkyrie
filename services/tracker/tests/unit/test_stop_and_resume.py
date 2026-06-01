import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
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
    initiate_stop_benchmark,
    process_benchmark,
    process_task,
    reset_to_in_progress_status,
    start_benchmark_request_to_benchmark,
)

client = TestClient(app)


class TestStopAndResume:
    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

    async def test_stop_and_resume(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        harness_config: HarnessConfig,
    ):
        """
        Tests stop and resume when some tasks have already completed.

        Test Cases:
            - Start benchmark with 5 tasks
            - 2 tasks are completed (finished), 3 are still pending
            - Stop benchmark - 3 tasks are stopped (pending -> stopped)
            - Resume benchmark - only the 3 tasks that are stopped should be resumed
            - Retry or resume benchmark - all 5 tasks should have evaluation results after completion
        """
        task_ids: list[str] = [
            "astropy__astropy-12907",
            "astropy__astropy-13033",
            "django__django-11066",
            "django__django-12325",
            "django__django-12858",
        ]

        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=2,
            task_ids=task_ids,
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

    async def test_resume_changes_task_alias_per_attempt(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ):
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
            for i in range(2)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        original_aliases = {task_row.task_id: task_row.alias for task_row in task_rows}

        async def _mock_request_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[task_row.task_id for task_row in task_rows])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)

        # resets the start time of the task, so the alias will be different the next time we run the task
        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=benchmark_row.benchmark_service(harness_config.daytona_secret_name, harness_config.aws),
            retry=True,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=[],
            org=self._test_org,
        )

        assert set(verified_task_ids) == set(original_aliases.keys())

        updated_task_rows = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        for task_row in updated_task_rows:
            assert task_row.alias != original_aliases[task_row.task_id]

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
        harness_config: HarnessConfig,
    ):
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
            benchmark_service=benchmark_row.benchmark_service(harness_config.daytona_secret_name, harness_config.aws),
            retry=False,
            retry_mode=retry_mode,
            rerun_task_ids=[],
            org=self._test_org,
        )

        database_session.refresh(task_row)
        assert verified_task_ids == [task_row.task_id]
        assert task_row.status == expected_status
        assert task_row.eval_resume_state == expected_state

    async def test_reset_lazily_creates_rows_for_unregistered_task_ids(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        process_benchmark_env: None,
        harness_config: HarnessConfig,
    ):
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
            benchmark_service=benchmark_row.benchmark_service(harness_config.daytona_secret_name, harness_config.aws),
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

    async def test_resume_runs_lazily_added_task_and_recomputes_final_score(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        process_benchmark_env: None,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ):
        """A FINISHED run + new --task-ids: the new task runs to completion and the final
        score is recomputed over the merged set (existing + new)."""
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
    ):
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

        @asynccontextmanager
        async def _unexpected_create_sandbox(*_args: Any, **_kwargs: Any):
            raise AssertionError("eval resume should not create a sandbox")
            yield

        async def _mock_resume_evaluation(
            _self: BenchmarkServiceClient,
            task_id: str,
            *_args: Any,
            eval_resume_state: dict[str, Any],
            on_eval_resume_state: Any,
            **_kwargs: Any,
        ) -> dict[str, Any]:
            assert task_id == "task_0"
            assert eval_resume_state == {"artifact_prefix": "s3://bucket/run"}
            on_eval_resume_state({"artifact_prefix": "s3://bucket/run", "job_id": "job-1"})
            return {"score": 1.0}

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.buffer_logs", Mock())
        monkeypatch.setattr("tracker.utils.create_sandbox", _unexpected_create_sandbox)
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
                creation_semaphore=asyncio.Semaphore(1),
            )
        finally:
            await benchmark_service.close()

        database_session.refresh(task_row)
        evaluation = database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).one()
        assert result == {"task_0": {"score": 1.0}}
        assert task_row.status == TaskStatus.FINISHED
        assert task_row.eval_resume_state == {"artifact_prefix": "s3://bucket/run", "job_id": "job-1"}
        assert evaluation.instance_id is None

    async def test_process_task_keeps_stopped_eval_resume_task_stopped(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ):
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

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.buffer_logs", Mock())
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
        harness_config: HarnessConfig,
    ):
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        observed_headers: dict[str, str] = {}
        captured_request_json: dict[str, Any] = {}

        async def _mock_reset_to_in_progress_status(
            *_args: Any, benchmark_service: BenchmarkServiceClient, **_kwargs: Any
        ):
            observed_headers.update(benchmark_service._headers)
            return ["task_0"]

        class _MockKicker:
            def with_labels(self, **_kwargs: Any) -> "_MockKicker":
                return self

            async def kiq(self, **kwargs: Any) -> None:
                captured_request_json.update(kwargs["start_benchmark_request_json"])

        monkeypatch.setattr("main.reset_to_in_progress_status", _mock_reset_to_in_progress_status)
        monkeypatch.setattr("main.process_benchmark.kicker", lambda: _MockKicker())

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}",
            json={"task_ids": [], "service_headers": {}},
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"
        assert captured_request_json["service_headers"]["X-Descope-Api-Key"] == "tracker-api-key"

    async def test_running_retry_noops_without_error_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ):
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
    ):
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
    ):
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

        captured_task_ids: list[str] = []

        class _MockKicker:
            def with_labels(self, **_kwargs: Any) -> "_MockKicker":
                return self

            async def kiq(self, **kwargs: Any) -> None:
                captured_task_ids.extend(kwargs["verified_task_ids"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)
        monkeypatch.setattr("main.process_benchmark.kicker", lambda: _MockKicker())

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        assert captured_task_ids == ["task_error"]

        task_statuses = {
            task.task_id: task.status
            for task in database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        }
        assert task_statuses == {
            "task_error": TaskStatus.PENDING,
            "task_pending": TaskStatus.PENDING,
            "task_finished": TaskStatus.FINISHED,
        }

    async def test_running_retry_repairs_error_and_later_finalizes_same_run(
        self,
        contract: AgentContractRequest,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        process_benchmark_env: None,
        harness_config: HarnessConfig,
    ):
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

        captured_retry_task_ids: list[str] = []
        final_score_inputs: list[dict[str, Any]] = []
        sandbox_count = 0

        class _MockKicker:
            def with_labels(self, **_kwargs: Any) -> "_MockKicker":
                return self

            async def kiq(self, **kwargs: Any) -> None:
                captured_retry_task_ids.extend(kwargs["verified_task_ids"])

        @asynccontextmanager
        async def _mock_create_sandbox(*_args: Any, **_kwargs: Any):
            nonlocal sandbox_count
            sandbox_count += 1
            mock_sandbox = AsyncMock()
            mock_sandbox.id = f"mock-sandbox-id-{sandbox_count}"
            yield mock_sandbox

        async def _mock_final_score(*_args: Any, evaluation_results: dict[str, Any], **_kwargs: Any):
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

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
        monkeypatch.setattr("main.process_benchmark.kicker", lambda: _MockKicker())

        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")

        assert response.status_code == 200
        assert captured_retry_task_ids == ["task_retry"]

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=captured_retry_task_ids,
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
    ):
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
        database_session.add(task_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        tracked_task = TrackedTask(asyncio.sleep(0), org=self._test_org)
        tracked_task._status = TrackedTaskStatus.WAITING  # type: ignore[attr-defined]

        cancel_mock = Mock()

        def _cancel(*_args: Any, **_kwargs: Any) -> None:
            tracked_task._status = TrackedTaskStatus.DONE  # type: ignore[attr-defined]

        cancel_mock.side_effect = _cancel
        tracked_task._task = Mock(cancel=cancel_mock, done=lambda: False)  # type: ignore[assignment]

        monitor = TaskMonitor(benchmark_row.id, {task_row.task_id: tracked_task}, org=self._test_org)
        monitor._TRACK_INTERVAL = 0

        await monitor.track_tasks()

        cancel_mock.assert_called_once()
        assert monitor._task_tracking == {}
        tracked_task._coro.close()

    async def test_force_stop_edge_case(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ):
        """
        Reproduces the bug where force stop is called after the worker has already
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

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        # Set benchmark status to STOPPING
        await initiate_stop_benchmark(benchmark_row, database_session, force=True, org=self._test_org)
        assert benchmark_row.status == BenchmarkStatus.STOPPING

        async def _empty_list_sandboxes(*_args: Any, **_kwargs: Any):
            if False:
                yield None

        mock_provider = Mock()
        mock_provider.list_sandboxes = _empty_list_sandboxes
        mock_service = Mock()
        mock_service.get_sandbox_provider.return_value = mock_provider
        mock_service.close = AsyncMock()
        monkeypatch.setattr(
            Benchmark,
            "benchmark_service",
            lambda *_args, **_kwargs: mock_service,  # type: ignore
        )

        # Force stopping the sandboxes results in the benchmark row being stopped
        await force_stop_sandboxes(
            benchmark_row, database_session, harness_config.daytona_secret_name, harness_config.aws, self._test_org
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.STOPPED
