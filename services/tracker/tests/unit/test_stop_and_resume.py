import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import FinalScoreResponse, Resources, RetrieveTaskResponse, VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import (
    TaskMonitor,
    TrackedTask,
    TrackedTaskStatus,
    force_stop_sandboxes,
    initiate_stop_benchmark,
    process_benchmark,
    reset_to_in_progress_status,
    start_benchmark_request_to_benchmark,
)

client = TestClient(app)


class TestStopAndResume:
    _test_org = Org(id=TEST_ORG_ID, name="default")

    @staticmethod
    async def _mock_request_retrieve_task(*args: Any, **kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            docker_image="test-image:latest",
            problem_path="/tmp/problem_statement.txt",
            cwd="/testbed",
            resources=Resources(vcpu=2, memory=4, disk=5),
        )

    @staticmethod
    async def _mock_request_evaluate_instance(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"status": "success", "score": 1.0}

    @staticmethod
    async def _mock_request_final_score(
        *args: Any, evaluation_results: dict[str, Any], **kwargs: Any
    ) -> FinalScoreResponse:
        tasks_evaluated = list(evaluation_results.keys())
        return FinalScoreResponse(
            tasks_evaluated=tasks_evaluated,
            final_score=50.0,
            metadata={"resolved_tasks": [], "unresolved_tasks": tasks_evaluated},
        )

    async def test_stop_and_resume(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
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

        @asynccontextmanager
        async def _mock_create_sandbox(*args: Any, **kwargs: Any):
            mock_sandbox = AsyncMock()
            mock_sandbox.id = "mock-sandbox-id"
            yield mock_sandbox

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

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, self._test_org)
        database_session.add(benchmark_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.create_sandbox", _mock_create_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", self._mock_request_retrieve_task)
        monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", self._mock_request_evaluate_instance)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", self._mock_request_final_score)

        # Create tasks - 2 tasks are finished, 3 tasks are pending
        finished_task_ids = task_ids[:2]
        pending_task_ids = task_ids[2:]

        for task_id in finished_task_ids:
            task_row = Task(org_id=TEST_ORG_ID, task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            database_session.add(task_row)

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

        async def _mock_request_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=pending_task_ids)

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_request_verify_task_ids)

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=database_session,
            benchmark_service=start_benchmark_request.benchmark_service,
            retry=False,
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
            rerun_task_ids=[],
            org=self._test_org,
        )

        assert set(verified_task_ids) == set(original_aliases.keys())

        updated_task_rows = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        for task_row in updated_task_rows:
            assert task_row.alias != original_aliases[task_row.task_id]

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

        # Mock daytona client since its not required
        mock_daytona = AsyncMock()
        mock_daytona.list = AsyncMock(return_value=AsyncMock(items=[], total_pages=0, page=1))
        monkeypatch.setattr(
            Benchmark,
            "benchmark_service",
            lambda *_args, **_kwargs: Mock(daytona_client=mock_daytona),  # type: ignore
        )

        # Force stopping the sandboxes results in the benchmark row being stopped
        await force_stop_sandboxes(
            benchmark_row, database_session, harness_config.daytona_secret_name, harness_config.aws, self._test_org
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.STOPPED
