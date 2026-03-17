import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock

from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select

from main import app
from benchmark_service.client import BenchmarkServiceClient
from tracker.utils import start_benchmark_request_to_benchmark
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkStatus, Task, TaskStatus
from benchmark_service.schemas import FinalScoreResponse, Resources, RetrieveTaskResponse, VerifyTaskIdsResponse
from tracker.types import StartBenchmarkRequest
from tracker.utils import TaskMonitor, TrackedTask, TrackedTaskStatus, initiate_stop_benchmark, process_benchmark, reset_to_in_progress_status
from tests.unit.conftest import TEST_HARNESS_CONFIG

client = TestClient(app)


class TestStopAndResume:
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
            harness_config=TEST_HARNESS_CONFIG,
        )

        benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request)
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
            task_row = Task(task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            database_session.add(task_row)

        for task_id in pending_task_ids:
            task_row = Task(task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.PENDING)
            database_session.add(task_row)

        database_session.commit()

        # Stop benchmark - only tasks that are pending become stopped
        await initiate_stop_benchmark(benchmark_row, database_session, force=False)

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
        )
        # Only 3 tasks should be verified for resume (the 3 tasks that are stopped)
        assert len(verified_task_ids) == 3
        assert set(verified_task_ids) == set(pending_task_ids)

        # Run process_benchmark to complete the remaining tasks (the 3 tasks that are pending)
        await process_benchmark(
            start_benchmark_request_json=benchmark_row.start_benchmark_request(TEST_HARNESS_CONFIG).model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verified_task_ids,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message

    async def test_resume_changes_task_alias_per_attempt(
        self, example_benchmark_object: Benchmark, database_session: Session, monkeypatch: MonkeyPatch
    ):
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        task_rows = [
            Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED) for i in range(2)
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
            benchmark_service=benchmark_row.benchmark_service(
                TEST_HARNESS_CONFIG.daytona_secret_name, TEST_HARNESS_CONFIG.aws
            ),
            retry=True,
            rerun_task_ids=[],
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

        task_row = Task(task_id="task_0", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
        database_session.add(task_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        tracked_task = TrackedTask(asyncio.sleep(0))
        tracked_task._status = TrackedTaskStatus.WAITING  # type: ignore[attr-defined]

        cancel_mock = Mock()

        def _cancel(*_args: Any, **_kwargs: Any) -> None:
            tracked_task._status = TrackedTaskStatus.DONE  # type: ignore[attr-defined]

        cancel_mock.side_effect = _cancel
        tracked_task._task = Mock(cancel=cancel_mock, done=lambda: False)  # type: ignore[assignment]

        monitor = TaskMonitor(benchmark_row.id, {task_row.task_id: tracked_task})
        monitor._TRACK_INTERVAL = 0

        await monitor.track_tasks()

        cancel_mock.assert_called_once()
        assert monitor._task_tracking == {}
        tracked_task._coro.close()

