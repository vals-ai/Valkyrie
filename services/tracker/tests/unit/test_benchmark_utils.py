from datetime import datetime
from functools import partial
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from httpx._models import Response
from pytest import MonkeyPatch
from sqlmodel import Session, col, func, select, update

from main import app
from tests.unit.test_fastapi_server import client
from tracker.benchmark_service import BenchmarkService
from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.database.session import get_session
from tracker.exceptions import TrackerServiceError
from tracker.types import FinalScoreResponse, StartRunRequest, VerifyTaskIdsResponse
from tracker.utils import create_task_rows, fetch_benchmark_row, set_benchmark_final_status


class TestBenchmarkUtils:
    async def _mock_request_verify_task_ids(
        self, *args: Any, task_ids: list[str], **kwargs: Any
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    async def _mock_process_benchmark(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def _mock_request_final_score(
        self, *args: Any, final_score: float, metadata: dict[str, Any], tasks_evaluated: list[str], **kwargs: Any
    ) -> FinalScoreResponse:
        return FinalScoreResponse(final_score=final_score, metadata=metadata, tasks_evaluated=tasks_evaluated)

    def test_stop_benchmark(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests the flow of updating the benchmark related objects to the proper states when stopping a benchmark

        Test Cases:
            - Benchmark can be stopped if it is in progress and tasks that have not started yet exist
            - After stopping, the benchmark status is "stopping" and tasks have been set to "stopped"
            - Tasks not in starting state are left alone
        """

        def get_test_session():
            yield database_session

        app.dependency_overrides[get_session] = get_test_session

        # Create benchmark that has already been started
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        # create tasks, some which are starting and some which are in progress
        initial_task_rows: list[Task] = []
        for i in range(5):
            initial_task_rows.append(Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STARTING))
        for i in range(5, 10):
            initial_task_rows.append(
                Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.IN_PROGRESS)
            )
        database_session.add_all(initial_task_rows)
        database_session.commit()

        # Test request to stop the benchmark
        response: Response = client.post(f"/stop-run/{benchmark_row.id}")
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        # Check that the benchmark status is now "stopping"
        benchmark_row = fetch_benchmark_row(benchmark_row.id, database_session)
        assert benchmark_row.status == BenchmarkStatus.STOPPING

        # Task status in starting state should be set to "stopped" / otherwise known as no starting tasks left
        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.STARTING)
        ).one()

        assert task_rows == 0

        # Check the right amount of tasks are in stopped state
        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.STOPPED)
        ).one()
        assert task_rows == 5

        # The remaining tasks have been left alone in in progress state
        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.IN_PROGRESS)
        ).one()

        assert task_rows == 5

    def test_stop_benchmark_edge_cases(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests edge cases for stopping a benchmark

        Test Cases:
            - Cannot stop a benchmark that is not in progress
            - Cannot stop a benchmark where all tasks have already started
            - Errors are raised and returned to the client
        """

        def get_test_session():
            yield database_session

        app.dependency_overrides[get_session] = get_test_session

        # Cannot stop a benchmark that is not in progress
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.FINISHED
        database_session.add(benchmark_row)
        database_session.commit()

        # Fail to stop the run
        response: Response = client.post(f"/stop-run/{benchmark_row.id}")
        assert response.status_code == 400

        # Cannot stop a run with no tasks yet to start
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        # Add some tasks, all have already started

        task_rows = [
            Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.IN_PROGRESS) for i in range(5)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        # Error is returned because we have no starting tasks left to stop
        response = client.post(f"/stop-run/{benchmark_row.id}")
        assert response.status_code == 500

    def test_resume_benchmark(
        self, example_benchmark_object: Benchmark, database_session: Session, monkeypatch: MonkeyPatch
    ):
        """
        Tests the flow of updating the benchmark related objects to the proper states when resuming a benchmark

        Test Cases:
            - Benchmark can be resumed if it is in a stopped state and a single task with the stopped status exists
            - After resuming, the benchmark status is "in progress" and tasks have been set to "starting" that were in the stopped state
            - Only the status of stopped tasks are updated
        """

        def get_test_session():
            yield database_session

        app.dependency_overrides[get_session] = get_test_session

        # Create benchmark that has already been stopped
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Add some tasks, non-starting (stopped and finished tasks only)
        task_rows: list[Task] = []
        for i in range(5):
            task_rows.append(Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED))
        for i in range(5, 10):
            task_rows.append(Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED))
        database_session.add_all(task_rows)
        database_session.commit()

        # Fetch all the task ids that are stopped
        task_ids = database_session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.STOPPED)
        ).all()

        assert len(task_ids) == 5

        # Patch the verify endpoint to return the same task ids
        monkeypatch.setattr(
            BenchmarkService,
            "request_verify_task_ids",
            partial(self._mock_request_verify_task_ids, task_ids=list(task_ids)),
        )

        # Ignore the process benchmark task (not testing that here)
        monkeypatch.setattr(
            "main.process_benchmark.kiq",
            self._mock_process_benchmark,
        )

        # Test request to resume the benchmark
        response: Response = client.post(f"/resume-run/{benchmark_row.id}")
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        # Validate stopped tasks are now in starting state
        task_ids = database_session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.STARTING)
        ).all()
        assert len(task_ids) == 5

        # Validate the benchmark is now in progress state
        benchmark_row = fetch_benchmark_row(benchmark_row.id, database_session)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

    def test_resume_benchmark_edge_cases(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests edge cases for resuming a benchmark

        Test Cases:
            - Cannot resume a benchmark that is not in a stopped state
            - Cannot resume a benchmark where all tasks have already finished
            - Errors are raised and returned to the client
            - Can recreate the same environment the benchmark was started in
        """

        def get_test_session():
            yield database_session

        app.dependency_overrides[get_session] = get_test_session

        # Benchmark is not in a stopped state
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        # failure code to resume the benchmark
        response: Response = client.post(f"/resume-run/{benchmark_row.id}")
        assert response.status_code == 400

        # Set benchmark to stopped state but add only finished tasks
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # All of them finished
        task_rows = [
            Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED) for i in range(5)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        # error is returned because we have no stopped tasks to resume
        response = client.post(f"/resume-run/{benchmark_row.id}")
        assert response.status_code == 500

        # Ensure that we can recreate the environment the benchmark was started in
        original_start_run_request = StartRunRequest(
            contract_name="claude_code",
            benchmark_name="swebench",
            concurrency=5,
            task_ids=["task_0", "task_1", "task_2", "task_3", "task_4"],
            slice_str=":10",
        )

        benchmark_row = BenchmarkService.start_run_request_to_benchmark_object(original_start_run_request)

        recreated_start_run_request = benchmark_row.start_run_request
        assert recreated_start_run_request == original_start_run_request

    def test_create_task_rows(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests different scenarios for creating task rows

        Test Cases:
            - No tasks exist in the database already
            - Some tasks exist in the database already
            - No duplicate tasks are created
            - All returned tasks are in the starting state
        """

        # Create benchmark in progress state
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        # Verified tasks to create
        verified_task_ids = [f"task_{i}" for i in range(5)]

        # Creates all tasks in starting state
        task_rows = create_task_rows(verified_task_ids, benchmark_row, database_session)
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.STARTING for task_row in task_rows)

        # Same order is returned as the verified task ids are passed in (must be deterministic)
        for i, task_row in enumerate(task_rows):
            assert task_row[0] == verified_task_ids[i]

        # Try calling the same method again when the tasks already exist
        task_rows = create_task_rows(verified_task_ids, benchmark_row, database_session)
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.STARTING for task_row in task_rows)

        # No duplicate tasks are created and they are all in the starting state
        all_tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        assert len(all_tasks) == len(verified_task_ids)
        assert all(task.status == TaskStatus.STARTING for task in all_tasks)

    async def test_set_benchmark_final_status(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests the end to end flow when stopping and resuming a benchmark

        Test Cases:
            - Error is raised if tasks are still in the starting or in progress state
            - Benchmark status is set to finished if all tasks are finished
            - Benchmark status is set to stopped if any tasks are stopped
        """

        # Create benchmark
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        # Create some starting tasks
        task_ids = [f"task_{i}" for i in range(5)]
        task_rows = create_task_rows(task_ids, benchmark_row, database_session)
        assert len(task_rows) == len(task_ids)
        assert all(task_row[1].status == TaskStatus.STARTING for task_row in task_rows)

        # Error is raised because tasks are still in the starting state
        with pytest.raises(TrackerServiceError):
            set_benchmark_final_status(benchmark_row, database_session)

        # Make all tasks in finished state
        # NOTE: Need to manually set the finished_at timestamp because the event listener is not triggered with bulk updates
        database_session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .values(status=TaskStatus.FINISHED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        database_session.commit()

        # Benchmark status is set to finished
        set_benchmark_final_status(benchmark_row, database_session)
        database_session.refresh(benchmark_row, attribute_names=["status"])
        assert benchmark_row.status == BenchmarkStatus.FINISHED

        # Reset benchmark status to in progress
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        # Change some tasks to the stopped state
        stopped_tasks = task_ids[:2]
        database_session.exec(
            update(Task)
            .where(col(Task.task_id).in_(stopped_tasks))
            .values(status=TaskStatus.STOPPED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        database_session.commit()

        # Benchmark status is set to stopped when stopped tasks exist
        set_benchmark_final_status(benchmark_row, database_session)
        database_session.refresh(benchmark_row, attribute_names=["status"])
        assert benchmark_row.status == BenchmarkStatus.STOPPED
