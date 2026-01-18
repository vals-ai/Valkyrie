from functools import partial
from typing import Any

from httpx._models import Response
from pytest import MonkeyPatch
from sqlmodel import Session, col, func, select

from main import app
from tests.unit.test_fastapi_server import client
from tracker.benchmark_service import BenchmarkService
from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.database.session import get_session
from tracker.types import VerifyTaskIdsResponse
from tracker.utils import fetch_benchmark_row


class TestBenchmarkUtils:
    async def _mock_request_verify_task_ids(
        self, *args: Any, task_ids: list[str], **kwargs: Any
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    async def _mock_process_benchmark(self, *args: Any, **kwargs: Any) -> None:
        pass

    def test_stop_benchmark(self, example_benchmark_object: Benchmark, database_session: Session):
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
