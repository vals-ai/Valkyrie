from datetime import datetime
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import pytest
from benchmark_service.schemas import FinalScoreResponse
from httpx._models import Response
from sqlmodel import Session, col, func, select, update

from tests.unit.test_fastapi_server import client
from tests.conftest import TEST_ORG_ID
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.exceptions import TrackerServiceError
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import (
    create_task_rows,
    fetch_benchmark_row,
    set_benchmark_final_status,
    start_benchmark_request_to_benchmark,
)


class TestBenchmarkUtils:
    _test_org = Org(id=TEST_ORG_ID, name="default")

    async def _mock_request_final_score(
        self, *args: Any, final_score: float, metadata: dict[str, Any], tasks_evaluated: list[str], **kwargs: Any
    ) -> FinalScoreResponse:
        return FinalScoreResponse(final_score=final_score, metadata=metadata, tasks_evaluated=tasks_evaluated)

    def test_stop_benchmark(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests the flow of updating the benchmark related objects to the proper states when stopping a benchmark

        Test Cases:
            - Benchmark can be stopped if it is in progress and tasks that have not pending yet exist
            - After stopping, the benchmark status is "stopping" and tasks have been set to "stopped"
            - Tasks not in pending state are left alone
        """

        # Create benchmark that has already been started
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        # create tasks, some which are pending and some which are in progress
        initial_task_rows: list[Task] = []
        for i in range(5):
            initial_task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.PENDING)
            )
        for i in range(5, 8):
            initial_task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.IN_PROGRESS)
            )
        for i in range(8, 10):
            initial_task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.EVALUATING)
            )
        database_session.add_all(initial_task_rows)
        database_session.commit()

        # Test request to stop the benchmark
        response: Response = client.post(f"/stop-benchmark/{benchmark_row.id}?force=false")
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        # Check that the benchmark status is now "stopping"
        benchmark_row = fetch_benchmark_row(benchmark_row.id, database_session, self._test_org)
        assert benchmark_row.status == BenchmarkStatus.STOPPING

        # Task status in pending state should be set to "stopped" / otherwise known as no pending tasks left
        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.PENDING)
        ).one()

        assert task_rows == 0

        # Check the right amount of tasks are in stopped state
        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.STOPPED)
        ).one()
        assert task_rows == 7

        # The remaining tasks have been left alone in in progress state
        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.IN_PROGRESS)
        ).one()

        assert task_rows == 3

    def test_stop_benchmark_edge_cases(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests edge cases for stopping a benchmark

        Test Cases:
            - Cannot stop a benchmark that is not in progress
            - Cannot stop a benchmark where all tasks have already started
            - Errors are raised and returned to the client
        """

        # Cannot stop a benchmark that is not in progress
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.FINISHED
        database_session.add(benchmark_row)
        database_session.commit()

        # Fail to stop the run
        response: Response = client.post(f"/stop-benchmark/{benchmark_row.id}?force=false")
        assert response.status_code == 400

    def test_resume_benchmark(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests the flow of updating the benchmark related objects to the proper states when resuming a benchmark

        Test Cases:
            - Benchmark can be resumed if it is in a stopped state and a single task with the stopped status exists
            - After resuming, the benchmark status is "in progress" and tasks have been set to "pending" that were in the stopped state
            - Only the status of stopped tasks are updated
            - Can resume a benchmark with tasks that have the status error
        """

        # Create benchmark that has already been stopped
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Add some tasks, non-pending (stopped and finished tasks only)
        task_rows: list[Task] = []
        for i in range(5):
            task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
            )
        for i in range(5, 10):
            task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            )
        database_session.add_all(task_rows)
        database_session.commit()

        # Fetch all the task ids that are stopped
        task_ids = database_session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.STOPPED)
        ).all()

        assert len(task_ids) == 5

        # Test request to resume the benchmark
        response: Response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false")
        assert response.status_code == 200, response.text
        assert response.json() == {"status": "success"}

        # Validate stopped tasks are now in pending state
        task_ids = database_session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.PENDING)
        ).all()
        assert len(task_ids) == 5

        # Validate the benchmark is now in progress state
        benchmark_row = fetch_benchmark_row(benchmark_row.id, database_session, self._test_org)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

        # Reset benchmark row to stopped state
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Fetch all tasks
        fetched_task_rows: Sequence[Task] = database_session.exec(
            select(Task).where(col(Task.benchmark) == benchmark_row.id)
        ).all()
        assert len(fetched_task_rows) == 10

        # Change half of them to error and the other half reset to stopped
        for i, task_row in enumerate(fetched_task_rows):
            if i < 5:
                task_row.status = TaskStatus.ERROR
            else:
                task_row.status = TaskStatus.STOPPED

        database_session.commit()

        # Call resume benchmark with retry enabled
        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true")
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        # Validate all tasks are now in pending state
        fetched_task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == benchmark_row.id)).all()
        assert len(fetched_task_rows) == 10
        assert all(task_row.status == TaskStatus.PENDING for task_row in fetched_task_rows)

    def test_resume_benchmark_edge_cases(
        self,
        contract: AgentContractRequest,
        example_benchmark_object: Benchmark,
        database_session: Session,
        harness_config: HarnessConfig,
    ):
        """
        Tests edge cases for resuming a benchmark

        Test Cases:
            - Cannot resume a benchmark that is not in a stopped state
            - Cannot resume a benchmark where all tasks have already finished
            - Errors are raised and returned to the client
            - Can recreate the same environment the benchmark was started in
            - Can force resume a task and validate the task ids passed in
        """

        # Benchmark is not in a stopped state
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        # failure code to resume the benchmark
        response: Response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false")
        assert response.status_code == 400

        # Set benchmark to stopped state but add only finished tasks
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # All of them finished
        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            for i in range(5)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        # No stopped tasks to resume, but this is allowed (re-runs post-task steps like lambda)
        response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false")
        assert response.status_code == 200

        # Ensure that we can recreate the environment the benchmark was started in
        original_start_benchmark_request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=5,
            task_ids=["task_0", "task_1", "task_2", "task_3", "task_4"],
            slice_str=":10",
            harness_config=harness_config,
        )

        benchmark_row = start_benchmark_request_to_benchmark(original_start_benchmark_request, self._test_org)

        recreated_start_benchmark_request = benchmark_row.start_benchmark_request(harness_config)
        assert recreated_start_benchmark_request == original_start_benchmark_request

        # Assert we have 5 tasks in the database
        task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == example_benchmark_object.id)).all()
        assert len(task_rows) == 5

        # Change one task to stopped state
        database_session.exec(
            update(Task).where(col(Task.task_id) == task_rows[0].task_id).values(status=TaskStatus.STOPPED)
        )
        database_session.commit()

        # Task id is provided as a force parameter but does not exist in dataset
        response = client.post(
            f"/retry-or-resume-benchmark/{example_benchmark_object.id}?retry=false",
            json={"task_ids": ["task_5"]},
        )
        assert response.status_code == 500
        assert "task_5" in response.json()["detail"]

        # Assert all tasks but 0 are in finished state
        task_rows = database_session.exec(
            select(Task).where(
                (col(Task.benchmark) == example_benchmark_object.id) & (col(Task.task_id) != task_rows[0].task_id)
            )
        ).all()
        task_ids = [task_row.task_id for task_row in task_rows]

        assert all(task_row.status == TaskStatus.FINISHED for task_row in task_rows)

        # Try again with the correct task ids

        response = client.post(
            f"/retry-or-resume-benchmark/{example_benchmark_object.id}?retry=false",
            json={"task_ids": task_ids},
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        # Validate the tasks are now in pending state
        task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == example_benchmark_object.id)).all()
        assert len(task_rows) == 5
        assert all(task_row.status == TaskStatus.PENDING for task_row in task_rows)

    def test_create_task_rows(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests different scenarios for creating task rows

        Test Cases:
            - No tasks exist in the database already
            - Some tasks exist in the database already
            - No duplicate tasks are created
            - All returned tasks are in the pending state
        """

        # Create benchmark in progress state
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        # Verified tasks to create
        verified_task_ids = [f"task_{i}" for i in range(5)]

        # Creates all tasks in pending state
        task_rows = create_task_rows(verified_task_ids, benchmark_row, database_session, self._test_org)
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        # Same order is returned as the verified task ids are passed in (must be deterministic)
        for i, task_row in enumerate(task_rows):
            assert task_row[0] == verified_task_ids[i]

        # Try calling the same method again when the tasks already exist
        task_rows = create_task_rows(verified_task_ids, benchmark_row, database_session, self._test_org)
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        # No duplicate tasks are created and they are all in the pending state
        all_tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        assert len(all_tasks) == len(verified_task_ids)
        assert all(task.status == TaskStatus.PENDING for task in all_tasks)

    async def test_set_benchmark_final_status(self, example_benchmark_object: Benchmark, database_session: Session):
        """
        Tests the end to end flow when stopping and resuming a benchmark

        Test Cases:
            - Error is raised if tasks are still in the pending or in progress state
            - Benchmark status is set to finished if all tasks are finished
            - Benchmark status is set to stopped if any tasks are stopped
        """

        # Create benchmark
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        # Create some pending tasks
        task_ids = [f"task_{i}" for i in range(5)]
        task_rows = create_task_rows(task_ids, benchmark_row, database_session, self._test_org)
        assert len(task_rows) == len(task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        # Error is raised because tasks are still in the pending state
        with pytest.raises(TrackerServiceError):
            set_benchmark_final_status(benchmark_row, database_session, self._test_org)

        # Make all tasks in finished state
        # NOTE: Need to manually set the finished_at timestamp because the event listener is not triggered with bulk updates
        database_session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .values(status=TaskStatus.FINISHED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        database_session.commit()

        # Benchmark status is set to finished
        set_benchmark_final_status(benchmark_row, database_session, self._test_org)
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
        set_benchmark_final_status(benchmark_row, database_session, self._test_org)
        database_session.refresh(benchmark_row, attribute_names=["status"])
        assert benchmark_row.status == BenchmarkStatus.STOPPED
