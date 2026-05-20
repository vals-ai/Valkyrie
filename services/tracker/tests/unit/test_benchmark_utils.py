from datetime import datetime
from typing import Any, Sequence
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
from httpx._models import Response
from sqlmodel import Session, col, func, select, update

from tests.conftest import TEST_ORG_ID
from tests.unit.test_fastapi_server import client
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import TrackerServiceError
from tracker.types import FetchBenchmarksRequest, HarnessConfig, StartBenchmarkRequest
from tracker.utils import (
    commit_task_error,
    create_task_rows,
    fetch_benchmark_row,
    fetch_final_score_inputs,
    fetch_filtered_benchmark_rows,
    has_runnable_tasks,
    set_benchmark_final_status,
    start_benchmark_request_to_benchmark,
)


class TestBenchmarkUtils:
    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

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
        monkeypatch: pytest.MonkeyPatch,
    ):
        """
        Tests edge cases for resuming a benchmark

        Test Cases:
            - Running benchmark retry with no error tasks is a no-op
            - Cannot resume a benchmark where all tasks have already finished
            - Errors are raised and returned to the client
            - Can recreate the same environment the benchmark was started in
            - Can force resume a task and validate the task ids passed in
        """

        # Running benchmark retry with no error tasks is a no-op
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        response: Response = client.post(f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false")
        assert response.status_code == 200

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

        benchmark_row = start_benchmark_request_to_benchmark(original_start_benchmark_request, self._test_starter)

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
        async def _verify_rejecting_task_5(*_args: Any, task_ids: list[str] | None, **_kwargs: Any) -> Any:
            if task_ids and "task_5" in task_ids:
                raise BenchmarkServiceError("task_5 does not exist in the dataset")
            return VerifyTaskIdsResponse(task_ids=task_ids or [])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_rejecting_task_5)

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

        task_rows = create_task_rows(["task_1"], benchmark_row, database_session, self._test_org)
        assert [task_id for task_id, _ in task_rows] == ["task_1"]

    def test_fetch_final_score_inputs_waits_for_runnable_tasks(
        self, example_benchmark_object: Benchmark, database_session: Session
    ):
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        finished_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_finished",
            benchmark=benchmark_row.id,
            status=TaskStatus.FINISHED,
        )
        error_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_error",
            benchmark=benchmark_row.id,
            status=TaskStatus.ERROR,
        )
        stopped_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_stopped",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
        )
        pending_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_pending",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
        )
        database_session.add_all([finished_task, error_task, stopped_task, pending_task])
        database_session.commit()

        database_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=finished_task.id, result={"score": 1.0}))
        database_session.commit()

        assert has_runnable_tasks(database_session, benchmark_row, self._test_org)

        pending_task.status = TaskStatus.ERROR
        database_session.add(pending_task)
        database_session.commit()

        assert not has_runnable_tasks(database_session, benchmark_row, self._test_org)
        assert fetch_final_score_inputs(database_session, benchmark_row, self._test_org) == {
            "task_finished": {"score": 1.0},
            "task_error": None,
            "task_stopped": None,
            "task_pending": None,
        }

    def test_commit_task_error_spans_status_transition(
        self, example_benchmark_object: Benchmark, database_session: Session, monkeypatch: pytest.MonkeyPatch
    ):
        log_records: list[dict[str, Any]] = []
        span_records: list[dict[str, Any]] = []

        def fake_info(message: str, *args: object, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
            log_records.append({"message": message, **(extra or {})})

        class MockSpan:
            def __init__(self, record: dict[str, Any]) -> None:
                self._record = record

            def __enter__(self) -> "MockSpan":
                self._record["entered"] = True
                return self

            def __exit__(self, *_args: object) -> None:
                self._record["exited"] = True

        def fake_span(message: str, **attributes: Any) -> MockSpan:
            record = {"message": message, **attributes}
            span_records.append(record)
            return MockSpan(record)

        monkeypatch.setattr("tracker.utils.logger.info", fake_info)
        monkeypatch.setattr("tracker.utils.logfire.span", fake_span)

        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=example_benchmark_object.id,
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(task_row)
        database_session.commit()

        commit_task_error(task_row, database_session, "agent failed")

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        transition_record = next(record for record in span_records if record["message"] == "task.status_transition")
        assert transition_record["from_status"] == TaskStatus.IN_PROGRESS.value
        assert transition_record["to_status"] == TaskStatus.ERROR.value
        assert transition_record["task_id"] == "task_0"
        assert transition_record["benchmark_id"] == str(example_benchmark_object.id)
        assert transition_record["entered"] and transition_record["exited"]
        assert transition_record["has_error_message"] is True
        assert not any(record["message"].startswith("task.status_transition") for record in log_records)

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


def test_benchmark_persists_started_by_columns(
    database_session: Session,
    example_benchmark_object: Benchmark,
):
    example_benchmark_object.started_by_id = "K2abc"
    example_benchmark_object.started_by_email = "alice@vals.ai"
    database_session.add(example_benchmark_object)
    database_session.commit()

    refetched = database_session.get(Benchmark, example_benchmark_object.id)
    assert refetched is not None
    assert refetched.started_by_id == "K2abc"
    assert refetched.started_by_email == "alice@vals.ai"


def test_benchmark_started_by_columns_default_none(
    database_session: Session,
    example_benchmark_object: Benchmark,
):
    database_session.add(example_benchmark_object)
    database_session.commit()

    refetched = database_session.get(Benchmark, example_benchmark_object.id)
    assert refetched is not None
    assert refetched.started_by_id is None
    assert refetched.started_by_email is None


def _make_benchmark(
    session: Session,
    contract: AgentContractRequest,
    *,
    started_by_email: str | None,
    name: str = "swebench",
) -> Benchmark:
    bench = Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        arguments=BenchmarkArguments(contract=contract, concurrency=1),
        started_by_email=started_by_email,
        started_by_id="K-" + (started_by_email or "none"),
    )
    session.add(bench)
    session.commit()
    return bench


def test_fetch_filtered_started_by_single(database_session: Session, contract: AgentContractRequest):
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    _make_benchmark(database_session, contract, started_by_email="alice@vals.ai")
    _make_benchmark(database_session, contract, started_by_email="bob@vals.ai")
    _make_benchmark(database_session, contract, started_by_email=None)

    rows, total = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(started_by=["alice@vals.ai"], limit=10),
        database_session,
        org,
    )
    assert total == 1
    assert [r.started_by_email for r in rows] == ["alice@vals.ai"]


def test_fetch_filtered_started_by_multiple(database_session: Session, contract: AgentContractRequest):
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    _make_benchmark(database_session, contract, started_by_email="alice@vals.ai")
    _make_benchmark(database_session, contract, started_by_email="bob@vals.ai")
    _make_benchmark(database_session, contract, started_by_email="carol@vals.ai")

    rows, total = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(started_by=["alice@vals.ai", "bob@vals.ai"], limit=10),
        database_session,
        org,
    )
    assert total == 2
    assert sorted(r.started_by_email for r in rows) == ["alice@vals.ai", "bob@vals.ai"]


def test_fetch_filtered_started_by_case_insensitive(database_session: Session, contract: AgentContractRequest):
    """Uppercase input matches the lower-cased rows in the DB."""
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    _make_benchmark(database_session, contract, started_by_email="alice@vals.ai")

    rows, total = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(started_by=["ALICE@VALS.AI"], limit=10),
        database_session,
        org,
    )
    assert total == 1
    assert rows[0].started_by_email == "alice@vals.ai"


def test_fetch_filtered_started_by_none_skips_filter(database_session: Session, contract: AgentContractRequest):
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    _make_benchmark(database_session, contract, started_by_email="alice@vals.ai")
    _make_benchmark(database_session, contract, started_by_email=None)

    rows, total = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(started_by=None, limit=10),
        database_session,
        org,
    )
    assert total == 2


def test_fetch_filtered_started_by_strips_whitespace(database_session: Session, contract: AgentContractRequest):
    """Trailing whitespace on a filter email matches the clean DB row."""
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    _make_benchmark(database_session, contract, started_by_email="alice@vals.ai")

    rows, total = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(started_by=["  alice@vals.ai  "], limit=10),
        database_session,
        org,
    )
    assert total == 1
    assert rows[0].started_by_email == "alice@vals.ai"


def test_fetch_filtered_started_by_does_not_leak_across_orgs(database_session: Session, contract: AgentContractRequest):
    """Filter applies on top of scoped_select(Benchmark, org) — cross-org rows must not leak."""

    other_org = Org(id=uuid4(), name="other-tenant")
    database_session.add(other_org)
    database_session.commit()

    other_org_bench = Benchmark(
        org_id=other_org.id,
        name="swebench",
        arguments=BenchmarkArguments(contract=contract, concurrency=1),
        started_by_email="alice@vals.ai",
        started_by_id="K-other",
    )
    database_session.add(other_org_bench)
    database_session.commit()

    default_org = database_session.get(Org, TEST_ORG_ID)
    assert default_org is not None
    _make_benchmark(database_session, contract, started_by_email="alice@vals.ai")

    rows, total = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(started_by=["alice@vals.ai"], limit=10),
        database_session,
        default_org,
    )
    assert total == 1
    assert rows[0].org_id == TEST_ORG_ID
