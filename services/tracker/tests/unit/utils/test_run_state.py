"""Unit tests for benchmark state transitions and queries.

Run: uv run pytest tests/unit/utils/test_run_state.py
"""

from datetime import datetime
from typing import Any, Sequence
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi import HTTPException
from fastapi.testclient import TestClient
from httpx._models import Response
from sqlmodel import Session, col, func, select, update
from starlette.requests import Request

import tracker.utils.harness_config as harness_config_module
from main import app
from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import TrackerServiceError
from tracker.executor.release_control import promote_release
from tracker.types import FetchBenchmarksRequest, HarnessConfig, StartBenchmarkRequest
from tracker.utils import (
    commit_task_error,
    create_task_rows,
    fetch_benchmark_row,
    fetch_filtered_benchmark_rows,
    fetch_final_score_inputs,
    fetch_harness_config,
    fetch_sandbox_provider_config,
    has_runnable_tasks,
    set_benchmark_final_status,
    start_benchmark_request_to_benchmark,
)

_parse_log_retention_policy = getattr(harness_config_module, "_parse_log_retention_policy")

client = TestClient(app)


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest, database_session: Session) -> Benchmark:
    """Build state-transition benchmarks with a persisted executor release identity."""
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
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version
    return benchmark


class TestRunState:
    """Benchmark state transitions, task rows, and finalization."""

    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org, access_key_id=None, email=None, name=None)

    def test_fetch_sandbox_provider_config_combines_provider_type_with_secret(
        self, harness_config: HarnessConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sandbox provider config should combine client-selected type with the production secret shape.

        Test cases:
        - A selected provider type is added to DAYTONA_* secret values.
        """
        secrets = {
            "provider-secret": {
                "DAYTONA_API_KEY": "key",
                "DAYTONA_API_URL": "url",
                "DAYTONA_TARGET": "target",
            },
        }

        def fetch_secret(name: str, _client_provider: object) -> dict[str, str]:
            return secrets[name]

        monkeypatch.setattr("tracker.utils.resources.fetch_aws_secret", fetch_secret)

        provider_config = fetch_sandbox_provider_config(
            "provider-secret",
            AWSRuntime.from_harness_config(harness_config).clients,
            "daytona",
        )
        assert provider_config.model_dump(mode="json") == {
            "type": "daytona",
            "DAYTONA_API_KEY": "key",
            "DAYTONA_API_URL": "url",
            "DAYTONA_TARGET": "target",
        }

    def test_stop_benchmark(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        harness_headers: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tests the flow of updating the benchmark related objects to the proper states when stopping a benchmark

        Test Cases:
            - A graceful whole-run stop leaves the benchmark stopping.
            - Queued and evaluating tasks stop while in-progress tasks continue.
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

        locked_fetches: list[bool] = []

        def capture_locked_fetch(
            benchmark_id: UUID,
            session: Session,
            org: Org,
            *,
            for_update: bool = False,
        ) -> Benchmark:
            locked_fetches.append(for_update)
            return fetch_benchmark_row(benchmark_id, session, org, for_update=for_update)

        monkeypatch.setattr("main.fetch_benchmark_row", capture_locked_fetch)

        # Test request to stop the benchmark
        response: Response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=false",
            headers=harness_headers,
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        assert locked_fetches == [True]

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

        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.IN_PROGRESS)
        ).one()

        assert task_rows == 3

    def test_stop_benchmark_edge_cases(self, example_benchmark_object: Benchmark, database_session: Session) -> None:
        """Tests edge cases for stopping a benchmark

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

    def test_resume_benchmark(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        harness_headers: dict[str, str],
    ) -> None:
        """Tests the flow of updating the benchmark related objects to the proper states when resuming a benchmark

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
        response: Response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
            headers=harness_headers,
        )
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
        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true",
            headers=harness_headers,
        )
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
        harness_headers: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tests edge cases for resuming a benchmark

        Test Cases:
            - Running benchmark retry with no error tasks is a no-op
            - Cannot resume a benchmark where all tasks have already finished
            - Downstream errors return a stable generic client detail
            - Can recreate the same environment the benchmark was started in
            - Can force resume a task and validate the task ids passed in
        """

        # Running benchmark retry with no error tasks is a no-op
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        response: Response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
            headers=harness_headers,
        )
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
        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
            headers=harness_headers,
        )
        assert response.status_code == 200

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Ensure that we can recreate the environment the benchmark was started in
        original_start_benchmark_request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=5,
            task_ids=["task_0", "task_1", "task_2", "task_3", "task_4"],
            slice_str=":10",
            harness_config=harness_config.model_copy(update={"sandbox_provider_secret_name": "ModalSecrets"}),
            sandbox_provider="modal",
        )

        benchmark_row = start_benchmark_request_to_benchmark(
            original_start_benchmark_request,
            self._test_starter,
            aws_managed=False,
        )
        assert benchmark_row.arguments.sandbox_provider_secret_name == "ModalSecrets"

        recreated_start_benchmark_request = benchmark_row.access_key_start_benchmark_request(harness_config)
        assert recreated_start_benchmark_request == original_start_benchmark_request.model_copy(
            update={
                "harness_config": harness_config.model_copy(update={"sandbox_provider_secret_name": "ModalSecrets"}),
            }
        )

        # Assert we have 5 tasks in the database
        task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == example_benchmark_object.id)).all()
        assert len(task_rows) == 5

        # Change one task to stopped state
        database_session.exec(
            update(Task).where(col(Task.task_id) == task_rows[0].task_id).values(status=TaskStatus.STOPPED)
        )
        database_session.commit()

        # Task id is provided as a force parameter but does not exist in dataset
        async def _verify_rejecting_task_5(
            *_args: Any,
            task_ids: list[str] | None,
            **_kwargs: Any,
        ) -> VerifyTaskIdsResponse:
            if task_ids and "task_5" in task_ids:
                raise BenchmarkServiceError("task_5 does not exist in the dataset")
            return VerifyTaskIdsResponse(task_ids=task_ids or [])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_rejecting_task_5)

        response = client.post(
            f"/retry-or-resume-benchmark/{example_benchmark_object.id}?retry=false",
            json={"task_ids": ["task_5"]},
            headers=harness_headers,
        )
        assert response.status_code == 500
        assert response.json() == {"detail": "Benchmark service request failed"}

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
            headers=harness_headers,
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        # Validate the tasks are now in pending state
        task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == example_benchmark_object.id)).all()
        assert len(task_rows) == 5
        assert all(task_row.status == TaskStatus.PENDING for task_row in task_rows)

    def test_running_benchmark_rejects_secret_overrides_without_retry(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        harness_headers: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        original_arguments = benchmark_row.arguments.model_copy(deep=True)
        enqueue = AsyncMock()
        monkeypatch.setattr("main._enqueue_executor_dispatch", enqueue)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
            headers=harness_headers,
            json={"secrets": {"MODEL_API_KEY": "replacement"}},
        )

        assert response.status_code == 409
        assert response.json() == {"detail": "Secret overrides require retry=true while a run is in progress."}
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert benchmark_row.arguments == original_arguments
        enqueue.assert_not_awaited()

    def test_create_task_rows(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
    ) -> None:
        """Tests different scenarios for creating task rows

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
        authority = executor_authority(benchmark_row, session=database_session)

        # Verified tasks to create
        verified_task_ids = [f"task_{i}" for i in range(5)]

        # Creates all tasks in pending state
        task_rows = create_task_rows(
            verified_task_ids, benchmark_row, database_session, self._test_org, authority=authority
        )
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        # Same order is returned as the verified task ids are passed in (must be deterministic)
        for i, task_row in enumerate(task_rows):
            assert task_row[0] == verified_task_ids[i]

        # Try calling the same method again when the tasks already exist
        task_rows = create_task_rows(
            verified_task_ids, benchmark_row, database_session, self._test_org, authority=authority
        )
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        # No duplicate tasks are created and they are all in the pending state
        all_tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        assert len(all_tasks) == len(verified_task_ids)
        assert all(task.status == TaskStatus.PENDING for task in all_tasks)

        task_rows = create_task_rows(["task_1"], benchmark_row, database_session, self._test_org, authority=authority)
        assert [task_id for task_id, _ in task_rows] == ["task_1"]

    def test_fetch_final_score_inputs_waits_for_runnable_tasks(
        self, example_benchmark_object: Benchmark, database_session: Session
    ) -> None:
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
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority: Any,
    ) -> None:
        log_records: list[dict[str, Any]] = []
        span_records: list[dict[str, Any]] = []

        def fake_info(message: str, *_args: object, extra: dict[str, Any] | None = None, **_kwargs: Any) -> None:
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

        monkeypatch.setattr("tracker.utils.task_execution.logger.info", fake_info)
        monkeypatch.setattr("tracker.observability.tracing.logfire.span", fake_span)

        database_session.add(example_benchmark_object)
        database_session.commit()
        authority = executor_authority(example_benchmark_object, session=database_session)

        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=example_benchmark_object.id,
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(task_row)
        database_session.commit()

        commit_task_error(
            task_row,
            database_session,
            "agent failed",
            producer="tracker",
            operation="process_task",
            error_type="RuntimeError",
            authority=authority,
        )

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_result = database_session.exec(
            select(ErrorResult).where(ErrorResult.task == task_row.id).where(ErrorResult.org_id == TEST_ORG_ID)
        ).one()
        assert error_result.error_message == "agent failed"
        assert error_result.producer == "tracker"
        assert error_result.operation == "process_task"
        assert error_result.error_type == "RuntimeError"
        assert error_result.cause_code is None
        assert error_result.retry_scheduled is False
        assert error_result.failed_attempt_number is None
        transition_record = next(record for record in span_records if record["message"] == "task.status_transition")
        assert transition_record["from_status"] == TaskStatus.IN_PROGRESS.value
        assert transition_record["to_status"] == TaskStatus.ERROR.value
        assert transition_record["task_id"] == "task_0"
        assert transition_record["benchmark_id"] == str(example_benchmark_object.id)
        assert transition_record["entered"] and transition_record["exited"]
        assert transition_record["has_error_message"]
        assert not any(record["message"].startswith("task.status_transition") for record in log_records)

    def test_commit_task_error_rolls_back_when_started_at_is_stale(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()
        authority = executor_authority(example_benchmark_object, session=database_session)
        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="stale-task",
            benchmark=example_benchmark_object.id,
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(task_row)
        database_session.commit()

        committed = commit_task_error(
            task_row,
            database_session,
            "stale failure",
            producer="tracker",
            operation="process_task",
            error_type="RuntimeError",
            expected_started_at=datetime(2000, 1, 1, tzinfo=ZoneInfo("UTC")),
            authority=authority,
        )

        assert committed is False
        database_session.expire_all()
        persisted_task = database_session.get(Task, task_row.id)
        assert persisted_task is not None
        assert persisted_task.status == TaskStatus.IN_PROGRESS
        assert persisted_task.finished_at is None
        assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all() == []

    async def test_set_benchmark_final_status(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tests the end to end flow when stopping and resuming a benchmark

        Test Cases:
            - Error is raised if tasks are still in the pending or in progress state
            - Benchmark status is set to finished if all tasks are finished
            - Benchmark status is set to stopped if any tasks are stopped
        """

        finalized_statuses: list[str] = []

        def record_span(name: str, **attributes: Any) -> MagicMock:
            if name == "run.finalized":
                finalized_statuses.append(attributes["status"])
            return MagicMock()

        monkeypatch.setattr("tracker.utils.run_orchestration.observability_span", record_span)

        # Create benchmark
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        # Create some pending tasks
        task_ids = [f"task_{i}" for i in range(5)]
        task_rows = create_task_rows(task_ids, benchmark_row, database_session, self._test_org, authority=authority)
        assert len(task_rows) == len(task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        # Error is raised because tasks are still in the pending state
        with pytest.raises(TrackerServiceError):
            set_benchmark_final_status(benchmark_row, database_session, self._test_org, authority=authority)

        # Make all tasks in finished state
        # NOTE: Need to manually set the finished_at timestamp because the event listener is not triggered with bulk updates
        database_session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .values(status=TaskStatus.FINISHED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        database_session.commit()

        # Benchmark status is set to finished
        set_benchmark_final_status(benchmark_row, database_session, self._test_org, authority=authority)
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
        set_benchmark_final_status(benchmark_row, database_session, self._test_org, authority=authority)
        database_session.refresh(benchmark_row, attribute_names=["status"])
        assert benchmark_row.status == BenchmarkStatus.STOPPED
        assert finalized_statuses == ["FINISHED", "STOPPED"]


class TestFetchStartedByFilter:
    """Run filtering by starter email."""

    def test_fetch_filtered_started_by_single(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)
        make_benchmark(started_by_email="bob@vals.ai", session=database_session)
        make_benchmark(started_by_email=None, session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["alice@vals.ai"], limit=10),
            database_session,
            org,
        )
        assert total == 1
        assert [r.started_by_email for r in rows] == ["alice@vals.ai"]

    def test_fetch_filtered_started_by_multiple(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)
        make_benchmark(started_by_email="bob@vals.ai", session=database_session)
        make_benchmark(started_by_email="carol@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["alice@vals.ai", "bob@vals.ai"], limit=10),
            database_session,
            org,
        )
        assert total == 2
        assert sorted(email for r in rows if isinstance((email := r.started_by_email), str)) == [
            "alice@vals.ai",
            "bob@vals.ai",
        ]

    def test_fetch_filtered_started_by_case_insensitive(self, database_session: Session) -> None:
        """Uppercase input matches the lower-cased rows in the DB."""
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["ALICE@VALS.AI"], limit=10),
            database_session,
            org,
        )
        assert total == 1
        assert rows[0].started_by_email == "alice@vals.ai"

    def test_fetch_filtered_started_by_none_skips_filter(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)
        make_benchmark(started_by_email=None, session=database_session)

        _, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=None, limit=10),
            database_session,
            org,
        )
        assert total == 2

    def test_fetch_filtered_started_by_strips_whitespace(self, database_session: Session) -> None:
        """Trailing whitespace on a filter email matches the clean DB row."""
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["  alice@vals.ai  "], limit=10),
            database_session,
            org,
        )
        assert total == 1
        assert rows[0].started_by_email == "alice@vals.ai"

    def test_fetch_filtered_started_by_does_not_leak_across_orgs(self, database_session: Session) -> None:
        """Filter applies on top of scoped_select(Benchmark, org) — cross-org rows must not leak."""

        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()

        make_benchmark(
            org_id=other_org.id,
            started_by_email="alice@vals.ai",
            started_by_id="K-other",
            session=database_session,
        )

        default_org = database_session.get(Org, TEST_ORG_ID)
        assert default_org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["alice@vals.ai"], limit=10),
            database_session,
            default_org,
        )
        assert total == 1
        assert rows[0].org_id == TEST_ORG_ID


def test_fetch_filtered_by_label(database_session: Session) -> None:
    """Label filtering should ignore case while preserving stored label casing.

    Test cases:
    - A mixed-case label returns when queried with different casing.
    - Unlabeled and differently labeled runs are excluded.
    """
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    make_benchmark(started_by_email="alice@vals.ai", label="Nightly", session=database_session)
    make_benchmark(started_by_email="bob@vals.ai", label="manual", session=database_session)
    make_benchmark(started_by_email="carol@vals.ai", label=None, session=database_session)

    rows, total, _ = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(label="nightLY", limit=10),
        database_session,
        org,
    )

    assert total == 1
    assert rows[0].label == "Nightly"


class TestLogRetentionPolicy:
    """Log retention policy validation at helper and request boundaries."""

    def test_parse_log_retention_policy_rejects_invalid_value(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            _parse_log_retention_policy("not-a-number", source="test")

        assert exc_info.value.status_code == 400

    def test_fetch_harness_config_rejects_invalid_retention_header(self) -> None:
        request = Request(
            {
                "type": "http",
                "headers": [
                    (b"x-harness-aws-access-key-id", b"A"),
                    (b"x-harness-aws-secret-access-key", b"s"),
                    (b"x-harness-aws-default-region", b"us-east-1"),
                    (b"x-harness-s3-bucket", b"bucket"),
                    (b"x-harness-log-retention-policy", b"not-a-number"),
                ],
            }
        )

        with pytest.raises(HTTPException) as exc_info:
            fetch_harness_config(request)

        assert exc_info.value.status_code == 400
