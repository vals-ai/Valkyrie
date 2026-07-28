"""Tests for Tracker FastAPI route behavior.

Run: uv run pytest tests/unit/test_main.py
"""

import io
import logging
import tarfile
from collections.abc import AsyncIterator
from datetime import timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from benchmark_service.client import (
    BenchmarkServiceClient,
    BenchmarkServiceError,
    BenchmarkServiceUnauthenticatedError,
)
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
from dateutil.parser import isoparse
from descope.descope_client import DescopeClient
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select

from main import app, tracker_service_error_handler
from tests.unit.utils.task_execution_support import MockKicker
from tests.utils import TEST_ORG_ID, async_iterator
from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    DocentReadingStatus,
    ErrorResult,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import TrackerServiceError
from tracker.types import (
    BenchmarkTableRow,
    FetchBenchmarksRequest,
    FinalViewResponse,
    HarnessConfig,
    StartBenchmarkRequest,
)
from tracker.utils import fetch_harness_config, update_benchmark_concurrency

client = TestClient(app)


async def _verify_single_task_id(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
    """Return the single task used by benchmark start route tests."""
    return VerifyTaskIdsResponse(task_ids=["task_0"])


class TestTrackerAPI:
    """Tracker API route behavior and error responses."""

    async def _mock_verify_task_ids_error(self, *_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        raise Exception("Error verifying task ids")

    def test_health_check(self, monkeypatch: MonkeyPatch) -> None:
        """Test health check of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Response contains expected format
        """
        # Mock database connection check for unit tests
        monkeypatch.setattr("main.check_database_connection", lambda: True)

        response = client.get("/health")

        assert response.status_code == 200

        assert response.json() == {"status": "ok"}

    @pytest.mark.parametrize("concurrency", [0, -1, 1.5, "2", True])
    def test_update_benchmark_concurrency_rejects_non_positive_or_non_integer_values(
        self,
        concurrency: object,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": concurrency},
        )

        assert response.status_code == 422
        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.arguments.concurrency == 5

    def test_update_benchmark_concurrency_persists_full_arguments_and_is_idempotent(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        original_arguments = example_benchmark_object.arguments
        database_session.add(example_benchmark_object)
        database_session.commit()

        first_response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )
        retry_response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )

        expected_response = {
            "benchmark_id": str(example_benchmark_object.id),
            "status": BenchmarkStatus.IN_PROGRESS.value,
            "concurrency": 9,
        }
        assert first_response.status_code == 200
        assert first_response.json() == expected_response
        assert retry_response.status_code == 200
        assert retry_response.json() == expected_response

        database_session.expire_all()
        persisted = database_session.get(Benchmark, example_benchmark_object.id)
        assert persisted is not None
        assert persisted.arguments == original_arguments.model_copy(update={"concurrency": 9})

    def test_update_benchmark_concurrency_hides_missing_and_other_org_runs(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()
        missing_response = client.patch(
            f"/benchmarks/{uuid4()}/concurrency",
            json={"concurrency": 9},
        )
        other_org = Org(id=uuid4(), name="other")
        monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: other_org)

        response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )

        assert missing_response.status_code == 404
        assert missing_response.json() == {"detail": "Not found"}
        assert response.status_code == 404
        assert response.json() == {"detail": "Not found"}
        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.arguments.concurrency == 5

    def test_update_benchmark_concurrency_rejects_non_active_run(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        example_benchmark_object.status = BenchmarkStatus.STOPPED
        database_session.add(example_benchmark_object)
        database_session.commit()

        response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )

        assert response.status_code == 409
        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.arguments.concurrency == 5

    def test_update_benchmark_concurrency_refreshes_preloaded_state_before_mutating(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        with Session(bind=database_session.get_bind()) as concurrent_session:
            concurrent_benchmark = concurrent_session.get(Benchmark, example_benchmark_object.id)
            assert concurrent_benchmark is not None
            concurrent_benchmark.status = BenchmarkStatus.STOPPED
            concurrent_session.add(concurrent_benchmark)
            concurrent_session.commit()

        result = update_benchmark_concurrency(
            example_benchmark_object.id,
            9,
            database_session,
            Org(id=TEST_ORG_ID, name="default"),
        )

        assert result.status == BenchmarkStatus.STOPPED
        database_session.expire_all()
        persisted = database_session.get(Benchmark, example_benchmark_object.id)
        assert persisted is not None
        assert persisted.status == BenchmarkStatus.STOPPED
        assert persisted.arguments.concurrency == 5

    def test_update_benchmark_concurrency_returns_snapshot_from_locked_transaction(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()
        commit_update = database_session.commit

        def commit_then_stop() -> None:
            commit_update()
            with Session(bind=database_session.get_bind()) as concurrent_session:
                concurrent_benchmark = concurrent_session.get(Benchmark, example_benchmark_object.id)
                assert concurrent_benchmark is not None
                concurrent_benchmark.status = BenchmarkStatus.STOPPED
                concurrent_session.add(concurrent_benchmark)
                concurrent_session.commit()

        monkeypatch.setattr(database_session, "commit", commit_then_stop)

        result = update_benchmark_concurrency(
            example_benchmark_object.id,
            9,
            database_session,
            Org(id=TEST_ORG_ID, name="default"),
        )

        assert result.status == BenchmarkStatus.IN_PROGRESS
        database_session.expire_all()
        persisted = database_session.get(Benchmark, example_benchmark_object.id)
        assert persisted is not None
        assert persisted.status == BenchmarkStatus.STOPPED
        assert persisted.arguments.concurrency == 9

    def test_trailing_slash_does_not_redirect(self, monkeypatch: MonkeyPatch) -> None:
        """
        Verify non-canonical route paths cannot construct redirects from forwarded request data.

        Test cases:
        - The canonical health path remains available.
        - The trailing-slash variant returns not found without a Location header.
        """
        monkeypatch.setattr("main.check_database_connection", lambda: True)

        canonical_response = client.get("/health")
        slash_response = client.get(
            "/health/",
            headers={"Host": "example.invalid", "X-Forwarded-Proto": "https"},
            follow_redirects=False,
        )

        assert canonical_response.status_code == 200
        assert slash_response.status_code == 404
        assert "location" not in slash_response.headers

    def test_analyze_benchmark_enforces_state_and_reuses_cached_result(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        """Docent analysis must reject invalid runs and return a completed cached result.

        Test cases:
        - An active run cannot be analyzed.
        - A finished uncached run requires an analyzer Lambda.
        - A finished cached run returns its stored reading-plan URL without invoking Lambda.
        """
        database_session.add(example_benchmark_object)
        database_session.commit()

        active_response = client.post(
            f"/analyze-benchmark/{example_benchmark_object.id}",
            json={"lambda_function": "docent-analyzer"},
        )
        assert active_response.status_code == 400
        assert "must be FINISHED" in active_response.json()["detail"]

        example_benchmark_object.status = BenchmarkStatus.FINISHED
        database_session.add(example_benchmark_object)
        database_session.commit()
        missing_lambda_response = client.post(f"/analyze-benchmark/{example_benchmark_object.id}", json={})
        assert missing_lambda_response.status_code == 400
        assert "No ingest_lambda provided" in missing_lambda_response.json()["detail"]

        example_benchmark_object.docent_reading_status = DocentReadingStatus.DONE
        example_benchmark_object.docent_reading_url = "https://results.example/reading-plan"
        database_session.add(example_benchmark_object)
        database_session.commit()
        cached_response = client.post(f"/analyze-benchmark/{example_benchmark_object.id}", json={})
        assert cached_response.status_code == 200
        assert cached_response.json() == {
            "status": "done",
            "reading_plan_url": "https://results.example/reading-plan",
        }

    async def test_tracker_service_error_hides_internal_detail(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """
        Verify internal Tracker errors retain diagnostics only in server logs.

        Test cases:
        - The client receives a stable generic detail.
        - The full exception remains available to operators in logs.
        """
        sensitive_detail = "sensitive-internal-tracker-detail"

        def capture_exception(_exception: BaseException) -> None:
            return None

        monkeypatch.setattr("sentry_sdk.capture_exception", capture_exception)

        with pytest.raises(HTTPException) as exc_info:
            await tracker_service_error_handler(MagicMock(), TrackerServiceError(sensitive_detail))

        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Tracker service operation failed"
        assert sensitive_detail in caplog.text

    async def test_fetch_benchmark_tasks_hides_downstream_error(self, monkeypatch: MonkeyPatch) -> None:
        """
        Verify downstream benchmark-service diagnostics are not reflected to callers.

        Test cases:
        - The endpoint retains its gateway-error status.
        - The downstream sentinel is absent from the stable response detail.
        """
        sensitive_detail = "sensitive-downstream-provider-detail"

        async def _raise_downstream_error(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            raise BenchmarkServiceError(sensitive_detail)

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _raise_downstream_error)

        response = client.post(
            "/fetch-benchmark-tasks",
            json={"benchmark_name": "swebench", "dataset": "verified"},
        )

        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to fetch task ids from benchmark service"}
        assert sensitive_detail not in response.text

    async def test_fetch_benchmark_tasks(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_1", "task_2"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        response = client.post(
            "/fetch-benchmark-tasks",
            json={
                "benchmark_name": "swebench",
                "dataset": "verified",
            },
        )

        assert response.status_code == 200
        assert response.json() == {"task_ids": ["task_1", "task_2"]}

    async def test_start_benchmark(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        """Test start benchmark of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Start timestamp is in UTC timezone
            - Returning task count provided from the verify_task_ids function
            - Benchmark row has been created and pushed to the database
        """

        # Example request sent from the cli to the fastapi server
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[f"task_{i}" for i in range(500)])

        monkeypatch.setattr(
            BenchmarkServiceClient,
            "verify_task_ids",
            _mock_verify_task_ids,
        )

        # Send request to start the run and ensure that the start response is returned
        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
        )

        # Test case 1. Returns 200 OK
        assert response.status_code == 200
        json_response = response.json()

        # Test case 2. Benchmark row has been created and pushed to the database
        benchmark_row = database_session.get(Benchmark, UUID(json_response["benchmark_id"]))
        assert benchmark_row

        # Secondary test. Arguments is correct serialized into the database
        assert benchmark_row.arguments == BenchmarkArguments(
            contract=request.contract,
            concurrency=request.concurrency,
            task_ids=None,
            slice_str=None,
            sandbox_provider_secret_name=harness_config.sandbox_provider_secret_name,
        )

        # Test case 3. Start timestamp is in UTC timezone and matches the benchmark row
        assert isoparse(json_response["started_at"]) == benchmark_row.started_at.replace(tzinfo=timezone.utc)

        # Test case 4. Returning task count provided from the verify_task_ids function
        assert json_response["task_count"] == 500

        # Remaining fields match what we passed into the request
        assert json_response["benchmark_name"] == request.benchmark_name
        assert json_response["agent_name"] == request.contract.name
        assert json_response["concurrency"] == request.concurrency

    async def test_start_benchmark_returns_502_when_benchmark_service_is_unreachable(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_health_check(*_args: Any, **_kwargs: Any) -> None:
            raise httpx.ConnectError("Name or service not known")

        monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)

        no_raise_client = TestClient(app, raise_server_exceptions=False)
        response = no_raise_client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 502
        assert response.json() == {"detail": "Benchmark service 'swebench' is not reachable"}

    async def test_start_benchmark_forwards_tracker_api_key_to_benchmark_service(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        mock_kicker: MockKicker,
    ) -> None:
        observed_headers: dict[str, str] = {}

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_health_check(
            service_client: BenchmarkServiceClient, *_args: Any, **_kwargs: Any
        ) -> dict[str, str]:
            observed_headers.update(getattr(service_client, "_headers"))
            return {"status": "ok"}

        async def _mock_verify_task_ids(
            service_client: BenchmarkServiceClient, *_args: Any, **_kwargs: Any
        ) -> VerifyTaskIdsResponse:
            observed_headers.update(getattr(service_client, "_headers"))
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["service_headers"]["X-Descope-Api-Key"] == "tracker-api-key"

    async def test_start_benchmark_keeps_selected_provider_secret_with_harness_headers(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
        mock_kicker: MockKicker,
    ) -> None:
        """Start requests should keep the provider secret chosen by the client.

        Test cases:
        - Harness headers provide AWS config without a provider secret.
        - The selected provider secret from the request body is stored and queued.
        """
        selected_harness_config = harness_config.model_copy(update={"sandbox_provider_secret_name": "ModalSecrets"})
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=selected_harness_config,
            sandbox_provider="modal",
        )

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={
                "x-harness-aws-access-key-id": harness_config.aws.aws_access_key_id,
                "x-harness-aws-secret-access-key": harness_config.aws.aws_secret_access_key,
                "x-harness-aws-default-region": harness_config.aws.aws_default_region,
                "x-harness-s3-bucket": harness_config.s3_bucket,
                "x-harness-log-group": harness_config.log_group,
                "x-harness-log-retention-policy": str(harness_config.log_retention_policy),
            },
        )

        assert response.status_code == 200
        benchmark_row = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert benchmark_row
        assert benchmark_row.arguments.sandbox_provider == "modal"
        assert benchmark_row.arguments.sandbox_provider_secret_name == "ModalSecrets"

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["sandbox_provider"] == "modal"
        assert queued_request["harness_config"]["sandbox_provider_secret_name"] == "ModalSecrets"

    async def test_fetch_benchmark(self, database_session: Session, example_benchmark_object: Benchmark) -> None:
        """Test fetch benchmark of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Raising exception if benchmark row is not found
            - Existing benchmark without discovered tasks returns empty progress
            - Benchmark details are returned in the response
            - Benchmark details are updated as benchmark progresses
            - Run-level errors are returned only after the benchmark reaches ERROR
        """

        # Test case 1. Return 404 Not Found if benchmark does not exist
        query_params = {"benchmark_id": str(uuid4())}
        response = client.get("/fetch-benchmark", params=query_params)
        assert response.status_code == 404

        # Add benchmark row to the database to fetch
        benchmark_row = example_benchmark_object

        database_session.add(benchmark_row)
        database_session.commit()

        # Fetch during the interval between benchmark creation and task discovery.
        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/fetch-benchmark", params=query_params)

        assert response.status_code == 200

        details = response.json()["details"]
        assert details["total_tasks"] == 0
        assert details["finished_tasks"] == 0
        assert details["task_breakdown"] == {}

        # Push some task rows that we can use to check the progress of the benchmark
        task_rows = [Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id) for i in range(10)]
        database_session.add_all(task_rows)
        database_session.commit()

        # Send request to fetch the benchmark and ensure that the fetch response is returned
        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/fetch-benchmark", params=query_params)

        # Test case 2. Returns 200 OK
        assert response.status_code == 200
        details = response.json().get("details")
        assert details

        # Test case 3. Benchmark details are returned in the response
        # NOTE: Let's just check the fields that are being tracked and are due to change
        assert details.get("status") == BenchmarkStatus.IN_PROGRESS
        assert details.get("total_tasks") == 10
        assert details.get("finished_tasks") == 0
        assert response.json().get("error_message") is None

        # Test case 4. Benchmark details are updated as benchmark progresses
        # Change a few to in progress, finished and error
        # NOTE: as of now only error and finished are "task ended" states
        for i, task_row in enumerate(task_rows[:9]):
            if i < 3:
                task_row.status = TaskStatus.IN_PROGRESS
            elif i < 6:
                task_row.status = TaskStatus.FINISHED

                # Create evaluation result rows for finished tasks
                evaluation_result_row = EvaluationResult(
                    org_id=TEST_ORG_ID,
                    task=task_row.id,
                    instance_id=str(uuid4()),
                    result={"finished": True},
                )

                database_session.add(evaluation_result_row)
            else:
                task_row.status = TaskStatus.ERROR
                database_session.add(
                    ErrorResult(
                        org_id=TEST_ORG_ID,
                        task=task_row.id,
                        error_message="Error occured during task execution or evaluation",
                    )
                )

            database_session.add(task_row)

        database_session.commit()

        # Send request to fetch the benchmark and ensure that the fetch response is returned
        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/fetch-benchmark", params=query_params)
        details = response.json().get("details")
        assert details

        # Test case 5. Benchmark details are updated as benchmark progresses
        assert details.get("total_tasks") and details["total_tasks"] == 10
        assert details.get("finished_tasks") and details["finished_tasks"] == 6

        final_evaluation_row = FinalEvaluation(
            org_id=TEST_ORG_ID,
            benchmark=benchmark_row.id,
            final_score=83.25,
            properties={},
        )
        database_session.add(final_evaluation_row)
        database_session.commit()
        database_session.expire_all()

        response = client.get("/fetch-benchmark", params=query_params)

        # Test case 6. Final score is returned when the benchmark has a final evaluation
        assert response.status_code == 200
        assert response.json().get("final_score") == 83.25

        benchmark_row.status = BenchmarkStatus.ERROR
        benchmark_row.error_message = "Dominant task error affecting 10/10 tasks"
        database_session.add(benchmark_row)
        database_session.commit()

        response = client.get("/fetch-benchmark", params=query_params)

        # Test case 7. Terminal errors return the stored run-level message
        assert response.status_code == 200
        assert response.json().get("error_message") == "Dominant task error affecting 10/10 tasks"

    async def test_retrieve_results(
        self, monkeypatch: MonkeyPatch, database_session: Session, example_benchmark_object: Benchmark
    ) -> None:
        """Test the retrieve results endpoint of the fastapi server.

        Test Cases:
            - 404 on invalid benchmark id
            - Final evaluation is ommited if benchmark has not finished yet
            - Evaluation results are returned as the tasks are being completed
            - Works when no tasks are completed
            - Base fields are included within response
            - Tasks stopped field is populated when we stop the benchmark
            - Task errors field is populated when we encounter an error
        """

        # Test case 1. 404 on invalid benchmark id
        query_params = {"benchmark_id": str(uuid4())}
        response = client.get("/retrieve-results", params=query_params)
        assert response.status_code == 404

        # Add benchmark row
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/retrieve-results", params=query_params)
        assert response.status_code == 200
        response_json = response.json()

        # Base fields are included within response
        assert response_json.get("benchmark_name") == benchmark_row.name
        assert response_json.get("status") == benchmark_row.status

        # Test case 2. Final evaluation is ommited if benchmark has not finished yet
        assert response_json.get("final_evaluation") is None

        # Test case 3. Empty evaluation result are returned if no tasks are completed
        assert response_json.get("evaluation_results") == {}

        # Test case 4. Evaluation results are returned as the tasks are being completed
        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            for i in range(10)
        ]
        evaluation_result_rows = [
            EvaluationResult(org_id=TEST_ORG_ID, task=task_row.id, instance_id=str(uuid4()), result={"finished": True})
            for task_row in task_rows
        ]
        database_session.add_all(task_rows)
        database_session.add_all(evaluation_result_rows)
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params)
        assert response.status_code == 200

        # NOTE: We have defaults so we need to exclude none to get the same response as the user
        response_json = FinalViewResponse(**response.json()).model_dump(exclude_none=True)

        # Test case 5. Evaluation results are returned if they exist even if benchmark has not finished yet
        assert response_json.get("evaluation_results")
        assert len(response_json.get("evaluation_results", {})) == 10

        # No stopped tasks or task_errors fields in response
        assert "tasks_stopped" not in response_json
        assert "task_errors" not in response_json

        # Change benchmark status to finished and add final evaluation row
        # Refresh to get the latest state
        database_session.refresh(benchmark_row)
        benchmark_row.status = BenchmarkStatus.FINISHED
        database_session.add(benchmark_row)
        database_session.commit()

        final_evaluation_row = FinalEvaluation(
            org_id=TEST_ORG_ID,
            benchmark=benchmark_row.id,
            final_score=100,
            properties={"resolved_tasks": [], "unresolved_tasks": []},
        )
        database_session.add(final_evaluation_row)
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params)
        assert response.status_code == 200
        response_json = response.json()

        # Test case 6. Final evaluation now exists
        assert response_json.get("final_evaluation")
        assert response_json.get("final_evaluation").get("final_score") == 100

        # Test case 7. Tasks stopped field is populated when we stop the benchmark
        # Stop the benchmark
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Add some new tasks with the status stopped
        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
            for i in range(11, 21)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params)
        assert response.status_code == 200
        response_json = response.json()

        # NOTE: We chose to use a number instead of a list or string since some benchmarks have a lot of tasks
        assert response_json.get("tasks_stopped") == 10
        assert len(response_json.get("evaluation_results")) == 10

        # Test case 8. Task errors field is populated when we encounter an error
        # Add some new tasks with the status error (one with ErrorResult and one without)
        error_message = "Error occured during task execution or evaluation"
        task_rows = [
            Task(
                org_id=TEST_ORG_ID,
                task_id=f"task_{i}",
                benchmark=benchmark_row.id,
                status=TaskStatus.ERROR,
            )
            for i in range(22, 24)
        ]
        database_session.add_all(task_rows)
        database_session.flush()
        database_session.add(
            ErrorResult(
                org_id=TEST_ORG_ID,
                task=task_rows[0].id,
                error_message=error_message,
            )
        )
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params)
        assert response.status_code == 200
        response_json = response.json()

        # Ensure we did not lose any previous data
        assert response_json.get("tasks_stopped") == 10
        assert len(response_json.get("evaluation_results")) == 10
        assert response_json.get("final_evaluation")

        # Check for tasks with error
        assert response_json.get("task_errors")
        assert len(response_json.get("task_errors")) == 2

        # Error message we saved was returned in the response
        assert response_json.get("task_errors").get("task_22") == error_message

        # If we did not get an error message, we return a default message
        assert response_json.get("task_errors").get("task_23") == "No error message was provided"

        # Test case 9. task_ids subset filters evaluation_results and recomputes final_score
        observed_headers: dict[str, str] = {}
        observed_results: dict[str, Any] = {}

        async def _mock_final_score(client: BenchmarkServiceClient, **kwargs: Any) -> FinalScoreResponse:
            observed_headers.update(getattr(client, "_headers"))
            observed_results.clear()
            observed_results.update(kwargs["evaluation_results"])
            ids = list(kwargs["evaluation_results"].keys())
            return FinalScoreResponse(tasks_evaluated=ids, final_score=float(len(ids)), metadata={})

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
        response = client.get(
            "/retrieve-results",
            params=[("benchmark_id", str(benchmark_row.id)), ("task_ids", "task_1"), ("task_ids", "task_3")],
            headers={"X-Api-Key": "tracker-api-key"},
        )
        assert response.status_code == 200
        body = response.json()
        assert set(body["evaluation_results"]) == {"task_1", "task_3"}
        assert body["final_evaluation"]["final_score"] == 2.0
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"

        # Test case 10. A requested task without a result (stopped/errored/missing) is still
        # scored: it is passed to the benchmark service as {task_id: None}, contributing to
        # the denominator rather than being silently dropped from the subset.
        response = client.get(
            "/retrieve-results",
            params=[("benchmark_id", str(benchmark_row.id)), ("task_ids", "task_1"), ("task_ids", "task_11")],
            headers={"X-Api-Key": "tracker-api-key"},
        )
        assert response.status_code == 200
        assert observed_results.keys() == {"task_1", "task_11"}
        assert observed_results["task_11"] is None
        assert response.json()["final_evaluation"]["final_score"] == 2.0

    async def test_benchmark_error_handling(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """Test benchmark error handling of the fastapi server.

        Test Cases:
            - Verify failure returns 502 with the error message
            - No benchmark row is created when pre-flight checks fail
        """

        # Expection is raised if verify task ids fails
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", self._mock_verify_task_ids_error)

        row_count_before = len(database_session.exec(select(Benchmark)).all())

        # Example request sent from the cli to the fastapi server
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        response = client.post("/start-benchmark", json=request.model_dump())

        # Test case 1. Verify failure returns a stable 502 without internal detail
        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to verify task ids"}
        assert "Error verifying task ids" not in response.text

        # Test case 2. No benchmark row is created when pre-flight checks fail
        row_count_after = len(database_session.exec(select(Benchmark)).all())
        assert row_count_after == row_count_before

    async def test_fetch_benchmarks(self, database_session: Session, example_benchmark_object: Benchmark) -> None:
        """Test fetch benchmarks of the fastapi server.

        Test Cases:
            - Fetch using no filters all all, returns 5 benchmarks with total count
            - can fetch using contract name, benchamrk name and status
            - Can order by started at
            - Edge cases with no benchmarks found
        """

        fetch_benchmarks_request = FetchBenchmarksRequest()

        # When no benchmarks have been created yet, we return an empty list and total count of 0
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("benchmarks") == []
        assert response_json.get("total_count") == 0

        # Add benchmark row to the database to fetch
        database_session.add(example_benchmark_object)
        database_session.commit()

        # Add benchmark name to be a random string (expected no matches)
        fetch_benchmarks_request.benchmark_name = [str(uuid4())]

        # When we fetch with no benchmarks found, we return an empty list and total count of 0
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("benchmarks") == []
        assert response_json.get("total_count") == 0

        # Create 4 more benchmark rows that have the same data
        benchmark_rows = [
            Benchmark(org_id=TEST_ORG_ID, id=uuid4(), name="swebench", arguments=example_benchmark_object.arguments)
            for _ in range(4)
        ]
        for benchmark_row in benchmark_rows:
            database_session.add(benchmark_row)
            database_session.commit()

        # Create benchmark with unique data
        unique_contract = AgentContractRequest(
            name="terminus_2",
            install_cmd="echo installing dependencies...",
            run_cmd="echo running agent...",
        )
        unique_benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name="terminal_bench",
            arguments=BenchmarkArguments(
                contract=unique_contract,
                concurrency=5,
                task_ids=None,
                slice_str=None,
                dataset="terminal-bench-2.1",
            ),
        )
        database_session.add(unique_benchmark)
        database_session.commit()

        # Search for the 4 benchmarks just created + the original one we added before
        fetch_benchmarks_request.benchmark_name = ["swebench"]
        fetch_benchmarks_request.agent_name = ["dummy"]
        fetch_benchmarks_request.status = [BenchmarkStatus.IN_PROGRESS]

        # When we fetch with benchmarks found, we return a 200 OK
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("total_count") == 5
        assert len(response_json.get("benchmarks")) == 5

        expected_fields = set(BenchmarkTableRow.model_fields.keys())
        for row in response_json["benchmarks"]:
            assert set(row.keys()) == expected_fields

        # Clear filters and search again (checking limit and total)
        fetch_benchmarks_request.benchmark_name = None  # type: ignore[assignment]
        fetch_benchmarks_request.agent_name = None  # type: ignore[assignment]
        fetch_benchmarks_request.status = None  # type: ignore[assignment]

        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()

        # There are 6 total benchmarks (default limit is 50, so all are returned)
        assert response_json.get("total_count") == 6
        assert len(response_json.get("benchmarks")) == 6

        # Change benchmark status to finished and search again
        unique_benchmark.status = BenchmarkStatus.FINISHED
        database_session.add(unique_benchmark)
        database_session.commit()

        # Search for finished benchmarks
        fetch_benchmarks_request.status = [BenchmarkStatus.FINISHED]

        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()

        # There is 1 finished benchmark
        assert response_json.get("total_count") == 1
        assert len(response_json.get("benchmarks")) == 1

    async def test_start_benchmark_accepts_custom_service_from_request(
        self,
        contract: AgentContractRequest,
        harness_config: HarnessConfig,
    ) -> None:
        allowed_url = "http://internal-swebench.example.com:8001"
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=5,
            task_ids=None,
            harness_config=harness_config,
            custom_benchmark_service=allowed_url,
        )

        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 200

    async def test_fetch_benchmarks_filters_by_dataset(
        self, database_session: Session, contract: AgentContractRequest
    ) -> None:
        benchmark_rows = [
            Benchmark(
                org_id=TEST_ORG_ID,
                name="terminal-bench",
                arguments=BenchmarkArguments(contract=contract, concurrency=1, dataset=None),
            ),
            Benchmark(
                org_id=TEST_ORG_ID,
                name="terminal-bench",
                arguments=BenchmarkArguments(contract=contract, concurrency=1, dataset="default"),
            ),
            Benchmark(
                org_id=TEST_ORG_ID,
                name="terminal-bench",
                arguments=BenchmarkArguments(contract=contract, concurrency=1, dataset="terminal-bench-2.1"),
            ),
        ]
        database_session.add_all(benchmark_rows)
        database_session.commit()

        response = client.get("/fetch-benchmarks", params={"dataset": "terminal-bench-2.1", "limit": 10})

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_count"] == 1
        assert response_json["benchmarks"][0]["dataset"] == "terminal-bench-2.1"

        response = client.get("/fetch-benchmarks", params={"dataset": "default", "limit": 10})

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_count"] == 2
        assert {row["dataset"] for row in response_json["benchmarks"]} == {"default"}

    async def test_start_benchmark_writes_started_by_columns(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        test_org = Org(id=TEST_ORG_ID, name="default")
        app.dependency_overrides[get_current_starter] = lambda: RequestIdentity(
            org=test_org,
            access_key_id="K2abc",
            email="alice@vals.ai",
            name="Alice",
        )

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 200
        benchmark_id = UUID(response.json()["benchmark_id"])

        benchmark_row = database_session.get(Benchmark, benchmark_id)
        assert benchmark_row is not None
        assert benchmark_row.started_by_id == "K2abc"
        assert benchmark_row.started_by_email == "alice@vals.ai"

    async def test_start_benchmark_self_hosted_writes_nulls(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        # The autouse override_starter fixture already returns a self-hosted identity.
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        response = client.post("/start-benchmark", json=request.model_dump())
        assert response.status_code == 200

        benchmark_id = UUID(response.json()["benchmark_id"])
        benchmark_row = database_session.get(Benchmark, benchmark_id)
        assert benchmark_row is not None
        assert benchmark_row.started_by_id is None
        assert benchmark_row.started_by_email is None

    async def test_start_benchmark_warns_when_email_claim_missing(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Hosted-mode start with an email-less access key emits a one-shot warning. Self-hosted
        identity (access_key_id is None) must NOT warn — that's the bug fix for the warning
        firing on every authenticated request.
        """
        test_org = Org(id=TEST_ORG_ID, name="default")
        app.dependency_overrides[get_current_starter] = lambda: RequestIdentity(
            org=test_org,
            access_key_id="K2abc",
            email=None,
            name=None,
        )

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        with caplog.at_level(logging.WARNING, logger="main"):
            response = client.post("/start-benchmark", json=request.model_dump())
        assert response.status_code == 200
        benchmark_id = response.json()["benchmark_id"]

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "email" in r.message]
        assert len(warnings) == 1
        assert "K2abc" in warnings[0].message
        assert getattr(warnings[0], "benchmark_id") == benchmark_id

    async def test_init_org_returns_email_claim_present(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("main.AUTH_REQUIRED", True)

        mock_client = MagicMock(spec=DescopeClient)
        mock_client.exchange_access_key.return_value = {
            "tenants": {"test-tenant": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "tenants": {"test-tenant": {}},
                "email": "alice@vals.ai",
                "name": "Alice",
            },
        }
        monkeypatch.setattr("tracker.auth._descope_client", mock_client)

        response = client.post("/init", headers={"X-Api-Key": "valid-key"})
        assert response.status_code == 200
        body = response.json()
        assert body["email_claim_missing"] is False
        assert body["org_name"] == "test-tenant"

    async def test_init_org_returns_email_claim_missing(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("main.AUTH_REQUIRED", True)

        mock_client = MagicMock(spec=DescopeClient)
        mock_client.exchange_access_key.return_value = {
            "tenants": {"test-tenant": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "tenants": {"test-tenant": {}},
            },
        }
        monkeypatch.setattr("tracker.auth._descope_client", mock_client)

        response = client.post("/init", headers={"X-Api-Key": "valid-key"})
        assert response.status_code == 200
        assert response.json()["email_claim_missing"] is True

    async def test_init_org_uses_bound_user_email_when_email_claim_missing(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("main.AUTH_REQUIRED", True)

        mock_client = MagicMock(spec=DescopeClient)
        mock_client.exchange_access_key.return_value = {
            "tenants": {"test-tenant": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "tenants": {"test-tenant": {}},
                "customClaims": {"user_id": "U2abc"},
            },
        }
        mock_client.mgmt.user.load_by_user_id.return_value = {
            "user": {
                "email": "alice@vals.ai",
                "displayName": "Alice",
            },
        }
        monkeypatch.setattr("tracker.auth._descope_client", mock_client)

        response = client.post("/init", headers={"X-Api-Key": "valid-key"})
        assert response.status_code == 200
        assert response.json()["email_claim_missing"] is False
        mock_client.mgmt.user.load_by_user_id.assert_called_once_with("U2abc")

    async def test_fetch_benchmarks_includes_started_by_email(
        self,
        contract: AgentContractRequest,
        database_session: Session,
    ) -> None:
        bench = Benchmark(
            org_id=TEST_ORG_ID,
            name="swebench",
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
            started_by_email="alice@vals.ai",
            started_by_id="K2abc",
        )
        database_session.add(bench)
        database_session.commit()

        response = client.get("/fetch-benchmarks", params={"limit": 10, "offset": 0})
        assert response.status_code == 200

        rows = response.json()["benchmarks"]
        assert any(r["started_by_email"] == "alice@vals.ai" for r in rows)

    async def test_fetch_benchmarks_started_by_query_param_filters(
        self,
        contract: AgentContractRequest,
        database_session: Session,
    ) -> None:
        """Regression: FastAPI's Depends(PydanticModel) doesn't bind list[str] from query params;
        started_by must be declared as a separate Query() parameter on the endpoint.
        """
        for email in ("alice@vals.ai", "bob@vals.ai", None):
            database_session.add(
                Benchmark(
                    org_id=TEST_ORG_ID,
                    name="swebench",
                    arguments=BenchmarkArguments(contract=contract, concurrency=1),
                    started_by_email=email,
                    started_by_id=f"K-{email or 'none'}",
                )
            )
        database_session.commit()

        response = client.get("/fetch-benchmarks", params={"started_by": "alice@vals.ai", "limit": 10})
        assert response.status_code == 200
        rows = response.json()["benchmarks"]
        assert len(rows) == 1
        assert rows[0]["started_by_email"] == "alice@vals.ai"

        # Repeated query params for multi-value filter
        response = client.get(
            "/fetch-benchmarks",
            params=[("started_by", "alice@vals.ai"), ("started_by", "bob@vals.ai"), ("limit", "10")],
        )
        assert response.status_code == 200
        rows = response.json()["benchmarks"]
        assert len(rows) == 2
        assert {r["started_by_email"] for r in rows} == {"alice@vals.ai", "bob@vals.ai"}

    async def test_run_label_is_persisted_fetchable_and_filterable(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        """Run labels should persist on start and be visible through fetch and list.

        Test cases:
            - Start stores the label on the benchmark row.
            - Fetch and list responses expose the label, and list can filter by it.
        """

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
            label="nightly",
        )
        start_response = client.post("/start-benchmark", json=request.model_dump())
        assert start_response.status_code == 200
        benchmark_id = UUID(start_response.json()["benchmark_id"])

        benchmark_row = database_session.get(Benchmark, benchmark_id)
        assert benchmark_row is not None
        assert benchmark_row.label == "nightly"

        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_0", status=TaskStatus.PENDING, benchmark=benchmark_id)
        )
        database_session.commit()

        fetch_response = client.get("/fetch-benchmark", params={"benchmark_id": str(benchmark_id)})
        assert fetch_response.status_code == 200
        assert fetch_response.json()["label"] == "nightly"

        list_response = client.get("/fetch-benchmarks", params={"label": "nightly", "limit": 10})
        assert list_response.status_code == 200
        rows = list_response.json()["benchmarks"]
        assert len(rows) == 1
        assert rows[0]["id"] == str(benchmark_id)
        assert rows[0]["label"] == "nightly"

    async def test_fetch_benchmark_metadata_includes_started_by_email(
        self,
        contract: AgentContractRequest,
        database_session: Session,
    ) -> None:
        bench = Benchmark(
            org_id=TEST_ORG_ID,
            name="swebench",
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
            started_by_email="alice@vals.ai",
            started_by_id="K2abc",
        )
        database_session.add(bench)
        database_session.commit()

        response = client.get(f"/fetch-benchmark-metadata/{bench.id}")
        assert response.status_code == 200
        assert response.json()["started_by_email"] == "alice@vals.ai"

    async def test_fetch_run_outputs_streams_tar(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        observed_prefixes: list[str] = []

        async def _mock_list_s3_objects(prefix: str, *_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
            observed_prefixes.append(prefix)
            yield f"{prefix}output.txt"

        async def _mock_download_many_from_s3(
            keys: AsyncIterator[str], *_args: Any, **_kwargs: Any
        ) -> AsyncIterator[tuple[str, bytes]]:
            async for key in keys:
                yield key, b"output contents"

        monkeypatch.setattr("main.list_s3_objects", _mock_list_s3_objects)
        monkeypatch.setattr("main.download_many_from_s3", _mock_download_many_from_s3)

        response = client.get(
            f"/fetch-run-outputs/{example_benchmark_object.id}",
            params={"task_ids": ["task_1", "task_2"]},
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-tar"
        assert response.headers["content-disposition"] == (
            f"attachment; filename=benchmark_{example_benchmark_object.id}_outputs.tar"
        )
        assert observed_prefixes == [
            f"benchmarks/{example_benchmark_object.id}/task_1/",
            f"benchmarks/{example_benchmark_object.id}/task_2/",
        ]

        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as tar:
            assert tar.getnames() == ["task_1/output.txt", "task_2/output.txt"]
            task_file = tar.extractfile("task_1/output.txt")
            assert task_file is not None
            assert task_file.read() == b"output contents"

    async def test_fetch_run_outputs_omits_unsafe_tar_members(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        """Never copy unsafe S3 key suffixes into a downloaded tar."""
        database_session.add(example_benchmark_object)
        database_session.commit()

        async def _mock_list_s3_objects(prefix: str, *_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
            yield f"{prefix}task/../../outside.txt"
            yield f"{prefix}task/..\\outside.txt"
            yield f"{prefix}C:/outside.txt"
            yield f"{prefix}task//outside.txt"
            yield f"{prefix}task/hidden\x00.txt"
            yield f"{prefix}task/output.txt"

        async def _mock_download_many_from_s3(
            keys: AsyncIterator[str], *_args: Any, **_kwargs: Any
        ) -> AsyncIterator[tuple[str, bytes]]:
            async for key in keys:
                yield key, b"output contents"

        monkeypatch.setattr("main.list_s3_objects", _mock_list_s3_objects)
        monkeypatch.setattr("main.download_many_from_s3", _mock_download_many_from_s3)

        response = client.get(f"/fetch-run-outputs/{example_benchmark_object.id}")

        assert response.status_code == 200
        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as tar:
            assert tar.getnames() == ["task/output.txt"]

    async def test_fetch_run_outputs_returns_404_when_all_members_are_unsafe(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        """Do not download outputs when every listed tar member name is unsafe."""
        database_session.add(example_benchmark_object)
        database_session.commit()

        async def _mock_list_s3_objects(prefix: str, *_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
            yield f"{prefix}task/../../outside.txt"
            yield f"{prefix}task/hidden\x00.txt"

        download_many_from_s3 = MagicMock()
        monkeypatch.setattr("main.list_s3_objects", _mock_list_s3_objects)
        monkeypatch.setattr("main.download_many_from_s3", download_many_from_s3)

        response = client.get(f"/fetch-run-outputs/{example_benchmark_object.id}")

        assert response.status_code == 404
        assert response.json() == {"detail": f"No outputs found for run '{example_benchmark_object.id}'"}
        download_many_from_s3.assert_not_called()

    async def test_fetch_run_outputs_returns_404_when_empty(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        def _mock_list_s3_objects(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
            return async_iterator(())

        monkeypatch.setattr("main.list_s3_objects", _mock_list_s3_objects)

        response = client.get(f"/fetch-run-outputs/{example_benchmark_object.id}")

        assert response.status_code == 404
        assert response.json() == {"detail": f"No outputs found for run '{example_benchmark_object.id}'"}

    async def test_benchmark_service_unauthenticated_error_returns(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        """Test that BenchmarkServiceUnauthenticatedError returns 502 without capturing to Sentry.

        Test Cases:
            - /start-benchmark returns 502 when benchmark service returns 401
            - /fetch-benchmark-tasks returns 502 when benchmark service returns 401
            - /retry-or-resume-benchmark returns 502 when benchmark service returns 401
            - None of the above cases capture the exception to Sentry
        """
        captured: list[Exception] = []

        def capture_exception(exception: Exception) -> None:
            captured.append(exception)

        monkeypatch.setattr("sentry_sdk.capture_exception", capture_exception)

        async def _raise_unauth(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            raise BenchmarkServiceUnauthenticatedError("401 Unauthorized")

        no_raise_client = TestClient(app, raise_server_exceptions=False)

        # Case 1: /start-benchmark
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _raise_unauth)
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )

        response = no_raise_client.post("/start-benchmark", json=request.model_dump())
        assert response.status_code == 502
        assert response.json() == {"detail": "Benchmark service authentication failed"}
        assert "401 Unauthorized" not in response.text

        # Case 2: /fetch-benchmark-tasks
        response = no_raise_client.post(
            "/fetch-benchmark-tasks",
            json={"benchmark_name": "swebench"},
        )
        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to fetch task ids from benchmark service"}
        assert "401 Unauthorized" not in response.text

        # Case 3: /retry-or-resume-benchmark — needs at least one task so verify_task_ids is reached
        benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name="swebench",
            status=BenchmarkStatus.ERROR,
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
        )
        database_session.add(benchmark)
        database_session.commit()
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark.id, status=TaskStatus.ERROR)
        )
        database_session.commit()

        response = no_raise_client.post(
            f"/retry-or-resume-benchmark/{benchmark.id}",
            json={"task_ids": [], "service_headers": {}},
            params={"retry": "true"},
        )
        assert response.status_code == 502
        assert response.json() == {"detail": "Benchmark service authentication failed"}
        assert "401 Unauthorized" not in response.text

        # None of the three cases should have reached Sentry
        assert captured == []

    def test_fetch_benchmark_returns_400_when_harness_headers_missing(
        self,
        harness_config: HarnessConfig,
    ) -> None:
        """Missing X-Harness-* headers should return 400, not 500 KeyError."""
        app.dependency_overrides.pop(fetch_harness_config)
        try:
            response = client.get("/fetch-benchmark", params={"benchmark_id": str(uuid4())})
            assert response.status_code == 400
            assert "Missing harness config header" in response.json()["detail"]
        finally:
            app.dependency_overrides[fetch_harness_config] = lambda: harness_config
