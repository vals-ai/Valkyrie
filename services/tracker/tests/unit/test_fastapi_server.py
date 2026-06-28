from collections.abc import AsyncIterator
import io
import logging
import tarfile
from datetime import timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceUnauthenticatedError
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
from dateutil.parser import isoparse
from descope import DescopeClient
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_starter
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.types import (
    AWSCredentials,
    BenchmarkTableRow,
    FetchBenchmarksRequest,
    FinalViewResponse,
    HarnessConfig,
    StartBenchmarkRequest,
)
from tracker.utils import fetch_harness_config

client = TestClient(app)


class TestFastapiServer:
    async def _mock_verify_task_ids_error(self, *args: Any, **kwargs: Any) -> VerifyTaskIdsResponse:
        raise Exception("Error verifying task ids")

    def test_health_check(self, monkeypatch: MonkeyPatch):
        """
        Test health check of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Response contains expected format
        """
        # Mock database connection check for unit tests
        monkeypatch.setattr("main.check_database_connection", lambda: True)

        response = client.get("/health")

        assert response.status_code == 200

        assert response.json() == {"status": "ok"}

    async def test_fetch_benchmark_tasks(
        self,
        monkeypatch: MonkeyPatch,
    ):
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
    ):
        """
        Test start benchmark of the fastapi server.

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
    ):
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_health_check(*_args: Any, **_kwargs: Any):
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
    ):
        observed_headers: dict[str, str] = {}
        captured_request_json: dict[str, Any] = {}

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_health_check(service_client: BenchmarkServiceClient, *args: Any, **kwargs: Any):
            observed_headers.update(service_client._headers)
            return {"status": "ok"}

        async def _mock_verify_task_ids(service_client: BenchmarkServiceClient, *args: Any, **kwargs: Any):
            observed_headers.update(service_client._headers)
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        class _MockKicker:
            def with_labels(self, **_kwargs: Any) -> "_MockKicker":
                return self

            async def kiq(self, **kwargs: Any) -> None:
                captured_request_json.update(kwargs["start_benchmark_request_json"])

        monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)
        monkeypatch.setattr("main.process_benchmark.kicker", lambda: _MockKicker())

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"
        assert captured_request_json["service_headers"]["X-Descope-Api-Key"] == "tracker-api-key"

    async def test_fetch_benchmark(self, database_session: Session, example_benchmark_object: Benchmark):
        """
        Test fetch benchmark of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Raising exception if benchmark row is not found
            - Benchmark details are returned in the response
            - Benchmark details are updated as benchmark progresses
        """

        # Test case 1. Return 404 Not Found if benchmark does not exist
        query_params = {"benchmark_id": str(uuid4())}
        response = client.get("/fetch-benchmark", params=query_params)
        assert response.status_code == 404

        # Add benchmark row to the database to fetch
        benchmark_row = example_benchmark_object

        database_session.add(benchmark_row)
        database_session.commit()

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
                task_row.error_message = "Error occured during task execution or evaluation"

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

    async def test_retrieve_results(
        self, monkeypatch: MonkeyPatch, database_session: Session, example_benchmark_object: Benchmark
    ):
        """
        Test the retrieve results endpoint of the fastapi server.

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
        # Add some new tasks with the status error (One with erro message and one without)
        error_message = "Error occured during task execution or evaluation"
        task_rows = [
            Task(
                org_id=TEST_ORG_ID,
                task_id=f"task_{i}",
                benchmark=benchmark_row.id,
                status=TaskStatus.ERROR,
                error_message=error_message if i == 22 else None,
            )
            for i in range(22, 24)
        ]
        database_session.add_all(task_rows)
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
            observed_headers.update(client._headers)
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
    ):
        """
        Test benchmark error handling of the fastapi server.

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

        # Test case 1. Verify failure returns 502 with the error message
        assert response.status_code == 502
        assert "Error verifying task ids" in response.json()["detail"]

        # Test case 2. No benchmark row is created when pre-flight checks fail
        row_count_after = len(database_session.exec(select(Benchmark)).all())
        assert row_count_after == row_count_before

    async def test_fetch_benchmarks(self, database_session: Session, example_benchmark_object: Benchmark):
        """
        Test fetch benchmarks of the fastapi server.

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
    ):
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

    async def test_fetch_benchmarks_filters_by_dataset(self, database_session: Session, contract: AgentContractRequest):
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
    ):
        test_org = Org(id=TEST_ORG_ID, name="default")
        app.dependency_overrides[get_current_starter] = lambda: RequestIdentity(
            org=test_org,
            access_key_id="K2abc",
            email="alice@vals.ai",
            name="Alice",
        )

        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

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
    ):
        # The autouse override_starter fixture already returns a self-hosted identity.
        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

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
    ):
        """Hosted-mode start with an email-less access key emits a one-shot warning. Self-hosted
        identity (access_key_id is None) must NOT warn — that's the bug fix for the warning
        firing on every authenticated request."""
        test_org = Org(id=TEST_ORG_ID, name="default")
        app.dependency_overrides[get_current_starter] = lambda: RequestIdentity(
            org=test_org,
            access_key_id="K2abc",
            email=None,
            name=None,
        )

        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

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
        assert warnings[0].benchmark_id == benchmark_id

    async def test_init_org_returns_email_claim_present(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
    ):
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
        database_session: Session,
    ):
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
        database_session: Session,
    ):
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
    ):
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
    ):
        """Regression: FastAPI's Depends(PydanticModel) doesn't bind list[str] from query params;
        started_by must be declared as a separate Query() parameter on the endpoint."""
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
    ):
        """Run labels should persist on start and be visible through fetch and list.

        Test cases:
            - Start stores the label on the benchmark row.
            - Fetch and list responses expose the label, and list can filter by it.
        """

        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

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
    ):
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
    ):
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

    async def test_fetch_run_outputs_returns_404_when_empty(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ):
        database_session.add(example_benchmark_object)
        database_session.commit()

        async def _mock_list_s3_objects(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
            if False:
                yield ""

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
    ):
        """
        Test that BenchmarkServiceUnauthenticatedError returns 502 without capturing to Sentry.

        Test Cases:
            - /start-benchmark returns 502 when benchmark service returns 401
            - /fetch-benchmark-tasks returns 502 when benchmark service returns 401
            - /retry-or-resume-benchmark returns 502 when benchmark service returns 401
            - None of the above cases capture the exception to Sentry
        """
        captured: list[Exception] = []
        monkeypatch.setattr("sentry_sdk.capture_exception", lambda exc: captured.append(exc))  # type: ignore

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

        # Case 2: /fetch-benchmark-tasks
        response = no_raise_client.post(
            "/fetch-benchmark-tasks",
            json={"benchmark_name": "swebench"},
        )
        assert response.status_code == 502

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

        # None of the three cases should have reached Sentry
        assert captured == []

    def test_fetch_benchmark_returns_400_when_harness_headers_missing(self):
        """Missing X-Harness-* headers should return 400, not 500 KeyError."""
        app.dependency_overrides.pop(fetch_harness_config)
        try:
            response = client.get("/fetch-benchmark", params={"benchmark_id": str(uuid4())})
            assert response.status_code == 400
            assert "Missing harness config header" in response.json()["detail"]
        finally:
            app.dependency_overrides[fetch_harness_config] = lambda: HarnessConfig(
                aws=AWSCredentials(
                    aws_access_key_id="test-aws-access-key-id",
                    aws_secret_access_key="test-aws-secret-access-key",
                    aws_default_region="test-aws-default-region",
                ),
                s3_bucket="test-bucket",
                log_group="test-log-group",
                log_retention_policy=30,
                sandbox_provider_secret_name="test-daytona-secret",
            )
