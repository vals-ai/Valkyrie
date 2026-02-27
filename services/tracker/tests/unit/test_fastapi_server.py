import json
from datetime import timezone
from typing import Any
from uuid import UUID, uuid4

from dateutil.parser import isoparse
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session

from main import app
from benchmark_service.client import BenchmarkServiceClient
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Task,
    TaskStatus,
)
from benchmark_service.schemas import VerifyTaskIdsResponse
from tracker.types import (
    FetchBenchmarksRequest,
    FinalViewResponse,
    HarnessConfig,
    StartBenchmarkRequest,
)
from tests.unit.conftest import TEST_HARNESS_CONFIG

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

    async def test_start_benchmark(
        self, contract: AgentContractRequest, monkeypatch: MonkeyPatch, database_session: Session
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
            harness_config=TEST_HARNESS_CONFIG,
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
        )

        # Test case 3. Start timestamp is in UTC timezone and matches the benchmark row
        assert isoparse(json_response["started_at"]) == benchmark_row.started_at.replace(tzinfo=timezone.utc)

        # Test case 4. Returning task count provided from the verify_task_ids function
        assert json_response["task_count"] == 500

        # Remaining fields match what we passed into the request
        assert json_response["benchmark_name"] == request.benchmark_name
        assert json_response["agent_name"] == request.contract.name
        assert json_response["concurrency"] == request.concurrency

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
        task_rows = [Task(task_id=f"task_{i}", benchmark=benchmark_row.id) for i in range(10)]
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

    async def test_retrieve_results(self, database_session: Session, example_benchmark_object: Benchmark):
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
            Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED) for i in range(10)
        ]
        evaluation_result_rows = [
            EvaluationResult(task=task_row.id, instance_id=str(uuid4()), result={"finished": True})
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
            benchmark=benchmark_row.id, final_score=100, properties={"resolved_tasks": [], "unresolved_tasks": []}
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
            Task(task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED) for i in range(11, 21)
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

    async def test_benchmark_error_handling(
        self, contract: AgentContractRequest, database_session: Session, monkeypatch: MonkeyPatch
    ):
        """
        Test benchmark error handling of the fastapi server.

        Test Cases:
            - Returns error message from exception
            - Benchmark row is marked as error and error message is set
        """

        # Expection is raised if verify task ids fails
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", self._mock_verify_task_ids_error)

        # Example request sent from the cli to the fastapi server
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=TEST_HARNESS_CONFIG,
        )

        # Send request to start the benchmark and ensure that the start response is returned
        response = client.post("/start-benchmark", json=request.model_dump())

        # Test case 1. Returns error message from exception
        assert response.status_code == 500
        response_json = response.json()
        detail = json.loads(response_json.get("detail", "{}"))

        # benchmark id and error message are included in the response
        assert detail
        assert detail.get("benchmark_id")
        assert "Error verifying task ids" in detail.get("error_message")

        # Test case 2. Benchmark row is marked as error and error message is set
        benchmark_row = database_session.get(Benchmark, UUID(detail.get("benchmark_id")))
        assert benchmark_row
        assert benchmark_row.status == BenchmarkStatus.ERROR
        assert benchmark_row.error_message == detail.get("error_message")

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
        fetch_benchmarks_request.benchmark_name = str(uuid4())

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
            Benchmark(id=uuid4(), name="swebench", arguments=example_benchmark_object.arguments) for _ in range(4)
        ]
        for benchmark_row in benchmark_rows:
            database_session.add(benchmark_row)
            database_session.commit()

        # Create benchmark with unique data
        unique_contract = AgentContractRequest(
            name="terminus_2",
            artifacts=[],
            install_cmd="echo installing dependencies...",
            run_cmd="echo running agent...",
        )
        unique_benchmark = Benchmark(
            name="terminal_bench",
            arguments=BenchmarkArguments(contract=unique_contract, concurrency=5, task_ids=None, slice_str=None),
        )
        database_session.add(unique_benchmark)
        database_session.commit()

        # Search for the 4 benchmarks just created + the original one we added before
        fetch_benchmarks_request.benchmark_name = "swebench"
        fetch_benchmarks_request.agent_name = "claude_code"
        fetch_benchmarks_request.status = BenchmarkStatus.IN_PROGRESS

        # When we fetch with benchmarks found, we return a 200 OK
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("total_count") == 5
        assert len(response_json.get("benchmarks")) == 5

        # Clear filters and search again (checking limit and total)
        fetch_benchmarks_request.benchmark_name = None
        fetch_benchmarks_request.agent_name = None
        fetch_benchmarks_request.status = None

        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()

        # There are 6 total benchmarks
        assert response_json.get("total_count") == 6

        # Limit will always be 5
        assert len(response_json.get("benchmarks")) == 5

        # Change benchmark status to finished and search again
        unique_benchmark.status = BenchmarkStatus.FINISHED
        database_session.add(unique_benchmark)
        database_session.commit()

        # Search for finished benchmarks
        fetch_benchmarks_request.status = BenchmarkStatus.FINISHED

        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()

        # There is 1 finished benchmark
        assert response_json.get("total_count") == 1
        assert len(response_json.get("benchmarks")) == 1
