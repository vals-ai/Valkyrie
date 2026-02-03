from asyncio import gather
from functools import partial
from sqlite3 import OperationalError
from typing import Any

from pytest import MonkeyPatch
from sqlmodel import Session, select

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    Task,
    TaskStatus,
)
from tracker.types import SetupTaskResponse, StartBenchmarkRequest
from tracker.utils import process_benchmark, process_task


class TestProcessBenchmark:
    @staticmethod
    async def _mock_install_dependencies(*args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    async def _mock_run_agent(*args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    async def _mock_upload_contract(*args: Any, **kwargs: Any) -> None:
        pass

    async def _test_request_evaluate_instance(
        self,
        original_method: Any,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, str]:
        """
        Test when the model finishes running and before the evaluation begins.
        expected status of the task is evaluating
        """
        from sqlmodel import Session

        import tracker.utils

        task_id: str = args[0]

        with Session(bind=tracker.utils.engine, expire_on_commit=False) as test_session:  # type: ignore[attr-defined]
            task_row = test_session.exec(select(Task).where(Task.task_id == task_id).limit(1)).first()

            assert task_row is not None, f"Task {task_id} not found"
            assert task_row.status == TaskStatus.EVALUATING, (
                f"Task {task_id} status is {task_row.status}, expected EVALUATING"
            )

        evaluation_result = await original_method(*args, **kwargs)

        return evaluation_result

    async def _test_run_agent(self, original_method: Any, *args: Any, **kwargs: Any) -> None:
        """
        Test task status is in progress before we start running the agent
        (confirms we move from pending to in progress status)
        """
        from sqlmodel import Session

        import tracker.utils

        task_id = args[3]

        with Session(bind=tracker.utils.engine, expire_on_commit=False) as test_session:  # type: ignore[attr-defined]
            task_row = test_session.exec(select(Task).where(Task.task_id == task_id).limit(1)).first()
            assert task_row is not None
            assert task_row.status == TaskStatus.IN_PROGRESS

    async def test_process_task(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        benchmark_service: BenchmarkService,
        monkeypatch: MonkeyPatch,
    ):
        task_id = "astropy__astropy-12907"
        task_row_mapping: dict[str, Task] = {}

        # Dependencies required to process the task that the user sends
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench", contract=contract, concurrency=5, task_ids=[task_id]
        )

        benchmark_row = BenchmarkService.start_benchmark_request_to_benchmark_object(start_benchmark_request)

        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(task_id=task_id, benchmark=benchmark_row.id)
        task_row_mapping[task_id] = task_row
        database_session.add(task_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        # Mock upload contract since we don't have actual contract files
        monkeypatch.setattr(
            "tracker.utils.upload_agent_artifacts",
            self._mock_upload_contract,
        )

        monkeypatch.setattr(
            "tracker.utils.install_agent_dependencies",
            self._mock_install_dependencies,
        )

        original_run_agent = self._mock_run_agent
        monkeypatch.setattr(
            "tracker.utils.run_agent",
            partial(self._test_run_agent, original_run_agent),
        )

        original_evaluate = benchmark_service.request_evaluate_instance
        monkeypatch.setattr(
            benchmark_service,
            "request_evaluate_instance",
            partial(self._test_request_evaluate_instance, original_evaluate),
        )

        # Starts and evaluates a single task inside using the benchmark service
        _ = await process_task(task_row, start_benchmark_request, benchmark_service, benchmark_row.id, task_id)

        # Ensure that the evaluation result is viewable from the database after the task has been processed
        evaluation_result = database_session.exec(
            select(EvaluationResult).where(EvaluationResult.task == task_row_mapping[task_id].id).limit(1)
        ).first()

        assert evaluation_result is not None

        # Ensure that the task status has been updated to finished
        database_session.refresh(task_row_mapping[task_id])
        assert task_row_mapping[task_id].status == TaskStatus.FINISHED

    async def test_process_benchmark(
        self, contract: AgentContractRequest, database_session: Session, monkeypatch: MonkeyPatch
    ):
        # Task ids sent by user to be processed
        task_ids: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]

        # Start run request sent by user to start the benchmark
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench", contract=contract, concurrency=5, task_ids=task_ids
        )

        # Create benchmark row inside of start run request
        benchmark_row = BenchmarkService.start_benchmark_request_to_benchmark_object(start_benchmark_request)

        database_session.add(benchmark_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        # Mock upload contract since we don't have actual contract files
        monkeypatch.setattr(
            "tracker.utils.upload_agent_artifacts",
            TestProcessBenchmark._mock_upload_contract,
        )

        monkeypatch.setattr(
            "tracker.utils.install_agent_dependencies",
            TestProcessBenchmark._mock_install_dependencies,
        )

        monkeypatch.setattr(
            "tracker.utils.run_agent",
            TestProcessBenchmark._mock_run_agent,
        )

        # Run the benchmark
        await process_benchmark(start_benchmark_request.model_dump(), str(benchmark_row.id), task_ids)

        # Benchmark status is updated to finished once the benchmark is done running
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED

        # Evaluation results exist for each task
        evaluation_results = benchmark_row.fetch_evaluation_results(database_session)

        # Same amount of evaluation results and same task ids
        assert len(evaluation_results) == len(task_ids)
        assert set(evaluation_results.keys()) == set(task_ids)

        # All evaluation results have been returned
        for evaluation_result in evaluation_results.values():
            assert evaluation_result is not None

        tasks = database_session.exec(
            select(Task).where((Task.benchmark == benchmark_row.id) & (Task.status == TaskStatus.FINISHED))
        ).all()

        # All tasks have been marked as finished
        assert len(tasks) == len(task_ids)

    async def test_process_benchmark_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ):
        """
        Test that we are correctly handling when a benchmark errors out

        Test Cases:
            - Benchmark row is marked as error if at any point we get an error outside of processing the tasks
            - Error message is set on the benchmark row
        """

        # Example tasks from swebench
        task_ids: list[str] = ["astropy__astropy-12907"]
        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench", contract=contract, concurrency=5, task_ids=task_ids
        )

        # Create benchmark row inside of start run request
        benchmark_row = BenchmarkService.start_benchmark_request_to_benchmark_object(start_benchmark_request)

        database_session.add(benchmark_row)
        database_session.commit()

        original_commit = Session.commit
        commit_count = {"count": 0}

        # Fail on the second commit
        def mock_commit_with_error(self: Session) -> None:
            commit_count["count"] += 1
            if commit_count["count"] == 1:
                raise OperationalError("Handled database error")

            original_commit(self)

        # Monkey patch failure to push benchmark row to the database
        monkeypatch.setattr(Session, "commit", mock_commit_with_error)
        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        monkeypatch.setattr(
            "tracker.utils.install_agent_dependencies",
            self._mock_install_dependencies,
        )

        monkeypatch.setattr(
            "tracker.utils.run_agent",
            self._mock_run_agent,
        )

        # Run the benchmark (error is not raised and instead handled)
        await process_benchmark(start_benchmark_request.model_dump(), str(benchmark_row.id), task_ids)

        # Benchmark status is updated to error once the benchmark is done running
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.ERROR
        assert "Handled database error" in (benchmark_row.error_message or "")

    async def test_process_task_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ):
        """
        Test that we are correctly handling when a task errors out

        Test Cases:
            - Task row is marked as error if process task fails
            - Error message is set on the task row
        """

        task_ids: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]

        start_benchmark_request = StartBenchmarkRequest(
            benchmark_name="swebench", contract=contract, concurrency=5, task_ids=task_ids
        )

        benchmark_row = BenchmarkService.start_benchmark_request_to_benchmark_object(start_benchmark_request)

        database_session.add(benchmark_row)
        database_session.commit()

        original_request_setup_task = BenchmarkService.request_setup_task

        async def mock_request_setup_task(self: Any, task_id: str, instance_id: str) -> SetupTaskResponse:
            if task_id == "astropy__astropy-13033":
                raise Exception("Exception raised while setting up the task")

            return await original_request_setup_task(self, task_id, instance_id)

        # Setup fails for the second task
        monkeypatch.setattr(
            BenchmarkService,
            "request_setup_task",
            mock_request_setup_task,
        )

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        # Mock upload contract since we don't have actual contract files
        monkeypatch.setattr(
            "tracker.utils.upload_agent_artifacts",
            TestProcessBenchmark._mock_upload_contract,
        )

        # Mock the install dependencies part in case dependencies break it does not affect this test
        monkeypatch.setattr(
            "tracker.utils.install_agent_dependencies",
            TestProcessBenchmark._mock_install_dependencies,
        )

        # Mock run agent part because we don't have an agent inside of the sandbox
        monkeypatch.setattr(
            "tracker.utils.run_agent",
            TestProcessBenchmark._mock_run_agent,
        )

        await process_benchmark(start_benchmark_request.model_dump(), str(benchmark_row.id), task_ids)

        # Benchmark status is still finished even though one task has errored out
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message

        tasks = database_session.exec(
            select(Task).where((Task.benchmark == benchmark_row.id) & (Task.status == TaskStatus.ERROR))
        ).all()

        # One task has been marked as error
        assert len(tasks) == 1

        # Error message is set on the task row
        assert "Exception raised while setting up the task" in (tasks[0].error_message or "")
        assert tasks[0].status == TaskStatus.ERROR

        final_evaluation = benchmark_row.final_evaluation
        assert final_evaluation

        evaluation_results = benchmark_row.fetch_evaluation_results(database_session)
        assert evaluation_results is not None
        assert len(list(evaluation_results.keys())) == 1, "Only one task should be evaluated"

    async def test_can_run_same_task(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ):
        """
        Test that we can run the same task at the same time as long as the benchmark is different.
        NOTE: This is important because we want to be able to run the same benchmark across multiple models at the same time.

        Test Cases:
            - Sandbox does not crash if we start the same task twice at the same time
        """

        # We will run the same task across two benchmarks at the same time
        task_id = "astropy__astropy-12907"

        # Create two benchmarks that we will run the same task across
        # NOTE: cannot use example object since we cannot clone the object
        benchmarks: list[Benchmark] = []
        for _ in range(2):
            benchmark_row = Benchmark(
                name="swebench",
                arguments=BenchmarkArguments(
                    contract=contract,
                    concurrency=5,
                    task_ids=[task_id],
                ),
            )
            benchmarks.append(benchmark_row)
            database_session.add(benchmark_row)

        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        # Mock installing and running agent part
        monkeypatch.setattr(
            "tracker.utils.upload_agent_artifacts",
            TestProcessBenchmark._mock_upload_contract,
        )

        monkeypatch.setattr(
            "tracker.utils.install_agent_dependencies",
            TestProcessBenchmark._mock_install_dependencies,
        )

        monkeypatch.setattr(
            "tracker.utils.run_agent",
            TestProcessBenchmark._mock_run_agent,
        )

        await gather(
            *[
                process_benchmark(
                    benchmark_row.start_benchmark_request.model_dump(),
                    str(benchmark_row.id),
                    benchmark_row.arguments.task_ids or [],
                )
                for benchmark_row in benchmarks
            ]
        )
