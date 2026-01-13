from asyncio import Semaphore
from functools import partial
from sqlite3 import OperationalError
from typing import Any

from pytest import MonkeyPatch
from sqlmodel import Session, select

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import Benchmark, BenchmarkArguments, BenchmarkStatus, EvaluationResult, Task, TaskStatus
from tracker.sandbox import run_agent
from tracker.types import StartRunRequest
from tracker.utils import process_benchmark, process_task


class TestProcessBenchmark:
    async def _test_request_evaluate_instance(
        self,
        original_method: Any,
        database_session: Session,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, str]:
        """
        Test when the model finishes running and before the evaluation begins.
        expected status of the task is evaluating
        """
        task_id: str = args[0]

        task_row = database_session.exec(select(Task).where(Task.task_id == task_id).limit(1)).first()

        assert task_row is not None
        assert task_row.status == TaskStatus.EVALUATING

        evaluation_result = await original_method(*args, **kwargs)

        return evaluation_result

    async def _test_run_agent(self, original_method: Any, database_session: Session, *args: Any, **kwargs: Any) -> None:
        """
        Test task status is pending before we start running the agent
        (confirms we move from starting to in progress status)
        """
        task_id: str = args[2]
        task_row = database_session.exec(select(Task).where(Task.task_id == task_id).limit(1)).first()
        assert task_row is not None
        assert task_row.status == TaskStatus.IN_PROGRESS

        await original_method(*args, **kwargs)

    async def test_process_task(
        self, database_session: Session, benchmark_service: BenchmarkService, monkeypatch: MonkeyPatch
    ):
        task_id = "astropy__astropy-12907"
        task_row_mapping: dict[str, Task] = {}

        # Dependencies required to process the task that the user sends
        start_run_request = StartRunRequest(
            benchmark_name="swebench", contract_name="claude_code", concurrency=5, task_ids=[task_id]
        )

        # Concurrency control for each task being processed inside of the benchmark
        semaphore = Semaphore(start_run_request.concurrency)

        benchmark_row = Benchmark(
            name=benchmark_service.name,
            arguments=BenchmarkArguments(
                contract_name=start_run_request.contract_name,
                concurrency=start_run_request.concurrency,
                task_ids=start_run_request.task_ids,
            ),
        )

        database_session.add(benchmark_row)
        database_session.commit()

        task_row = Task(task_id=task_id, benchmark=benchmark_row.id)
        task_row_mapping[task_id] = task_row
        database_session.add(task_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        original_evaluate = benchmark_service.request_evaluate_instance
        monkeypatch.setattr(
            benchmark_service,
            "request_evaluate_instance",
            partial(self._test_request_evaluate_instance, original_evaluate, database_session),
        )

        # Starts and evaluates a single task inside using the benchmark service
        _, _ = await process_task(task_row_mapping, start_run_request, semaphore, benchmark_service, task_id)

        # Ensure that the evaluation result is viewable from the database after the task has been processed
        evaluation_result = database_session.exec(
            select(EvaluationResult).where(EvaluationResult.task == task_row_mapping[task_id].id).limit(1)
        ).first()

        assert evaluation_result is not None

        # Ensure that the task status has been updated to finished
        database_session.refresh(task_row_mapping[task_id])
        assert task_row_mapping[task_id].status == TaskStatus.FINISHED

    async def test_process_benchmark(
        self, database_session: Session, benchmark_service: BenchmarkService, monkeypatch: MonkeyPatch
    ):
        # Task ids sent by user to be processed
        task_ids: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]

        # Start run request sent by user to start the benchmark
        start_run_request = StartRunRequest(
            benchmark_name="swebench", contract_name="claude_code", concurrency=5, task_ids=task_ids
        )

        # Create benchmark row inside of start run request
        benchmark_row = Benchmark(
            name=benchmark_service.name,
            arguments=BenchmarkArguments(
                contract_name=start_run_request.contract_name,
                concurrency=start_run_request.concurrency,
                task_ids=start_run_request.task_ids,
            ),
        )

        database_session.add(benchmark_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        original_run_agent = run_agent
        monkeypatch.setattr(
            "tracker.sandbox.run_agent",
            partial(self._test_run_agent, original_run_agent, database_session),
        )

        # Run the benchmark
        await process_benchmark(start_run_request, benchmark_row.id, task_ids, benchmark_service, database_session)

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
        self, database_session: Session, benchmark_service: BenchmarkService, monkeypatch: MonkeyPatch
    ):
        """
        Test that we are correctly handling when a benchmark errors out

        Test Cases:
            - Benchmark row is marked as error if at any point we get an error outside of processing the tasks
            - Error message is set on the benchmark row
        """

        # Example tasks from swebench
        task_ids: list[str] = ["astropy__astropy-12907"]
        start_run_request = StartRunRequest(
            benchmark_name="swebench", contract_name="claude_code", concurrency=5, task_ids=task_ids
        )

        # Create benchmark row inside of start run request
        benchmark_row = Benchmark(
            name=benchmark_service.name,
            arguments=BenchmarkArguments(
                contract_name=start_run_request.contract_name,
                concurrency=start_run_request.concurrency,
                task_ids=start_run_request.task_ids,
            ),
        )

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

        # Run the benchmark (error is not raised and instead handled)
        await process_benchmark(start_run_request, benchmark_row.id, task_ids, benchmark_service, database_session)

        # Benchmark status is updated to error once the benchmark is done running
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.ERROR
        assert benchmark_row.error_message == "Handled database error"

    async def test_process_task_error(
        self, database_session: Session, benchmark_service: BenchmarkService, monkeypatch: MonkeyPatch
    ):
        """
        Test that we are correctly handling when a task errors out

        Test Cases:
            - Task row is marked as error if process task fails
            - Error message is set on the task row
        """

        task_ids: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]

        start_run_request = StartRunRequest(
            benchmark_name="swebench", contract_name="claude_code", concurrency=5, task_ids=task_ids
        )

        benchmark_row = Benchmark(
            name=benchmark_service.name,
            arguments=BenchmarkArguments(
                contract_name=start_run_request.contract_name,
                concurrency=start_run_request.concurrency,
                task_ids=start_run_request.task_ids,
            ),
        )

        database_session.add(benchmark_row)
        database_session.commit()

        count = {"count": 0}

        async def mock_request_setup_task(original_method: Any, task_id: str, instance_id: str) -> None:
            count["count"] += 1
            if count["count"] == 2:
                raise Exception("Exception raised while setting up the task")

            return await original_method(task_id, instance_id)

        original_request_setup_task = benchmark_service.request_setup_task

        # Setup fails for the second task
        monkeypatch.setattr(
            benchmark_service,
            "request_setup_task",
            partial(mock_request_setup_task, original_request_setup_task),
        )

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)

        await process_benchmark(start_run_request, benchmark_row.id, task_ids, benchmark_service, database_session)

        # Benchmark status is still finished even though one task has errored out
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message

        tasks = database_session.exec(
            select(Task).where((Task.benchmark == benchmark_row.id) & (Task.status == TaskStatus.ERROR))
        ).all()

        # One task has been marked as error
        assert len(tasks) == 1

        # Error message is set on the task row
        assert tasks[0].error_message == "Exception raised while setting up the task"
        assert tasks[0].status == TaskStatus.ERROR

        final_evaluation = benchmark_row.fetch_final_evaluation(database_session)
        assert final_evaluation

        evaluation_results = final_evaluation.fetch_evaluation_results(database_session)
        assert evaluation_results is not None
        assert len(list(evaluation_results.keys())) == 1, "Only one task should be evaluated"
