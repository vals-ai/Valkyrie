import logging
import traceback
import uuid
from asyncio import Semaphore, create_subprocess_exec, gather
from pathlib import Path
from random import sample
from typing import Any, cast
from uuid import UUID

import pytest
from benchmark_service import BenchmarkService
from daytona import AsyncDaytona
from pytest import MonkeyPatch
from sqlmodel import Session, col, create_engine, inspect, select
from tests.utils import build_task_environment
from tracker.database.models import Benchmark, BenchmarkStatus, EvaluationResult, FinalEvaluation, Task, TaskStatus

logger = logging.getLogger(__name__)


class TestDatabaseIntegration:
    async def _create_task(self, database_session: Session, task_id: str, benchmark_id: UUID) -> Task:
        task_row = Task(task_id=task_id, benchmark_id=benchmark_id)
        database_session.add(task_row)
        database_session.flush()

        return task_row

    async def _create_evaluation_result(
        self, database_session: Session, task_row: Task, evaluation_result: dict[str, Any]
    ) -> EvaluationResult:
        instance_id = evaluation_result["instance_id"]
        evaluation_result_row = EvaluationResult(
            task_id=cast(UUID, task_row.id), instance_id=instance_id, result=evaluation_result
        )
        database_session.add(evaluation_result_row)

        task_row.status = TaskStatus.FINISHED
        database_session.add(task_row)
        database_session.flush()

        return evaluation_result_row

    async def _create_final_evaluation(
        self, database_session: Session, benchmark_row: Benchmark, final_score_result: dict[str, Any]
    ) -> FinalEvaluation:
        final_evaluation_row = FinalEvaluation(
            benchmark_id=cast(UUID, benchmark_row.id),
            final_score=final_score_result["final_score"],
            resolved_tasks=final_score_result["resolved_tasks"],
            unresolved_tasks=final_score_result["unresolved_tasks"],
        )
        database_session.add(final_evaluation_row)
        database_session.flush()

        return final_evaluation_row

    async def test_create_tables(self, monkeypatch: MonkeyPatch, tmp_path: Path):
        """
        Test that the session.py file creates the database and tables when ran

        Test Cases:
        - When the session.py file is ran, the tracker.db file is created where expected
        - More than one table is created in the tracker.db file
        """

        try:
            database_location = tmp_path / "tracker.db"
            monkeypatch.setattr("tracker.database.session._DATABASE_LOCATION", str(database_location))
            monkeypatch.setenv("TEST_DATABASE_LOCATION", str(database_location))

            result = await create_subprocess_exec(
                "uv",
                "run",
                "python",
                "-m",
                "tracker.database.session",
            )
            stdout, stderr = await result.communicate()
            return_code = result.returncode

            if return_code != 0:
                pytest.fail(
                    f"Failed to create tables: {(stdout or b'').decode('utf-8')}: {(stderr or b'').decode('utf-8')}",
                )

            assert database_location.exists(), "Database file exists in location specified"

            engine = create_engine(f"sqlite:///{database_location}")

            inspector = inspect(engine)
            tables = inspector.get_table_names()
            assert len(tables) > 0, "Tables were not created in the database as expected"
        except Exception as e:
            pytest.fail(
                f"Failed to create tables: {e}: {traceback.format_exc()}",
            )

    async def test_database_integrity(self, database_session: Session):
        """
        Test the integrity of the database

        Test Cases:
            - Benchmark table finished_at timestamp is automatically set when the status is updated to finished
            - Task table finished_at timestamp is automatically set when the status is updated to finished
        """

        # Test the benchmark table
        benchmark_row = Benchmark(name="SWEBench benchmark")
        database_session.add(benchmark_row)

        # When created its in pending status
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert benchmark_row.finished_at is None

        # When the status is updated to finished, the finished_at timestamp should be set
        benchmark_row.status = BenchmarkStatus.FINISHED
        database_session.add(benchmark_row)
        database_session.flush()
        assert benchmark_row.finished_at, "Should be auto generated when the status is updated to finished"

        # Test the task table
        task_row = Task(task_id="task_id_1", benchmark_id=cast(UUID, benchmark_row.id))
        database_session.add(task_row)

        # When created its in starting status
        assert task_row.status == TaskStatus.STARTING
        assert task_row.finished_at is None

        # When the status is updated to finished, the finished_at timestamp should be set
        task_row.status = TaskStatus.FINISHED
        database_session.add(task_row)
        database_session.flush()
        assert task_row.finished_at, "Should be auto generated when the status is updated to finished"

    async def test_database_relations(self, database_session: Session):
        """
        Test the relationships between the tables and ensure that they are correctly being built

        Test Cases:
            - Benchmark table is created and a row can be pushed to the database
            - Task table is created and a row can be pushed to the database
            - EvaluationResult table is created and a row can be pushed to the database
        """

        # Add a new benchmark row to the database
        benchmark_name = "SWEBench benchmark"
        benchmark_row = Benchmark(name=benchmark_name)
        database_session.add(benchmark_row)

        # Can fetch it using the same id that it was created with
        fetched_benchmark_row = database_session.get(Benchmark, benchmark_row.id)

        # Base test cases
        assert fetched_benchmark_row
        assert fetched_benchmark_row.name == benchmark_row.name
        assert fetched_benchmark_row.started_at is not None
        assert fetched_benchmark_row.finished_at is None
        assert fetched_benchmark_row.status == BenchmarkStatus.IN_PROGRESS

        task_ids = ["task_id_1", "task_id_2", "task_id_3"]
        for task_id in task_ids:
            _ = await self._create_task(database_session, task_id, cast(UUID, benchmark_row.id))

        # Can fetch the tasks based off the benchmark id and that the tasks are created as expected
        fetch_tasks_query = (
            select(Task).where(Task.benchmark_id == benchmark_row.id).order_by(col(Task.started_at).asc())
        )
        fetched_tasks = database_session.exec(fetch_tasks_query).all()

        # Base test cases
        assert len(fetched_tasks) == len(task_ids)
        for task_id, task_row in zip(task_ids, fetched_tasks):
            assert task_row.task_id == task_id
            assert task_row.benchmark_id == benchmark_row.id
            assert task_row.status == TaskStatus.STARTING
            assert task_row.started_at is not None
            assert task_row.finished_at is None

        # Can add an evaluation result to the task when it is finished
        simulated_evaluation_results: dict[str, dict[str, Any]] = {
            task_id: {
                "task_id": task_id,
                "instance_id": str(uuid.uuid4()),
                "patch_successfully_applied": True,
                "resolved": True,
                "resolution_status": "FULL",
            }
            for task_id in task_ids
        }

        for task_id, evaluation_result in simulated_evaluation_results.items():
            # Fetch the task row based off of the human readable task_id and the benchmark foreign key
            task_row_query = (
                select(Task).where(Task.task_id == task_id and Task.benchmark_id == benchmark_row.id).limit(1)
            )
            task_row = database_session.exec(task_row_query).first()

            assert task_row is not None

            # Create the evaluation result row
            _ = await self._create_evaluation_result(database_session, task_row, evaluation_result)

        # Once the evaluation is completed, we can check if the task rows have been updated with the finished_at timestamp and status
        fetch_tasks_query = select(Task).where(Task.benchmark_id == benchmark_row.id)
        fetched_tasks = database_session.exec(fetch_tasks_query).all()

        assert len(fetched_tasks) == len(task_ids)
        for task_row in fetched_tasks:
            assert task_row.status == TaskStatus.FINISHED
            assert task_row.finished_at, (
                "Finished at timestamp should be auto generated when the task status has been updated"
            )

    async def _evaluate_instance(
        self,
        database_session: Session,
        benchmark_service: BenchmarkService,
        daytona_client: AsyncDaytona,
        task_row: Task,
        task_data: dict[str, str],
    ) -> dict[str, str]:
        docker_image: str = task_data["docker_image"]

        # Change the status of the task to evaluating before we start evaluation
        task_row.status = TaskStatus.EVALUATING
        database_session.add(task_row)
        database_session.flush()

        async with build_task_environment(daytona_client, task_row.task_id, docker_image) as sandbox:
            request_setup = task_data["request_setup"]
            if request_setup:
                response = await benchmark_service.request_setup_task(
                    task_id=task_row.task_id, instance_id=str(sandbox.id)
                )
                assert response == {"status": "ok"}

            response = await benchmark_service.request_evaluate_instance(
                task_id=task_row.task_id, instance_id=sandbox.id
            )

            return response

    async def test_end_to_end(
        self, database_session: Session, benchmark_service: BenchmarkService, daytona_client: AsyncDaytona
    ):
        """
        Test the end to end flow when using database with a benchmark service

        Test Cases:
            - Create a benchmark row to initiate a benchmark
            - Apply concurrency to tasks and ensure that the tasks are correctly being added to the database
            - As evaluation results come in, ensure that we are correclty adding them to the database
            - The metadata from the final evaluation can be fetched from the database
        """

        try:
            # Ensure that the benchmark service is running
            response = await benchmark_service.request_health_check()
            assert response == {"status": "ok"}

            # Create benchmark row to initiate a benchmark
            benchmark_row = Benchmark(name=benchmark_service.name)
            database_session.add(benchmark_row)
            database_session.flush()

            # Request all of the task ids from the benchmark service
            response = await benchmark_service.request_verify_task_ids(task_ids=None)
            task_ids = response.get("task_ids")

            # Returned all of the task ids from the swebench service
            assert task_ids is not None
            assert len(task_ids) == 500

            # NOTE: For testing we only are going to random sample the dataset
            # Also to mimic limits we are going to apply a concurrency limit
            task_ids = sample(task_ids, 10)

            logger.info(f"Sample of task ids returned: {task_ids[:10]}")

            # Create the task rows for each task we are going to run
            task_row_mapping: dict[str, Task] = {}
            for task_id in task_ids:
                task_row = await self._create_task(database_session, task_id, cast(UUID, benchmark_row.id))
                task_row_mapping[task_id] = task_row

            logger.info(f"Sample of task row mapping: {str(list(task_row_mapping.values())[:250])}")

            semaphore = Semaphore(5)
            evaluation_results: dict[str, dict[str, Any]] = {}

            task_information = await benchmark_service.request_retrieve_tasks(task_ids=task_ids)

            async def process_task(task_id: str, task_data: dict[str, str]) -> None:
                async with semaphore:
                    task_row = task_row_mapping[task_id]
                    evaluation_result = await self._evaluate_instance(
                        database_session, benchmark_service, daytona_client, task_row, task_data
                    )
                    evaluation_result_row = await self._create_evaluation_result(
                        database_session, task_row, evaluation_result
                    )
                    evaluation_results[task_id] = evaluation_result_row.result

            _ = await gather(*[process_task(task_id, task_data) for task_id, task_data in task_information.items()])

            logger.info(f"Sample of evaluation results: {str(list(evaluation_results.values())[:100])}")

            # When all tasks have been evaluated, we can make a request to get the final evaluation score
            response = await benchmark_service.request_final_score(evaluation_results=evaluation_results)

            logger.info(f"Final score response: {str(response)[:250]}")

            # Create the final evaluation row and add it to the database
            evaluation_result_row = await self._create_final_evaluation(database_session, benchmark_row, response)

            # Fetch the evaluation results from the final evaluation row
            fetched_evaluation_results = evaluation_result_row.fetch_evaluation_results(database_session)

            results = fetched_evaluation_results.all()
            assert len(results) == len(list(evaluation_results.keys()))

            logger.info(f"Sample of fetched evaluation results: {str(results[:100])}")

            for evaluation_result in results:
                task_row = database_session.get(Task, evaluation_result.task_id)
                assert task_row is not None
                assert evaluation_results.get(task_row.task_id)

            # Verify that the final evaluation row matches what we have in the database
            assert evaluation_result_row.final_score == response["final_score"]
            assert evaluation_result_row.resolved_tasks == response["resolved_tasks"]
            assert evaluation_result_row.unresolved_tasks == response["unresolved_tasks"]

        except Exception as e:
            pytest.fail(f"End to end test failed: {e}", pytrace=False)
