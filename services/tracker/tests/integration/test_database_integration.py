import logging
import uuid
from asyncio import Semaphore, gather
from random import sample
from typing import Any
from uuid import UUID

import pytest
from daytona import AsyncDaytona
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, inspect, select

from tests.utils import build_task_environment
from tracker.benchmark_service import BenchmarkService
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Task,
    TaskStatus,
)
from tracker.types import FinalScoreResponse, RetrieveTaskResponse

logger = logging.getLogger(__name__)


class TestDatabaseIntegration:
    async def _create_task(self, database_session: Session, task_id: str, benchmark_id: UUID) -> Task:
        task_row = Task(task_id=task_id, benchmark=benchmark_id)
        database_session.add(task_row)
        database_session.flush()

        return task_row

    async def _create_evaluation_result(
        self, database_session: Session, task_row: Task, evaluation_result: dict[str, Any]
    ) -> EvaluationResult:
        evaluation_result_row = EvaluationResult(
            task=task_row.id, instance_id=str(uuid.uuid4()), result=evaluation_result
        )
        database_session.add(evaluation_result_row)

        task_row.status = TaskStatus.FINISHED
        database_session.add(task_row)
        database_session.flush()

        return evaluation_result_row

    async def _create_final_evaluation(
        self, database_session: Session, benchmark_row: Benchmark, final_score_response: FinalScoreResponse
    ) -> FinalEvaluation:
        final_evaluation_row = FinalEvaluation(
            benchmark=benchmark_row.id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )
        database_session.add(final_evaluation_row)
        database_session.flush()

        return final_evaluation_row

    async def test_create_tables(self, postgres_engine: Engine):
        """
        Test that the database tables are created correctly.

        Test Cases:
        - All expected tables are created in the database
        - More than one table is created
        """
        inspector = inspect(postgres_engine)
        tables = inspector.get_table_names()

        assert len(tables) > 0, "Tables were not created in the database as expected"

        # Verify expected tables exist
        expected_tables = {"benchmark", "task", "evaluationresult", "finalevaluation"}
        assert expected_tables.issubset(set(tables)), f"Missing tables. Found: {tables}"

    async def test_database_integrity(self, database_session: Session, example_benchmark_object: Benchmark):
        """
        Test the integrity of the database

        Test Cases:
            - Benchmark table finished_at timestamp is automatically set when the status is updated to finished
            - Task table finished_at timestamp is automatically set when the status is updated to finished
        """

        # Test the benchmark table
        benchmark_row = example_benchmark_object
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
        task_row = Task(task_id="task_id_1", benchmark=benchmark_row.id)
        database_session.add(task_row)

        # When created its in pending status
        assert task_row.status == TaskStatus.PENDING
        assert task_row.finished_at is None

        # When the status is updated to finished, the finished_at timestamp should be set
        task_row.status = TaskStatus.FINISHED
        database_session.add(task_row)
        database_session.flush()
        assert task_row.finished_at, "Should be auto generated when the status is updated to finished"

        # When task status is marked as error, the finished_at timestamp should be set
        task_row = Task(task_id="task_id_2", benchmark=benchmark_row.id)
        task_row.status = TaskStatus.ERROR
        database_session.add(task_row)
        database_session.flush()
        assert task_row.finished_at, "Should be auto generated when the status is updated to error"

        # Reset the benchmark row to in progress status
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.finished_at = None
        database_session.add(benchmark_row)
        database_session.flush()

        # When the benchmark status is marked as error, the finished_at timestamp should be set
        benchmark_row.status = BenchmarkStatus.ERROR
        database_session.add(benchmark_row)
        database_session.flush()
        assert benchmark_row.finished_at, "Should be auto generated when the status is updated to error"

    async def test_database_relations(self, database_session: Session, example_benchmark_object: Benchmark):
        """
        Test the relationships between the tables and ensure that they are correctly being built

        Test Cases:
            - Benchmark table is created and a row can be pushed to the database
            - Task table is created and a row can be pushed to the database
            - EvaluationResult table is created and a row can be pushed to the database
        """

        # Add a new benchmark row to the database
        benchmark_row = example_benchmark_object
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
            _ = await self._create_task(database_session, task_id, benchmark_row.id)

        # Can fetch the tasks based off the benchmark id and that the tasks are created as expected
        fetch_tasks_query = select(Task).where(Task.benchmark == benchmark_row.id).order_by(col(Task.started_at).asc())
        fetched_tasks = database_session.exec(fetch_tasks_query).all()

        # Base test cases
        assert len(fetched_tasks) == len(task_ids)
        for task_id, task_row in zip(task_ids, fetched_tasks):
            assert task_row.task_id == task_id
            assert task_row.benchmark == benchmark_row.id
            assert task_row.status == TaskStatus.PENDING
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
            task_row_query = select(Task).where(Task.task_id == task_id and Task.benchmark == benchmark_row.id).limit(1)
            task_row = database_session.exec(task_row_query).first()

            assert task_row is not None

            # Create the evaluation result row
            _ = await self._create_evaluation_result(database_session, task_row, evaluation_result)

        # Once the evaluation is completed, we can check if the task rows have been updated with the finished_at timestamp and status
        fetch_tasks_query = select(Task).where(Task.benchmark == benchmark_row.id)
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
        task_data: RetrieveTaskResponse,
    ) -> dict[str, str]:
        docker_image: str = task_data.docker_image

        # Change the status of the task to evaluating before we start evaluation
        task_row.status = TaskStatus.EVALUATING
        database_session.add(task_row)
        database_session.flush()

        async with build_task_environment(daytona_client, task_row.task_id, docker_image) as sandbox:
            request_setup = task_data.request_setup
            if request_setup:
                response = await benchmark_service.request_setup_task(
                    task_id=task_row.task_id, instance_id=str(sandbox.id)
                )
                assert response.status == "ok"

            response = await benchmark_service.request_evaluate_instance(
                task_id=task_row.task_id, instance_id=sandbox.id
            )

            return response

    async def test_end_to_end(
        self,
        database_session: Session,
        benchmark_service: BenchmarkService,
        daytona_client: AsyncDaytona,
        example_benchmark_object: Benchmark,
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
            assert response.status == "ok"

            # Create benchmark row to initiate a benchmark
            benchmark_row = example_benchmark_object
            database_session.add(benchmark_row)
            database_session.flush()

            # Request all of the task ids from the benchmark service
            verify_response = await benchmark_service.request_verify_task_ids(task_ids=None, slice_str=None)

            # Returned all of the task ids from the swebench service
            assert verify_response.task_ids is not None
            assert len(verify_response.task_ids) == 500

            # NOTE: For testing we only are going to random sample the dataset
            # Also to mimic limits we are going to apply a concurrency limit
            task_ids = sample(verify_response.task_ids, 10)

            logger.info(f"Sample of task ids returned: {task_ids[:10]}")

            # Create the task rows for each task we are going to run
            task_row_mapping: dict[str, Task] = {}
            for task_id in task_ids:
                task_row = await self._create_task(database_session, task_id, benchmark_row.id)
                task_row_mapping[task_id] = task_row

            logger.info(f"Sample of task row mapping: {str(list(task_row_mapping.values())[:250])}")

            semaphore = Semaphore(5)
            evaluation_results: dict[str, dict[str, Any] | None] = {}

            async def process_task(task_id: str) -> None:
                try:
                    async with semaphore:
                        task_data = await benchmark_service.request_retrieve_task(task_id=task_id)
                        task_row = task_row_mapping[task_id]
                        evaluation_result = await self._evaluate_instance(
                            database_session, benchmark_service, daytona_client, task_row, task_data
                        )
                        evaluation_result_row = await self._create_evaluation_result(
                            database_session, task_row, evaluation_result
                        )
                        evaluation_results[task_id] = evaluation_result_row.result
                except Exception as e:
                    logger.error(f"Process task failed: {e}")

            _ = await gather(*[process_task(task_id) for task_id in task_ids])

            logger.info(f"Sample of evaluation results: {str(list(evaluation_results.values())[:100])}")

            # When all tasks have been evaluated, we can make a request to get the final evaluation score
            final_score_response = await benchmark_service.request_final_score(evaluation_results=evaluation_results)

            logger.info(f"Final score response: {str(final_score_response)[:250]}")

            # Create the final evaluation row and add it to the database
            final_evaluation_row = await self._create_final_evaluation(
                database_session, benchmark_row, final_score_response
            )

            # Fetch the evaluation results from the final evaluation row
            fetched_evaluation_results = benchmark_row.fetch_evaluation_results(database_session)

            results = list(fetched_evaluation_results.keys())
            assert len(results) == len(list(evaluation_results.keys()))

            logger.info(f"Sample of fetched evaluation results: {str(results[:100])}")

            # check that the evaluation results retrieved match the ones stored in the database
            for task_id, evaluation_result in fetched_evaluation_results.items():
                evaluation_result_row = database_session.exec(
                    select(EvaluationResult)
                    .join(Task, col(EvaluationResult.task) == col(Task.id))
                    .where(col(Task.task_id) == task_id)
                ).first()
                assert evaluation_result_row is not None
                assert evaluation_result == evaluation_result_row.result

            # Verify that the final evaluation row matches what we have in the database
            assert final_evaluation_row.final_score == final_score_response.final_score
            assert final_evaluation_row.properties == final_score_response.metadata

        except Exception as e:
            pytest.fail(f"End to end test failed: {e}")
