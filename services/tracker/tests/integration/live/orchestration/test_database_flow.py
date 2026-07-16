"""Run with `uv run pytest tests/integration/live/orchestration/test_database_flow.py`.

Exercise the live database flow through the benchmark service and sandbox provider.
"""

import uuid
from asyncio import Semaphore, gather
from typing import Any
from uuid import UUID

import pytest
from benchmark_service import SandboxProvider, SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import FinalScoreResponse, RetrieveTaskResponse
from sqlmodel import Session, col, select

from tests.utils import TEST_ORG_ID
from tracker.database.models import Benchmark, EvaluationResult, FinalEvaluation, Task, TaskStatus
from tracker.sandbox import create_sandbox

pytestmark = pytest.mark.usefixtures("tracker_database")


def create_task_row(database_session: Session, task_id: str, benchmark_id: UUID) -> Task:
    """Persist a pending task for the live database flow."""
    task_row = Task(org_id=TEST_ORG_ID, task_id=task_id, benchmark=benchmark_id)
    database_session.add(task_row)
    database_session.flush()
    return task_row


def create_evaluation_result(
    database_session: Session,
    task_row: Task,
    evaluation_result: dict[str, Any],
) -> EvaluationResult:
    """Persist a successful evaluation and finish its task."""
    evaluation_result_row = EvaluationResult(
        org_id=TEST_ORG_ID,
        task=task_row.id,
        instance_id=str(uuid.uuid4()),
        result=evaluation_result,
    )
    database_session.add(evaluation_result_row)
    task_row.status = TaskStatus.FINISHED
    database_session.add(task_row)
    database_session.flush()
    return evaluation_result_row


def create_final_evaluation(
    database_session: Session,
    benchmark_row: Benchmark,
    final_score_response: FinalScoreResponse,
) -> FinalEvaluation:
    """Persist the service's final score for the benchmark."""
    final_evaluation_row = FinalEvaluation(
        org_id=TEST_ORG_ID,
        benchmark=benchmark_row.id,
        final_score=final_score_response.final_score,
        properties=final_score_response.metadata,
    )
    database_session.add(final_evaluation_row)
    database_session.flush()
    return final_evaluation_row


async def evaluate_instance(
    database_session: Session,
    benchmark_service: BenchmarkServiceClient,
    sandbox_provider: SandboxProvider,
    sandbox_provider_config: SandboxProviderConfig,
    benchmark_row: Benchmark,
    task_row: Task,
    task_data: RetrieveTaskResponse,
    creation_semaphore: Semaphore,
) -> dict[str, Any]:
    """Set up and evaluate one service task in a live sandbox."""
    task_row.status = TaskStatus.EVALUATING
    database_session.add(task_row)
    database_session.flush()

    async with create_sandbox(
        sandbox_provider,
        task_row.task_id,
        task_data.source,
        task_data.resources,
        creation_semaphore,
        labels={"Benchmark": benchmark_row.name, "Id": str(benchmark_row.id), "Task": task_row.task_id},
    ) as sandbox:
        setup_response = await benchmark_service.setup_task(
            task_id=task_row.task_id,
            instance_id=str(sandbox.id),
            sandbox_provider=sandbox_provider_config,
        )
        assert setup_response.status == "ok"
        return await benchmark_service.evaluate_instance(
            task_id=task_row.task_id,
            instance_id=sandbox.id,
            sandbox_provider=sandbox_provider_config,
        )


async def test_live_results_round_trip_through_tracker_database(
    database_session: Session,
    benchmark_service: BenchmarkServiceClient,
    sandbox_provider: SandboxProvider,
    sandbox_provider_config: SandboxProviderConfig,
    example_benchmark_object: Benchmark,
    creation_semaphore: Semaphore,
) -> None:
    """Live evaluation results must round-trip through tracker rows without loss.

    Test cases:
    - Three service tasks run concurrently in real sandboxes and persist successful evaluations.
    - Final score metadata and fetched result payloads match the stored rows.
    """
    health = await benchmark_service.health_check()
    assert health.status == "ok"

    benchmark_row = example_benchmark_object
    database_session.add(benchmark_row)
    database_session.flush()

    verify_response = await benchmark_service.verify_task_ids(task_ids=None, slice_str=None)
    assert verify_response.task_ids is not None
    assert len(verify_response.task_ids) == 500
    task_ids = verify_response.task_ids[:3]
    task_rows = {task_id: create_task_row(database_session, task_id, benchmark_row.id) for task_id in task_ids}
    evaluation_results: dict[str, dict[str, Any]] = {}
    concurrency = Semaphore(3)

    async def process_task(task_id: str) -> None:
        async with concurrency:
            task_data = await benchmark_service.retrieve_task(task_id=task_id)
            task_row = task_rows[task_id]
            evaluation_result = await evaluate_instance(
                database_session,
                benchmark_service,
                sandbox_provider,
                sandbox_provider_config,
                benchmark_row,
                task_row,
                task_data,
                creation_semaphore,
            )
            evaluation_row = create_evaluation_result(database_session, task_row, evaluation_result)
            evaluation_results[task_id] = evaluation_row.result

    await gather(*(process_task(task_id) for task_id in task_ids))
    assert set(evaluation_results) == set(task_ids)

    final_score_response = await benchmark_service.final_score(evaluation_results=evaluation_results)
    final_evaluation_row = create_final_evaluation(database_session, benchmark_row, final_score_response)
    fetched_evaluation_results = benchmark_row.fetch_evaluation_results(database_session)

    assert set(fetched_evaluation_results) == set(task_ids)
    for task_id, evaluation_result in fetched_evaluation_results.items():
        evaluation_result_row = database_session.exec(
            select(EvaluationResult)
            .join(Task, col(EvaluationResult.task) == col(Task.id))
            .where(col(Task.task_id) == task_id)
            .where(col(Task.benchmark) == benchmark_row.id)
        ).one()
        expected = {
            **evaluation_result_row.result,
            "agent_caused_exit_reason": evaluation_result_row.agent_caused_exit_reason,
            "attempts": 1,
        }
        assert evaluation_result == expected

    assert final_evaluation_row.final_score == final_score_response.final_score
    assert final_evaluation_row.properties == final_score_response.metadata
