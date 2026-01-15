import asyncio
import json
from asyncio import Semaphore, gather
from collections.abc import AsyncGenerator
from functools import cached_property
from typing import Any, NamedTuple
from uuid import UUID

from sqlmodel import Session, case, col, func, select

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import Benchmark, BenchmarkStatus, EvaluationResult, FinalEvaluation, Task, TaskStatus
from tracker.database.session import engine
from tracker.logger import get_logger
from tracker.sandbox import create_sandbox, install_dependencies, run_agent, upload_contract_to_sandbox
from tracker.types import (
    BenchmarkDetails,
    FetchBenchmarkResponse,
    StartRunRequest,
)

logger = get_logger(__name__, stream=True)


async def process_task(
    task_row_mapping: dict[str, Task],
    start_run_request: StartRunRequest,
    semaphore: Semaphore,
    benchmark_service: BenchmarkService,
    task_id: str,
) -> dict[str, dict[str, Any] | None]:
    async with semaphore:
        with Session(bind=engine, expire_on_commit=False) as task_session:
            task_row = task_row_mapping[task_id]
            try:
                task_data = await benchmark_service.request_retrieve_task(task_id=task_id)

                task_row = task_session.merge(task_row)
                task_row.status = TaskStatus.IN_PROGRESS
                task_session.add(task_row)
                task_session.commit()

                async with create_sandbox(
                    benchmark_service.daytona_client, task_row.task_id, task_data.docker_image
                ) as sandbox:
                    # Upload the contract to the sandbox after creating and install the dependencies
                    await upload_contract_to_sandbox(sandbox, start_run_request.contract_name)
                    await install_dependencies(sandbox, start_run_request.contract_name)

                    # Setup task if requested
                    if task_data.request_setup:
                        _ = await benchmark_service.request_setup_task(task_row.task_id, sandbox.id)

                    # Run the agent inside of the sandbox
                    # NOTE: Currently only testing when agent does not need a response, in the future run agent will return a json to evaluate it needed
                    await run_agent(
                        sandbox, start_run_request.contract_name, task_row.task_id, task_data.problem_statement
                    )

                    # Update the status to evaluating once we finish running the agent
                    task_row.status = TaskStatus.EVALUATING
                    task_session.add(task_row)
                    task_session.commit()

                    # Evaluate the instance
                    # NOTE: only really good for when we need to evaluate the container (for just evaluating a text response we can delegate before this)
                    evaluation_result = await benchmark_service.request_evaluate_instance(task_row.task_id, sandbox.id)

                    # Save the evaluation result to the database with the task row
                    evaluation_result_row = EvaluationResult(
                        task=task_row.id, instance_id=sandbox.id, result=evaluation_result
                    )
                    task_session.add(evaluation_result_row)

                    # Mark the task status as finished since we have finished processing the task
                    task_row.status = TaskStatus.FINISHED
                    task_session.add(task_row)
                    task_session.commit()

                    return {task_id: evaluation_result_row.result}
            except Exception as e:
                error_message = str(e)
                commit_task_error(task_row, task_session, error_message)
                return {task_id: None}


async def process_benchmark(
    start_run_request: StartRunRequest,
    benchmark_id: UUID,
    verified_task_ids: list[str],
    benchmark_service: BenchmarkService,
    session: Session,
) -> None:
    benchmark_row = session.get(Benchmark, benchmark_id)

    if not benchmark_row:
        raise ValueError(f"Benchmark with id {benchmark_id} not found")

    try:
        # Create tasks inside of the database for each task id
        task_row_mapping: dict[str, Task] = {}
        for task_id in verified_task_ids:
            task_row = Task(task_id=task_id, benchmark=benchmark_row.id)
            task_row_mapping[task_id] = task_row

        session.add_all(list(task_row_mapping.values()))
        session.commit()

        semaphore = Semaphore(start_run_request.concurrency)

        evaluation_result_rows: list[dict[str, dict[str, Any] | None]] = await gather(
            *[
                process_task(task_row_mapping, start_run_request, semaphore, benchmark_service, task_id)
                for task_id in verified_task_ids
            ]
        )

        # NOTE: Tasks with errors will still need to be included inside of the final score calculation to ensure that they are accounted for
        evaluation_results: dict[str, dict[str, Any] | None] = {
            task_id: evaluation_result
            for result_dict in evaluation_result_rows
            for task_id, evaluation_result in result_dict.items()
        }

        # Calculate the final score based off the tasks that were ran
        final_score_response = await benchmark_service.request_final_score(evaluation_results=evaluation_results)

        # Create the final evaluation row and add it to the database
        final_evaluation_row = FinalEvaluation(
            benchmark=benchmark_row.id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )

        session.add(final_evaluation_row)
        session.commit()

        # Mark benchmark as completed
        # NOTE: Finished at will be automatically set by an event when the status becomes finished
        benchmark_row.status = BenchmarkStatus.FINISHED
        session.add(benchmark_row)
        session.commit()
    except Exception as e:
        error_message = str(e)
        commit_benchmark_error(benchmark_row, session, error_message)


class TaskCounts(NamedTuple):
    total_tasks: int
    finished_tasks: int
    failed_tasks: int


class BenchmarkContext:
    _benchmark_row: Benchmark
    _session: Session

    def __init__(self, benchmark_row: Benchmark, session: Session):
        self._benchmark_row = benchmark_row
        self._session = session

    @property
    def _status(self) -> BenchmarkStatus:
        return self._benchmark_row.status

    @cached_property
    def _task_counts(self) -> TaskCounts:
        finished_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR]

        statement = (
            select(
                func.count().label("total_tasks"),
                func.count(case((col(Task.status).in_(finished_statuses), 1))).label("finished_tasks"),
                func.count(case((Task.status == TaskStatus.ERROR, 1))).label("failed_tasks"),
            )
            .select_from(Task)
            .where(Task.benchmark == self._benchmark_row.id)
        )

        result = self._session.exec(statement).one()

        task_counts = TaskCounts(total_tasks=result[0], finished_tasks=result[1], failed_tasks=result[2])

        return task_counts

    @cached_property
    def benchmark_details(self) -> BenchmarkDetails:
        return BenchmarkDetails(
            status=self._status,
            started_at=self._benchmark_row.started_at,
            total_tasks=self._task_counts.total_tasks,
            finished_tasks=self._task_counts.finished_tasks,
        )


def fetch_evaluation_results(benchmark_id: UUID, session: Session) -> dict[str, dict[str, Any]]:
    """Select all evaluation results for a given benchmark"""
    statement = (
        select(EvaluationResult, Task.task_id)
        .join(Task, col(EvaluationResult.task) == col(Task.id))
        .where(Task.benchmark == benchmark_id)
    )
    results = session.exec(statement).all()

    evaluation_results: dict[str, dict[str, Any]] = {}
    for evaluation_result, task_id in results:
        result_data = evaluation_result.result
        evaluation_results[task_id] = result_data

    return evaluation_results


def commit_benchmark_error(benchmark_row: Benchmark, session: Session, error_message: str) -> None:
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.add(benchmark_row)
    session.commit()


def commit_task_error(task_row: Task, session: Session, error_message: str) -> None:
    task_row.status = TaskStatus.ERROR
    task_row.error_message = error_message
    session.add(task_row)
    session.commit()


async def stream_benchmark_results(benchmark_id: UUID, session: Session) -> AsyncGenerator[str]:
    """
    Generate Server-Sent Events with benchmark updates. User connects to this when they want to view live updates of a benchmark.

    Usage from client:
        curl -X GET http://<endpoint>/stream-benchmark-results/<benchmark_id>?connect=true

    Returns:
        AsyncGenerator[str]
    """
    EVENT_COMPLETE = "event: complete\n\n"
    EVENT_ERROR = "event: error\ndata:"
    DATA_PREFIX = "data:"
    DISCONNECT = "event: disconnect\n\n"
    try:
        while True:
            with Session(bind=session.bind) as fresh_session:
                fresh_benchmark = fresh_session.get(Benchmark, benchmark_id)
                if not fresh_benchmark:
                    yield f"{EVENT_ERROR} {json.dumps({'error': 'Benchmark not found'})}\n\n"
                    break

                fresh_session.refresh(fresh_benchmark)
                benchmark_context = BenchmarkContext(fresh_benchmark, fresh_session)

                response_data = FetchBenchmarkResponse(
                    benchmark_name=fresh_benchmark.name,
                    benchmark_id=fresh_benchmark.id,
                    details=benchmark_context.benchmark_details,
                )

                yield f"{DATA_PREFIX} {response_data.model_dump_json()}\n\n"

                if fresh_benchmark.status in [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR]:
                    yield EVENT_COMPLETE
                    break

            await asyncio.sleep(60)

    except asyncio.CancelledError:
        logger.info(f"Client disconnected from benchmark {benchmark_id} stream")
        yield DISCONNECT
