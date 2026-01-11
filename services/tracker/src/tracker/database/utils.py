from asyncio import Semaphore, gather
from functools import cached_property
from typing import Any, NamedTuple
from uuid import UUID

from sqlmodel import Session, case, col, func, select

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import Benchmark, BenchmarkStatus, EvaluationResult, FinalEvaluation, Task, TaskStatus
from tracker.database.session import engine
from tracker.sandbox import create_sandbox, install_dependencies, run_agent, upload_contract_to_sandbox
from tracker.types import (
    BenchmarkDetails,
    StartRunRequest,
)


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

    # Create tasks inside of the database for each task id
    task_row_mapping: dict[str, Task] = {}
    for task_id in verified_task_ids:
        task_row = Task(task_id=task_id, benchmark_id=benchmark_row.id)
        task_row_mapping[task_id] = task_row

    session.add_all(list(task_row_mapping.values()))
    session.commit()

    semaphore = Semaphore(start_run_request.concurrency)

    async def process_task(task_id: str) -> EvaluationResult:
        async with semaphore:
            # TODO: This endpoint was made for retrieving task info for a group of tasks
            # Turns out its a better design to retrieve a single task at a time so that it fits better with a semaphore
            task_data = (await benchmark_service.request_retrieve_tasks(task_ids=[task_id]))[task_id]

            with Session(bind=engine) as task_session:
                task_row = task_row_mapping[task_id]
                # Update the task status to in progress
                task_row.status = TaskStatus.IN_PROGRESS
                task_session.add(task_row)
                task_session.commit()

                async with create_sandbox(
                    benchmark_service.daytona_client, task_row.task_id, task_data["docker_image"]
                ) as sandbox:
                    # Upload the contract to the sandbox after creating and install the dependencies
                    await upload_contract_to_sandbox(sandbox, start_run_request.contract_name)
                    await install_dependencies(sandbox, start_run_request.contract_name)

                    # Setup task if requested
                    if task_data["request_setup"]:
                        _ = await benchmark_service.request_setup_task(task_row.task_id, sandbox.id)

                    # Run the agent inside of the sandbox
                    # NOTE: Currently only testing when agent does not need a response, in the future run agent will return a json to evaluate it needed
                    await run_agent(
                        sandbox, start_run_request.contract_name, task_row.task_id, task_data["problem_statement"]
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
                        task_id=task_row.id, instance_id=sandbox.id, result=evaluation_result
                    )
                    task_session.add(evaluation_result_row)

                    # Mark the task status as finished since we have finished processing the task
                    task_row.status = TaskStatus.FINISHED
                    task_session.add(task_row)
                    task_session.commit()

                    return evaluation_result_row

    evaluation_result_rows: list[EvaluationResult] = await gather(
        *[process_task(task_id) for task_id in verified_task_ids]
    )

    evaluation_results: dict[str, dict[str, Any]] = {
        str(evaluation_result_row.task_id): evaluation_result_row.result
        for evaluation_result_row in evaluation_result_rows
    }

    # Calculate the final score based off the tasks that were ran
    final_score: dict[str, Any] = await benchmark_service.request_final_score(evaluation_results=evaluation_results)

    # Create the final evaluation row and add it to the database
    final_evaluation_row = FinalEvaluation(
        benchmark_id=benchmark_row.id,
        final_score=final_score["final_score"],
        # TODO: Remove these fields because not each task will have a resolved or unresolved task
        resolved_tasks=final_score["resolved_tasks"],
        unresolved_tasks=final_score["unresolved_tasks"],
    )

    session.add(final_evaluation_row)
    session.commit()

    # Mark benchmark as completed
    # NOTE: Finished at will be automatically set by an event when the status becomes finished
    benchmark_row.status = BenchmarkStatus.FINISHED
    session.add(benchmark_row)
    session.commit()


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
            .where(Task.benchmark_id == self._benchmark_row.id)
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
        .join(Task, col(EvaluationResult.task_id) == col(Task.id))
        .where(Task.benchmark_id == benchmark_id)
    )
    results = session.exec(statement).all()

    evaluation_results = {task_id: evaluation_result.result for evaluation_result, task_id in results}

    return evaluation_results
