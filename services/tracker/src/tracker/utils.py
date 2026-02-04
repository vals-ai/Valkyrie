import asyncio
import json
import traceback
from asyncio import Semaphore, gather
from collections.abc import AsyncGenerator, Coroutine
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, NamedTuple, Sequence, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from daytona import AsyncDaytona, AsyncPaginatedSandboxes, AsyncSandbox, SandboxState
from sqlmodel import Session, asc, case, col, delete, desc, func, or_, select, update

from tracker.benchmark_service import BenchmarkService
from tracker.config import broker
from tracker.database.models import Benchmark, BenchmarkStatus, EvaluationResult, FinalEvaluation, Task, TaskStatus
from tracker.database.session import engine
from tracker.exceptions import TrackerServiceError
from tracker.logger import get_logger
from tracker.sandbox import create_sandbox, install_agent_dependencies, run_agent, upload_agent_artifacts
from tracker.types import (
    BenchmarkDetails,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    Order,
    StartBenchmarkRequest,
)

logger = get_logger(__name__)


class TrackedTaskStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    DONE = "done"


class TrackedTask:
    _coro: Coroutine[Any, Any, Any]
    _status: str
    _task: asyncio.Task[Any] | None

    def __init__(self, coro: Coroutine[Any, Any, Any]):
        self._coro = coro
        self._status = TrackedTaskStatus.WAITING
        self._task = None

    @property
    def status(self) -> str:
        return self._status

    @property
    def task(self) -> asyncio.Task[Any] | None:
        return self._task

    async def run(
        self, semaphore: asyncio.Semaphore, task_row: Task, session: Session
    ) -> dict[str, dict[str, Any] | None]:
        async def _wrap_coro():
            """Need to have a task created even if we are not running the coroutine so that we can cancel it before its running"""
            async with semaphore:
                self._status = TrackedTaskStatus.RUNNING
                return await self._coro

        try:
            self._task = asyncio.create_task(_wrap_coro())
            return await self._task
        except asyncio.CancelledError:
            # Need to clean up the coroutine if we cancelled the task
            self._coro.close()

            # When we cancel we return the task id still so that we can track the task when we create the final evaluation row
            return {task_row.task_id: None}
        except Exception as e:
            error_message = f"Task error was not handled: {str(e)}\n{traceback.format_exc()}"
            task_row_merged = session.merge(task_row)
            commit_task_error(task_row_merged, session, error_message)

            return {task_row.task_id: None}
        finally:
            self._status = TrackedTaskStatus.DONE


class TaskMonitor:
    _benchmark_row: Benchmark
    _session: Session
    _task_tracking: dict[str, TrackedTask]
    _TRACK_INTERVAL: int = 2

    def __init__(self, benchmark_row: Benchmark, session: Session, task_tracking: dict[str, TrackedTask]):
        self._benchmark_row = benchmark_row
        self._session = session
        self._task_tracking = task_tracking

    def _fetch_task_row(self, task_id: str) -> Task:
        task_row = self._session.exec(
            select(Task).where(Task.task_id == task_id).where(Task.benchmark == self._benchmark_row.id).limit(1)
        ).first()
        if not task_row:
            raise ValueError(f"Task with id {task_id} not found")

        return task_row

    def _validate_task(self, task_id: str) -> bool:
        """
        Runs while waiting to be aquired by the semaphore.

        If the task status has been set to stopped we return False to exit the task early.

        Returns:
            True if the task should continue to be processed, False if the task should be stopped early

        """
        self._session.expire_all()
        self._session.refresh(self._benchmark_row)

        task_row = self._fetch_task_row(task_id)

        # If task has been stopped or benchmark has occured an error we need to exit
        if task_row.status == TaskStatus.STOPPED or self._benchmark_row.status == BenchmarkStatus.ERROR:
            return False

        return True

    def _check_is_waiting(self, task: TrackedTask) -> bool:
        """
        Checks if the task is waiting to be aquired by the semaphore.

        Returns:
            True if the task is waiting to be aquired by the semaphore, False if it has been aquired
        """
        if task.task is None or task.status == TrackedTaskStatus.WAITING:
            return True

        return False

    async def track_tasks(self) -> None:
        """
        Tracks all the tasks and removes the tasks that are no longer waiting to be aquired by the semaphore.

        Removes the tasks that are no longer waiting to be aquired by the semaphore.
        """

        exit_condition_met: bool = False

        while not exit_condition_met:
            tasks_to_check: list[str] = list(self._task_tracking.keys())
            for task_id in tasks_to_check:
                task = self._task_tracking[task_id]

                if not self._check_is_waiting(task):
                    del self._task_tracking[task_id]

                if not self._validate_task(task_id) and task.task:
                    task.task.cancel(f"Task {task_id} has been invalidated. Benchmark has been requested to stop")

                    if task_id in self._task_tracking:
                        del self._task_tracking[task_id]

            if not self._task_tracking:
                exit_condition_met = True

            await asyncio.sleep(self._TRACK_INTERVAL)


def fetch_benchmark_row(benchmark_id: UUID, session: Session) -> Benchmark:
    """Util to clean up pattern to fetch benchmark row"""
    benchmark_row = session.get(Benchmark, benchmark_id)
    if not benchmark_row:
        raise ValueError(f"Benchmark with id {benchmark_id} not found")

    return benchmark_row


def handle_early_exit(task_row: Task, task_session: Session) -> None:
    task_row.status = TaskStatus.STOPPED
    task_session.add(task_row)
    task_session.commit()


async def process_task(
    task_row: Task,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkService,
    benchmark_id: UUID,
    task_id: str,
) -> dict[str, dict[str, Any] | None]:
    """
    Processes a task and returns the evaluation result

    NOTE: When we close the sandbox the agent process will be killed and we will instantly go to evaluating,
    the evaluation will fail since the instance no longer exists. We handle this inside of the exception caught.
    """
    with Session(bind=engine, expire_on_commit=False) as task_session:
        benchmark_row = fetch_benchmark_row(benchmark_id, task_session)

        # Merge the task row to get the latest state from the database
        task_row = task_session.merge(task_row)

        # If user has requested to stop the benchmark we exit before we process the task
        if benchmark_row.status == BenchmarkStatus.STOPPING:
            handle_early_exit(task_row, task_session)
            return {task_id: None}

        try:
            task_data = await benchmark_service.request_retrieve_task(task_id=task_id)

            # Labels that show up in the UI we can use to filter sandboxes
            labels = {
                "Benchmark": benchmark_row.name,
                "Id": str(benchmark_row.id),
                "Task": task_row.task_id,
            }

            async with create_sandbox(
                daytona=benchmark_service.daytona_client,
                sandbox_name=task_row.alias,
                image=task_data.docker_image,
                labels=labels,
                env_vars=start_benchmark_request.contract.env,
            ) as sandbox:
                try:
                    task_row.status = TaskStatus.IN_PROGRESS
                    task_session.add(task_row)
                    task_session.commit()

                    # Upload the contract to the sandbox after creating and install the dependencies
                    await upload_agent_artifacts(sandbox, start_benchmark_request.contract)
                    await install_agent_dependencies(sandbox, start_benchmark_request.contract)

                    # Setup task if requested
                    if task_data.request_setup:
                        _ = await benchmark_service.request_setup_task(task_row.task_id, sandbox.id)

                    # Run the agent inside of the sandbox
                    agent_output = await run_agent(
                        sandbox, start_benchmark_request.contract, task_data.problem_statement, task_id, task_data.cwd
                    )

                    # Update the status to evaluating once we finish running the agent
                    task_row.status = TaskStatus.EVALUATING
                    task_session.add(task_row)
                    task_session.commit()

                    # Evaluate the instance
                    # NOTE: only really good for when we need to evaluate the container (for just evaluating a text response we can delegate before this)
                    logger.info(f"Evaluating agent {start_benchmark_request.contract.name} in sandbox {sandbox.name}")
                    evaluation_result = await benchmark_service.request_evaluate_instance(task_row.task_id, sandbox.id)

                    # Save the evaluation result to the database with the task row
                    evaluation_result_row = EvaluationResult(
                        task=task_row.id, instance_id=sandbox.id, result=evaluation_result, agent_output=agent_output
                    )
                    task_session.add(evaluation_result_row)

                    # Mark the task status as finished since we have finished processing the task
                    task_row.status = TaskStatus.FINISHED
                    task_session.add(task_row)
                    task_session.commit()

                    return {task_id: evaluation_result_row.result}
                except Exception as e:
                    # Error can come from the sandbox being destroyed
                    task_session.refresh(task_row)
                    if task_row.status == TaskStatus.STOPPED:
                        return {task_id: None}

                    raise e from e
        except Exception as e:
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(error_message)

            commit_task_error(task_row, task_session, error_message)

            return {task_id: None}


def set_benchmark_final_status(benchmark_row: Benchmark, session: Session) -> None:
    """
    Delegates status depending on if any tasks have been stopped.
    """

    # Check if any tasks are still in the pending or in progress state
    tasks_not_finished: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.status).in_([TaskStatus.PENDING, TaskStatus.IN_PROGRESS]))
    ).one()

    if tasks_not_finished:
        raise TrackerServiceError(
            f"Cannot set final status for benchmark {benchmark_row.id} because tasks are still in the pending or in progress state."
        )

    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    # Default status is finished, if we stopped any tasks the benchmark status is stopped
    # Later we can use the stopped status to determine if we can resume the benchmark
    benchmark_status = BenchmarkStatus.FINISHED
    if tasks_stopped:
        benchmark_status = BenchmarkStatus.STOPPED

    benchmark_row.status = benchmark_status
    session.add(benchmark_row)
    session.commit()


def create_task_rows(
    verified_task_ids: list[str], benchmark_row: Benchmark, session: Session
) -> Sequence[tuple[str, Task]]:
    """
    Create task_rows that do not already exist in the database for the benchmark row.

    NOTE: Only return pending tasks to support resuming the benchmark.
    """

    # Find task ids that already exist so that we can filter them out
    existing_task_ids: Sequence[str] = session.exec(
        select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(col(Task.task_id).in_(verified_task_ids))
    ).all()

    # NOTE: Must maintain same order that was passed in
    task_ids_to_create = [task_id for task_id in verified_task_ids if task_id not in existing_task_ids]

    for task_id in task_ids_to_create:
        task_row = Task(task_id=task_id, benchmark=benchmark_row.id)
        session.add(task_row)

    session.commit()
    session.expire_all()

    # Fetch all task rows with the status of pending
    pending_task_rows: Sequence[tuple[str, Task]] = session.exec(
        select(Task.task_id, Task).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.PENDING)
    ).all()

    return pending_task_rows


async def fetch_missing_tasks(
    session: Session, benchmark_row: Benchmark, evaluation_results: dict[str, dict[str, Any]]
):
    remaining_task_results_query = cast(
        Sequence[tuple[str, dict[str, Any]]],
        session.exec(
            select(Task.task_id, EvaluationResult.result)  # pyright: ignore[reportUnknownArgumentType]
            .join(EvaluationResult, col(Task.id) == col(EvaluationResult.task))
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.task_id).notin_(list(evaluation_results.keys())))
        ).all(),
    )

    remaining_task_results: dict[str, dict[str, Any] | None] = {
        task_id: evaluation_result for task_id, evaluation_result in remaining_task_results_query
    }
    return remaining_task_results


@broker.task
async def process_benchmark(
    start_benchmark_request_json: dict[str, Any],
    benchmark_id_str: str,
    verified_task_ids: list[str],
) -> None:
    # Was serialized to make it compatible with the broker
    start_benchmark_request: StartBenchmarkRequest = StartBenchmarkRequest(**start_benchmark_request_json)
    benchmark_id: UUID = UUID(benchmark_id_str)

    # NOTE: Will get ugly if we error on session create
    with Session(bind=engine, expire_on_commit=False) as session:
        benchmark_service = start_benchmark_request.benchmark_service

        benchmark_row = fetch_benchmark_row(benchmark_id, session)

        try:
            # Create tasks inside of the database for each task id
            task_rows: Sequence[tuple[str, Task]] = create_task_rows(verified_task_ids, benchmark_row, session)

            task_row_ids: set[str] = {task_id for task_id, _ in task_rows}
            missing_task_ids: list[str] = [task_id for task_id in verified_task_ids if task_id not in task_row_ids]
            if missing_task_ids:
                raise TrackerServiceError(
                    f"Race condition occured when resuming benchmark {benchmark_id}. Missing task ids: {', '.join(missing_task_ids)}"
                )

            # Load the tasks we are going to be tracking
            tracked_tasks: dict[str, TrackedTask] = {
                task_id: TrackedTask(
                    process_task(task_row, start_benchmark_request, benchmark_service, benchmark_id, task_id)
                )
                for task_id, task_row in task_rows
            }

            # Start the monitor to track the state the tasks are in and cancel them when no longer valid
            monitor = TaskMonitor(benchmark_row, session, tracked_tasks)
            monitor_task = asyncio.create_task(monitor.track_tasks())

            semaphore = Semaphore(start_benchmark_request.concurrency)

            evaluation_result_rows: list[dict[str, dict[str, Any] | None]] = await gather(
                *[tracked_tasks[task_id].run(semaphore, task_row, session) for task_id, task_row in task_rows]
            )

            await monitor_task

            # NOTE: Tasks with errors will still need to be included inside of the final score calculation to ensure that they are accounted for
            evaluation_results: dict[str, dict[str, Any] | None] = {
                task_id: evaluation_result
                for result_dict in evaluation_result_rows
                for task_id, evaluation_result in result_dict.items()
            }

            # Fetch remaining tasks (in case this benchmark was resumed)
            remaining_task_results = await fetch_missing_tasks(session, benchmark_row, evaluation_results)

            evaluation_results.update(remaining_task_results)

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

            set_benchmark_final_status(benchmark_row, session)
        except Exception as e:
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            commit_benchmark_error(benchmark_row, session, error_message)
        finally:
            await benchmark_service.daytona_client.close()


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
        finished_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]

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

    @property
    def _task_breakdown(self) -> dict[TaskStatus, int]:
        """
        Returns a mapping between the task status and the number of tasks in that status

        Provides a breakdown of the benchmark.
        """
        statement = (
            select(Task.status, func.count(col(Task.id)))
            .select_from(Task)
            .where(Task.benchmark == self._benchmark_row.id)
            .group_by(Task.status)
            .having(func.count(col(Task.id)) > 0)  # Exclude all with count of 0
        )

        result = self._session.exec(statement).all()

        if not result:
            raise TrackerServiceError(
                f"No tasks have been discovered for benchmark {self._benchmark_row.id}, cannot provide task breakdown"
            )

        return {TaskStatus(status): count for status, count in result}

    @cached_property
    def benchmark_details(self) -> BenchmarkDetails:
        return BenchmarkDetails(
            status=self._status,
            started_at=self._benchmark_row.started_at,
            total_tasks=self._task_counts.total_tasks,
            finished_tasks=self._task_counts.finished_tasks,
            task_breakdown=self._task_breakdown,
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
        if evaluation_result.agent_output:
            result_data["agent_output"] = evaluation_result.agent_output

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
    PULL_INTERVAL = 5

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

                if fresh_benchmark.status in [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]:
                    yield EVENT_COMPLETE
                    break

            await asyncio.sleep(PULL_INTERVAL)

    except asyncio.CancelledError:
        logger.info(f"Client disconnected from benchmark {benchmark_id} stream")
        yield DISCONNECT


async def initiate_stop_benchmark(benchmark_row: Benchmark, session: Session, force: bool) -> None:
    """
    Sets the flags to initiate the stopping process for a benchmark.

    Benchmark - Stopping status
    Tasks - Stopped status

    NOTE: Tasks that have already started will continue to run and finish.
    """
    try:
        # Update all rows where tasks are pending to stopped
        result = session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.status) == TaskStatus.PENDING)
            .values(status=TaskStatus.STOPPED)
        )
        session.commit()

        # If we have stopped any tasks or are forcing the benchmark to stop, set the benchmark status to stopping
        if result.rowcount > 0 or force:
            benchmark_row.status = BenchmarkStatus.STOPPING
            session.add(benchmark_row)
            session.commit()
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error stopping benchmark {benchmark_row.id}: {str(e)}") from e


async def stop_sandbox(sandbox: AsyncSandbox, daytona_client: AsyncDaytona) -> str | None:
    try:
        # Wait for the sandbox to be in a valid deletion state
        await sandbox.wait_for_sandbox_start(timeout=0)

        # Delete the sandbox
        await daytona_client.delete(sandbox)

        return None
    except Exception as e:
        return f"{str(e)}: {traceback.format_exc()}"


async def fetch_sandboxes(benchmark_row: Benchmark, daytona_client: AsyncDaytona, page: int) -> AsyncPaginatedSandboxes:
    return await daytona_client.list(
        labels={"Benchmark": benchmark_row.name, "Id": str(benchmark_row.id)}, limit=10, page=page
    )


async def sandbox_generator(
    benchmark_row: Benchmark, daytona_client: AsyncDaytona
) -> AsyncGenerator[AsyncSandbox, None]:
    """
    Generator that yields all sandboxes for a given benchmark in paginated chunks of 10.

    NOTE: At the time of this implementation there are several things weird with the dayyona api
        1. If you delete the sandboxes in the list, the next page should be the first page (repopulated)
        2. Final state is not DESTROYED, but rather DESTROYING
        3. The total_pages count is not updated as you delete sandboxes
    """

    paginated_sandboxes: AsyncPaginatedSandboxes = await fetch_sandboxes(benchmark_row, daytona_client, 1)

    total_pages = paginated_sandboxes.total_pages
    while (paginated_sandboxes.page <= total_pages) and paginated_sandboxes.items:
        sandboxes = paginated_sandboxes.items
        for sandbox in sandboxes:
            if sandbox.state in [SandboxState.DESTROYING, SandboxState.DESTROYED]:
                continue

            yield sandbox

        # NOTE: Since we deleted the first 10 the first page will be populated with the next 10 sandboxes
        paginated_sandboxes = await fetch_sandboxes(benchmark_row, daytona_client, int(paginated_sandboxes.page))


async def force_stop_sandboxes(benchmark_row: Benchmark, session: Session) -> None:
    """
    Stops and deletes all sandboxes which are in progress or evaluating.
    NOTE: If task is not in progress but sandbox exists, we kill it and leave the task status as is.

    Raises:
        TrackerServiceError: If there are any errors stopping the sandboxes
    """
    daytona_client: AsyncDaytona = benchmark_row.benchmark_service.daytona_client

    # Update all tasks being processed to stopped
    session.exec(
        update(Task)
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.status).in_([TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]))
        .values(status=TaskStatus.STOPPED)
    )

    session.commit()

    # Iterate through each running sandbox and stop it, collecting error messages
    results: dict[str, str | None] = {}
    async for sandbox in sandbox_generator(benchmark_row, daytona_client):
        result = await stop_sandbox(sandbox, daytona_client)

        results[sandbox.name] = result

    error_message: str = "\n".join(
        f"{task_alias}: {error_message}" for task_alias, error_message in results.items() if error_message
    )

    if error_message:
        raise TrackerServiceError(f"Unexpected errors stopping sandboxes:\n{error_message}")


async def initiate_resume_benchmark(
    benchmark_row: Benchmark, session: Session, benchmark_service: BenchmarkService, retry: bool, force: list[str]
) -> list[str]:
    """
    Resets benchmark and task status to flag resuming the benchmark.

    Retry: we reset objects with an error status ontop of the stopped status
    Force: even if task has been finished we restart it

    Benchmark - In progress status
    Tasks - Pending status

    NOTE: Will raise if benchmark is in a stopped state with no stopped tasks.
    """
    try:
        retry_statuses = [TaskStatus.STOPPED]
        if retry:
            retry_statuses.append(TaskStatus.ERROR)

        filter_query = [
            col(Task.benchmark) == benchmark_row.id,
            or_(
                col(Task.status).in_(retry_statuses),
                col(Task.task_id).in_(force),
            ),
        ]

        # Check if there are any tasks that have been stopped
        task_ids = session.exec(select(Task.id, Task.task_id).where(*filter_query)).all()

        # id is task row primary key, task_id is the task id
        task_mapping: dict[UUID, str] = {id: task_id for id, task_id in task_ids}

        # Ensure we are not missing any tasks that were requested (skips if force is empty)
        missing_task_ids = [task_id for task_id in force if task_id not in task_mapping.values()]
        if missing_task_ids:
            raise TrackerServiceError(
                f"{', '.join(missing_task_ids)} was requested to be force resumed but does not exist in the dataset"
            )

        if not task_ids:
            raise TrackerServiceError(
                f"No tasks for benchmark {benchmark_row.id} can be resumed because all tasks are finished"
            )

        # Verify the task ids are still valid before priming to resume
        # Raises if any task ids are invalid
        verify_response = await benchmark_service.request_verify_task_ids(
            task_ids=list(task_mapping.values()), slice_str=None
        )

        # Set the benchmark status to in progress to flag resuming the benchmark
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        session.add(benchmark_row)
        session.commit()

        # Set the task status to pending to flag resuming the tasks
        session.exec(
            update(Task)
            .where(*filter_query)
            .values(  # Reset to defaults
                status=TaskStatus.PENDING,
                started_at=datetime.now(ZoneInfo("UTC")),
                error_message=None,
                finished_at=None,
            )
        )

        # Delete all evaluation results for the tasks (unlikely they exist)
        session.exec(delete(EvaluationResult).where(col(EvaluationResult.task).in_(list(task_mapping.keys()))))

        session.commit()

        return verify_response.task_ids
    except TrackerServiceError:
        raise
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error resuming benchmark {benchmark_row.id}: {str(e)}") from e


def fetch_filtered_benchmark_rows(request: FetchBenchmarksRequest, session: Session) -> tuple[Sequence[Benchmark], int]:
    """
    Creates a query to fetch benchmark rows from the database based on the fetch benchmark request.

    Args:
        request: FetchBenchmarksRequest

    Returns:
        tuple[Sequence[Benchmark], int]
        Sequence of benchmark rows and total count of benchmark rows

    """
    query = select(Benchmark)

    if request.agent_name:
        query = query.where(func.json_extract(Benchmark.arguments, "$.contract.name") == request.agent_name)

    if request.benchmark_name:
        query = query.where(Benchmark.name == request.benchmark_name)

    if request.status:
        query = query.where(Benchmark.status == request.status)

    if request.order_by == Order.DESC:
        query = query.order_by(desc(Benchmark.started_at))
    else:
        query = query.order_by(asc(Benchmark.started_at))

    total_count = session.exec(select(func.count()).select_from(query.subquery())).one()

    if not total_count:
        return [], 0

    query = query.limit(request.limit).offset(request.offset)

    benchmark_rows: Sequence[Benchmark] = session.exec(query).all()

    return benchmark_rows, total_count
