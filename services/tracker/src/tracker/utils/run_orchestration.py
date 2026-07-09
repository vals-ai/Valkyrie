"""Run-level coordination: creating task rows, running all tasks, and finalizing the run."""

import asyncio
import traceback
from asyncio import Semaphore, gather
from datetime import datetime
from typing import Any, Literal, Sequence, TypedDict, cast
from uuid import UUID

import logfire
import sentry_sdk
from benchmark_service.client import BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from opentelemetry import trace
from sqlmodel import Session, col, desc, func, select

from tracker._lambda import invoke_lambda, lambda_client
from tracker.aws.cloudwatch_logs import create_benchmark_log_group
from tracker.aws.s3 import (
    copy_agent_to_benchmark,
)
from tracker.config import broker
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    ErrorResult,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import engine
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.types import (
    FinalViewResponse,
    HarnessConfig,
    StartBenchmarkRequest,
)

from tracker.utils.resources import (
    create_benchmark_service_client_from_request,
    fetch_benchmark_row,
    fetch_sandbox_provider_config,
)
from tracker.utils.reporting import create_final_view, upload_final_view
from tracker.utils.task_execution import TaskMonitor, TrackedTask, is_expected_attempt, process_task

logger = get_logger(__name__)

_SANDBOX_CREATION_CAP: int = 10
_RUNNABLE_TASK_STATUSES = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
_TERMINAL_BENCHMARK_STATUSES = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]


class TaskBatch(TypedDict):
    kind: Literal["start", "retry"]
    attempts: dict[str, str]


def set_benchmark_final_status(benchmark_row: Benchmark, session: Session, org: Org) -> None:
    """
    Delegates status depending on if any tasks have been stopped.
    """

    # Check if any tasks are still in the pending or in progress state
    tasks_not_finished: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).in_(_RUNNABLE_TASK_STATUSES))
    ).one()

    # Tasks will be in a non-finished state if something interrupts them while they are running and the state errors here
    if tasks_not_finished:
        raise TrackerServiceError(
            f"Cannot set final status for run {benchmark_row.id} because tasks are still runnable."
        )

    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    # Default status is finished, if we stopped any tasks the benchmark status is stopped
    # Later we can use the stopped status to determine if we can resume the benchmark
    benchmark_status = BenchmarkStatus.FINISHED
    if tasks_stopped:
        benchmark_status = BenchmarkStatus.STOPPED

    benchmark_row.status = benchmark_status
    benchmark_row.error_message = None
    session.add(benchmark_row)
    session.commit()


def create_task_rows(
    verified_task_ids: list[str],
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
) -> Sequence[tuple[str, Task]]:
    """
    Create task_rows that do not already exist in the database for the benchmark row.

    NOTE: Only return runnable tasks to support resuming the benchmark.
    """
    # Find task ids that already exist so that we can filter them out
    existing_task_ids: Sequence[str] = session.exec(
        select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(col(Task.task_id).in_(verified_task_ids))
    ).all()

    # NOTE: Must maintain same order that was passed in
    task_ids_to_create = [task_id for task_id in verified_task_ids if task_id not in existing_task_ids]

    for task_id in task_ids_to_create:
        task_row = Task(org_id=org.id, task_id=task_id, benchmark=benchmark_row.id)
        session.add(task_row)

    session.commit()
    session.expire_all()

    task_rows = session.exec(
        select(Task.task_id, Task)
        .where(Task.benchmark == benchmark_row.id)
        .where(Task.org_id == org.id)
        .where(col(Task.task_id).in_(verified_task_ids))
        .where(col(Task.status).in_([TaskStatus.PENDING, TaskStatus.EVALUATING]))
    ).all()

    task_rows_by_id: dict[str, Task] = {task_id: task_row for task_id, task_row in task_rows}
    return [(task_id, task_rows_by_id[task_id]) for task_id in verified_task_ids if task_id in task_rows_by_id]


def has_runnable_tasks(session: Session, benchmark_row: Benchmark, org: Org) -> bool:
    return (
        session.exec(
            select(Task.id)
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.org_id == org.id)
            .where(col(Task.status).in_(_RUNNABLE_TASK_STATUSES))
        ).first()
        is not None
    )


def has_stopped_tasks(session: Session, benchmark_row: Benchmark, org: Org) -> bool:
    return (
        session.exec(
            select(Task.id)
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.org_id == org.id)
            .where(Task.status == TaskStatus.STOPPED)
        ).first()
        is not None
    )


def fetch_final_score_inputs(session: Session, benchmark_row: Benchmark, org: Org) -> dict[str, dict[str, Any] | None]:
    """
    Return a mapping of the task IDs to their latest evaluation results. If a task is not finished we default to None.
    """
    # Fetch task rows which belong to the benchmark we are running
    task_rows = cast(
        Sequence[tuple[UUID, str, TaskStatus]],
        session.exec(
            select(Task.id, Task.task_id, Task.status)
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.org_id) == org.id)
        ).all(),
    )

    # Fetch all results from tasks that are finished
    task_row_ids = [task_row_id for task_row_id, _task_id, status in task_rows if status == TaskStatus.FINISHED]
    result_rows = cast(
        Sequence[tuple[UUID, dict[str, Any]]],
        session.exec(
            select(EvaluationResult.task, EvaluationResult.result)  # pyright: ignore[reportUnknownArgumentType]
            .where(col(EvaluationResult.task).in_(task_row_ids))
            .where(col(EvaluationResult.org_id) == org.id)
            .order_by(desc(EvaluationResult.created_at))
        ).all(),
    )

    # Group results by task row ID
    latest_results: dict[UUID, dict[str, Any]] = {}
    for task_row_id, result in result_rows:
        latest_results.setdefault(task_row_id, result)

    # Return mapping between task IDs and their latest evaluation results
    return {
        task_id: latest_results.get(task_row_id) if status == TaskStatus.FINISHED else None
        for task_row_id, task_id, status in task_rows
    }


def _fetch_benchmark_for_update(benchmark_id: UUID, session: Session, org: Org) -> Benchmark:
    return session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark_id)
        .where(Benchmark.org_id == org.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    ).one()


def _fetch_tasks_for_update(benchmark_id: UUID, session: Session, org: Org) -> Sequence[Task]:
    return session.exec(
        select(Task)
        .where(Task.benchmark == benchmark_id)
        .where(Task.org_id == org.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    ).all()


def _generation_changed(task_rows: Sequence[Task], run_attempts: dict[str, datetime]) -> bool:
    tasks_by_id = {task.task_id: task for task in task_rows}
    return set(tasks_by_id) != set(run_attempts) or any(
        not is_expected_attempt(tasks_by_id[task_id], started_at) for task_id, started_at in run_attempts.items()
    )


# Pin the Taskiq task name to its pre-refactor value so in-flight messages
# enqueued as `tracker.utils:process_benchmark` still match after the module move.
@broker.task("tracker.utils:process_benchmark")
@logfire.instrument("process_benchmark")
async def process_benchmark(
    start_benchmark_request_json: dict[str, Any],
    benchmark_id_str: str,
    verified_task_ids: list[str] | TaskBatch,
) -> None:
    # Was serialized to make it compatible with the broker
    start_benchmark_request: StartBenchmarkRequest = StartBenchmarkRequest(**start_benchmark_request_json)
    benchmark_id: UUID = UUID(benchmark_id_str)
    harness_config: HarnessConfig = start_benchmark_request.harness_config
    sandbox_provider_config = fetch_sandbox_provider_config(
        harness_config.sandbox_provider_secret_name,
        harness_config.aws,
        start_benchmark_request.sandbox_provider,
    )
    benchmark_service = create_benchmark_service_client_from_request(start_benchmark_request)

    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)
    trace.get_current_span().set_attributes(
        {
            "benchmark_id": benchmark_id_str,
            "benchmark_name": start_benchmark_request.benchmark_name,
            "agent_name": start_benchmark_request.contract.name,
            "task_count": len(
                verified_task_ids if isinstance(verified_task_ids, list) else verified_task_ids["attempts"]
            ),
        }
    )

    # Create notifier if webhook is configured
    notifier: SlackNotifier | None = None
    if start_benchmark_request.webhook_secret_name and start_benchmark_request.webhook_intervals:
        notifier = SlackNotifier(
            secret_name=start_benchmark_request.webhook_secret_name,
            aws=harness_config.aws,
            intervals=start_benchmark_request.webhook_intervals,
        )

    # Resolve the org from the benchmark row (no org check on first fetch since the benchmark was just created by our system)
    with Session(bind=engine) as session:
        benchmark_row = session.get(Benchmark, benchmark_id)
        if not benchmark_row:
            raise TrackerServiceError(f"Run with id {benchmark_id} not found")
        org = session.exec(select(Org).where(Org.id == benchmark_row.org_id)).one()

    finalization_deferred = False
    execution_attempts: dict[str, datetime] = {}
    run_attempts: dict[str, datetime] = {}

    try:
        if isinstance(verified_task_ids, list):
            execution_task_ids = verified_task_ids
            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                task_rows: Sequence[tuple[str, Task]] = create_task_rows(
                    execution_task_ids, benchmark_row, session, org
                )
                run_tasks = session.exec(
                    select(Task).where(Task.benchmark == benchmark_id).where(Task.org_id == org.id)
                ).all()
                run_attempts = {task.task_id: task.started_at for task in run_tasks}
                execution_attempts = {
                    task_id: run_attempts[task_id] for task_id in execution_task_ids if task_id in run_attempts
                }

            missing_task_ids = [task_id for task_id in execution_task_ids if task_id not in execution_attempts]
            if missing_task_ids:
                raise TrackerServiceError(
                    f"Race condition occured when resuming run {benchmark_id}. Missing task ids: {', '.join(missing_task_ids)}"
                )
        else:
            batch_kind = cast(str, verified_task_ids["kind"])
            match batch_kind:
                case "start" | "retry":
                    execution_attempts = {
                        task_id: datetime.fromisoformat(started_at)
                        for task_id, started_at in verified_task_ids["attempts"].items()
                    }
                case unknown_kind:
                    raise TrackerServiceError(f"Unknown task batch kind: {unknown_kind}")

            execution_task_ids = list(execution_attempts)
            with Session(bind=engine) as session:
                _fetch_benchmark_for_update(benchmark_id, session, org)
                run_tasks = _fetch_tasks_for_update(benchmark_id, session, org)
                run_attempts = {task.task_id: task.started_at for task in run_tasks}
                tasks_by_id = {task.task_id: task for task in run_tasks}
                if any(
                    task_id not in tasks_by_id or not is_expected_attempt(tasks_by_id[task_id], expected_started_at)
                    for task_id, expected_started_at in execution_attempts.items()
                ):
                    finalization_deferred = True
                    return
                task_rows = [
                    (task_id, tasks_by_id[task_id])
                    for task_id in execution_task_ids
                    if tasks_by_id[task_id].status in [TaskStatus.PENDING, TaskStatus.EVALUATING]
                ]

        # Copy the agent into the benchmarks S3 folder
        await copy_agent_to_benchmark(
            str(benchmark_id),
            start_benchmark_request.contract.name,
            harness_config.aws,
            harness_config.s3_bucket,
        )

        # Create benchmark cloudwatch log group
        create_benchmark_log_group(
            str(benchmark_id), harness_config.aws, harness_config.log_group, harness_config.log_retention_policy
        )

        # Semaphore to isolate concurrent sandboxes that are being made for the benchmark
        creation_semaphore = Semaphore(_SANDBOX_CREATION_CAP)

        # Load the tasks we are going to be tracking
        tracked_tasks: dict[str, TrackedTask] = {
            task_id: TrackedTask(
                process_task(
                    task_row,
                    start_benchmark_request,
                    benchmark_service,
                    benchmark_id,
                    task_id,
                    harness_config,
                    org,
                    sandbox_provider_config=sandbox_provider_config,
                    creation_semaphore=creation_semaphore,
                ),
                org,
                task_row.started_at,
            )
            for task_id, task_row in task_rows
        }

        # Start the monitor to track the state the tasks are in and cancel them when no longer valid
        monitor = TaskMonitor(benchmark_id, tracked_tasks, org, notifier=notifier)
        monitor_task = asyncio.create_task(monitor.track_tasks())

        semaphore = Semaphore(start_benchmark_request.concurrency)

        await gather(*[tracked_tasks[task_id].run(semaphore, task_row) for task_id, task_row in task_rows])

        await monitor_task

        with Session(bind=engine) as session:
            benchmark_row = _fetch_benchmark_for_update(benchmark_id, session, org)
            score_task_rows = _fetch_tasks_for_update(benchmark_id, session, org)
            if (benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES and verified_task_ids) or any(
                task.status in _RUNNABLE_TASK_STATUSES for task in score_task_rows
            ):
                finalization_deferred = True
                return
            score_snapshot = {task.task_id: (task.started_at, task.status) for task in score_task_rows}
            evaluation_results = fetch_final_score_inputs(session, benchmark_row, org)

            if not any(result is not None for result in evaluation_results.values()) and any(
                task.status == TaskStatus.STOPPED for task in score_task_rows
            ):
                set_benchmark_final_status(benchmark_row, session, org)
                return

        if not any(result is not None for result in evaluation_results.values()):
            raise TrackerServiceError("No tasks were completed successfully")

        # Calculate the final score based off the tasks that were ran
        final_score_response = await benchmark_service.final_score(
            evaluation_results=evaluation_results, dataset=start_benchmark_request.dataset
        )

        # Create the final evaluation row and add it to the database
        final_evaluation_row = FinalEvaluation(
            org_id=org.id,
            benchmark=benchmark_id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )
        with Session(bind=engine) as session:
            benchmark_row = _fetch_benchmark_for_update(benchmark_id, session, org)
            final_task_rows = _fetch_tasks_for_update(benchmark_id, session, org)
            if (
                (benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES and verified_task_ids)
                or {task.task_id for task in final_task_rows} != set(score_snapshot)
                or any(
                    task.status in _RUNNABLE_TASK_STATUSES
                    or not is_expected_attempt(task, score_snapshot[task.task_id][0])
                    or task.status != score_snapshot[task.task_id][1]
                    for task in final_task_rows
                )
            ):
                finalization_deferred = True
                return

            # Delete existing final evaluation if re-running
            if benchmark_row.final_evaluation:
                session.delete(benchmark_row.final_evaluation)
                session.flush()

            session.add(final_evaluation_row)
            set_benchmark_final_status(benchmark_row, session, org)

            # Push the final benchmark view to the bucket
            final_view: FinalViewResponse = create_final_view(benchmark_row, session, org)

            try:
                await upload_final_view(benchmark_row, final_view, harness_config)
            except Exception:
                logger.warning("Failed to upload final benchmark view", exc_info=True)

            # If the user has chosen to invoke a lambda function at the end of the benchmark
            # We run it but do not let a failure affect the benchmark status
            arguments = benchmark_row.arguments
            if arguments.lambda_function:
                # Expose the benchmark arguments and the benchmark id inside of the lambda
                lambda_payload: dict[str, Any] = arguments.model_dump()
                lambda_payload["benchmark_id"] = str(benchmark_id)

                try:
                    invoke_lambda(lambda_client(harness_config.aws), arguments.lambda_function, lambda_payload)
                except Exception:
                    logger.warning("Failed to invoke final benchmark Lambda", exc_info=True)

    except BenchmarkServiceUnauthenticatedError as e:
        logfire.warn("process_benchmark failed due to benchmark service auth error")
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        logger.warning(error_message)
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            finalization_deferred = not _commit_benchmark_attempt_error(
                benchmark_row, session, org, execution_attempts, run_attempts, error_message
            )
    except BenchmarkServiceError as e:
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            finalization_deferred = not _commit_benchmark_attempt_error(
                benchmark_row, session, org, execution_attempts, run_attempts, str(e)
            )
    except Exception as e:
        logfire.exception("process_benchmark failed")
        sentry_sdk.capture_exception(e)
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            finalization_deferred = not _commit_benchmark_attempt_error(
                benchmark_row,
                session,
                org,
                execution_attempts,
                run_attempts,
                f"{str(e)}\n{traceback.format_exc()}",
            )
    finally:
        if not finalization_deferred:
            with Session(bind=engine) as session:
                # Handle any misalignments between the benchmark status and tasks
                finalization_deferred = not catch_errors_during_cleanup(
                    benchmark_id, session, org, execution_attempts, run_attempts
                )

        if notifier and not finalization_deferred:
            try:
                with Session(bind=engine) as session:
                    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                    notification_context = NotificationContext.from_benchmark(benchmark_row, session, org)
                    final_score = benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None
                    await notifier.send_terminal_notification(
                        notification_context,
                        status=benchmark_row.status,
                        final_score=final_score,
                        error_message=benchmark_row.error_message,
                    )
            except Exception as notification_error:
                logger.warning(f"Failed to send terminal notification: {notification_error}")

        await benchmark_service.close()


def commit_benchmark_error(benchmark_row: Benchmark, session: Session, error_message: str) -> None:
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.add(benchmark_row)
    session.commit()


def _commit_benchmark_attempt_error(
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    execution_attempts: dict[str, datetime],
    run_attempts: dict[str, datetime],
    error_message: str,
) -> bool:
    benchmark_row = _fetch_benchmark_for_update(benchmark_row.id, session, org)
    if benchmark_row.status == BenchmarkStatus.STOPPING:
        return True
    if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
        return False

    task_rows = _fetch_tasks_for_update(benchmark_row.id, session, org)
    owned_tasks = [
        task
        for task in task_rows
        if task.task_id in execution_attempts and is_expected_attempt(task, execution_attempts[task.task_id])
    ]
    ownership_lost = len(owned_tasks) != len(execution_attempts) or _generation_changed(task_rows, run_attempts)
    for task in owned_tasks:
        if task.status not in _RUNNABLE_TASK_STATUSES:
            continue
        session.add(ErrorResult(org_id=org.id, task=task.id, error_message=error_message))
        task.status = TaskStatus.ERROR
        session.add(task)
    session.flush()
    if ownership_lost or any(task.status in _RUNNABLE_TASK_STATUSES for task in task_rows):
        session.commit()
        return False

    commit_benchmark_error(benchmark_row, session, error_message)
    return True


def catch_errors_during_cleanup(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    execution_attempts: dict[str, datetime],
    run_attempts: dict[str, datetime],
) -> bool:
    """
    On task exit we must clean up any edge cases so that it does not affect the users experience.
    There are sometimes fishy things that may occur with the sandboxes that if not dealt with could become an issue.

    1. Benchmark status must be in a finished state
    2. All tasks must be in a finished state
    """
    benchmark_row = _fetch_benchmark_for_update(benchmark_id, session, org)
    task_rows = _fetch_tasks_for_update(benchmark_id, session, org)
    owned_tasks = [
        task
        for task in task_rows
        if task.task_id in execution_attempts and is_expected_attempt(task, execution_attempts[task.task_id])
    ]
    ownership_lost = len(owned_tasks) != len(execution_attempts) or _generation_changed(task_rows, run_attempts)

    if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
        return not ownership_lost

    if benchmark_row.status == BenchmarkStatus.STOPPING:
        for task in owned_tasks:
            if task.status in _RUNNABLE_TASK_STATUSES:
                task.status = TaskStatus.STOPPED
                session.add(task)
        session.flush()
        if ownership_lost or any(task.status in _RUNNABLE_TASK_STATUSES for task in task_rows):
            session.commit()
            return False
        benchmark_row.status = BenchmarkStatus.STOPPED
        benchmark_row.error_message = None
        session.add(benchmark_row)
        session.commit()
        return True

    # Force non exited tasks from this worker's attempts to be ERROR
    task_terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    undetected_exit_tasks = [task for task in owned_tasks if task.status not in task_terminal_statuses]
    for task in undetected_exit_tasks:
        session.add(ErrorResult(org_id=org.id, task=task.id, error_message="Undetected exit of task"))
        task.status = TaskStatus.ERROR
        session.add(task)
    session.flush()

    if ownership_lost:
        session.commit()
        return False

    if any(task.status in _RUNNABLE_TASK_STATUSES for task in task_rows):
        session.commit()
        return False

    # Sweep stale RUNNING analyzer invocations to ERROR. The invoke_analyzer
    # helper uses try/finally so this only fires when the worker process was
    # killed mid-invocation (no try/finally cleanup ran).
    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_row.docent_reading_status = DocentReadingStatus.ERROR
        session.add(benchmark_row)

    # Force benchmark to ERROR so that the user knows they can retry any failed tasks
    commit_benchmark_error(
        benchmark_row,
        session,
        f"Run {benchmark_id} exited without finishing",
    )
    return True
