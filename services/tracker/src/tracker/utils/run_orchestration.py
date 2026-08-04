"""Run-level coordination: creating task rows, running all tasks, and finalizing the run."""

import asyncio
import traceback
from asyncio import Semaphore, gather
from typing import Any, Sequence, cast
from uuid import UUID

import logfire
import sentry_sdk
from benchmark_service.client import BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from opentelemetry import trace
from sqlmodel import Session, col, desc, func, select

from tracker._lambda import invoke_lambda, lambda_client
from tracker.aws.cloudwatch_logs import create_benchmark_log_group
from tracker.config import broker
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from executor_protocol import EXECUTOR_TASK_NAME
from tracker.database.session import engine
from tracker.executor.dispatch_control import record_dispatch_failure, terminalize_active_dispatches
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.logging import get_logger, run_id_var
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.types import (
    FinalViewResponse,
    HarnessConfig,
    StartBenchmarkRequest,
)

from tracker.utils.resources import (
    create_benchmark_service_client_from_request,
    fetch_run_row,
    fetch_sandbox_provider_config,
)
from tracker.utils.reporting import create_final_view, upload_final_view
from tracker.utils.task_error_summary import summarize_task_errors
from tracker.utils.task_execution import ResizableLimiter, TaskMonitor, TrackedTask, process_task

logger = get_logger(__name__)

_SANDBOX_CREATION_CAP: int = 10
_RUNNABLE_TASK_STATUSES = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]


def set_run_final_status(
    run_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> None:
    """Set the terminal run status based on whether any tasks were stopped."""

    lock_execution_authority(session, authority)

    # Check if any tasks are still in the pending or in progress state
    tasks_not_finished: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == run_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).in_(_RUNNABLE_TASK_STATUSES))
    ).one()

    # Tasks will be in a non-finished state if something interrupts them while they are running and the state errors here
    if tasks_not_finished:
        raise TrackerServiceError(f"Cannot set final status for run {run_row.id} because tasks are still runnable.")

    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == run_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    # Default status is finished; stopped tasks make the run resumable.
    run_status = BenchmarkStatus.FINISHED
    if tasks_stopped:
        run_status = BenchmarkStatus.STOPPED

    terminalize_active_dispatches(
        session,
        run_row.id,
        except_dispatch_id=authority.dispatch_id if run_status == BenchmarkStatus.FINISHED else None,
    )
    run_row.status = run_status
    run_row.error_message = None
    session.add(run_row)
    session.commit()


def create_task_rows(
    verified_task_ids: list[str],
    run_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> Sequence[tuple[str, Task]]:
    """
    Create task rows that do not already exist in the database for the run.

    NOTE: Only return runnable tasks to support resuming the run.
    """
    lock_execution_authority(session, authority)

    # Find task ids that already exist so that we can filter them out
    existing_task_ids: Sequence[str] = session.exec(
        select(Task.task_id).where(Task.benchmark == run_row.id).where(col(Task.task_id).in_(verified_task_ids))
    ).all()

    # NOTE: Must maintain same order that was passed in
    task_ids_to_create = [task_id for task_id in verified_task_ids if task_id not in existing_task_ids]

    for task_id in task_ids_to_create:
        task_row = Task(org_id=org.id, task_id=task_id, benchmark=run_row.id)
        session.add(task_row)

    session.commit()
    session.expire_all()

    task_rows = session.exec(
        select(Task.task_id, Task)
        .where(Task.benchmark == run_row.id)
        .where(Task.org_id == org.id)
        .where(col(Task.task_id).in_(verified_task_ids))
        .where(col(Task.status).in_([TaskStatus.PENDING, TaskStatus.EVALUATING]))
    ).all()

    task_rows_by_id: dict[str, Task] = {task_id: task_row for task_id, task_row in task_rows}
    return [(task_id, task_rows_by_id[task_id]) for task_id in verified_task_ids if task_id in task_rows_by_id]


def has_runnable_tasks(session: Session, run_row: Benchmark, org: Org) -> bool:
    return (
        session.exec(
            select(Task.id)
            .where(Task.benchmark == run_row.id)
            .where(Task.org_id == org.id)
            .where(col(Task.status).in_(_RUNNABLE_TASK_STATUSES))
        ).first()
        is not None
    )


def has_stopped_tasks(session: Session, run_row: Benchmark, org: Org) -> bool:
    return (
        session.exec(
            select(Task.id)
            .where(Task.benchmark == run_row.id)
            .where(Task.org_id == org.id)
            .where(Task.status == TaskStatus.STOPPED)
        ).first()
        is not None
    )


def fetch_final_score_inputs(session: Session, run_row: Benchmark, org: Org) -> dict[str, dict[str, Any] | None]:
    """
    Return a mapping of the task IDs to their latest evaluation results. If a task is not finished we default to None.
    """
    # Fetch task rows which belong to the run.
    task_rows = cast(
        Sequence[tuple[UUID, str, TaskStatus]],
        session.exec(
            select(Task.id, Task.task_id, Task.status)
            .where(col(Task.benchmark) == run_row.id)
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


async def finalize_all_error_run(
    run_id: UUID,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> bool:
    """Finalize a run whose tasks produced no evaluation results.

    Arguments
    - run_id: Run identifier to finalize.
    - org: Organization that owns the run.

    Returns
    - True when a concurrent retry defers finalization, otherwise False.
    """
    with Session(bind=engine) as session:
        run_row = fetch_run_row(run_id, session, org, for_update=True)
        lock_execution_authority(session, authority)
        if has_runnable_tasks(session, run_row, org):
            return True
        terminalize_active_dispatches(
            session,
            run_id,
            except_dispatch_id=authority.dispatch_id,
        )
        if has_stopped_tasks(session, run_row, org):
            set_run_final_status(run_row, session, org, authority=authority)
            return False
        task_errors = run_row.fetch_tasks_with_errors(session) or {}
        session.commit()

    error_message = await asyncio.to_thread(summarize_task_errors, task_errors)

    with Session(bind=engine) as session:
        run_row = fetch_run_row(run_id, session, org, for_update=True)
        if has_runnable_tasks(session, run_row, org):
            return True
        if has_stopped_tasks(session, run_row, org):
            set_run_final_status(run_row, session, org, authority=authority)
            return False
        # Mark the run as errored so future fetches return the discovered task errors.
        commit_run_error(run_row, session, error_message, authority=authority)
        return False


async def upload_final_view_if_current(
    run_row: Benchmark,
    final_view: FinalViewResponse,
    harness_config: HarnessConfig,
    authority: ExecutionAuthority,
) -> None:
    """Upload the canonical final view only while this dispatch still owns the run."""
    with Session(bind=engine) as session:
        lock_execution_authority(session, authority, require_in_progress=False)
        session.rollback()
    await upload_final_view(run_row, final_view, harness_config)


# Keep the Tracker producer and ExecutorHost on one stable Taskiq wire name.
@broker.task(EXECUTOR_TASK_NAME)
@logfire.instrument("process_run")
async def process_run(
    start_benchmark_request_json: dict[str, Any],
    benchmark_id_str: str,
    verified_task_ids: list[str],
    executor_dispatch_id: str,
) -> None:
    # Legacy broker argument names remain stable for already-enqueued tasks.
    try:
        authority = ExecutionAuthority(
            benchmark_id=UUID(benchmark_id_str),
            dispatch_id=UUID(executor_dispatch_id),
        )
    except ValueError as error:
        raise TrackerServiceError("Executor dispatch authority is invalid") from error

    run_request = StartBenchmarkRequest(**start_benchmark_request_json)
    run_id = authority.benchmark_id
    run_id_var.set(benchmark_id_str)
    harness_config: HarnessConfig = run_request.harness_config
    sandbox_provider_config = fetch_sandbox_provider_config(
        harness_config.sandbox_provider_secret_name,
        harness_config.aws,
        run_request.sandbox_provider,
    )
    benchmark_service = create_benchmark_service_client_from_request(run_request)

    sentry_sdk.set_tag("benchmark_name", run_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", run_request.contract.name)
    sentry_sdk.set_tag("run_id", benchmark_id_str)
    sentry_sdk.set_tag("benchmark_id", benchmark_id_str)
    trace.get_current_span().set_attributes(
        {
            "run_id": benchmark_id_str,
            "benchmark_id": benchmark_id_str,
            "benchmark_name": run_request.benchmark_name,
            "agent_name": run_request.contract.name,
            "task_count": len(verified_task_ids),
            "executor_dispatch_id": executor_dispatch_id,
        }
    )

    # Create notifier if webhook is configured
    notifier: SlackNotifier | None = None
    if run_request.webhook_secret_name and run_request.webhook_intervals:
        notifier = SlackNotifier(
            secret_name=run_request.webhook_secret_name,
            aws=harness_config.aws,
            intervals=run_request.webhook_intervals,
        )

    # Resolve the org from the new run row; it was just created by our system.
    with Session(bind=engine) as session:
        run_row = session.get(Benchmark, run_id)
        if not run_row:
            raise TrackerServiceError(f"Run with id {run_id} not found")
        org = session.exec(select(Org).where(Org.id == run_row.org_id)).one()

    finalization_deferred = False
    try:
        # Persisted CloudWatch names remain stable during the compatibility window.
        create_benchmark_log_group(
            str(run_id), harness_config.aws, harness_config.log_group, harness_config.log_retention_policy
        )

        # Create tasks inside of the database for each task id
        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org)
            task_rows: Sequence[tuple[str, Task]] = create_task_rows(
                verified_task_ids,
                run_row,
                session,
                org,
                authority=authority,
            )
            limiter = ResizableLimiter(run_row.arguments.concurrency)

        task_row_ids: set[str] = {task_id for task_id, _ in task_rows}
        missing_task_ids: list[str] = [task_id for task_id in verified_task_ids if task_id not in task_row_ids]
        if missing_task_ids:
            raise TrackerServiceError(
                f"Race condition occurred when resuming run {run_id}. Missing task ids: {', '.join(missing_task_ids)}"
            )

        # Semaphore to isolate concurrent sandboxes created for the run.
        creation_semaphore = Semaphore(_SANDBOX_CREATION_CAP)

        # Load the tasks we are going to be tracking
        tracked_tasks: dict[str, TrackedTask] = {
            task_id: TrackedTask(
                process_task(
                    task_row,
                    run_request,
                    benchmark_service,
                    run_id,
                    task_id,
                    harness_config,
                    org,
                    sandbox_provider_config=sandbox_provider_config,
                    creation_semaphore=creation_semaphore,
                    authority=authority,
                ),
                org,
                authority,
            )
            for task_id, task_row in task_rows
        }

        # Start the monitor to track the state the tasks are in and cancel them when no longer valid
        monitor = TaskMonitor(
            run_id,
            tracked_tasks,
            org,
            limiter=limiter,
            notifier=notifier,
            authority=authority,
        )
        monitor_task = asyncio.create_task(monitor.track_tasks())

        await gather(*[tracked_tasks[task_id].run(limiter, task_row) for task_id, task_row in task_rows])

        await monitor_task

        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org, for_update=True)
            lock_execution_authority(session, authority)
            if has_runnable_tasks(session, run_row, org):
                finalization_deferred = True
                return
            terminalize_active_dispatches(
                session,
                run_id,
                except_dispatch_id=authority.dispatch_id,
            )
            evaluation_results = fetch_final_score_inputs(session, run_row, org)
            session.commit()

        if not any(result is not None for result in evaluation_results.values()):
            finalization_deferred = await finalize_all_error_run(run_id, org, authority=authority)
            return

        final_score_response = await benchmark_service.final_score(
            evaluation_results=evaluation_results, dataset=run_request.dataset
        )

        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org, for_update=True)
            lock_execution_authority(session, authority)
            # final_score is a network call; a concurrent retry can make tasks runnable before we write FinalEvaluation.
            if has_runnable_tasks(session, run_row, org):
                finalization_deferred = True
                return

        # Create the final evaluation row and add it to the database
        final_evaluation_row = FinalEvaluation(
            org_id=org.id,
            benchmark=run_id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )

        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org, for_update=True)
            if has_runnable_tasks(session, run_row, org):
                finalization_deferred = True
                return

            # Delete existing final evaluation if re-running
            if run_row.final_evaluation:
                session.delete(run_row.final_evaluation)
                session.flush()

            session.add(final_evaluation_row)
            # Commit the final score and terminal status together while retry/resume is blocked.
            set_run_final_status(run_row, session, org, authority=authority)

            # Recheck dispatch authority after the status commit without holding the
            # Retry admission lock across external operations.
            lock_execution_authority(session, authority, require_in_progress=False)
            session.commit()

            final_view: FinalViewResponse = create_final_view(run_row, session, org)
            final_view_run = run_row
            lambda_function = run_row.arguments.lambda_function
            lambda_payload: dict[str, Any] | None = None
            if lambda_function:
                lambda_payload = run_row.arguments.model_dump()
                lambda_payload["run_id"] = str(run_id)
                lambda_payload["benchmark_id"] = str(run_id)
                lambda_payload["benchmark_name"] = run_row.name

        await upload_final_view_if_current(
            final_view_run,
            final_view,
            harness_config,
            authority,
        )

        # A Retry may win while the upload is in flight. Do not emit follow-on
        # callbacks for an execution that no longer owns the run.
        with Session(bind=engine) as authority_session:
            lock_execution_authority(
                authority_session,
                authority,
                require_in_progress=False,
            )
            authority_session.commit()

        if lambda_function and lambda_payload is not None:
            invoke_lambda(lambda_client(harness_config.aws), lambda_function, lambda_payload)

    except ExecutionAuthorityRevoked:
        finalization_deferred = True
    except BenchmarkServiceUnauthenticatedError as e:
        logfire.warn("process_run failed due to benchmark service auth error")
        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            logger.warning(error_message)
            commit_run_error(
                run_row,
                session,
                error_message,
                authority=authority,
                task_ids=verified_task_ids,
            )
    except BenchmarkServiceError as e:
        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org)
            error_message = str(e)
            commit_run_error(
                run_row,
                session,
                error_message,
                authority=authority,
                task_ids=verified_task_ids,
            )
    except Exception as e:
        logfire.exception("process_run failed")
        sentry_sdk.capture_exception(e)
        with Session(bind=engine) as session:
            run_row = fetch_run_row(run_id, session, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            commit_run_error(
                run_row,
                session,
                error_message,
                authority=authority,
                task_ids=verified_task_ids,
            )
    finally:
        authority_current = False
        if not finalization_deferred:
            with Session(bind=engine) as session:
                # Handle any misalignments between the run status and tasks.
                authority_current = catch_errors_during_cleanup(
                    run_id,
                    session,
                    org,
                    authority=authority,
                    task_ids=verified_task_ids,
                )

        if notifier and not finalization_deferred and authority_current:
            try:
                with Session(bind=engine) as session:
                    run_row = lock_execution_authority(
                        session,
                        authority,
                        require_in_progress=False,
                    )
                    notification_context = NotificationContext.from_run(run_row, session, org)
                    final_score = run_row.final_evaluation.final_score if run_row.final_evaluation else None
                    notification_status = run_row.status
                    notification_error_message = run_row.error_message
                    session.commit()
                    await notifier.send_terminal_notification(
                        notification_context,
                        status=notification_status,
                        final_score=final_score,
                        error_message=notification_error_message,
                    )
            except ExecutionAuthorityRevoked:
                pass
            except Exception as notification_error:
                logger.warning(f"Failed to send terminal notification: {notification_error}")

        await benchmark_service.close()


def commit_run_error(
    run_row: Benchmark,
    session: Session,
    error_message: str,
    *,
    authority: ExecutionAuthority,
    task_ids: list[str] | None = None,
) -> bool:
    try:
        run_row = lock_execution_authority(session, authority)
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    committed = record_dispatch_failure(
        session,
        benchmark=run_row,
        dispatch_id=authority.dispatch_id,
        task_ids=task_ids or [],
        error_message=error_message,
    )
    session.commit()
    return committed


def catch_errors_during_cleanup(
    run_id: UUID,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
    task_ids: list[str] | None = None,
) -> bool:
    """
    On task exit we must clean up any edge cases so that it does not affect the users experience.
    There are sometimes fishy things that may occur with the sandboxes that if not dealt with could become an issue.

    1. Run status must be in a finished state
    2. All tasks must be in a finished state
    """
    terminal_statuses = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]

    try:
        run_row = lock_execution_authority(
            session,
            authority,
            require_in_progress=False,
        )
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    if run_row.status in terminal_statuses:
        return True

    # Force non exited tasks to be ERROR
    task_terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    undetected_exit_tasks_query = (
        select(Task)
        .where(col(Task.benchmark) == run_id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).notin_(task_terminal_statuses))
    )
    if task_ids is not None:
        undetected_exit_tasks_query = undetected_exit_tasks_query.where(col(Task.task_id).in_(task_ids))
    undetected_exit_tasks = session.exec(undetected_exit_tasks_query).all()

    # Sweep stale RUNNING analyzer invocations to ERROR. The invoke_analyzer
    # helper uses try/finally so this only fires when the executor process was
    # killed mid-invocation (no try/finally cleanup ran).
    if run_row.docent_reading_status == DocentReadingStatus.RUNNING:
        run_row.docent_reading_status = DocentReadingStatus.ERROR
        session.add(run_row)

    # Force the run to ERROR so the user knows they can retry failed tasks.
    error_message = f"Run {run_id} exited without finishing"
    committed = record_dispatch_failure(
        session,
        benchmark=run_row,
        dispatch_id=authority.dispatch_id,
        task_ids=[task.task_id for task in undetected_exit_tasks],
        error_message=error_message,
    )
    session.commit()
    return committed
