"""Run-level coordination: creating task rows, running all tasks, and finalizing the run."""

import asyncio
import traceback
from asyncio import Semaphore, gather
from typing import Any, Sequence
from uuid import UUID

import logfire
import sentry_sdk
from benchmark_service.client import BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from opentelemetry import trace
from sqlmodel import Session

from tracker._lambda import invoke_lambda, lambda_client
from tracker.aws.cloudwatch_logs import create_benchmark_log_group
from tracker.config import broker
from tracker.database.repositories import (
    BenchmarkRepository,
    ExecutorControlRepository,
    RunControlRepository,
    TaskExecutionRepository,
    TaskRepository,
)
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    FinalEvaluation,
    Org,
    Task,
)
from tracker.database.session import engine
from tracker.database.transaction import open_tracker_transaction
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.execution_authority import ExecutionAuthority
from executor_protocol import EXECUTOR_TASK_NAME
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
from tracker.utils.task_error_summary import summarize_task_errors
from tracker.utils.task_execution import ResizableLimiter, TaskMonitor, TrackedTask, process_task

logger = get_logger(__name__)

_SANDBOX_CREATION_CAP: int = 10


def set_benchmark_final_status(
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
    run_control_repository: RunControlRepository,
    task_execution_repository: TaskExecutionRepository,
    executor_control_repository: ExecutorControlRepository,
    benchmark_repository: BenchmarkRepository,
) -> None:
    """
    Delegates status depending on if any tasks have been stopped.
    """

    task_execution_repository.lock_execution_authority(authority)

    tasks_not_finished = run_control_repository.count_runnable_tasks(benchmark_row.id, org.id)

    # Tasks will be in a non-finished state if something interrupts them while they are running and the state errors here
    if tasks_not_finished:
        raise TrackerServiceError(
            f"Cannot set final status for run {benchmark_row.id} because tasks are still runnable."
        )

    tasks_stopped = run_control_repository.count_stopped_tasks(benchmark_row.id, org.id)

    # Default status is finished, if we stopped any tasks the benchmark status is stopped
    # Later we can use the stopped status to determine if we can resume the benchmark
    benchmark_status = BenchmarkStatus.FINISHED
    if tasks_stopped:
        benchmark_status = BenchmarkStatus.STOPPED

    executor_control_repository.terminalize_active_dispatches(
        benchmark_row.id,
        except_dispatch_id=authority.dispatch_id if benchmark_status == BenchmarkStatus.FINISHED else None,
    )
    benchmark_repository.stage_final_status(benchmark_row, benchmark_status)
    session.commit()


def create_task_rows(
    verified_task_ids: list[str],
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
    task_repository: TaskRepository,
    task_execution_repository: TaskExecutionRepository,
) -> Sequence[tuple[str, Task]]:
    """
    Create task_rows that do not already exist in the database for the benchmark row.

    NOTE: Only return runnable tasks to support resuming the benchmark.
    """
    task_execution_repository.lock_execution_authority(authority)

    task_repository.create_missing_task_rows(
        benchmark_row.id,
        verified_task_ids,
        org.id,
    )
    session.commit()

    return task_repository.get_runnable_for_benchmark(benchmark_row.id, verified_task_ids, org.id)


async def finalize_all_error_run(
    benchmark_id: UUID,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> bool:
    """Finalize a run whose tasks produced no evaluation results.

    Arguments
    - benchmark_id: Run identifier to finalize.
    - org: Organization that owns the run.

    Returns
    - True when a concurrent retry defers finalization, otherwise False.
    """
    with open_tracker_transaction(engine) as transaction:
        session = transaction.session
        benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org, for_update=True)
        transaction.task_execution.lock_execution_authority(authority)
        if transaction.run_control.count_runnable_tasks(benchmark_row.id, org.id):
            return True
        transaction.executor_control.terminalize_active_dispatches(
            benchmark_id,
            except_dispatch_id=authority.dispatch_id,
        )
        if transaction.run_control.count_stopped_tasks(benchmark_row.id, org.id):
            set_benchmark_final_status(
                benchmark_row,
                session,
                org,
                authority=authority,
                run_control_repository=transaction.run_control,
                task_execution_repository=transaction.task_execution,
                executor_control_repository=transaction.executor_control,
                benchmark_repository=transaction.benchmarks,
            )
            return False
        task_errors = transaction.reporting.get_task_errors(benchmark_id, org.id) or {}
        transaction.commit()

    error_message = await asyncio.to_thread(summarize_task_errors, task_errors)

    with open_tracker_transaction(engine) as transaction:
        session = transaction.session
        benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org, for_update=True)
        if transaction.run_control.count_runnable_tasks(benchmark_row.id, org.id):
            return True
        if transaction.run_control.count_stopped_tasks(benchmark_row.id, org.id):
            set_benchmark_final_status(
                benchmark_row,
                session,
                org,
                authority=authority,
                run_control_repository=transaction.run_control,
                task_execution_repository=transaction.task_execution,
                executor_control_repository=transaction.executor_control,
                benchmark_repository=transaction.benchmarks,
            )
            return False

        # Mark the run as errored so future fetches return the discovered task errors.
        commit_benchmark_error(
            benchmark_row,
            session,
            error_message,
            authority=authority,
            task_execution_repository=transaction.task_execution,
            executor_control_repository=transaction.executor_control,
        )
        return False


async def upload_final_view_if_current(
    benchmark: Benchmark,
    final_view: FinalViewResponse,
    harness_config: HarnessConfig,
    authority: ExecutionAuthority,
) -> None:
    """Upload the canonical final view only while this dispatch still owns the run."""
    with open_tracker_transaction(engine) as transaction:
        transaction.task_execution.lock_execution_authority(authority, require_in_progress=False)
        transaction.rollback()
    await upload_final_view(benchmark, final_view, harness_config)


# Keep the Tracker producer and ExecutorHost on one stable Taskiq wire name.
@broker.task(EXECUTOR_TASK_NAME)
@logfire.instrument("process_benchmark")
async def process_benchmark(
    start_benchmark_request_json: dict[str, Any],
    benchmark_id_str: str,
    verified_task_ids: list[str],
    executor_dispatch_id: str,
) -> None:
    try:
        authority = ExecutionAuthority(
            benchmark_id=UUID(benchmark_id_str),
            dispatch_id=UUID(executor_dispatch_id),
        )
    except ValueError as error:
        raise TrackerServiceError("Executor dispatch authority is invalid") from error

    # Was serialized to make it compatible with the broker
    start_benchmark_request: StartBenchmarkRequest = StartBenchmarkRequest(**start_benchmark_request_json)
    benchmark_id = authority.benchmark_id
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
            "task_count": len(verified_task_ids),
            "executor_dispatch_id": executor_dispatch_id,
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
    with open_tracker_transaction(engine) as transaction:
        benchmark_row = transaction.benchmarks.get_by_id(benchmark_id)
        if not benchmark_row:
            raise TrackerServiceError(f"Run with id {benchmark_id} not found")
        org = transaction.organizations.get_by_id(benchmark_row.org_id)
        if org is None:
            raise TrackerServiceError(f"Organization {benchmark_row.org_id} not found")

    finalization_deferred = False
    try:
        # Create benchmark cloudwatch log group
        create_benchmark_log_group(
            str(benchmark_id), harness_config.aws, harness_config.log_group, harness_config.log_retention_policy
        )

        # Create tasks inside of the database for each task id
        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org)
            task_rows: Sequence[tuple[str, Task]] = create_task_rows(
                verified_task_ids,
                benchmark_row,
                transaction.session,
                org,
                authority=authority,
                task_repository=transaction.tasks,
                task_execution_repository=transaction.task_execution,
            )
            limiter = ResizableLimiter(benchmark_row.arguments.concurrency)

        task_row_ids: set[str] = {task_id for task_id, _ in task_rows}
        missing_task_ids: list[str] = [task_id for task_id in verified_task_ids if task_id not in task_row_ids]
        if missing_task_ids:
            raise TrackerServiceError(
                f"Race condition occurred when resuming run {benchmark_id}. Missing task ids: {', '.join(missing_task_ids)}"
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
                    authority=authority,
                ),
                org,
                authority,
            )
            for task_id, task_row in task_rows
        }

        # Start the monitor to track the state the tasks are in and cancel them when no longer valid
        monitor = TaskMonitor(
            benchmark_id,
            tracked_tasks,
            org,
            limiter=limiter,
            notifier=notifier,
            authority=authority,
        )
        monitor_task = asyncio.create_task(monitor.track_tasks())

        await gather(*[tracked_tasks[task_id].run(limiter, task_row) for task_id, task_row in task_rows])

        await monitor_task

        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org, for_update=True)
            transaction.task_execution.lock_execution_authority(authority)
            if transaction.run_control.count_runnable_tasks(benchmark_row.id, org.id):
                finalization_deferred = True
                return
            transaction.executor_control.terminalize_active_dispatches(
                benchmark_id,
                except_dispatch_id=authority.dispatch_id,
            )
            evaluation_results = transaction.reporting.fetch_final_score_inputs(benchmark_row.id, org.id)
            transaction.commit()

        if not any(result is not None for result in evaluation_results.values()):
            finalization_deferred = await finalize_all_error_run(benchmark_id, org, authority=authority)
            return

        # Calculate the final score based off the tasks that were ran
        final_score_response = await benchmark_service.final_score(
            evaluation_results=evaluation_results, dataset=start_benchmark_request.dataset
        )

        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org, for_update=True)
            transaction.task_execution.lock_execution_authority(authority)
            # final_score is a network call; a concurrent retry can make tasks runnable before we write FinalEvaluation.
            if transaction.run_control.count_runnable_tasks(benchmark_row.id, org.id):
                finalization_deferred = True
                return

        # Create the final evaluation row and add it to the database
        final_evaluation_row = FinalEvaluation(
            org_id=org.id,
            benchmark=benchmark_id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )

        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org, for_update=True)
            if transaction.run_control.count_runnable_tasks(benchmark_row.id, org.id):
                finalization_deferred = True
                return

            transaction.benchmarks.replace_final_evaluation(benchmark_row, final_evaluation_row)
            # Commit the final score and terminal status together while retry/resume is blocked.
            set_benchmark_final_status(
                benchmark_row,
                transaction.session,
                org,
                authority=authority,
                run_control_repository=transaction.run_control,
                task_execution_repository=transaction.task_execution,
                executor_control_repository=transaction.executor_control,
                benchmark_repository=transaction.benchmarks,
            )
            # Recheck dispatch authority after the status commit without holding the
            # Retry admission lock across external operations.
            transaction.task_execution.lock_execution_authority(authority, require_in_progress=False)
            transaction.commit()

            final_view: FinalViewResponse = create_final_view(
                benchmark_row,
                transaction.reporting,
                org,
            )
            final_view_benchmark = benchmark_row
            lambda_function = benchmark_row.arguments.lambda_function
            lambda_payload: dict[str, Any] | None = None
            if lambda_function:
                lambda_payload = benchmark_row.arguments.model_dump()
                lambda_payload["benchmark_id"] = str(benchmark_id)
                lambda_payload["benchmark_name"] = benchmark_row.name

        await upload_final_view_if_current(
            final_view_benchmark,
            final_view,
            harness_config,
            authority,
        )

        # A Retry may win while the upload is in flight. Do not emit follow-on
        # callbacks for an execution that no longer owns the run.
        with open_tracker_transaction(engine) as transaction:
            transaction.task_execution.lock_execution_authority(authority, require_in_progress=False)
            transaction.commit()

        if lambda_function and lambda_payload is not None:
            invoke_lambda(lambda_client(harness_config.aws), lambda_function, lambda_payload)

    except ExecutionAuthorityRevoked:
        finalization_deferred = True
    except BenchmarkServiceUnauthenticatedError as e:
        logfire.warn("process_benchmark failed due to benchmark service auth error")
        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            logger.warning(error_message)
            commit_benchmark_error(
                benchmark_row,
                transaction.session,
                error_message,
                authority=authority,
                task_execution_repository=transaction.task_execution,
                executor_control_repository=transaction.executor_control,
                task_ids=verified_task_ids,
            )
    except BenchmarkServiceError as e:
        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org)
            error_message = str(e)
            commit_benchmark_error(
                benchmark_row,
                transaction.session,
                error_message,
                authority=authority,
                task_execution_repository=transaction.task_execution,
                executor_control_repository=transaction.executor_control,
                task_ids=verified_task_ids,
            )
    except Exception as e:
        logfire.exception("process_benchmark failed")
        sentry_sdk.capture_exception(e)
        with open_tracker_transaction(engine) as transaction:
            benchmark_row = fetch_benchmark_row(benchmark_id, transaction.benchmarks, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            commit_benchmark_error(
                benchmark_row,
                transaction.session,
                error_message,
                authority=authority,
                task_execution_repository=transaction.task_execution,
                executor_control_repository=transaction.executor_control,
                task_ids=verified_task_ids,
            )
    finally:
        authority_current = False
        if not finalization_deferred:
            with open_tracker_transaction(engine) as transaction:
                # Handle any misalignments between the benchmark status and tasks
                authority_current = catch_errors_during_cleanup(
                    benchmark_id,
                    transaction.session,
                    org,
                    authority=authority,
                    task_ids=verified_task_ids,
                    benchmark_repository=transaction.benchmarks,
                    task_execution_repository=transaction.task_execution,
                    task_repository=transaction.tasks,
                    executor_control_repository=transaction.executor_control,
                )

        if notifier and not finalization_deferred and authority_current:
            try:
                with open_tracker_transaction(engine) as transaction:
                    benchmark_row = transaction.task_execution.lock_execution_authority(
                        authority,
                        require_in_progress=False,
                    )
                    notification_context = NotificationContext.from_benchmark(benchmark_row, transaction.reporting, org)
                    final_score = benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None
                    terminal_notification = (
                        notification_context,
                        benchmark_row.status,
                        final_score,
                        benchmark_row.error_message,
                    )
                    transaction.commit()
                notification_context, notification_status, final_score, notification_error_message = (
                    terminal_notification
                )
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


def commit_benchmark_error(
    benchmark_row: Benchmark,
    session: Session,
    error_message: str,
    *,
    authority: ExecutionAuthority,
    task_execution_repository: TaskExecutionRepository,
    executor_control_repository: ExecutorControlRepository,
    task_ids: list[str] | None = None,
) -> bool:
    try:
        benchmark_row = task_execution_repository.lock_execution_authority(authority)
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    committed = executor_control_repository.record_dispatch_failure(
        benchmark=benchmark_row,
        dispatch_id=authority.dispatch_id,
        task_ids=task_ids or [],
        error_message=error_message,
    )
    session.commit()
    return committed


def catch_errors_during_cleanup(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
    task_ids: list[str] | None = None,
    benchmark_repository: BenchmarkRepository,
    task_execution_repository: TaskExecutionRepository,
    task_repository: TaskRepository,
    executor_control_repository: ExecutorControlRepository,
) -> bool:
    """
    On task exit we must clean up any edge cases so that it does not affect the users experience.
    There are sometimes fishy things that may occur with the sandboxes that if not dealt with could become an issue.

    1. Benchmark status must be in a finished state
    2. All tasks must be in a finished state
    """
    terminal_statuses = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]

    benchmark_row = fetch_benchmark_row(benchmark_id, benchmark_repository, org)
    try:
        benchmark_row = task_execution_repository.lock_execution_authority(
            authority,
            require_in_progress=False,
        )
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    if benchmark_row.status in terminal_statuses:
        return True

    # Force non exited tasks to be ERROR
    undetected_exit_tasks = task_repository.get_nonterminal_for_benchmark(
        benchmark_id,
        org.id,
        task_ids=task_ids,
    )

    # Sweep stale RUNNING analyzer invocations to ERROR. The invoke_analyzer
    # helper uses try/finally so this only fires when the executor process was
    # killed mid-invocation (no try/finally cleanup ran).
    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_repository.stage_docent_error(benchmark_row)

    # Force benchmark to ERROR so that the user knows they can retry any failed tasks.
    error_message = f"Run {benchmark_id} exited without finishing"
    committed = executor_control_repository.record_dispatch_failure(
        benchmark=benchmark_row,
        dispatch_id=authority.dispatch_id,
        task_ids=[task.task_id for task in undetected_exit_tasks],
        error_message=error_message,
    )
    session.commit()
    return committed
