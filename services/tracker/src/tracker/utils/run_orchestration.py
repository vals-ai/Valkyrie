"""Run-level coordination: creating task rows, running all tasks, and finalizing the run."""

import asyncio
import traceback
from asyncio import Semaphore, gather
from dataclasses import dataclass
from typing import Any, Sequence, cast
from uuid import UUID

import logfire
import sentry_sdk
from benchmark_service import SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from opentelemetry import trace
from pydantic import ValidationError
from sqlmodel import Session, col, desc, func, select

from tracker._lambda import dry_run_lambda, invoke_lambda
from tracker.aws.cloudwatch_logs import create_benchmark_log_group
from tracker.aws.resolver import deployment_aws_runtime
from tracker.aws.runtime import AWSRuntime
from tracker.aws.secrets import fetch_aws_secret, resolve_secrets
from tracker.config import AUTH_REQUIRED, broker
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
from tracker.database.session import engine
from tracker.executor.dispatch_control import record_dispatch_failure, terminalize_active_dispatches
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from executor_protocol import EXECUTOR_TASK_NAME
from tracker.logging import get_logger
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.outbound_security import validate_custom_service_destination
from tracker.types import (
    FinalViewResponse,
    HarnessConfig,
    ManagedExecutionContext,
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
_RUNNABLE_TASK_STATUSES = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]


def set_benchmark_final_status(
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> None:
    """
    Delegates status depending on if any tasks have been stopped.
    """

    lock_execution_authority(session, authority)

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

    terminalize_active_dispatches(
        session,
        benchmark_row.id,
        except_dispatch_id=authority.dispatch_id if benchmark_status == BenchmarkStatus.FINISHED else None,
    )
    benchmark_row.status = benchmark_status
    benchmark_row.error_message = None
    session.add(benchmark_row)
    session.commit()


def create_task_rows(
    verified_task_ids: list[str],
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> Sequence[tuple[str, Task]]:
    """
    Create task_rows that do not already exist in the database for the benchmark row.

    NOTE: Only return runnable tasks to support resuming the benchmark.
    """
    lock_execution_authority(session, authority)

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
    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        lock_execution_authority(session, authority)
        if has_runnable_tasks(session, benchmark_row, org):
            return True
        terminalize_active_dispatches(
            session,
            benchmark_id,
            except_dispatch_id=authority.dispatch_id,
        )
        if has_stopped_tasks(session, benchmark_row, org):
            set_benchmark_final_status(benchmark_row, session, org, authority=authority)
            return False
        task_errors = benchmark_row.fetch_tasks_with_errors(session) or {}
        session.commit()

    error_message = await asyncio.to_thread(summarize_task_errors, task_errors)

    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if has_runnable_tasks(session, benchmark_row, org):
            return True
        if has_stopped_tasks(session, benchmark_row, org):
            set_benchmark_final_status(benchmark_row, session, org, authority=authority)
            return False

        # Mark the run as errored so future fetches return the discovered task errors.
        commit_benchmark_error(
            benchmark_row,
            session,
            error_message,
            producer="tracker",
            operation="summarize_task_errors",
            error_type="AllTasksFailed",
            authority=authority,
        )
        return False


def _parse_start_benchmark_request(payload: dict[str, Any]) -> StartBenchmarkRequest:
    """Validate a queued request without serializing credential-bearing input in errors."""
    request: StartBenchmarkRequest | None
    try:
        request = StartBenchmarkRequest.model_validate(payload)
    except ValidationError as exc:
        # Log field locations only; rendering the full error would expose input
        # values, which include AWS credentials on this payload.
        logger.warning(
            f"Queued benchmark request failed validation: {exc.errors(include_url=False, include_input=False)}"
        )
        request = None

    if request is None:
        raise ValueError("Queued benchmark request is invalid and cannot be processed.")
    return request


@dataclass(frozen=True)
class _QueuedExecution:
    request: StartBenchmarkRequest
    benchmark_id: UUID
    verified_task_ids: list[str]
    aws_managed: bool


def _parse_queued_execution(
    start_benchmark_request_json: dict[str, Any] | None,
    benchmark_id_str: str | None,
    verified_task_ids: list[str] | None,
    execution_context_json: dict[str, Any] | None,
) -> _QueuedExecution:
    if execution_context_json is None:
        if start_benchmark_request_json is None or benchmark_id_str is None or verified_task_ids is None:
            raise ValueError("Queued benchmark request is incomplete and cannot be processed.")
        request = _parse_start_benchmark_request(start_benchmark_request_json)
        if request.harness_config is None:
            raise ValueError("Queued access-key benchmark request has no AWS configuration.")
        return _QueuedExecution(
            request=request,
            benchmark_id=UUID(benchmark_id_str),
            verified_task_ids=verified_task_ids,
            aws_managed=False,
        )

    if start_benchmark_request_json is not None or benchmark_id_str is not None or verified_task_ids is not None:
        raise ValueError("Queued benchmark request mixes access-key and managed execution inputs.")
    try:
        context = ManagedExecutionContext.model_validate(execution_context_json)
    except ValidationError:
        raise ValueError("Queued managed execution context is invalid and cannot be processed.") from None
    return _QueuedExecution(
        request=context.start_benchmark_request,
        benchmark_id=context.benchmark_id,
        verified_task_ids=context.verified_task_ids,
        aws_managed=True,
    )


def _queued_benchmark_id(
    benchmark_id_str: str | None,
    execution_context_json: dict[str, Any] | None,
) -> UUID:
    raw_benchmark_id = (
        execution_context_json.get("benchmark_id") if execution_context_json is not None else benchmark_id_str
    )
    try:
        return UUID(str(raw_benchmark_id))
    except (TypeError, ValueError):
        raise ValueError("Queued benchmark request has no valid benchmark ID.") from None


def _preflight_managed_aws(
    execution: _QueuedExecution,
    runtime: AWSRuntime,
) -> SandboxProviderConfig:
    """Verify executor-owned AWS access before starting sandbox work."""
    create_benchmark_log_group(str(execution.benchmark_id), runtime)
    request = execution.request
    provider_secret_name = request.sandbox_provider_secret_name
    if provider_secret_name is None:
        raise ValueError("Queued managed benchmark request has no sandbox provider secret name.")
    sandbox_provider_config = fetch_sandbox_provider_config(
        provider_secret_name,
        runtime.clients,
        request.sandbox_provider,
    )
    resolve_secrets(request.contract.secrets, runtime.clients)
    if request.webhook_secret_name and request.webhook_intervals:
        fetch_aws_secret(request.webhook_secret_name, runtime.clients)
    if request.lambda_function:
        dry_run_lambda(runtime.clients, request.lambda_function)
    return sandbox_provider_config


async def upload_final_view_if_current(
    benchmark: Benchmark,
    final_view: FinalViewResponse,
    aws_runtime: AWSRuntime,
    authority: ExecutionAuthority,
) -> None:
    """Upload the canonical final view only while this dispatch still owns the run."""
    with Session(bind=engine) as session:
        lock_execution_authority(session, authority, require_in_progress=False)
        session.rollback()
    await upload_final_view(benchmark, final_view, aws_runtime)


# Keep the Tracker producer and ExecutorHost on one stable Taskiq wire name.
@broker.task(EXECUTOR_TASK_NAME)
@logfire.instrument("process_benchmark", extract_args=("benchmark_id_str", "verified_task_ids"))
async def process_benchmark(
    start_benchmark_request_json: dict[str, Any] | None = None,
    benchmark_id_str: str | None = None,
    verified_task_ids: list[str] | None = None,
    execution_context_json: dict[str, Any] | None = None,
    *,
    executor_dispatch_id: str,
) -> None:
    benchmark_id = _queued_benchmark_id(
        benchmark_id_str,
        execution_context_json,
    )
    try:
        authority = ExecutionAuthority(
            benchmark_id=benchmark_id,
            dispatch_id=UUID(executor_dispatch_id),
        )
    except ValueError as error:
        raise TrackerServiceError("Executor dispatch authority is invalid") from error

    # Resolve the org from the benchmark row (no org check on first fetch since the benchmark was just created by our system)
    with Session(bind=engine) as session:
        benchmark_row = session.get(Benchmark, benchmark_id)
        if not benchmark_row:
            raise TrackerServiceError(f"Run with id {benchmark_id} not found")
        org = session.exec(select(Org).where(Org.id == benchmark_row.org_id)).one()

    finalization_deferred = False
    benchmark_service: BenchmarkServiceClient | None = None
    notifier: SlackNotifier | None = None
    try:
        execution = _parse_queued_execution(
            start_benchmark_request_json,
            benchmark_id_str,
            verified_task_ids,
            execution_context_json,
        )
        start_benchmark_request = execution.request
        verified_task_ids = execution.verified_task_ids

        sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
        sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)
        trace.get_current_span().set_attributes(
            {
                "benchmark_id": str(benchmark_id),
                "benchmark_name": start_benchmark_request.benchmark_name,
                "agent_name": start_benchmark_request.contract.name,
                "task_count": len(verified_task_ids),
                "executor_dispatch_id": executor_dispatch_id,
            }
        )

        if execution.aws_managed != benchmark_row.aws_managed:
            queued_mode = "managed" if execution.aws_managed else "access-key"
            stored_mode = "managed" if benchmark_row.aws_managed else "access-key"
            raise TrackerServiceError(
                f"Queued {queued_mode} execution does not match the stored {stored_mode} run mode"
            )

        if start_benchmark_request.custom_benchmark_service is not None:
            validate_custom_service_destination(
                start_benchmark_request.custom_benchmark_service,
                org_name=org.name,
                auth_required=AUTH_REQUIRED,
            )

        if execution.aws_managed:
            aws_runtime = deployment_aws_runtime(org.id)
            sandbox_provider_config = _preflight_managed_aws(execution, aws_runtime)
        else:
            harness_config = cast(HarnessConfig, start_benchmark_request.harness_config)
            aws_runtime = AWSRuntime.from_harness_config(harness_config)
            sandbox_provider_config = fetch_sandbox_provider_config(
                harness_config.sandbox_provider_secret_name,
                aws_runtime.clients,
                start_benchmark_request.sandbox_provider,
            )

        benchmark_service = create_benchmark_service_client_from_request(start_benchmark_request)
        if start_benchmark_request.webhook_secret_name and start_benchmark_request.webhook_intervals:
            notifier = SlackNotifier(
                secret_name=start_benchmark_request.webhook_secret_name,
                clients=aws_runtime.clients,
                intervals=start_benchmark_request.webhook_intervals,
            )

        if not execution.aws_managed:
            create_benchmark_log_group(str(benchmark_id), aws_runtime)

        # Create tasks inside of the database for each task id
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            task_rows: Sequence[tuple[str, Task]] = create_task_rows(
                verified_task_ids,
                benchmark_row,
                session,
                org,
                authority=authority,
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
                    aws_runtime,
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

        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
            lock_execution_authority(session, authority)
            if has_runnable_tasks(session, benchmark_row, org):
                finalization_deferred = True
                return
            terminalize_active_dispatches(
                session,
                benchmark_id,
                except_dispatch_id=authority.dispatch_id,
            )
            evaluation_results = fetch_final_score_inputs(session, benchmark_row, org)
            session.commit()

        if not any(result is not None for result in evaluation_results.values()):
            finalization_deferred = await finalize_all_error_run(benchmark_id, org, authority=authority)
            return

        # Calculate the final score based off the tasks that were ran
        final_score_response = await benchmark_service.final_score(
            evaluation_results=evaluation_results, dataset=start_benchmark_request.dataset
        )

        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
            lock_execution_authority(session, authority)
            # final_score is a network call; a concurrent retry can make tasks runnable before we write FinalEvaluation.
            if has_runnable_tasks(session, benchmark_row, org):
                finalization_deferred = True
                return

        # Create the final evaluation row and add it to the database
        final_evaluation_row = FinalEvaluation(
            org_id=org.id,
            benchmark=benchmark_id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )

        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
            if has_runnable_tasks(session, benchmark_row, org):
                finalization_deferred = True
                return

            # Delete existing final evaluation if re-running
            if benchmark_row.final_evaluation:
                session.delete(benchmark_row.final_evaluation)
                session.flush()

            session.add(final_evaluation_row)
            # Commit the final score and terminal status together while retry/resume is blocked.
            set_benchmark_final_status(benchmark_row, session, org, authority=authority)

            # Recheck dispatch authority after the status commit without holding the
            # Retry admission lock across external operations.
            lock_execution_authority(session, authority, require_in_progress=False)
            session.commit()

            final_view: FinalViewResponse = create_final_view(benchmark_row, session, org)
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
            aws_runtime,
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
            invoke_lambda(aws_runtime.clients, lambda_function, lambda_payload)

    except ExecutionAuthorityRevoked:
        finalization_deferred = True
    except BenchmarkServiceUnauthenticatedError as e:
        logfire.warn("process_benchmark failed due to benchmark service auth error")
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            logger.warning(error_message)
            commit_benchmark_error(
                benchmark_row,
                session,
                error_message,
                producer="benchmark_service",
                operation="authenticate",
                error_type=type(e).__name__,
                cause_code="authentication_failed",
                authority=authority,
                task_ids=verified_task_ids,
            )
    except BenchmarkServiceError as e:
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            error_message = str(e)
            commit_benchmark_error(
                benchmark_row,
                session,
                error_message,
                producer="benchmark_service",
                operation="process_benchmark",
                error_type=type(e).__name__,
                authority=authority,
                task_ids=verified_task_ids,
            )
    except Exception as e:
        logfire.exception("process_benchmark failed")
        sentry_sdk.capture_exception(e)
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            commit_benchmark_error(
                benchmark_row,
                session,
                error_message,
                producer="tracker",
                operation="process_benchmark",
                error_type=type(e).__name__,
                authority=authority,
                task_ids=verified_task_ids,
            )
    finally:
        authority_current = False
        if not finalization_deferred:
            with Session(bind=engine) as session:
                # Handle any misalignments between the benchmark status and tasks
                authority_current = catch_errors_during_cleanup(
                    benchmark_id,
                    session,
                    org,
                    authority=authority,
                    task_ids=verified_task_ids,
                )

        if notifier and not finalization_deferred and authority_current:
            try:
                with Session(bind=engine) as session:
                    benchmark_row = lock_execution_authority(
                        session,
                        authority,
                        require_in_progress=False,
                    )
                    notification_context = NotificationContext.from_benchmark(benchmark_row, session, org)
                    final_score = benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None
                    notification_status = benchmark_row.status
                    notification_error_message = benchmark_row.error_message
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

        if benchmark_service is not None:
            await benchmark_service.close()


def commit_benchmark_error(
    benchmark_row: Benchmark,
    session: Session,
    error_message: str,
    *,
    producer: str,
    operation: str,
    error_type: str,
    authority: ExecutionAuthority,
    cause_code: str | None = None,
    task_ids: list[str] | None = None,
) -> bool:
    try:
        benchmark_row = lock_execution_authority(session, authority)
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    committed = record_dispatch_failure(
        session,
        benchmark=benchmark_row,
        dispatch_id=authority.dispatch_id,
        task_ids=task_ids or [],
        error_message=error_message,
        producer=producer,
        operation=operation,
        error_type=error_type,
        cause_code=cause_code,
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
) -> bool:
    """
    On task exit we must clean up any edge cases so that it does not affect the users experience.
    There are sometimes fishy things that may occur with the sandboxes that if not dealt with could become an issue.

    1. Benchmark status must be in a finished state
    2. All tasks must be in a finished state
    """
    terminal_statuses = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]

    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
    try:
        benchmark_row = lock_execution_authority(
            session,
            authority,
            require_in_progress=False,
        )
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    if benchmark_row.status in terminal_statuses:
        return True

    # Force non exited tasks to be ERROR
    task_terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    undetected_exit_tasks_query = (
        select(Task)
        .where(col(Task.benchmark) == benchmark_id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).notin_(task_terminal_statuses))
    )
    if task_ids is not None:
        undetected_exit_tasks_query = undetected_exit_tasks_query.where(col(Task.task_id).in_(task_ids))
    undetected_exit_tasks = session.exec(undetected_exit_tasks_query).all()

    # Sweep stale RUNNING analyzer invocations to ERROR. The invoke_analyzer
    # helper uses try/finally so this only fires when the executor process was
    # killed mid-invocation (no try/finally cleanup ran).
    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_row.docent_reading_status = DocentReadingStatus.ERROR
        session.add(benchmark_row)

    # Force benchmark to ERROR so that the user knows they can retry any failed tasks.
    error_message = f"Run {benchmark_id} exited without finishing"
    committed = record_dispatch_failure(
        session,
        benchmark=benchmark_row,
        dispatch_id=authority.dispatch_id,
        task_ids=[task.task_id for task in undetected_exit_tasks],
        error_message=error_message,
        producer="tracker",
        operation="cleanup",
        error_type="UndetectedExecutorExit",
    )
    session.commit()
    return committed
