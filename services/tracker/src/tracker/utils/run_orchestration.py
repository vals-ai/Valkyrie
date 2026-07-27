"""Run-level coordination: creating task rows, running all tasks, and finalizing the run."""

import asyncio
import traceback
from asyncio import Semaphore, gather
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence, cast
from uuid import UUID

import logfire
import sentry_sdk
from benchmark_service import SandboxProvider, SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
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
from tracker.scheduler.admission import SandboxQueueContext, create_queue_context, recover_queued_pool
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
_RUNNABLE_TASK_STATUSES = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
_TERMINAL_BENCHMARK_STATUSES = (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED)
_TaskFingerprint = tuple[tuple[UUID, datetime, TaskStatus, UUID | None], ...]


async def _run_queued_tasks(
    *,
    benchmark_id: UUID,
    task_rows: Sequence[tuple[str, Task]],
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkServiceClient,
    harness_config: HarnessConfig,
    org: Org,
    sandbox_provider_config: SandboxProviderConfig,
    sandbox_provider: SandboxProvider,
    creation_semaphore: Semaphore,
    queue_context: SandboxQueueContext,
    notifier: SlackNotifier | None,
    record_cancellation: Callable[[dict[UUID, datetime]], bool],
) -> None:
    """Run durable queued rows with active work plus one local pending contender."""
    owned_task_row_ids = {task_row.id for _, task_row in task_rows}
    local_runners: dict[UUID, tuple[datetime, TrackedTask, asyncio.Task[dict[str, dict[str, Any] | None]]]] = {}
    tracked_tasks: dict[str, TrackedTask] = {}
    coordinator_done = asyncio.Event()
    monitor = TaskMonitor(
        benchmark_id,
        tracked_tasks,
        org,
        limiter=None,
        notifier=notifier,
        coordinator_done=coordinator_done,
    )
    with Session(bind=engine) as session:
        resumable_evaluations = session.exec(
            select(Task)
            .where(col(Task.id).in_(owned_task_row_ids))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org.id)
            .where(Task.status == TaskStatus.EVALUATING)
            .with_for_update()
        ).all()
        for task_row in resumable_evaluations:
            if task_row.eval_resume_state is None:
                continue
            resumed_at = datetime.now(task_row.started_at.tzinfo)
            if resumed_at <= task_row.started_at:
                resumed_at = task_row.started_at + timedelta(microseconds=1)
            task_row.started_at = resumed_at
            session.add(task_row)
        session.commit()

    await recover_queued_pool(queue_context)
    monitor_task = asyncio.create_task(monitor.track_tasks())
    cancellation_attempts: dict[UUID, datetime] | None = None

    def launch(task_row: Task) -> None:
        tracked_task = TrackedTask(
            process_task(
                task_row,
                start_benchmark_request,
                benchmark_service,
                benchmark_id,
                task_row.task_id,
                harness_config,
                org,
                sandbox_provider_config=sandbox_provider_config,
                sandbox_provider=sandbox_provider,
                creation_semaphore=creation_semaphore,
                queue_context=queue_context,
            ),
            org,
            task_row.started_at,
        )
        tracked_tasks[task_row.task_id] = tracked_task
        runner = asyncio.create_task(tracked_task.run(None, task_row))
        local_runners[task_row.id] = (task_row.started_at, tracked_task, runner)

    try:
        while True:
            for task_row_id, (_attempt, _tracked, runner) in list(local_runners.items()):
                if runner.done():
                    await runner
                    del local_runners[task_row_id]

            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                benchmark_task_rows = list(
                    session.exec(select(Task).where(Task.benchmark == benchmark_id).where(Task.org_id == org.id)).all()
                )
                current_rows = [task_row for task_row in benchmark_task_rows if task_row.id in owned_task_row_ids]

            current_by_id = {task_row.id: task_row for task_row in current_rows}
            active_attempts = {
                task_row_id: attempt
                for task_row_id, (attempt, _tracked, runner) in local_runners.items()
                if not runner.done()
            }
            active_task_row_ids = {
                task_row_id
                for task_row_id, attempt in active_attempts.items()
                if (current_task := current_by_id.get(task_row_id)) is not None and current_task.started_at == attempt
            }
            used_slots = sum(
                task_row.status in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS)
                or (
                    task_row.status == TaskStatus.EVALUATING
                    and (
                        task_row.eval_resume_state is None
                        or task_row.id not in owned_task_row_ids
                        or task_row.id in active_task_row_ids
                    )
                )
                for task_row in benchmark_task_rows
            )
            available_slots = max(benchmark_row.arguments.concurrency - used_slots, 0)

            if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
                break

            if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and available_slots:
                evaluation_candidates = sorted(
                    (
                        task_row
                        for task_row in current_rows
                        if task_row.status == TaskStatus.EVALUATING
                        and task_row.eval_resume_state is not None
                        and task_row.id not in active_attempts
                    ),
                    key=lambda task_row: (task_row.started_at, task_row.id),
                )
                for task_row in evaluation_candidates[:available_slots]:
                    launch(task_row)
                available_slots -= min(len(evaluation_candidates), available_slots)

            pending_contender_exists = any(
                current_by_id.get(task_row_id) is not None
                and current_by_id[task_row_id].status == TaskStatus.PENDING
                and current_by_id[task_row_id].started_at == attempt
                and not runner.done()
                for task_row_id, (attempt, _tracked, runner) in local_runners.items()
            )
            if not pending_contender_exists and available_slots and benchmark_row.status == BenchmarkStatus.IN_PROGRESS:
                pending_candidates = [
                    task_row
                    for task_row in current_rows
                    if task_row.status == TaskStatus.PENDING and task_row.id not in active_attempts
                ]
                if pending_candidates:
                    launch(min(pending_candidates, key=lambda task_row: (task_row.started_at, task_row.id)))

            actionable_rows_remain = any(
                task_row.status in (TaskStatus.PENDING, TaskStatus.BUILDING)
                or (task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None)
                for task_row in current_rows
            )
            if not local_runners and not actionable_rows_remain:
                break

            await asyncio.sleep(queue_context.poll_interval_seconds)
    except asyncio.CancelledError:
        cancellation_attempts = {
            task_row_id: attempt for task_row_id, (attempt, _tracked, _runner) in local_runners.items()
        }
        raise
    finally:
        coordinator_done.set()
        if any(not runner.done() for _attempt, _tracked, runner in local_runners.values()):
            for _attempt, _tracked, runner in local_runners.values():
                runner.cancel()
        await gather(
            *(runner for _attempt, _tracked, runner in local_runners.values()),
            return_exceptions=True,
        )
        await monitor_task
        if cancellation_attempts is not None:
            record_cancellation(cancellation_attempts)


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

    return _load_verified_task_rows(
        verified_task_ids,
        benchmark_row,
        session,
        org,
        statuses=[TaskStatus.PENDING, TaskStatus.EVALUATING],
    )


def _load_verified_task_rows(
    verified_task_ids: Sequence[str],
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    statuses: Sequence[TaskStatus] | None = None,
) -> Sequence[tuple[str, Task]]:
    task_rows_query = (
        select(Task.task_id, Task)
        .where(Task.benchmark == benchmark_row.id)
        .where(Task.org_id == org.id)
        .where(col(Task.task_id).in_(verified_task_ids))
    )
    if statuses is not None:
        task_rows_query = task_rows_query.where(col(Task.status).in_(statuses))
    task_rows = session.exec(task_rows_query).all()

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


def _fetch_final_score_state(
    session: Session,
    benchmark_row: Benchmark,
    org: Org,
    *,
    for_update: bool = False,
) -> tuple[dict[str, dict[str, Any] | None], _TaskFingerprint]:
    # Fetch task rows which belong to the benchmark we are running
    task_rows_query = (
        select(Task.id, Task.task_id, Task.started_at, Task.status)
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .order_by(col(Task.id))
    )
    if for_update:
        task_rows_query = task_rows_query.with_for_update()
    task_rows = cast(
        Sequence[tuple[UUID, str, datetime, TaskStatus]],
        session.exec(task_rows_query).all(),
    )

    # Fetch all results from tasks that are finished
    task_row_ids = [task_row_id for task_row_id, _task_id, _started_at, _status in task_rows]
    result_rows = cast(
        Sequence[tuple[UUID, UUID, dict[str, Any]]],
        session.exec(
            select(EvaluationResult.task, EvaluationResult.id, EvaluationResult.result)  # pyright: ignore[reportUnknownArgumentType]
            .where(col(EvaluationResult.task).in_(task_row_ids))
            .where(col(EvaluationResult.org_id) == org.id)
            .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id))
        ).all(),
    )
    # Group results by task row ID
    latest_results: dict[UUID, tuple[UUID, dict[str, Any]]] = {}
    for task_row_id, result_id, result in result_rows:
        latest_results.setdefault(task_row_id, (result_id, result))

    inputs = {
        task_id: latest_results[task_row_id][1]
        if status == TaskStatus.FINISHED and task_row_id in latest_results
        else None
        for task_row_id, task_id, _started_at, status in task_rows
    }
    fingerprint_rows: list[tuple[UUID, datetime, TaskStatus, UUID | None]] = []
    for task_row_id, _task_id, started_at, status in task_rows:
        latest_result = latest_results.get(task_row_id)
        fingerprint_rows.append(
            (
                task_row_id,
                started_at,
                status,
                latest_result[0] if latest_result is not None else None,
            )
        )
    fingerprint = tuple(fingerprint_rows)
    return inputs, fingerprint


def fetch_final_score_inputs(session: Session, benchmark_row: Benchmark, org: Org) -> dict[str, dict[str, Any] | None]:
    """Return each task's latest evaluation result, or None when it is not finished."""
    return _fetch_final_score_state(session, benchmark_row, org)[0]


async def finalize_all_error_run(benchmark_id: UUID, org: Org) -> bool:
    """Finalize a run whose tasks produced no evaluation results.

    Arguments
    - benchmark_id: Run identifier to finalize.
    - org: Organization that owns the run.

    Returns
    - True when another coordinator or concurrent retry defers finalization, otherwise False.
    """
    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
            return True
        if has_stopped_tasks(session, benchmark_row, org):
            set_benchmark_final_status(benchmark_row, session, org)
            return False
        _evaluation_results, task_fingerprint = _fetch_final_score_state(
            session,
            benchmark_row,
            org,
            for_update=True,
        )
        task_errors = benchmark_row.fetch_tasks_with_errors(session) or {}

    error_message = await asyncio.to_thread(summarize_task_errors, task_errors)

    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
            return True
        if has_runnable_tasks(session, benchmark_row, org):
            return True
        if has_stopped_tasks(session, benchmark_row, org):
            set_benchmark_final_status(benchmark_row, session, org)
            return False
        _evaluation_results, current_fingerprint = _fetch_final_score_state(
            session,
            benchmark_row,
            org,
            for_update=True,
        )
        if current_fingerprint != task_fingerprint:
            return True

        # Mark the run as errored so future fetches return the discovered task errors.
        commit_benchmark_error(benchmark_row, session, error_message)
        return False


# Pin the Taskiq task name to its pre-refactor value so in-flight messages
# enqueued as `tracker.utils:process_benchmark` still match after the module move.
@broker.task("tracker.utils:process_benchmark")
@logfire.instrument("process_benchmark")
async def process_benchmark(
    start_benchmark_request_json: dict[str, Any],
    benchmark_id_str: str,
    verified_task_ids: list[str],
) -> None:
    # Was serialized to make it compatible with the broker
    start_benchmark_request: StartBenchmarkRequest = StartBenchmarkRequest(**start_benchmark_request_json)
    benchmark_id: UUID = UUID(benchmark_id_str)
    harness_config: HarnessConfig = start_benchmark_request.harness_config

    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)
    trace.get_current_span().set_attributes(
        {
            "benchmark_id": benchmark_id_str,
            "benchmark_name": start_benchmark_request.benchmark_name,
            "agent_name": start_benchmark_request.contract.name,
            "task_count": len(verified_task_ids),
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
        if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
            return
        queued_run = benchmark_row.arguments.queue_pool_id is not None

    finalization_deferred = False
    failure_recorded = False
    post_task_finalization = False
    finalization_fingerprint: _TaskFingerprint | None = None
    queued_cancellation_recorded = False
    benchmark_service: BenchmarkServiceClient | None = None
    queue_context: SandboxQueueContext | None = None
    task_rows: Sequence[tuple[str, Task]] = ()
    run_task_rows: Sequence[tuple[str, Task]] = ()
    limiter: ResizableLimiter | None = None
    sandbox_provider_config: SandboxProviderConfig | None = None
    sandbox_provider: SandboxProvider | None = None

    def record_failure(error_message: str) -> bool:
        with Session(bind=engine) as session:
            return _commit_process_benchmark_error(
                benchmark_id,
                session,
                org,
                [task_row for _, task_row in task_rows],
                error_message,
                allow_terminal_without_task_transition=post_task_finalization,
                expected_fingerprint=finalization_fingerprint if post_task_finalization else None,
            )

    def record_queued_cancellation(owned_attempts: dict[UUID, datetime]) -> bool:
        nonlocal queued_cancellation_recorded
        with Session(bind=engine) as session:
            queued_cancellation_recorded = _commit_queued_cancellation(
                benchmark_id,
                session,
                org,
                owned_attempts,
                "Run was interrupted",
            )
        return queued_cancellation_recorded

    if not queued_run:
        sandbox_provider_config = fetch_sandbox_provider_config(
            harness_config.sandbox_provider_secret_name,
            harness_config.aws,
            start_benchmark_request.sandbox_provider,
        )
        benchmark_service = create_benchmark_service_client_from_request(start_benchmark_request)
        try:
            sandbox_provider = benchmark_service.get_sandbox_provider(sandbox_provider_config)
        except BaseException:
            await benchmark_service.close()
            benchmark_service = None
            raise

    try:
        if queued_run:
            # Queued work must exist before provider setup so a later coordinator can recover it.
            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                task_rows = create_task_rows(verified_task_ids, benchmark_row, session, org)
                run_task_rows = _load_verified_task_rows(verified_task_ids, benchmark_row, session, org)
                queued_pool_id = benchmark_row.arguments.queue_pool_id
                limiter = None
            assert queued_pool_id is not None

            try:
                sandbox_provider_config = fetch_sandbox_provider_config(
                    harness_config.sandbox_provider_secret_name,
                    harness_config.aws,
                    start_benchmark_request.sandbox_provider,
                )
                benchmark_service = create_benchmark_service_client_from_request(start_benchmark_request)
                sandbox_provider = benchmark_service.get_sandbox_provider(sandbox_provider_config)
                queue_context = create_queue_context(
                    engine=engine,
                    provider=sandbox_provider,
                )
            except Exception as error:
                logger.warning("Sandbox provider setup failed (%s)", type(error).__name__)
                raise TrackerServiceError("Sandbox provider configuration is unavailable") from error
            if queue_context.pool_id != queued_pool_id:
                raise TrackerServiceError("Configured sandbox provider does not match the run's queued provider pool")

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

        if not queued_run:
            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                task_rows = create_task_rows(verified_task_ids, benchmark_row, session, org)
                run_task_rows = task_rows
                limiter = ResizableLimiter(benchmark_row.arguments.concurrency)

        assert benchmark_service is not None
        assert sandbox_provider_config is not None
        assert sandbox_provider is not None
        task_row_ids: set[str] = {task_id for task_id, _ in run_task_rows}
        missing_task_ids: list[str] = [task_id for task_id in verified_task_ids if task_id not in task_row_ids]
        if missing_task_ids:
            raise TrackerServiceError(
                f"Race condition occured when resuming run {benchmark_id}. Missing task ids: {', '.join(missing_task_ids)}"
            )

        # Semaphore to isolate concurrent sandboxes that are being made for the benchmark
        creation_semaphore = Semaphore(_SANDBOX_CREATION_CAP)

        if queue_context is not None:
            await _run_queued_tasks(
                benchmark_id=benchmark_id,
                task_rows=run_task_rows,
                start_benchmark_request=start_benchmark_request,
                benchmark_service=benchmark_service,
                harness_config=harness_config,
                org=org,
                sandbox_provider_config=sandbox_provider_config,
                sandbox_provider=sandbox_provider,
                creation_semaphore=creation_semaphore,
                queue_context=queue_context,
                notifier=notifier,
                record_cancellation=record_queued_cancellation,
            )
        else:
            assert limiter is not None
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
                        sandbox_provider=sandbox_provider,
                        creation_semaphore=creation_semaphore,
                    ),
                    org,
                    task_row.started_at,
                )
                for task_id, task_row in run_task_rows
            }
            monitor = TaskMonitor(benchmark_id, tracked_tasks, org, limiter=limiter, notifier=notifier)
            monitor_task = asyncio.create_task(monitor.track_tasks())

            await gather(*[tracked_tasks[task_id].run(limiter, task_row) for task_id, task_row in run_task_rows])
            await monitor_task

        post_task_finalization = True
        task_rows = ()
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
                finalization_deferred = True
                return
            if has_runnable_tasks(session, benchmark_row, org):
                finalization_deferred = True
                return
            evaluation_results, task_fingerprint = _fetch_final_score_state(
                session,
                benchmark_row,
                org,
                for_update=True,
            )
            finalization_fingerprint = task_fingerprint

        if not any(result is not None for result in evaluation_results.values()):
            finalization_deferred = await finalize_all_error_run(benchmark_id, org)
            return

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
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
            if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES or has_runnable_tasks(session, benchmark_row, org):
                finalization_deferred = True
                return
            _current_results, current_fingerprint = _fetch_final_score_state(
                session,
                benchmark_row,
                org,
                for_update=True,
            )
            if current_fingerprint != task_fingerprint:
                finalization_deferred = True
                return

            # Delete existing final evaluation if re-running
            if benchmark_row.final_evaluation:
                session.delete(benchmark_row.final_evaluation)
                session.flush()

            session.add(final_evaluation_row)
            # Commit the final score and terminal status together while retry/resume is blocked.
            set_benchmark_final_status(benchmark_row, session, org)

            # Push the final benchmark view to the bucket
            final_view: FinalViewResponse = create_final_view(benchmark_row, session, org)

            await upload_final_view(benchmark_row, final_view, harness_config)

            # Invoke the configured lambda after the benchmark's final view is uploaded.
            arguments = benchmark_row.arguments
            if arguments.lambda_function:
                # Expose the benchmark arguments, id, and name inside of the lambda
                lambda_payload: dict[str, Any] = arguments.model_dump()
                lambda_payload["benchmark_id"] = str(benchmark_id)
                lambda_payload["benchmark_name"] = benchmark_row.name

                invoke_lambda(lambda_client(harness_config.aws), arguments.lambda_function, lambda_payload)

    except asyncio.CancelledError:
        finalization_deferred = (
            not queued_cancellation_recorded
            if queued_run and not post_task_finalization
            else not record_failure("Run was interrupted")
        )
        failure_recorded = True
        raise
    except BenchmarkServiceUnauthenticatedError as e:
        logfire.warn("process_benchmark failed due to benchmark service auth error")
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        logger.warning(error_message)
        finalization_deferred = not record_failure(error_message)
        failure_recorded = True
    except BenchmarkServiceError as e:
        finalization_deferred = not record_failure(str(e))
        failure_recorded = True
    except TrackerServiceError as e:
        finalization_deferred = not record_failure(str(e))
        failure_recorded = True
    except Exception as e:
        logfire.exception("process_benchmark failed")
        sentry_sdk.capture_exception(e)
        finalization_deferred = not record_failure(f"{str(e)}\n{traceback.format_exc()}")
        failure_recorded = True
    finally:
        try:
            if not finalization_deferred and not failure_recorded:
                with Session(bind=engine) as session:
                    # Handle any misalignments between the benchmark status and tasks
                    catch_errors_during_cleanup(benchmark_id, session, org)

            if notifier and not finalization_deferred:
                try:
                    with Session(bind=engine) as session:
                        benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                        notification_context = NotificationContext.from_benchmark(benchmark_row, session, org)
                        final_score = (
                            benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None
                        )
                        await notifier.send_terminal_notification(
                            notification_context,
                            status=benchmark_row.status,
                            final_score=final_score,
                            error_message=benchmark_row.error_message,
                        )
                except Exception as notification_error:
                    logger.warning(f"Failed to send terminal notification: {notification_error}")
        finally:
            if benchmark_service is not None:
                await benchmark_service.close()


def commit_benchmark_error(benchmark_row: Benchmark, session: Session, error_message: str) -> None:
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.add(benchmark_row)
    session.commit()


def _commit_process_benchmark_error(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    owned_task_rows: Sequence[Task],
    error_message: str,
    *,
    allow_terminal_without_task_transition: bool = False,
    expected_fingerprint: _TaskFingerprint | None = None,
) -> bool:
    benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
    if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
        return False

    expected_states = {task_row.id: (task_row.started_at, task_row.status) for task_row in owned_task_rows}
    transitioned_task = False
    if expected_states:
        current_rows = session.exec(
            select(Task)
            .where(col(Task.id).in_(expected_states))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org.id)
            .with_for_update()
        ).all()
        for task_row in current_rows:
            expected_started_at, expected_status = expected_states[task_row.id]
            if task_row.started_at != expected_started_at or task_row.status not in _RUNNABLE_TASK_STATUSES:
                continue
            if benchmark_row.arguments.queue_pool_id is not None and task_row.status != expected_status:
                continue
            session.add(ErrorResult(org_id=org.id, task=task_row.id, error_message="Run failed"))
            task_row.status = TaskStatus.ERROR
            session.add(task_row)
            transitioned_task = True

    session.flush()
    if expected_fingerprint is not None:
        _evaluation_results, current_fingerprint = _fetch_final_score_state(
            session,
            benchmark_row,
            org,
            for_update=True,
        )
        if current_fingerprint != expected_fingerprint:
            session.commit()
            return False
    if has_runnable_tasks(session, benchmark_row, org):
        session.commit()
        return False
    if (
        benchmark_row.arguments.queue_pool_id is not None
        and expected_states
        and not transitioned_task
        and not allow_terminal_without_task_transition
    ):
        session.commit()
        return False

    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_row.docent_reading_status = DocentReadingStatus.ERROR
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.add(benchmark_row)
    session.commit()
    return True


def _commit_queued_cancellation(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    owned_attempts: dict[UUID, datetime],
    error_message: str,
) -> bool:
    benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
    if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
        return False

    transitioned_task = False
    if owned_attempts:
        current_rows = session.exec(
            select(Task)
            .where(col(Task.id).in_(owned_attempts))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org.id)
            .with_for_update()
        ).all()
        for task_row in current_rows:
            irrecoverable = task_row.status == TaskStatus.IN_PROGRESS or (
                task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is None
            )
            if task_row.started_at != owned_attempts[task_row.id] or not irrecoverable:
                continue
            session.add(ErrorResult(org_id=org.id, task=task_row.id, error_message=error_message))
            task_row.status = TaskStatus.ERROR
            session.add(task_row)
            transitioned_task = True

    session.flush()
    if not transitioned_task or has_runnable_tasks(session, benchmark_row, org):
        session.commit()
        return False

    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_row.docent_reading_status = DocentReadingStatus.ERROR
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.add(benchmark_row)
    session.commit()
    return True


def catch_errors_during_cleanup(benchmark_id: UUID, session: Session, org: Org) -> None:
    """
    On task exit we must clean up any edge cases so that it does not affect the users experience.
    There are sometimes fishy things that may occur with the sandboxes that if not dealt with could become an issue.

    1. Benchmark status must be in a finished state
    2. All tasks must be in a finished state
    """
    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
    if benchmark_row.status in _TERMINAL_BENCHMARK_STATUSES:
        return

    runnable_tasks = session.exec(
        select(Task)
        .where(Task.benchmark == benchmark_id)
        .where(Task.org_id == org.id)
        .where(col(Task.status).in_(_RUNNABLE_TASK_STATUSES))
    ).all()
    _commit_process_benchmark_error(
        benchmark_id,
        session,
        org,
        runnable_tasks,
        f"Run {benchmark_id} exited without finishing",
    )
