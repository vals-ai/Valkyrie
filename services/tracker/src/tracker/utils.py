import asyncio
import io
import json
import time
import traceback
from asyncio import Semaphore, gather
from collections.abc import AsyncGenerator, Buffer, Coroutine
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, NamedTuple, Sequence, cast
from uuid import UUID
from zoneinfo import ZoneInfo

import logfire
import sentry_sdk
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from daytona import AsyncDaytona, AsyncPaginatedSandboxes, AsyncSandbox, SandboxState
from daytona.common.errors import DaytonaNotFoundError, DaytonaRateLimitError
from fastapi import Request
from opentelemetry import trace
from sqlalchemy import JSON, type_coerce
from sqlmodel import Session, asc, case, col, delete, desc, func, or_, select, update
from tenacity import retry as tenacity_retry
from tenacity import retry_if_exception_type, stop_after_attempt, wait_fixed

from tracker._lambda import invoke_lambda
from tracker.aws.cloudwatch_logs import create_benchmark_log_group, write_benchmark_log_event
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    copy_agent_to_benchmark,
    create_benchmark_url,
    get_agent_result_s3_key,
    upload_to_s3,
)
from tracker.aws.secrets import fetch_aws_secret, resolve_secrets
from tracker.config import ENVIRONMENT, broker
from tracker.database.models import (
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Org,
    RetryMode,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.scoping import scoped_select
from tracker.database.session import engine
from tracker.daytona_retry import daytona_retry_callback, wait_daytona_rate_limit
from tracker.exceptions import SandboxSetupError, TrackerServiceError
from tracker.logging import get_logger, task_id_var
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.observability import elapsed_ms, retry_callback
from tracker.sandbox import create_sandbox, delete_sandbox, run_agent, upload_agent_artifacts
from tracker.types import (
    AverageTaskBreakdown,
    AWSCredentials,
    BenchmarkDetails,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FinalViewResponse,
    HarnessConfig,
    Order,
    StartBenchmarkRequest,
)

logger = get_logger(__name__)

_SANDBOX_CREATION_CAP: int = 10
_PTY_TASK_RETRY_LIMIT: int = 1


def fetch_daytona_headers(daytona_secret_name: str, aws: AWSCredentials) -> dict[str, str]:
    """Fetch Daytona credentials from AWS Secrets Manager and return as headers for BenchmarkServiceClient."""
    daytona_keys: list[str] = ["DAYTONA_API_KEY", "DAYTONA_API_URL", "DAYTONA_TARGET"]

    secret = fetch_aws_secret(daytona_secret_name, aws)

    if not isinstance(secret, dict):
        raise TrackerServiceError(f"Expected a dict with all daytona keys inside, received a string {secret}")

    missing_keys = set(daytona_keys) - set(secret.keys())
    if missing_keys:
        raise TrackerServiceError(f"Missing following keys to use daytona {', '.join(missing_keys)}")

    missing_values = [key for key, value in secret.items() if not value]
    if missing_values:
        raise TrackerServiceError(f"Missing values for the following keys {', '.join(missing_values)}")

    return {
        "x-api-key": secret["DAYTONA_API_KEY"],
        "x-api-url": secret["DAYTONA_API_URL"],
        "x-target": secret["DAYTONA_TARGET"],
    }


def create_benchmark_service_client(
    url: str, daytona_secret_name: str, aws: AWSCredentials, service_headers: dict[str, str] | None = None
) -> BenchmarkServiceClient:
    """Create a BenchmarkServiceClient using Daytona credentials from AWS Secrets Manager."""
    headers = fetch_daytona_headers(daytona_secret_name, aws)
    if service_headers:
        headers.update(service_headers)
    return BenchmarkServiceClient(url=url, headers=headers)


def start_benchmark_request_to_benchmark(request: StartBenchmarkRequest, org: Org) -> Benchmark:
    """Convert a StartBenchmarkRequest to a Benchmark database model."""
    return Benchmark(
        org_id=org.id,
        name=request.benchmark_name,
        custom_benchmark_service=request.custom_benchmark_service,
        webhook_secret_name=request.webhook_secret_name,
        webhook_intervals=request.webhook_intervals,
        arguments=BenchmarkArguments(
            contract=request.contract,
            concurrency=request.concurrency,
            task_ids=request.task_ids,
            slice_str=request.slice_str,
            lambda_function=request.lambda_function,
            dataset=request.dataset,
        ),
    )


class TrackedTaskStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    DONE = "done"


class TrackedTask:
    _coro: Coroutine[Any, Any, Any]
    _status: str
    _task: asyncio.Task[Any] | None
    _org: Org

    def __init__(self, coro: Coroutine[Any, Any, Any], org: Org):
        self._coro = coro
        self._org = org
        self._status = TrackedTaskStatus.WAITING
        self._task = None

    @property
    def status(self) -> str:
        return self._status

    @property
    def task(self) -> asyncio.Task[Any] | None:
        return self._task

    async def run(self, semaphore: asyncio.Semaphore, task_row: Task) -> dict[str, dict[str, Any] | None]:
        async def _wrap_coro():
            """Need to have a task created even if we are not running the coroutine so that we can cancel it before its running"""
            async with semaphore:
                self._status = TrackedTaskStatus.RUNNING
                return await self._coro

        try:
            self._task = asyncio.create_task(_wrap_coro())
            return await self._task
        except asyncio.CancelledError:
            logger.warning(f"Task {task_row.task_id} was cancelled")
            # Need to clean up the coroutine if we cancelled the task
            self._coro.close()

            # When we cancel we return the task id still so that we can track the task when we create the final evaluation row
            return {task_row.task_id: None}
        except Exception as e:
            error_message = f"Task error was not handled: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_message)
            logfire.exception("tracked_task_run failed")
            sentry_sdk.capture_exception(e)
            with Session(bind=engine) as session:
                task = fetch_task_row(task_row.id, session, self._org)
                commit_task_error(task, session, error_message)

            return {task_row.task_id: None}
        finally:
            self._status = TrackedTaskStatus.DONE


class TaskMonitor:
    _benchmark_id: UUID
    _task_tracking: dict[str, TrackedTask]
    _notifier: SlackNotifier | None
    _org: Org
    _TRACK_INTERVAL: int = 2

    def __init__(
        self, benchmark_id: UUID, task_tracking: dict[str, TrackedTask], org: Org, notifier: SlackNotifier | None = None
    ):
        self._benchmark_id = benchmark_id
        self._task_tracking = task_tracking
        self._org = org
        self._notifier = notifier

    def _fetch_task_row(self, task_id: str) -> Task:
        with Session(bind=engine) as session:
            task_row = session.exec(
                select(Task)
                .where(Task.task_id == task_id)
                .where(Task.benchmark == self._benchmark_id)
                .where(Task.org_id == self._org.id)
                .limit(1)
            ).first()

            if not task_row:
                raise ValueError(f"Task with id {task_id} not found")

            return task_row

    def _validate_task(self, task_id: str) -> bool:
        """
        If the task status has been set to stopped we return False to exit the task early.

        Returns:
            True if the task should continue to be processed, False if the task should be stopped early

        """
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(self._benchmark_id, session, self._org)
            task_row = self._fetch_task_row(task_id)

            # If task has been stopped or benchmark has errored we need to exit
            if task_row.status == TaskStatus.STOPPED or benchmark_row.status == BenchmarkStatus.ERROR:
                return False

        return True

    async def _check_notifications(self) -> None:
        """Check notification thresholds using DB task counts."""
        if not self._notifier:
            return

        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(self._benchmark_id, session, self._org)
            notification_context = NotificationContext.from_benchmark(benchmark_row, session, self._org)
            await self._notifier.check_and_notify(notification_context)

    async def track_tasks(self) -> None:
        """
        Tracks tasks and cancels them when they are no longer valid.
        """

        exit_condition_met: bool = False

        while not exit_condition_met and self._task_tracking:
            tasks_to_check: list[str] = list(self._task_tracking.keys())
            for task_id in tasks_to_check:
                task = self._task_tracking[task_id]

                if task.status == TrackedTaskStatus.DONE:
                    del self._task_tracking[task_id]
                    continue

                if not self._validate_task(task_id) and task.task is not None and not task.task.done():
                    task.task.cancel(f"Task {task_id} has been invalidated. Run has been requested to stop")

            await self._check_notifications()

            if not self._task_tracking:
                exit_condition_met = True

            await asyncio.sleep(self._TRACK_INTERVAL)


def fetch_benchmark_row(benchmark_id: UUID, session: Session, org: Org) -> Benchmark:
    """Fetch benchmark row with org validation. Raises domain errors (not HTTPException) for use in background tasks."""
    benchmark_row = session.get(Benchmark, benchmark_id)
    if not benchmark_row:
        raise ValueError(f"Run with id {benchmark_id} not found")
    if benchmark_row.org_id != org.id:
        raise ValueError(f"Run {benchmark_id} does not belong to org {org.id}")
    return benchmark_row


def handle_early_exit(task_row: Task, task_session: Session) -> None:
    _commit_task_status(task_row, task_session, TaskStatus.STOPPED)


def fetch_task_row(task_id: UUID, session: Session, org: Org) -> Task:
    """Fetch task row with org validation. Raises domain errors (not HTTPException) for use in background tasks."""
    task_row = session.get(Task, task_id)
    if not task_row:
        raise TrackerServiceError(f"Task with id {task_id} not found")
    if task_row.org_id != org.id:
        raise TrackerServiceError(f"Task {task_id} does not belong to org {org.id}")
    return task_row


def buffer_logs(
    log_queue: asyncio.Queue[str], stream_key: str, aws: AWSCredentials, log_group: str, force_flush: bool = False
) -> None:
    """
    Buffers the logs in the queue and waits till they are full before streaming them to CloudWatch.
    """
    if not log_queue.full() and not force_flush:
        return

    messages: list[str] = []
    while not log_queue.empty():
        messages.append(log_queue.get_nowait())

    message = "".join(messages)
    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, write_benchmark_log_event, stream_key, message, aws, log_group)


def save_eval_resume_state(task_row_id: UUID, org: Org, eval_resume_state: dict[str, Any]) -> None:
    with Session(bind=engine) as session:
        task = fetch_task_row(task_row_id, session, org)
        task.eval_resume_state = eval_resume_state
        session.commit()


def _commit_task_status(
    task: Task,
    session: Session,
    to_status: TaskStatus,
    *,
    error_message: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    from_status = task.status
    span_attributes = {
        "benchmark_id": str(task.benchmark),
        "task_id": task.task_id,
        "from_status": from_status.value,
        "to_status": to_status.value,
        **(extra or {}),
    }
    if error_message is not None:
        span_attributes["has_error_message"] = True

    with logfire.span("task.status_transition", **span_attributes):
        task.status = to_status
        if error_message is not None:
            task.error_message = error_message
        session.add(task)
        session.commit()


def commit_task_status_transition(task_row_id: UUID, session: Session, org: Org, to_status: TaskStatus) -> None:
    fetch_start = time.monotonic()
    task = fetch_task_row(task_row_id, session, org)
    _commit_task_status(task, session, to_status, extra={"fetch_duration_ms": elapsed_ms(fetch_start)})


@logfire.instrument("process_task")
@tenacity_retry(
    retry=retry_if_exception_type(SandboxSetupError),
    stop=stop_after_attempt(_PTY_TASK_RETRY_LIMIT + 1),
    wait=wait_fixed(2),
    before_sleep=retry_callback("valkyrie.task"),
    reraise=True,
)
async def process_task(
    task_row: Task,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkServiceClient,
    benchmark_id: UUID,
    task_id: str,
    harness_config: HarnessConfig,
    org: Org,
    creation_semaphore: Semaphore,
) -> dict[str, dict[str, Any] | None]:
    """
    Processes a task and returns the evaluation result

    NOTE: When we close the sandbox the agent process will be killed and we will instantly go to evaluating,
    the evaluation will fail since the instance no longer exists. We handle this inside of the exception caught.
    """
    task_id_var.set(task_id)
    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)
    trace.get_current_span().set_attributes(
        {
            "task_id": task_id,
            "benchmark_id": str(benchmark_id),
            "benchmark_name": start_benchmark_request.benchmark_name,
            "agent_name": start_benchmark_request.contract.name,
        }
    )

    with Session(bind=engine) as task_session:
        benchmark_row = fetch_benchmark_row(benchmark_id, task_session, org)
        task_row = task_session.merge(task_row)

        # If user has requested to stop the benchmark we exit before we process the task
        if benchmark_row.status == BenchmarkStatus.STOPPING:
            handle_early_exit(task_row, task_session)
            return {task_id: None}

    # Setup logging infrastructure before try block so it's always available
    # Suffix is required to version control streams, never delete between retires
    stream_suffix = f"{int(task_row.started_at.timestamp() * 1_000_000):x}"
    stream_key: str = f"{benchmark_id}:{task_id}_{stream_suffix}"
    log_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=20)

    last_log_time: float = time.monotonic()

    # Collects the logs and dumps them when the queue is full
    def log_output(data: str) -> None:
        nonlocal last_log_time
        last_log_time = time.monotonic()
        log_queue.put_nowait(data)
        buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group)

    # Auto flush if process takes a while to produce next log
    # If a process pauses without producing anymore logs, the logs we have collected get stuck
    async def auto_flush_logs() -> None:
        while True:
            await asyncio.sleep(1)
            if not log_queue.empty() and time.monotonic() - last_log_time >= 10:
                buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group, force_flush=True)

    flush_task = asyncio.create_task(auto_flush_logs())

    def on_eval_resume_state(state: dict[str, Any]) -> None:
        save_eval_resume_state(task_row.id, org, state)

    try:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                resume_eval_start_time = time.perf_counter()
                evaluation_result = await benchmark_service.resume_evaluation(
                    task_row.task_id,
                    eval_resume_state=task_row.eval_resume_state,
                    on_message=log_output,
                    on_eval_resume_state=on_eval_resume_state,
                    dataset=start_benchmark_request.dataset,
                )
                resume_eval_duration = time.perf_counter() - resume_eval_start_time
                evaluation_result_row = EvaluationResult(
                    org_id=org.id,
                    task=task_row.id,
                    instance_id=None,
                    result=evaluation_result,
                    agent_caused_exit_reason=None,
                )

                with Session(bind=engine) as task_session:
                    task_session.add(evaluation_result_row)
                    task_in_session = fetch_task_row(task_row.id, task_session, org)
                    if task_in_session.task_breakdown:
                        existing_breakdown = task_session.get(TaskBreakdown, task_in_session.task_breakdown)
                        existing_breakdown.evaluation_run_duration = resume_eval_duration
                    commit_task_status_transition(task_row.id, task_session, org, TaskStatus.FINISHED)

                    return {task_id: evaluation_result_row.result}
            except Exception as e:
                with Session(bind=engine) as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}

                raise e from e

        task_data = await benchmark_service.retrieve_task(task_id=task_id, dataset=start_benchmark_request.dataset)

        # Labels that show up in the UI we can use to filter sandboxes
        labels = {
            "Benchmark": start_benchmark_request.benchmark_name,
            "Id": str(benchmark_id),
            "Task": task_row.task_id,
        }

        with Session(bind=engine) as task_session:
            commit_task_status_transition(task_row.id, task_session, org, TaskStatus.BUILDING)

        env_vars = {
            **resolve_secrets(start_benchmark_request.contract.secrets, harness_config.aws),
            # Tags sandbox-internal OTel telemetry with our IDs + environment so traces/logs/metrics
            # are filterable per benchmark run and separable from other environments sharing the
            # same Daytona account (sandbox OTLP export is account-level).
            "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS": (
                f"benchmark_id={benchmark_id},task_id={task_row.task_id},environment={ENVIRONMENT}"
            ),
        }

        # We don't want to track the task until the sandbox is actually created.
        task_breakdown = TaskBreakdown()

        start_sandbox_build_time = time.perf_counter()
        async with create_sandbox(
            daytona=benchmark_service.daytona_client,
            sandbox_name=task_row.alias,
            image=task_data.docker_image,
            labels=labels,
            env_vars=env_vars,
            resources=task_data.resources,
            creation_semaphore=creation_semaphore,
        ) as sandbox:
            task_breakdown.sandbox_build_duration = time.perf_counter() - start_sandbox_build_time
            start_sandbox_run_time = time.perf_counter()

            try:
                with Session(bind=engine) as task_session:
                    commit_task_status_transition(task_row.id, task_session, org, TaskStatus.IN_PROGRESS)

                # Upload the contract to the sandbox after creating and install the dependencies
                await upload_agent_artifacts(
                    sandbox,
                    start_benchmark_request.contract,
                    str(benchmark_id),
                    harness_config.aws,
                    harness_config.s3_bucket,
                )

                _ = await benchmark_service.setup_task(
                    task_row.task_id, sandbox.id, on_message=log_output, dataset=start_benchmark_request.dataset
                )

                # Force flush the logs if anything has been buffered
                buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group, force_flush=True)

                # Compute the S3 key for the agent's output archive
                agent_output_s3_key = None
                if start_benchmark_request.contract.final_output:
                    agent_output_s3_key = get_agent_result_s3_key(str(benchmark_id), task_id, "agent_output.tar.gz")

                exit_reason, agent_run_time = await run_agent(
                    sandbox,
                    start_benchmark_request.contract,
                    task_data.problem_path,
                    task_id,
                    log_output,
                    task_data.cwd,
                    aws=harness_config.aws,
                    s3_bucket=harness_config.s3_bucket,
                    agent_output_s3_key=agent_output_s3_key,
                    agent_timeout=task_data.agent_timeout,
                    benchmark_id=str(benchmark_id),
                )
                logger.info(
                    "agent.run.complete",
                    extra={
                        "benchmark_id": str(benchmark_id),
                        "task_id": task_row.task_id,
                        "sandbox_id": sandbox.id,
                        "sandbox_name": sandbox.name,
                        "exit_reason": exit_reason.value if exit_reason else None,
                    },
                )

                task_breakdown.agent_run_duration = agent_run_time

                with Session(bind=engine) as task_session:
                    task_session.add(task_breakdown)
                    task_in_session = fetch_task_row(task_row.id, task_session, org)
                    task_in_session.task_breakdown = task_breakdown.id
                    commit_task_status_transition(task_row.id, task_session, org, TaskStatus.EVALUATING)

                # Evaluate the instance
                evaluation_start_time = time.perf_counter()

                logger.info(
                    "task.evaluation.start",
                    extra={
                        "benchmark_id": str(benchmark_id),
                        "task_id": task_row.task_id,
                        "sandbox_id": sandbox.id,
                        "sandbox_name": sandbox.name,
                    },
                )
                logger.info(f"Evaluating agent {start_benchmark_request.contract.name} in sandbox {sandbox.name}")
                evaluation_result = await benchmark_service.evaluate_instance(
                    task_row.task_id,
                    sandbox.id,
                    on_message=log_output,
                    on_eval_resume_state=on_eval_resume_state,
                    dataset=start_benchmark_request.dataset,
                )

                task_breakdown.evaluation_run_duration = time.perf_counter() - evaluation_start_time

                task_breakdown.sandbox_run_duration = time.perf_counter() - start_sandbox_run_time

                # Force flush the logs, maybe redundant since we have the one in finally:
                buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group, force_flush=True)

                # Save the evaluation result to the database with the task row
                # Record the termination reason if the agent did not exit cleanly (timeout / OS kill)
                evaluation_result_row = EvaluationResult(
                    org_id=org.id,
                    task=task_row.id,
                    instance_id=sandbox.id,
                    result=evaluation_result,
                    agent_caused_exit_reason=exit_reason,
                )

                with Session(bind=engine) as task_session:
                    task_session.add(evaluation_result_row)
                    task_in_session = fetch_task_row(task_row.id, task_session, org)
                    existing_breakdown = task_session.get(TaskBreakdown, task_in_session.task_breakdown)
                    existing_breakdown.evaluation_run_duration = task_breakdown.evaluation_run_duration
                    existing_breakdown.sandbox_run_duration = task_breakdown.sandbox_run_duration
                    commit_task_status_transition(task_row.id, task_session, org, TaskStatus.FINISHED)

                    return {task_id: evaluation_result_row.result}
            except Exception:
                with Session(bind=engine) as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}

                raise

    except SandboxSetupError as e:
        log_output(f"\n[ERROR] {e}")
        raise
    except Exception as e:
        logfire.exception("process_task failed")
        error_message = str(e)
        logger.error(error_message, exc_info=True)

        sentry_sdk.capture_exception(e)

        # include the error message
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message)

        return {task_id: None}
    finally:
        flush_task.cancel()
        buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group, force_flush=True)


def set_benchmark_final_status(benchmark_row: Benchmark, session: Session, org: Org) -> None:
    """
    Delegates status depending on if any tasks have been stopped.
    """

    # Check if any tasks are still in the pending or in progress state
    tasks_not_finished: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(
            col(Task.status).in_(
                [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
            )
        )
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

    runnable_statuses = [TaskStatus.PENDING, TaskStatus.EVALUATING]
    task_rows: Sequence[tuple[str, Task]] = session.exec(
        select(Task.task_id, Task)
        .where(Task.benchmark == benchmark_row.id)
        .where(col(Task.status).in_(runnable_statuses))
    ).all()

    return task_rows


async def fetch_missing_tasks(
    session: Session, benchmark_row: Benchmark, evaluation_results: dict[str, dict[str, Any] | None], org: Org
):
    remaining_task_results_query = cast(
        Sequence[tuple[str, dict[str, Any] | None]],
        session.exec(
            select(Task.task_id, EvaluationResult.result)  # pyright: ignore[reportUnknownArgumentType]
            .outerjoin(EvaluationResult, col(Task.id) == col(EvaluationResult.task))
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.task_id).notin_(list(evaluation_results.keys())))
        ).all(),
    )

    remaining_task_results: dict[str, dict[str, Any] | None] = {
        task_id: evaluation_result for task_id, evaluation_result in remaining_task_results_query
    }
    return remaining_task_results


@broker.task
@logfire.instrument("process_benchmark")
async def process_benchmark(
    start_benchmark_request_json: dict[str, Any],
    benchmark_id_str: str,
    verified_task_ids: list[str],
) -> None:
    # Was serialized to make it compatible with the broker
    start_benchmark_request: StartBenchmarkRequest = StartBenchmarkRequest(**start_benchmark_request_json)
    benchmark_id: UUID = UUID(benchmark_id_str)
    benchmark_service = start_benchmark_request.benchmark_service
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

    try:
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

        # Create tasks inside of the database for each task id
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            task_rows: Sequence[tuple[str, Task]] = create_task_rows(verified_task_ids, benchmark_row, session, org)

        task_row_ids: set[str] = {task_id for task_id, _ in task_rows}
        missing_task_ids: list[str] = [task_id for task_id in verified_task_ids if task_id not in task_row_ids]
        if missing_task_ids:
            raise TrackerServiceError(
                f"Race condition occured when resuming run {benchmark_id}. Missing task ids: {', '.join(missing_task_ids)}"
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
                    creation_semaphore=creation_semaphore,
                ),
                org,
            )
            for task_id, task_row in task_rows
        }

        # Start the monitor to track the state the tasks are in and cancel them when no longer valid
        monitor = TaskMonitor(benchmark_id, tracked_tasks, org, notifier=notifier)
        monitor_task = asyncio.create_task(monitor.track_tasks())

        semaphore = Semaphore(start_benchmark_request.concurrency)

        evaluation_result_rows: list[dict[str, dict[str, Any] | None]] = await gather(
            *[tracked_tasks[task_id].run(semaphore, task_row) for task_id, task_row in task_rows]
        )

        await monitor_task

        evaluation_results: dict[str, dict[str, Any] | None] = {}
        if any(result_dict for result_dict in evaluation_result_rows):
            # NOTE: Tasks with errors will still need to be included inside of the final score calculation to ensure that they are accounted for
            evaluation_results = {
                task_id: evaluation_result
                for result_dict in evaluation_result_rows
                for task_id, evaluation_result in result_dict.items()
            }

        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            # Fetch remaining tasks (in case this benchmark was resumed)
            remaining_task_results = await fetch_missing_tasks(session, benchmark_row, evaluation_results, org)

        evaluation_results.update(remaining_task_results)

        if not evaluation_results:
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
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            # Delete existing final evaluation if re-running
            if benchmark_row.final_evaluation:
                session.delete(benchmark_row.final_evaluation)
                session.flush()

            session.add(final_evaluation_row)
            session.commit()

            set_benchmark_final_status(benchmark_row, session, org)

            # Push the final benchmark view to the bucket
            final_view: FinalViewResponse = create_final_view(benchmark_row, session, org)

            await upload_final_view(benchmark_row, final_view, harness_config)

            # If the user has chosen to invoke a lambda function at the end of the benchmark
            # We run it but do not let a failure affect the benchmark status
            arguments = benchmark_row.arguments
            if arguments.lambda_function:
                # Expose the benchmark arguments and the benchmark id inside of the lambda
                lambda_payload: dict[str, Any] = arguments.model_dump()
                lambda_payload["benchmark_id"] = str(benchmark_id)

                invoke_lambda(arguments.lambda_function, lambda_payload, harness_config.aws)

    except Exception as e:
        logfire.exception("process_benchmark failed")
        sentry_sdk.capture_exception(e)
        with Session(bind=engine) as session:
            benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
            error_message = f"{str(e)}\n{traceback.format_exc()}"
            commit_benchmark_error(benchmark_row, session, error_message)
    finally:
        with Session(bind=engine) as session:
            # Handle any misalignments between the benchmark status and tasks
            catch_errors_during_cleanup(benchmark_id, session, org)

        if notifier:
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


class TaskCounts(NamedTuple):
    total_tasks: int
    finished_tasks: int
    failed_tasks: int


class BenchmarkContext:
    _benchmark_row: Benchmark
    _session: Session
    _org: Org

    def __init__(self, benchmark_row: Benchmark, session: Session, org: Org):
        self._benchmark_row = benchmark_row
        self._session = session
        self._org = org

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
            .where(Task.org_id == self._org.id)
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
            .where(Task.org_id == self._org.id)
            .group_by(Task.status)
            .having(func.count(col(Task.id)) > 0)  # Exclude all with count of 0
        )

        result = self._session.exec(statement).all()

        if not result:
            raise TrackerServiceError(
                f"No tasks have been discovered for run {self._benchmark_row.id}, cannot provide task breakdown"
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


def fetch_evaluation_results(benchmark_id: UUID, session: Session, org_id: UUID) -> dict[str, dict[str, Any]]:
    """Select all evaluation results for a given benchmark"""
    statement = (
        select(EvaluationResult, Task.task_id, TaskBreakdown)
        .join(Task, col(EvaluationResult.task) == col(Task.id))
        .outerjoin(TaskBreakdown, col(Task.task_breakdown) == col(TaskBreakdown.id))
        .where(Task.benchmark == benchmark_id)
        .where(Task.org_id == org_id)
    )
    results = session.exec(statement).all()

    evaluation_results: dict[str, dict[str, Any]] = {}
    for evaluation_result, task_id, task_breakdown in results:
        task_breakdown = cast(TaskBreakdown | None, task_breakdown)
        result_data = evaluation_result.result
        result_data["agent_caused_exit_reason"] = evaluation_result.agent_caused_exit_reason
        if task_breakdown is not None:
            result_data["task_breakdown"] = task_breakdown.model_dump()
        evaluation_results[task_id] = result_data

    return evaluation_results


def fetch_average_task_breakdown(benchmark_id: UUID, session: Session, org_id: UUID) -> AverageTaskBreakdown | None:
    """
    Fetch the average task breakdown for a given benchmark.

    Returns None if there are no task metrics available for the benchmark.
    """
    row = session.exec(
        select(
            func.avg(TaskBreakdown.sandbox_build_duration),
            func.avg(TaskBreakdown.agent_run_duration),
            func.avg(TaskBreakdown.evaluation_run_duration),
            func.avg(TaskBreakdown.sandbox_run_duration),
        )
        .join(Task, col(Task.task_breakdown) == col(TaskBreakdown.id))
        .where(Task.benchmark == benchmark_id)
        .where(Task.org_id == org_id)
    ).one()

    if all(v is None for v in row):
        return None

    return AverageTaskBreakdown(
        sandbox_build_duration=row[0],
        agent_run_duration=row[1],
        evaluation_run_duration=row[2],
        sandbox_run_duration=row[3],
    )


def commit_benchmark_error(benchmark_row: Benchmark, session: Session, error_message: str) -> None:
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.add(benchmark_row)
    session.commit()


def catch_errors_during_cleanup(benchmark_id: UUID, session: Session, org: Org) -> None:
    """
    On task exit we must clean up any edge cases so that it does not affect the users experience.
    There are sometimes fishy things that may occur with the sandboxes that if not dealt with could become an issue.

    1. Benchmark status must be in a finished state
    2. All tasks must be in a finished state
    """
    terminal_statuses = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]

    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
    if benchmark_row.status in terminal_statuses:
        return

    # Force non exited tasks to be ERROR
    task_terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    session.exec(
        update(Task)
        .where(col(Task.benchmark) == benchmark_id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).notin_(task_terminal_statuses))
        .values(status=TaskStatus.ERROR, error_message="Undetected exit of task")
    )
    session.commit()

    # Force benchmark to ERROR so that the user knows they can retry any failed tasks
    commit_benchmark_error(
        benchmark_row,
        session,
        f"Run {benchmark_id} exited without finishing",
    )


def commit_task_error(task_row: Task, session: Session, error_message: str) -> None:
    _commit_task_status(task_row, session, TaskStatus.ERROR, error_message=error_message)


async def stream_benchmark_results(
    benchmark_id: UUID, session: Session, harness_config: HarnessConfig, org: Org
) -> AsyncGenerator[str]:
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
                if not fresh_benchmark or fresh_benchmark.org_id != org.id:
                    yield f"{EVENT_ERROR} {json.dumps({'error': 'Run not found'})}\n\n"
                    break

                fresh_session.refresh(fresh_benchmark)
                benchmark_context = BenchmarkContext(fresh_benchmark, fresh_session, org)

                response_data = FetchBenchmarkResponse(
                    benchmark_name=fresh_benchmark.name,
                    benchmark_id=fresh_benchmark.id,
                    details=benchmark_context.benchmark_details,
                    s3_bucket_url=create_benchmark_url(
                        str(fresh_benchmark.id), harness_config.aws.aws_default_region, harness_config.s3_bucket
                    ),
                )

                yield f"{DATA_PREFIX} {response_data.model_dump_json()}\n\n"

                if fresh_benchmark.status in [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]:
                    yield EVENT_COMPLETE
                    break

            await asyncio.sleep(PULL_INTERVAL)

    except asyncio.CancelledError:
        logger.info(f"Client disconnected from benchmark {benchmark_id} stream")
        yield DISCONNECT


async def initiate_stop_benchmark(benchmark_row: Benchmark, session: Session, force: bool, org: Org) -> None:
    """
    Sets the flags to initiate the stopping process for a benchmark.

    Benchmark - Stopping status
    Tasks - Stopped status

    NOTE: Tasks that have already started will continue to run and finish.
    """
    try:
        # Update all rows where tasks are pending or building to stopped
        result = session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.status).in_([TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.EVALUATING]))
            .values(status=TaskStatus.STOPPED)
        )
        session.commit()

        # If we have stopped any tasks or are forcing the benchmark to stop, set the benchmark status to stopping
        if result.rowcount > 0 or force:
            benchmark_row.status = BenchmarkStatus.STOPPING
            session.add(benchmark_row)
            session.commit()
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error stopping run {benchmark_row.id}: {str(e)}") from e


async def stop_sandbox(sandbox: AsyncSandbox, daytona_client: AsyncDaytona) -> str | None:
    try:
        # Wait for the sandbox to be in a valid deletion state
        await sandbox.wait_for_sandbox_start(timeout=0)

        # Delete the sandbox
        await delete_sandbox(sandbox, daytona_client)

        return None
    except DaytonaNotFoundError:
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
        return None
    except Exception as e:
        return f"{str(e)}: {traceback.format_exc()}"


@tenacity_retry(
    retry=retry_if_exception_type(DaytonaRateLimitError),
    stop=stop_after_attempt(5),
    wait=wait_daytona_rate_limit(non_rate_limit_wait=wait_fixed(0)),
    before_sleep=daytona_retry_callback("valkyrie.sandbox.list", op="sandbox.list"),
    reraise=True,
)
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


async def force_stop_sandboxes(
    benchmark_row: Benchmark, session: Session, daytona_secret_name: str, aws: AWSCredentials, org: Org
) -> None:
    """
    Stops and deletes all sandboxes which are in progress or evaluating.
    NOTE: If task is not in progress but sandbox exists, we kill it and leave the task status as is.

    Raises:
        TrackerServiceError: If there are any errors stopping the sandboxes
    """
    daytona_client: AsyncDaytona = benchmark_row.benchmark_service(daytona_secret_name, aws).daytona_client

    # Update all tasks being processed to stopped
    session.exec(
        update(Task)
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).in_([TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]))
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

    # If all tasks are already in a stopped state, we need to update the final status here since the worker has exited
    finished_statuses: list[TaskStatus] = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    tasks_still_running: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).notin_(finished_statuses))
    ).one()

    if not tasks_still_running:
        benchmark_row.status = BenchmarkStatus.STOPPED
        session.add(benchmark_row)
        session.commit()

    if error_message:
        raise TrackerServiceError(f"Unexpected errors stopping sandboxes:\n{error_message}")


async def reset_to_in_progress_status(
    benchmark_row: Benchmark,
    session: Session,
    benchmark_service: BenchmarkServiceClient,
    retry: bool,
    retry_mode: RetryMode,
    rerun_task_ids: list[str],
    org: Org,
) -> list[str]:
    """
    Resets valid tasks to in progress and to allow for retrying or resuming the benchmark.

    Retry: we reset objects with an error status ontop of the stopped status
    Rerun Task IDs: even if task has been finished we restart it. If the task has no
        row yet, a fresh PENDING row is created when valid in the current dataset.

    Benchmark - In progress status
    Tasks - Pending status, or Evaluating status when retrying durable eval state

    NOTE: Will raise if benchmark is in a stopped state with no stopped tasks.
    """
    try:
        retry_statuses = [TaskStatus.STOPPED]
        if retry:
            retry_statuses.append(TaskStatus.ERROR)

        filter_query = [
            col(Task.benchmark) == benchmark_row.id,
            col(Task.org_id) == org.id,
            or_(
                col(Task.status).in_(retry_statuses),
                col(Task.task_id).in_(rerun_task_ids),
            ),
        ]

        existing_rows = session.exec(select(Task).where(*filter_query)).all()
        existing_by_task_id: dict[str, Task] = {task.task_id: task for task in existing_rows}
        new_task_ids = [tid for tid in rerun_task_ids if tid not in existing_by_task_id]

        # Allow re-running the end of the benchmark without running any tasks
        if not existing_rows and not new_task_ids:
            return []

        # Verify the task ids are still valid before priming to resume
        # Raises if any task ids are invalid
        all_requested_task_ids = list(existing_by_task_id.keys()) + new_task_ids
        verify_response = await benchmark_service.verify_task_ids(
            task_ids=all_requested_task_ids, slice_str=None, dataset=benchmark_row.arguments.dataset
        )

        # Set the benchmark status to in progress to flag resuming the benchmark
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        session.add(benchmark_row)
        session.commit()

        for task in existing_rows:
            task.status = (
                TaskStatus.EVALUATING
                if retry_mode == RetryMode.AUTO and task.eval_resume_state is not None
                else TaskStatus.PENDING
            )
            task.started_at = datetime.now(ZoneInfo("UTC"))
            task.error_message = None
            task.finished_at = None
            if retry_mode == RetryMode.FROM_SCRATCH:
                task.eval_resume_state = None
            session.add(task)

        for task_id in new_task_ids:
            session.add(Task(org_id=org.id, task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.PENDING))

        # Delete all evaluation results for the tasks (unlikely they exist)
        if existing_rows:
            session.exec(
                delete(EvaluationResult)
                .where(col(EvaluationResult.task).in_([task.id for task in existing_rows]))
                .where(col(EvaluationResult.org_id) == org.id)
            )

        session.commit()

        return verify_response.task_ids
    except (TrackerServiceError, BenchmarkServiceError):
        raise
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error resuming run {benchmark_row.id}: {str(e)}") from e


def fetch_filtered_benchmark_rows(
    request: FetchBenchmarksRequest, session: Session, org: Org
) -> tuple[Sequence[Benchmark], int]:
    """
    Creates a query to fetch benchmark rows from the database based on the fetch benchmark request.

    Args:
        request: FetchBenchmarksRequest

    Returns:
        tuple[Sequence[Benchmark], int]
        Sequence of benchmark rows and total count of benchmark rows

    """
    query = scoped_select(Benchmark, org)

    arguments_json = type_coerce(col(Benchmark.arguments), JSON)

    if request.agent_name:
        query = query.where(arguments_json["contract"]["name"].as_string() == request.agent_name)

    if request.model:
        query = query.where(arguments_json["contract"]["model"].as_string() == request.model)

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


class YieldingWriter(io.RawIOBase):
    """
    Custom writer that collects bytes and returns them to stream.
    """

    def __init__(self):
        super().__init__()
        self._buffer: bytearray = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, b: Buffer) -> int:
        data = bytes(b)
        self._buffer.extend(data)

        return len(data)

    def pop(self) -> bytes:
        if not self._buffer:
            return b""

        chunk = bytes(self._buffer)
        self._buffer.clear()

        return chunk


def create_final_view(benchmark_row: Benchmark, session: Session, org: Org) -> FinalViewResponse:
    """Creates final view of a benchmark that includes metadata about evaluations and score"""
    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    final_view: FinalViewResponse = FinalViewResponse(
        benchmark_name=benchmark_row.name,
        status=benchmark_row.status,
        error_message=benchmark_row.error_message,
        benchmark_id=benchmark_row.id,
        benchmark_arguments=benchmark_row.arguments,
        started_at=benchmark_row.started_at,
        finished_at=benchmark_row.finished_at,
        tasks_stopped=tasks_stopped or None,  # NOTE: Only include if we stopped the benchmark
        final_evaluation=benchmark_row.final_evaluation,
        evaluation_results=benchmark_row.fetch_evaluation_results(session),
        task_errors=benchmark_row.fetch_tasks_with_errors(session),
        average_task_breakdown=fetch_average_task_breakdown(benchmark_row.id, session, org.id),
    )

    return final_view


async def upload_final_view(
    benchmark_row: Benchmark, final_view: FinalViewResponse, harness_config: HarnessConfig
) -> str:
    """Uploads the final view to the root of the benchmark folder and returns the s3 key"""
    s3_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_row.id}/{benchmark_row.name}.json"
    await upload_to_s3(
        final_view.model_dump_json(indent=4, exclude_none=True).encode(),
        s3_key,
        harness_config.aws,
        harness_config.s3_bucket,
    )

    return s3_key


def fetch_harness_config(request: Request) -> HarnessConfig:
    """Constructs HarnessConfig from X-Harness-* request headers."""
    prefix = "x-harness-"
    flat = {
        key[len(prefix) :].replace("-", "_"): value for key, value in request.headers.items() if key.startswith(prefix)
    }
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id=flat["aws_access_key_id"],
            aws_secret_access_key=flat["aws_secret_access_key"],
            aws_default_region=flat["aws_default_region"],
            aws_session_token=flat.get("aws_session_token"),
        ),
        s3_bucket=flat["s3_bucket"],
        log_group=flat["log_group"],
        log_retention_policy=int(flat["log_retention_policy"]),
        daytona_secret_name=flat["daytona_secret_name"],
    )
