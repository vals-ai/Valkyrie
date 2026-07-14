"""Logic for executing, tracking, and transitioning the status of a single task."""

import asyncio
import json
import time
import traceback
from asyncio import Semaphore
from collections.abc import Coroutine
from contextlib import suppress
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import logfire
import sentry_sdk
from benchmark_service import (
    SandboxProviderConfig,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from opentelemetry import trace
from pydantic import ValidationError
from sqlmodel import Session, col, select, update
from tenacity import retry as tenacity_retry
from tenacity import retry_if_exception_type, stop_after_attempt, wait_fixed
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tracker.aws.cloudwatch_logs import write_benchmark_log_event
from tracker.aws.s3 import (
    get_agent_result_s3_key,
)
from tracker.aws.secrets import resolve_secrets
from tracker.config import ENVIRONMENT
from tracker.database.models import (
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.session import engine
from tracker.exceptions import OutputArtifactError, SandboxSetupError, TrackerServiceError
from tracker.logging import get_logger, task_id_var
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.observability import elapsed_ms, retry_callback
from tracker.sandbox import create_sandbox, run_agent, upload_agent_artifacts
from tracker.types import (
    AWSCredentials,
    HarnessConfig,
    StartBenchmarkRequest,
)

from tracker.utils.resources import fetch_benchmark_row, fetch_task_row

logger = get_logger(__name__)

_PTY_TASK_RETRY_LIMIT: int = 1


def _normalized_attempt_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def _exception_message(exc: BaseException) -> str:
    return str(exc).strip() or type(exc).__name__


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
            error_message = f"Task error was not handled: {_exception_message(e)}\n{traceback.format_exc()}"
            logger.error(error_message)
            logfire.exception("tracked_task_run failed")
            sentry_sdk.capture_exception(e)
            with Session(bind=engine) as session:
                task = fetch_task_row(task_row.id, session, self._org)
                commit_task_error(task, session, error_message, expected_started_at=task_row.started_at)

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


def handle_early_exit(task_row: Task, task_session: Session) -> bool:
    return _commit_task_status(
        task_row,
        task_session,
        TaskStatus.STOPPED,
        expected_started_at=task_row.started_at,
    )


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


def save_eval_resume_state(
    task_row_id: UUID,
    org: Org,
    eval_resume_state: dict[str, Any],
    *,
    expected_started_at: datetime | None = None,
) -> bool:
    with Session(bind=engine) as session:
        task_update = (
            update(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.status) != TaskStatus.STOPPED)
        )
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)

        result = session.exec(task_update.values(eval_resume_state=eval_resume_state))
        session.commit()
        return result.rowcount > 0


def _commit_task_status(
    task: Task,
    session: Session,
    to_status: TaskStatus,
    *,
    error_message: str | None = None,
    extra: dict[str, Any] | None = None,
    expected_started_at: datetime | None = None,
) -> bool:
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

    with logfire.span("task.status_transition", **span_attributes):  # pyright: ignore[reportArgumentType]
        values: dict[str, TaskStatus | datetime] = {"status": to_status}
        if to_status in [TaskStatus.FINISHED, TaskStatus.ERROR]:
            values["finished_at"] = datetime.now(ZoneInfo("UTC"))

        task_update = update(Task).where(col(Task.id) == task.id).where(col(Task.org_id) == task.org_id)
        if to_status != TaskStatus.STOPPED:
            task_update = task_update.where(col(Task.status) != TaskStatus.STOPPED)
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)

        result = session.exec(task_update.values(**values))
        if result.rowcount == 0:
            session.rollback()
            return False

        session.commit()
        return True


def commit_task_status_transition(
    task_row_id: UUID,
    session: Session,
    org: Org,
    to_status: TaskStatus,
    *,
    expected_started_at: datetime | None = None,
) -> bool:
    fetch_start = time.monotonic()
    task = fetch_task_row(task_row_id, session, org)
    return _commit_task_status(
        task,
        session,
        to_status,
        extra={"fetch_duration_ms": elapsed_ms(fetch_start)},
        expected_started_at=expected_started_at,
    )


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
    sandbox_provider_config: SandboxProviderConfig,
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

    requested_attempt_started_at = task_row.started_at
    with Session(bind=engine) as task_session:
        benchmark_row = fetch_benchmark_row(benchmark_id, task_session, org)
        task_row = fetch_task_row(task_row.id, task_session, org)

        if _normalized_attempt_time(task_row.started_at) != _normalized_attempt_time(requested_attempt_started_at):
            return {task_id: None}
        attempt_started_at = task_row.started_at
        if benchmark_row.status == BenchmarkStatus.STOPPING or task_row.status == TaskStatus.STOPPED:
            handle_early_exit(task_row, task_session)
            return {task_id: None}
        benchmark_name = benchmark_row.name
        benchmark_agent_name = benchmark_row.arguments.contract.name
        benchmark_started_by_email = benchmark_row.started_by_email

    # Setup logging infrastructure before try block so it's always available
    # Suffix is required to version control streams, never delete between retries
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
        save_eval_resume_state(task_row.id, org, state, expected_started_at=attempt_started_at)

    def task_is_stopped() -> bool:
        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            return task.status == TaskStatus.STOPPED or task.started_at != attempt_started_at

    try:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                resume_eval_start_time = time.perf_counter()
                # Reset timer to keep the last received message from the benchmarks service accurate
                last_log_time = time.monotonic()
                evaluation_result = await benchmark_service.resume_evaluation(
                    task_row.task_id,
                    eval_resume_state=task_row.eval_resume_state,
                    on_message=log_output,
                    on_eval_resume_state=on_eval_resume_state,
                    dataset=start_benchmark_request.dataset,
                    sandbox_provider=sandbox_provider_config,
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
                        assert existing_breakdown is not None
                        existing_breakdown.evaluation_run_duration = resume_eval_duration
                    if not commit_task_status_transition(
                        task_row.id,
                        task_session,
                        org,
                        TaskStatus.FINISHED,
                        expected_started_at=attempt_started_at,
                    ):
                        return {task_id: None}

                    return {task_id: evaluation_result_row.result}
            except Exception as e:
                with Session(bind=engine) as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}

                raise e from e

        task_data = await benchmark_service.retrieve_task(task_id=task_id, dataset=start_benchmark_request.dataset)
        sandbox_provider = benchmark_service.get_sandbox_provider(sandbox_provider_config)

        # Labels that show up in the UI we can use to filter sandboxes
        labels = {
            "Benchmark": start_benchmark_request.benchmark_name,
            "Id": str(benchmark_id),
            "Task": task_row.task_id,
        }

        with Session(bind=engine) as task_session:
            if not commit_task_status_transition(
                task_row.id,
                task_session,
                org,
                TaskStatus.BUILDING,
                expected_started_at=attempt_started_at,
            ):
                return {task_id: None}

        identity = {
            "benchmark_name": benchmark_name,
            "agent_name": benchmark_agent_name,
        }
        if benchmark_started_by_email:
            identity["email"] = benchmark_started_by_email

        env_vars = {
            **resolve_secrets(start_benchmark_request.contract.secrets, harness_config.aws),
            "RUN_ID": str(benchmark_id),
            "TASK_ID": task_row.task_id,
            "IDENTITY": json.dumps(identity),
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
            provider=sandbox_provider,
            sandbox_name=task_row.task_id,
            source=task_data.source,
            labels=labels,
            env_vars=env_vars,
            resources=task_data.resources,
            creation_semaphore=creation_semaphore,
        ) as sandbox:
            task_breakdown.sandbox_build_duration = time.perf_counter() - start_sandbox_build_time
            start_sandbox_run_time = time.perf_counter()

            try:
                with Session(bind=engine) as task_session:
                    if not commit_task_status_transition(
                        task_row.id,
                        task_session,
                        org,
                        TaskStatus.IN_PROGRESS,
                        expected_started_at=attempt_started_at,
                    ):
                        return {task_id: None}

                # Upload the contract to the sandbox after creating and install the dependencies
                await upload_agent_artifacts(
                    sandbox,
                    start_benchmark_request.contract,
                    str(benchmark_id),
                    harness_config.aws,
                    harness_config.s3_bucket,
                )

                # Reset timer to keep the last received message from the benchmarks service accurate
                last_log_time = time.monotonic()
                _ = await benchmark_service.setup_task(
                    task_row.task_id,
                    sandbox.id,
                    on_message=log_output,
                    dataset=start_benchmark_request.dataset,
                    sandbox_provider=sandbox_provider_config,
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
                    runtime_source=task_data.source,
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
                    if not commit_task_status_transition(
                        task_row.id,
                        task_session,
                        org,
                        TaskStatus.EVALUATING,
                        expected_started_at=attempt_started_at,
                    ):
                        return {task_id: None}

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
                # Reset timer to keep the last received message from the benchmarks service accurate
                last_log_time = time.monotonic()
                evaluation_result = await benchmark_service.evaluate_instance(
                    task_row.task_id,
                    sandbox.id,
                    on_message=log_output,
                    on_eval_resume_state=on_eval_resume_state,
                    dataset=start_benchmark_request.dataset,
                    sandbox_provider=sandbox_provider_config,
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
                    if existing_breakdown is None:
                        raise TrackerServiceError(f"Missing task breakdown for task {task_row.id}")
                    existing_breakdown.evaluation_run_duration = task_breakdown.evaluation_run_duration
                    existing_breakdown.sandbox_run_duration = task_breakdown.sandbox_run_duration
                    if not commit_task_status_transition(
                        task_row.id,
                        task_session,
                        org,
                        TaskStatus.FINISHED,
                        expected_started_at=attempt_started_at,
                    ):
                        return {task_id: None}

                    return {task_id: evaluation_result_row.result}
            except Exception:
                with Session(bind=engine) as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}

                raise

    except SandboxSetupError as e:
        if task_is_stopped():
            return {task_id: None}
        log_output(f"\n[ERROR] {_exception_message(e)}")
        raise
    except OutputArtifactError as e:
        if task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        logger.warning(error_message)
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)

        return {task_id: None}
    except ConnectionClosedError as e:
        if task_is_stopped():
            return {task_id: None}
        seconds = int(time.monotonic() - last_log_time)
        error_message = (
            f"Benchmark service WebSocket disconnected: {e}; last application message received {seconds}s ago"
        )
        logger.warning(error_message)
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)

        return {task_id: None}
    except ValidationError as e:
        if task_is_stopped():
            return {task_id: None}
        field_names = ", ".join(".".join(str(loc) for loc in err["loc"]) for err in e.errors())
        error_message = (
            f"Benchmark service returned an incompatible task response. Missing or invalid fields: {field_names}"
        )
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)

        return {task_id: None}
    except InvalidStatus as e:
        if task_is_stopped():
            return {task_id: None}
        error_message = f"Benchmark service rejected the WebSocket connection (HTTP {e.response.status_code})"
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)

        return {task_id: None}
    except BenchmarkServiceError as e:
        if task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)

        return {task_id: None}
    except Exception as e:
        if task_is_stopped():
            return {task_id: None}
        logfire.exception("process_task failed")
        error_message = _exception_message(e)
        logger.error(error_message, exc_info=True)

        sentry_sdk.capture_exception(e)

        # include the error message
        log_output(f"\n[ERROR] {error_message}")

        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)

        return {task_id: None}
    finally:
        flush_task.cancel()
        with suppress(asyncio.CancelledError):
            await flush_task
        buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group, force_flush=True)


def commit_task_error(
    task_row: Task,
    session: Session,
    error_message: str,
    *,
    expected_started_at: datetime | None = None,
) -> bool:
    session.add(ErrorResult(org_id=task_row.org_id, task=task_row.id, error_message=error_message))
    return _commit_task_status(
        task_row,
        session,
        TaskStatus.ERROR,
        error_message=error_message,
        expected_started_at=expected_started_at,
    )
