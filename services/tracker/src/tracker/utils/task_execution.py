"""Logic for executing, tracking, and transitioning the status of a single task."""

import asyncio
import json
import socket
import time
import traceback
from hashlib import sha256
from asyncio import Semaphore, to_thread
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import PurePosixPath
from types import TracebackType
from typing import Any, cast
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import logfire
import sentry_sdk
from benchmark_service import (
    ComposeSource,
    Sandbox,
    SandboxCreatedData,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxRecoveryAttempt,
)
from benchmark_service.client import BenchmarkServiceClient
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, col, func, select, update

from tracker.runtime.logs import task_log_stream_name
from tracker.runtime.secrets import resolve_secrets
from tracker.runtime.services import RuntimeServices
from tracker.runtime.artifacts import task_artifact_key
from tracker.runtime.model_gateway import GATEWAY_TOKEN_TTL_SECONDS, task_scoped_gateway_key
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.config import (
    ENVIRONMENT,
    EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS,
    EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN,
    EXTERNAL_SERVICE_GATEWAY_URL,
)
from tracker.database.models import (
    ActiveNativeOutput,
    AgentCausedExitReason,
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    EvaluationResult,
    FailureCategory,
    FailurePhase,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.session import engine
from tracker.exceptions import (
    AgentRunFailedError,
    CreditedTaskWallTimeExceeded,
    DependencySetupExhaustedError,
    ExecutionAuthorityRevoked,
    GenerationTerminationUnconfirmedError,
    InvalidSandboxConfigurationError,
    SandboxDNSResolutionError,
    SandboxSetupError,
    TrackerServiceError,
)
from tracker.egress import combine_run_egress_policies
from tracker.external_service_gateway import (
    AccountingSnapshot,
    GenerationAccountingSummary,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
)
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.executor.native_output import clear_native_output, publish_native_output
from tracker.generation_plan import GenerationPlan
from tracker.logging import get_logger
from tracker.observability import elapsed_ms, error_span, incr
from tracker.observability.sentry import capture_exception, clear_sandbox_context, task_scope
from tracker.observability.tracing import observability_span
from tracker.sandbox import (
    DependencySetupMode,
    SandboxCreationState,
    apply_egress_policy,
    create_agent_error_redactor,
    create_sandbox,
    install_agent_dependencies,
    run_agent,
    runtime_sandbox,
    sandbox_resolves,
    upload_agent_artifacts,
)
from tracker.sandbox_cost import SandboxLink, record_sandbox
from tracker.scheduler.admission import SandboxQueueContext, enter_queued_sandbox
from tracker.scheduler.store import (
    PostgresAdvisoryLock,
    is_reserved_queue_pool_id,
    queued_sandbox_name,
    task_evaluation_lock,
)
from tracker.types import (
    StartBenchmarkRequest,
)

from tracker.utils.failures import (
    BenchmarkServiceWebSocketDNSResolutionError,
    Disposition,
    Failure,
    classify_failure,
    decide,
    describe_failure,
)
from tracker.utils.resources import fetch_benchmark_row, fetch_task_row

logger = get_logger(__name__)
TASK_WALL_SECONDS = GATEWAY_TOKEN_TTL_SECONDS - 2 * 60 * 60

_PTY_TASK_RETRY_LIMIT: int = 1
_SANDBOX_RETRY_DELAY_SECONDS: float = 2
_DIRECT_ADMISSION_POLL_SECONDS: float = 2


async def _run_benchmark_service_websocket(operation: Coroutine[Any, Any, Any]) -> Any:
    """Translate DNS failures from benchmark-service WebSocket calls at the boundary."""
    try:
        return await operation
    except socket.gaierror as exc:
        raise BenchmarkServiceWebSocketDNSResolutionError(_exception_message(exc)) from exc


@dataclass
class _DependencySetupRecoveryState:
    mode: DependencySetupMode = DependencySetupMode.IN_PLACE_RETRIES


def _attested_inference_settings(contract: AgentContractRequest) -> dict[str, str]:
    """Settings benchmark setup may trust; empty unless the tracker resolved them."""
    if not contract.inference_settings_attested:
        return {}
    return {
        "VALKYRIE_AGENT_MODEL": contract.model or "",
        "VALKYRIE_AGENT_VARIANT": contract.kwargs.get("variant", ""),
    }


def _normalized_attempt_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def task_attempt_id(task_row_id: UUID, started_at: datetime) -> str:
    """Return the opaque identity of one task execution.

    It is derived from the task row and its started_at generation, so every
    sandbox within one execution shares it and a whole-task rerun changes it.
    """
    return queued_sandbox_name(task_row_id, started_at).removeprefix("queued-")


def _exception_message(exc: BaseException) -> str:
    """Keep exception types and Python's visible cause/context chain in task views."""

    def describe(error: BaseException) -> str:
        detail = str(error).strip()
        name = type(error).__name__
        return f"{name}: {detail}" if detail else name

    message = describe(exc)
    seen = {id(exc)}
    current = exc
    while True:
        if isinstance(current, TrackerServiceError) and not current.include_cause_in_task_error:
            break
        if current.__cause__ is not None:
            next_error = current.__cause__
            relation = "caused by"
        elif not current.__suppress_context__:
            next_error = current.__context__
            relation = "during handling of"
        else:
            break
        if next_error is None or id(next_error) in seen:
            break
        seen.add(id(next_error))
        message = f"{message} ({relation} {describe(next_error)})"
        current = next_error
    return message


def _record_failure_before_retry(
    task_row: Task,
    authority: ExecutionAuthority,
    exc: SandboxSetupError,
    failed_attempt_number: int,
) -> None:
    """Persist the failure that scheduled the next attempt while this execution owns the task."""
    with Session(bind=engine) as session:
        try:
            lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            session.rollback()
            return
        task = session.exec(
            select(Task)
            .where(col(Task.id) == task_row.id)
            .where(col(Task.org_id) == task_row.org_id)
            .where(col(Task.benchmark) == authority.benchmark_id)
            .where(col(Task.started_at) == task_row.started_at)
            .where(
                col(Task.status).in_(
                    (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
                )
            )
            .with_for_update()
        ).one_or_none()
        if task is None:
            session.rollback()
            return
        session.add(
            ErrorResult(
                org_id=task.org_id,
                task=task.id,
                error_message=_exception_message(exc),
                producer="sandbox_provider",
                operation="setup",
                error_type=type(exc).__name__,
                category=FailureCategory.INFRASTRUCTURE,
                cause_code=exc.cause_code,
                phase=exc.phase,
                transient=exc.transient,
                retry_scheduled=True,
                failed_attempt_number=failed_attempt_number,
            )
        )
        session.commit()


_TASK_RETRY_METRIC = "valkyrie.task"


def _observe_task_retry(attempt: SandboxRecoveryAttempt, exc: BaseException) -> None:
    """Emit the retry telemetry the Tenacity before_sleep hook owned before recovery
    moved into the benchmark-service client."""
    error_class = type(exc).__name__
    failure_category = classify_failure(exc).value
    with observability_span(
        "task.retry", attempt=attempt.number, error_class=error_class, failure_category=failure_category
    ):
        logger.warning(
            "retry.before_sleep",
            extra={
                "metric": _TASK_RETRY_METRIC,
                "fn": "_process_task_attempt",
                "attempt": attempt.number,
                "idle_for": _SANDBOX_RETRY_DELAY_SECONDS,
                "error_class": error_class,
                "failure_category": failure_category,
            },
        )
        incr(
            f"{_TASK_RETRY_METRIC}.retry",
            tags={"error_class": error_class, "failure_category": failure_category},
        )


class TrackedTaskStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    DONE = "done"


class ResizableLimiter:
    """A per-executor admission limit that can change without preempting admitted work."""

    def __init__(self, limit: int):
        if limit < 1:
            raise ValueError("Limit must be greater than 0")
        self._limit = limit
        self._in_flight = 0
        self._condition = asyncio.Condition()

    async def resize(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("Limit must be greater than 0")
        async with self._condition:
            previous_limit = self._limit
            self._limit = limit
            if limit > previous_limit:
                self._condition.notify_all()

    async def __aenter__(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._in_flight < self._limit)
            self._in_flight += 1

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        async with self._condition:
            self._in_flight -= 1
            self._condition.notify_all()


class TrackedTask:
    _coro: Coroutine[Any, Any, Any]
    _status: str
    _task: asyncio.Task[Any] | None
    _org: Org
    _attempt_started_at: datetime
    _authority: ExecutionAuthority

    def __init__(
        self,
        coro: Coroutine[Any, Any, Any],
        org: Org,
        authority: ExecutionAuthority,
        started_at: datetime,
    ):
        self._coro = coro
        self._org = org
        self._authority = authority
        self._attempt_started_at = _normalized_attempt_time(started_at)
        self._status = TrackedTaskStatus.WAITING
        self._task = None

    @property
    def status(self) -> str:
        return self._status

    @property
    def task(self) -> asyncio.Task[Any] | None:
        return self._task

    @property
    def attempt_started_at(self) -> datetime:
        return self._attempt_started_at

    async def run(
        self,
        limiter: ResizableLimiter | None,
        task_row: Task,
    ) -> dict[str, dict[str, Any] | None]:
        async def _wrap_coro():
            """Need to have a task created even if we are not running the coroutine so that we can cancel it before its running"""
            if limiter is None:
                self._status = TrackedTaskStatus.RUNNING

                return await self._coro
            async with limiter:
                self._status = TrackedTaskStatus.RUNNING
                return await self._coro

        with task_scope(task_row.task_id, attempt_started_at=self.attempt_started_at.isoformat()):
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
                failure = describe_failure(e, _exception_message(e), phase=getattr(e, "phase", None))
                error_message = f"Task error was not handled: {failure.message}\n{traceback.format_exc()}"
                logger.error(error_message, extra={"failure_category": failure.category.value})
                logfire.exception("tracked_task_run failed")
                with sentry_sdk.new_scope() as scope:
                    scope.set_tag("failure_category", failure.category.value)
                    capture_exception(e)
                with Session(bind=engine) as session:
                    task = fetch_task_row(task_row.id, session, self._org)
                    commit_task_error(
                        task,
                        session,
                        error_message,
                        producer=failure.producer,
                        operation=failure.operation,
                        error_type=failure.error_type,
                        category=failure.category,
                        cause_code=failure.cause_code,
                        phase=failure.phase,
                        transient=failure.transient,
                        expected_started_at=task_row.started_at,
                        authority=self._authority,
                    )

                return {task_row.task_id: None}
            finally:
                self._status = TrackedTaskStatus.DONE


class TaskMonitor:
    _benchmark_id: UUID
    _task_tracking: dict[str, TrackedTask]
    _org: Org
    _limiter: ResizableLimiter | None
    _coordinator_done: asyncio.Event | None
    _cancellation_requested: set[str]
    _authority: ExecutionAuthority
    _TRACK_INTERVAL: int = 2

    def __init__(
        self,
        benchmark_id: UUID,
        task_tracking: dict[str, TrackedTask],
        org: Org,
        limiter: ResizableLimiter | None,
        *,
        authority: ExecutionAuthority,
        coordinator_done: asyncio.Event | None = None,
    ):
        self._benchmark_id = benchmark_id
        self._task_tracking = task_tracking
        self._org = org
        self._limiter = limiter
        self._coordinator_done = coordinator_done
        self._cancellation_requested = set()
        self._authority = authority

    def _load_state(
        self, session: Session, task_ids: list[str]
    ) -> tuple[Benchmark, dict[str, tuple[TaskStatus, datetime]]]:
        benchmark_row = fetch_benchmark_row(self._benchmark_id, session, self._org)
        task_states = {
            task_id: (TaskStatus(status), started_at)
            for task_id, status, started_at in session.exec(
                select(Task.task_id, Task.status, Task.started_at)
                .where(col(Task.task_id).in_(task_ids))
                .where(Task.benchmark == self._benchmark_id)
                .where(Task.org_id == self._org.id)
            ).all()
        }

        for task_id in task_ids:
            if task_id not in task_states:
                raise ValueError(f"Task with id {task_id} not found")

        return benchmark_row, task_states

    def _read_tick(self, task_ids: list[str]) -> tuple[bool, Benchmark | None, dict[str, tuple[TaskStatus, datetime]]]:
        with Session(bind=engine) as session:
            try:
                lock_execution_authority(session, self._authority)
            except ExecutionAuthorityRevoked:
                authority_current = False
            else:
                authority_current = True
            session.rollback()

            if task_ids:
                benchmark_row, task_states = self._load_state(session, task_ids)
                return authority_current, benchmark_row, task_states

        return authority_current, None, {}

    async def track_tasks(self) -> None:
        """
        Tracks tasks and cancels them when they are no longer valid.
        """

        while self._task_tracking or (self._coordinator_done is not None and not self._coordinator_done.is_set()):
            tasks_to_check = [
                task_id
                for task_id, tracked_task in self._task_tracking.items()
                if tracked_task.status != TrackedTaskStatus.DONE
            ]
            try:
                authority_current, benchmark_row, task_states = self._read_tick(tasks_to_check)
            except OperationalError:
                logger.warning("Task monitor database read failed; retrying next tick", exc_info=True)
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue

            for task_id, tracked_task in list(self._task_tracking.items()):
                if tracked_task.status == TrackedTaskStatus.DONE:
                    del self._task_tracking[task_id]
                    self._cancellation_requested.discard(task_id)

            if not self._task_tracking:
                if self._coordinator_done is not None and self._coordinator_done.is_set():
                    break
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue

            assert benchmark_row is not None
            if self._limiter is not None:
                await self._limiter.resize(benchmark_row.arguments.concurrency)

            for task_id in tasks_to_check:
                tracked_task = self._task_tracking[task_id]
                status, started_at = task_states[task_id]
                invalid = (
                    not authority_current
                    or status == TaskStatus.STOPPED
                    or benchmark_row.status == BenchmarkStatus.ERROR
                    or _normalized_attempt_time(started_at) != tracked_task.attempt_started_at
                )
                task = tracked_task.task
                if invalid and task is not None and not task.done() and task_id not in self._cancellation_requested:
                    self._cancellation_requested.add(task_id)
                    task.cancel(f"Task {task_id} has been invalidated. Run has been requested to stop")

            await asyncio.sleep(self._TRACK_INTERVAL)


def handle_early_exit(task_row: Task, task_session: Session, authority: ExecutionAuthority) -> bool:
    return _commit_task_status(
        task_row,
        task_session,
        TaskStatus.STOPPED,
        authority=authority,
        expected_started_at=task_row.started_at,
    )


def save_eval_resume_state(
    task_row_id: UUID,
    org: Org,
    eval_resume_state: dict[str, Any],
    *,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    connection: Connection | None = None,
) -> bool:
    with Session(bind=connection if connection is not None else engine) as session:
        try:
            lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            session.rollback()
            return False
        task_update = (
            update(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.status) == TaskStatus.EVALUATING)
            .where(col(Task.started_at) == expected_started_at)
        )

        result = session.exec(task_update.values(eval_resume_state=eval_resume_state))
        session.commit()
        return result.rowcount > 0


def _commit_task_status(
    task: Task,
    session: Session,
    to_status: TaskStatus,
    *,
    authority: ExecutionAuthority,
    error_message: str | None = None,
    extra: dict[str, Any] | None = None,
    expected_started_at: datetime | None = None,
    expected_status: TaskStatus | tuple[TaskStatus, ...] | None = None,
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

    with observability_span("task.status_transition", **span_attributes):
        try:
            lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            session.rollback()
            return False
        values: dict[str, TaskStatus | datetime] = {"status": to_status}
        if to_status in [TaskStatus.FINISHED, TaskStatus.ERROR]:
            values["finished_at"] = datetime.now(ZoneInfo("UTC"))

        task_update = update(Task).where(col(Task.id) == task.id).where(col(Task.org_id) == task.org_id)
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)
        if expected_status is None:
            task_update = task_update.where(
                col(Task.status).not_in((TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED))
            )
        elif isinstance(expected_status, tuple):
            task_update = task_update.where(col(Task.status).in_(expected_status))
        else:
            task_update = task_update.where(col(Task.status) == expected_status)

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
    authority: ExecutionAuthority,
    expected_started_at: datetime | None = None,
    expected_status: TaskStatus | tuple[TaskStatus, ...] | None = None,
) -> bool:
    fetch_start = time.monotonic()
    task = fetch_task_row(task_row_id, session, org)
    return _commit_task_status(
        task,
        session,
        to_status,
        extra={"fetch_duration_ms": elapsed_ms(fetch_start)},
        expected_started_at=expected_started_at,
        expected_status=expected_status,
        authority=authority,
    )


async def _create_generation_deadline_controller(
    contract: AgentContractRequest,
    plan: GenerationPlan,
    resolved_secrets: dict[str, str],
    scoped_env_vars: dict[str, str],
) -> ExternalServiceDeadlineController:
    if not (
        EXTERNAL_SERVICE_GATEWAY_URL
        and EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN
        and contract.inference_settings_attested
        and contract.model
        and contract.model.strip()
        and resolved_secrets.get("MODEL_GATEWAY_URL")
        and resolved_secrets.get("MODEL_GATEWAY_API_KEY")
    ):
        return ExternalServiceDeadlineController(base_allowance_seconds=plan.allowance_seconds)

    accounting_id = sha256(scoped_env_vars["MODEL_GATEWAY_API_KEY"].encode()).hexdigest()
    client = ExternalServiceGatewayClient(
        EXTERNAL_SERVICE_GATEWAY_URL, control_token=EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN
    )
    return ExternalServiceDeadlineController(
        client=client,
        snapshot=AccountingSnapshot(accounting_id=accounting_id, cumulative_neutral_overhead_ms=0, revision=0),
        base_allowance_seconds=plan.allowance_seconds,
        credit_cap_seconds=EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS,
    )


def _persist_generation_accounting_summary(
    summary: GenerationAccountingSummary,
    *,
    task_breakdown: TaskBreakdown,
    task_row_id: UUID,
    org: Org,
    authority: ExecutionAuthority,
    expected_started_at: datetime,
    open_task_session: Callable[[], Session],
) -> None:
    with open_task_session() as task_session:
        try:
            lock_execution_authority(task_session, authority)
        except ExecutionAuthorityRevoked:
            task_session.rollback()
            raise
        task_in_session = task_session.exec(
            select(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.benchmark) == authority.benchmark_id)
            .with_for_update()
        ).one_or_none()
        if (
            task_in_session is None
            or task_in_session.status == TaskStatus.STOPPED
            or _normalized_attempt_time(task_in_session.started_at) != _normalized_attempt_time(expected_started_at)
        ):
            task_session.rollback()
            raise ExecutionAuthorityRevoked("Task attempt changed before accounting persistence")
        task_breakdown.accounting_id = summary.accounting_id
        task_breakdown.base_generation_allowance_seconds = summary.base_generation_allowance_seconds
        task_breakdown.cumulative_time_credit_cap_seconds = summary.cumulative_time_credit_cap_seconds
        task_breakdown.external_service_overhead_seconds = summary.external_service_overhead_seconds
        task_breakdown.external_service_credit_applied_seconds = summary.external_service_credit_applied_seconds
        task_breakdown.external_service_credit_revision = summary.external_service_credit_revision
        task_breakdown.generation_elapsed_seconds = summary.generation_elapsed_seconds
        task_session.add(task_breakdown)
        task_session.flush()
        task_in_session.task_breakdown = task_breakdown.id
        task_session.commit()


async def process_task(
    task_row: Task,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkServiceClient,
    benchmark_id: UUID,
    task_id: str,
    runtime: RuntimeServices,
    org: Org,
    sandbox_provider_config: SandboxProviderConfig,
    creation_semaphore: Semaphore,
    authority: ExecutionAuthority,
    *,
    sandbox_provider: SandboxProvider | None = None,
    queue_context: SandboxQueueContext | None = None,
    started_by_email: str | None = None,
) -> dict[str, dict[str, Any] | None]:
    """Process one task while retaining dependency recovery state across sandbox attempts."""
    dependency_setup_recovery = _DependencySetupRecoveryState()

    with observability_span(
        "task.started",
        benchmark_id=str(benchmark_id),
        task_id=task_id,
        benchmark_name=start_benchmark_request.benchmark_name,
        agent_name=start_benchmark_request.contract.name,
    ):
        pass

    async def run_attempt(
        recovery_attempt: SandboxRecoveryAttempt,
    ) -> dict[str, dict[str, Any] | None]:
        clear_sandbox_context()
        stream_key = f"{benchmark_id}:{task_log_stream_name(task_id, task_row.started_at)}"
        async with TaskLogBuffer(runtime.logs, stream_key) as task_logs:
            return await _process_task_attempt(
                task_row=task_row,
                start_benchmark_request=start_benchmark_request,
                benchmark_service=benchmark_service,
                benchmark_id=benchmark_id,
                task_id=task_id,
                runtime=runtime,
                task_logs=task_logs,
                org=org,
                sandbox_provider_config=sandbox_provider_config,
                creation_semaphore=creation_semaphore,
                dependency_setup_recovery=dependency_setup_recovery,
                recovery_attempt=recovery_attempt,
                authority=authority,
                sandbox_provider=sandbox_provider,
                queue_context=queue_context,
            )

    def record_attempt_failure(attempt: SandboxRecoveryAttempt, exc: Exception) -> None:
        _observe_task_retry(attempt, exc)
        if isinstance(exc, SandboxSetupError):
            _record_failure_before_retry(task_row, authority, exc, attempt.number)

    result = await benchmark_service.run_with_sandbox_recovery(
        task_id=task_id,
        run_id=str(benchmark_id),
        attempt_id=task_attempt_id(task_row.id, task_row.started_at),
        email=started_by_email,
        operation=run_attempt,
        dataset=start_benchmark_request.dataset,
        retryable_attempt_errors=(SandboxSetupError,),
        default_max_attempts=_PTY_TASK_RETRY_LIMIT + 1,
        retry_delay_s=_SANDBOX_RETRY_DELAY_SECONDS,
        on_retry=record_attempt_failure,
    )

    if result.get(task_id) is not None:
        with observability_span(
            "task.completed",
            benchmark_id=str(benchmark_id),
            task_id=task_id,
        ):
            pass

    return result


def _admit_direct_task(task_row: Task, org: Org, authority: ExecutionAuthority) -> bool | None:
    with Session(bind=engine) as session:
        try:
            benchmark = lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            return None
        task = fetch_task_row(task_row.id, session, org)
        if task.started_at != task_row.started_at or task.status in (
            TaskStatus.STOPPED,
            TaskStatus.FINISHED,
            TaskStatus.ERROR,
        ):
            return None
        if task.status == TaskStatus.PENDING:
            active = session.exec(
                select(func.count(col(Task.id)))
                .where(Task.benchmark == benchmark.id, Task.org_id == org.id)
                .where(col(Task.status).in_((TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)))
            ).one()
            if active >= benchmark.arguments.concurrency:
                return False
        if commit_task_status_transition(
            task.id,
            session,
            org,
            TaskStatus.BUILDING,
            expected_started_at=task.started_at,
            expected_status=task.status,
            authority=authority,
        ):
            return True
        return None


async def _acquire_queue_preparation(stack: AsyncExitStack, context: SandboxQueueContext | None) -> None:
    """Bound task retrieval and provider preparation, not the subsequent admission wait."""
    if context is not None and is_reserved_queue_pool_id(context.pool_id):
        await stack.enter_async_context(context.preparation_slots)


async def _process_task_attempt(
    task_row: Task,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkServiceClient,
    benchmark_id: UUID,
    task_id: str,
    runtime: RuntimeServices,
    task_logs: TaskLogBuffer,
    org: Org,
    sandbox_provider_config: SandboxProviderConfig,
    creation_semaphore: Semaphore,
    dependency_setup_recovery: _DependencySetupRecoveryState,
    recovery_attempt: SandboxRecoveryAttempt,
    authority: ExecutionAuthority,
    *,
    sandbox_provider: SandboxProvider | None = None,
    queue_context: SandboxQueueContext | None = None,
) -> dict[str, dict[str, Any] | None]:
    """
    Processes a task and returns the evaluation result

    NOTE: When we close the sandbox the agent process will be killed and we will instantly go to evaluating,
    the evaluation will fail since the instance no longer exists. We handle this inside of the exception caught.
    """
    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)

    phase = FailurePhase.ADMISSION

    requested_attempt_started_at = task_row.started_at
    with Session(bind=engine) as task_session:
        try:
            benchmark_row = lock_execution_authority(task_session, authority)
        except ExecutionAuthorityRevoked:
            task_session.rollback()
            return {task_id: None}
        task_row = fetch_task_row(task_row.id, task_session, org)

        if _normalized_attempt_time(task_row.started_at) != _normalized_attempt_time(requested_attempt_started_at):
            return {task_id: None}
        attempt_started_at = task_row.started_at
        if benchmark_row.status == BenchmarkStatus.STOPPING or task_row.status == TaskStatus.STOPPED:
            handle_early_exit(task_row, task_session, authority)
            return {task_id: None}
        expected_failure_status = (
            TaskStatus.EVALUATING
            if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None
            else None
        )
        benchmark_name = benchmark_row.name
        benchmark_agent_name = benchmark_row.arguments.contract.name
        benchmark_started_by_email = benchmark_row.started_by_email

    # Setup logging infrastructure before try block so it's always available.
    # Version streams by task attempt so retries never overwrite earlier logs.
    task_stream_name = task_log_stream_name(task_id, task_row.started_at)
    log_output = task_logs.write

    logger.info(
        "Task output stream selected",
        extra={
            "benchmark_id": str(benchmark_id),
            "task_id": task_id,
            "cloudwatch_log_url": runtime.log_locations.task_location(
                str(benchmark_id),
                task_stream_name,
            ),
        },
    )

    evaluation_lock: PostgresAdvisoryLock | None = None
    evaluation_lock_acquired = False
    agent_attempt_started = False

    def require_wall_seconds_remaining(deadline_at: datetime) -> float:
        if deadline_at.tzinfo is None:
            deadline_at = deadline_at.replace(tzinfo=UTC)
        remaining = (deadline_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise CreditedTaskWallTimeExceeded("Task wall-time limit reached")
        return remaining

    @asynccontextmanager
    async def enforce_wall_deadline(deadline_at: datetime | None) -> AsyncGenerator[asyncio.Timeout | None]:
        if deadline_at is None:
            yield None
            return
        remaining = require_wall_seconds_remaining(deadline_at)
        wall: asyncio.Timeout
        try:
            async with asyncio.timeout_at(asyncio.get_running_loop().time() + remaining) as wall:
                yield wall
        except TimeoutError as error:
            if wall.expired():
                raise CreditedTaskWallTimeExceeded("Task wall-time limit reached") from error
            raise

    def ensure_wall_deadline() -> datetime:
        with Session(bind=engine) as wall_session:
            lock_execution_authority(wall_session, authority)
            owned = wall_session.exec(
                select(Task).where(col(Task.id) == task_row.id).where(col(Task.org_id) == org.id).with_for_update()
            ).one()
            if owned.started_at != attempt_started_at or owned.status == TaskStatus.STOPPED:
                raise ExecutionAuthorityRevoked("Task attempt no longer owns the wall deadline")
            if owned.credited_wall_deadline_at is None:
                owned.credited_wall_deadline_at = datetime.now(UTC) + timedelta(seconds=TASK_WALL_SECONDS)
                wall_session.add(owned)
                wall_session.commit()
            task_row.credited_wall_deadline_at = owned.credited_wall_deadline_at
            return owned.credited_wall_deadline_at

    preparation_stack = AsyncExitStack()

    def open_task_session() -> Session:
        bind = evaluation_lock.connection if evaluation_lock_acquired and evaluation_lock is not None else engine
        return Session(bind=bind)

    evaluation_resume_state = task_row.eval_resume_state
    sandbox_id_for_recovery: str | None = None
    exit_reason: AgentCausedExitReason | None = None
    evaluation_start_time: float | None = None
    start_sandbox_run_time: float | None = None

    def on_eval_resume_state(state: dict[str, Any]) -> None:
        nonlocal evaluation_resume_state
        evaluation_resume_state = state
        save_eval_resume_state(
            task_row.id,
            org,
            state,
            expected_started_at=attempt_started_at,
            authority=authority,
            connection=evaluation_lock.connection if evaluation_lock_acquired and evaluation_lock is not None else None,
        )

    sandbox_writes: set[asyncio.Future[None]] = set()
    sandbox_write_order = asyncio.Lock()

    async def save_announced_sandbox(link: SandboxLink, sandbox_id: str) -> None:
        async with sandbox_write_order:
            await asyncio.to_thread(record_sandbox, link, sandbox_id)

    async def drain_sandbox_writes() -> None:
        if not sandbox_writes:
            return
        drain = asyncio.ensure_future(asyncio.gather(*sandbox_writes, return_exceptions=True))
        cancelled = False
        while not drain.done():
            try:
                await asyncio.shield(drain)
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    def on_sandbox_created(created: SandboxCreatedData) -> None:
        link = SandboxLink(benchmark_id=benchmark_id, provider=created.provider)
        write = asyncio.ensure_future(save_announced_sandbox(link, created.sandbox_id))
        sandbox_writes.add(write)
        write.add_done_callback(sandbox_writes.discard)

    async def resume_evaluation_with_wall(state: dict[str, Any]) -> Any:
        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            result = await _run_benchmark_service_websocket(
                benchmark_service.resume_evaluation(
                    task_row.task_id,
                    eval_resume_state=state,
                    on_message=log_output,
                    on_eval_resume_state=on_eval_resume_state,
                    on_sandbox_created=on_sandbox_created,
                    dataset=start_benchmark_request.dataset,
                    sandbox_provider=sandbox_provider_config,
                )
            )
            if task_row.credited_wall_deadline_at is not None:
                require_wall_seconds_remaining(task_row.credited_wall_deadline_at)
            return result

    def execution_is_current(expected_status: TaskStatus | None = None) -> bool:
        with open_task_session() as task_session:
            try:
                lock_execution_authority(task_session, authority)
            except ExecutionAuthorityRevoked:
                task_session.rollback()
                return False
            task = fetch_task_row(task_row.id, task_session, org)
            is_current = (
                task.status != TaskStatus.STOPPED
                and task.started_at == attempt_started_at
                and (expected_status is None or task.status == expected_status)
            )
            task_session.rollback()
            return is_current

    def task_is_stopped() -> bool:
        return not execution_is_current()

    def return_queued_task_to_pending() -> bool:
        """Release a queued task so a fresh-sandbox retry can re-admit it."""
        if queue_context is None:
            return True
        with Session(bind=engine) as task_session:
            return commit_task_status_transition(
                task_row.id,
                task_session,
                org,
                TaskStatus.PENDING,
                expected_started_at=attempt_started_at,
                expected_status=expected_failure_status,
                authority=authority,
            )

    def commit_terminal_error(failure: Failure) -> dict[str, dict[str, Any] | None]:
        exc = failure.exc
        with error_span(
            "task.error",
            exc,
            benchmark_id=str(benchmark_id),
            task_id=task_id,
            producer=failure.producer,
            operation=failure.operation,
            error_type=failure.error_type,
            cause_code=failure.cause_code or "",
            failure_category=failure.category.value,
            failure_phase=phase.value,
            transient="" if failure.transient is None else failure.transient,
        ):
            logger.error(
                "Task execution failed",
                exc_info=(type(exc), exc, exc.__traceback__),
                extra={
                    "benchmark_id": str(benchmark_id),
                    "task_id": task_id,
                    "producer": failure.producer,
                    "operation": failure.operation,
                    "error_type": failure.error_type,
                    "cause_code": failure.cause_code or "",
                    "failure_category": failure.category.value,
                    "failure_phase": phase.value,
                    "transient": failure.transient,
                },
            )
            with sentry_sdk.new_scope() as scope:
                scope.set_tag("failure_category", failure.category.value)
                scope.set_tag("failure_phase", phase.value)
                capture_exception(exc)
        with open_task_session() as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            commit_task_error(
                task,
                task_session,
                failure.message,
                producer=failure.producer,
                operation=failure.operation,
                error_type=failure.error_type,
                category=failure.category,
                cause_code=failure.cause_code,
                phase=phase,
                transient=failure.transient,
                expected_started_at=attempt_started_at,
                expected_status=expected_failure_status,
                authority=authority,
            )
        return {task_id: None}

    def commit_wall_error(error: CreditedTaskWallTimeExceeded) -> dict[str, dict[str, Any] | None]:
        return commit_terminal_error(
            Failure(
                error,
                str(error),
                FailureCategory.UNKNOWN,
                "tracker",
                "task_wall_deadline",
                phase,
                cause_code="credited_task_wall_time_exceeded",
            )
        )

    def commit_resumed_evaluation(
        evaluation_result_row: EvaluationResult,
        evaluation_run_duration: float,
        sandbox_run_duration: float | None,
    ) -> dict[str, dict[str, Any] | None]:
        with open_task_session() as task_session:
            task_session.add(evaluation_result_row)
            task_in_session = fetch_task_row(task_row.id, task_session, org)
            if task_in_session.task_breakdown:
                existing_breakdown = task_session.get(TaskBreakdown, task_in_session.task_breakdown)
                assert existing_breakdown is not None
                existing_breakdown.evaluation_run_duration = evaluation_run_duration
                if sandbox_run_duration is not None:
                    existing_breakdown.sandbox_run_duration = sandbox_run_duration
            if not commit_task_status_transition(
                task_row.id,
                task_session,
                org,
                TaskStatus.FINISHED,
                expected_started_at=attempt_started_at,
                expected_status=(TaskStatus.EVALUATING, TaskStatus.ERROR),
                authority=authority,
            ):
                return {task_id: None}
            return {task_id: evaluation_result_row.result}

    def recreate_sandbox(failure: Failure) -> dict[str, dict[str, Any] | None]:
        """Hand the task back to run_with_sandbox_recovery, which retries it on a fresh sandbox.

        The raised SandboxSetupError carries this attempt's phase and cause code, since the
        retry row and the final error row (once retries run out) are written outside this function.
        """
        exc = failure.exc
        if not return_queued_task_to_pending():
            return {task_id: None}
        log_output(f"\n[ERROR] {_exception_message(exc)}")
        retry = exc if isinstance(exc, SandboxSetupError) else SandboxSetupError(str(exc))
        retry.phase = phase
        retry.cause_code = failure.cause_code
        if retry is exc:
            raise retry
        raise retry from exc

    async def recover_evaluation_stream_failure(error_message: str) -> dict[str, dict[str, Any] | None] | None:
        """Resume an interrupted evaluation when the service has persisted continuation state."""
        if evaluation_resume_state is None:
            return None
        if task_is_stopped():
            return {task_id: None}

        recovery_message = f"{error_message}; resuming evaluation from durable benchmark state"
        logger.warning(recovery_message)
        log_output(f"\n[WARN] {recovery_message}\n")
        resume_eval_start_time = time.perf_counter()
        try:
            task_logs.last_log_time = time.monotonic()
            evaluation_result = await resume_evaluation_with_wall(evaluation_resume_state)
        except CreditedTaskWallTimeExceeded as wall_error:
            if task_is_stopped():
                return {task_id: None}
            return commit_wall_error(wall_error)
        except Exception as resume_error:
            if task_is_stopped():
                return {task_id: None}
            failure = describe_failure(resume_error, _exception_message(resume_error), phase=phase)
            if isinstance(resume_error, BenchmarkServiceWebSocketDNSResolutionError):
                detail = f"WebSocket DNS resolution failed during resume: {_exception_message(resume_error)}"
            else:
                failure = replace(failure, producer="benchmark_service", operation="resume_evaluation")
                detail = f"resume failed: {failure.message}"
            failure = replace(failure, message=f"{recovery_message}; {detail}")
            log_output(f"\n[ERROR] {failure.message}")
            return commit_terminal_error(failure)

        finished_at = time.perf_counter()
        evaluation_run_duration = finished_at - (evaluation_start_time or resume_eval_start_time)
        sandbox_run_duration = finished_at - start_sandbox_run_time if start_sandbox_run_time is not None else None
        evaluation_result_value = cast(dict[str, Any], evaluation_result)
        evaluation_result_row = EvaluationResult(
            org_id=org.id,
            task=task_row.id,
            instance_id=sandbox_id_for_recovery,
            result=evaluation_result_value,
            agent_caused_exit_reason=exit_reason,
        )
        return commit_resumed_evaluation(evaluation_result_row, evaluation_run_duration, sandbox_run_duration)

    if task_row.credited_wall_deadline_at is not None:
        try:
            require_wall_seconds_remaining(task_row.credited_wall_deadline_at)
        except CreditedTaskWallTimeExceeded as error:
            return commit_wall_error(error)

    try:
        evaluation_resume_state = task_row.eval_resume_state
        if task_row.status == TaskStatus.EVALUATING and evaluation_resume_state is not None:
            phase = FailurePhase.EVALUATION
            evaluation_lock = task_evaluation_lock(engine, task_row.id)
            evaluation_lock_acquired = await evaluation_lock.__aenter__()
            if not evaluation_lock_acquired:
                return {task_id: None}

            with open_task_session() as ownership_session:
                try:
                    lock_execution_authority(ownership_session, authority)
                except ExecutionAuthorityRevoked:
                    ownership_session.rollback()
                    return {task_id: None}
                dispatch_created_at = ownership_session.exec(
                    select(col(ExecutorDispatch.created_at)).where(col(ExecutorDispatch.id) == authority.dispatch_id)
                ).one()
                owned_task = ownership_session.exec(
                    select(Task)
                    .where(col(Task.id) == task_row.id)
                    .where(col(Task.org_id) == org.id)
                    .where(col(Task.status) == TaskStatus.EVALUATING)
                    .where(col(Task.started_at) == dispatch_created_at)
                    .with_for_update()
                ).one_or_none()
                if owned_task is None or owned_task.eval_resume_state is None:
                    ownership_session.rollback()
                    return {task_id: None}
                evaluation_resume_state = owned_task.eval_resume_state
                ownership_session.rollback()

            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                resume_eval_start_time = time.perf_counter()
                # Reset timer to keep the last received message from the benchmarks service accurate
                task_logs.last_log_time = time.monotonic()
                evaluation_result = await resume_evaluation_with_wall(evaluation_resume_state)
                resume_eval_duration = time.perf_counter() - resume_eval_start_time
                evaluation_result_row = EvaluationResult(
                    org_id=org.id,
                    task=task_row.id,
                    instance_id=None,
                    result=cast(dict[str, Any], evaluation_result),
                    agent_caused_exit_reason=None,
                )

                return commit_resumed_evaluation(evaluation_result_row, resume_eval_duration, None)
            except SandboxNotFoundError:
                with Session(bind=engine) as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}
                try:
                    await recovery_attempt.retrieve_task()
                except Exception:
                    # Recovery remains disabled when its benchmark policy cannot
                    # be loaded, but that lookup failure must not hide the
                    # provider-confirmed sandbox loss that interrupted grading.
                    logger.warning(
                        "Failed to load sandbox recovery policy after grading sandbox loss",
                        exc_info=True,
                    )
                raise
            except Exception as e:
                with open_task_session() as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}

                raise e from e

        if queue_context is None:
            while not (admitted := await to_thread(_admit_direct_task, task_row, org, authority)):
                if admitted is None:
                    return {task_id: None}
                await asyncio.sleep(_DIRECT_ADMISSION_POLL_SECONDS)

        await _acquire_queue_preparation(preparation_stack, queue_context)
        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            task_data = await recovery_attempt.retrieve_task()
        # Generation creates an unrestricted network; Docker cannot replace it between stages.
        unrestricted_docker = sandbox_provider_config.type == "docker"
        run_egress_policy = combine_run_egress_policies(
            task_data.egress.run, start_benchmark_request.contract.egress_allowlist
        )
        if unrestricted_docker and any(
            policy != "*"
            for policy in (
                start_benchmark_request.contract.install_egress_policy,
                task_data.egress.setup_task,
                run_egress_policy,
                task_data.egress.evaluation,
            )
        ):
            raise InvalidSandboxConfigurationError(
                "Local Docker execution requires unrestricted egress for every stage; "
                "egress allowlists and blocked-network policies are not supported"
            )
        if sandbox_provider is None:
            sandbox_provider = benchmark_service.get_sandbox_provider(sandbox_provider_config)

        plan = GenerationPlan.from_task(task_data)

        # Labels that show up in the UI we can use to filter sandboxes.
        # Benchmark/Id/Task are read back by sandbox._audit_sandbox_delete.
        labels = {
            "Benchmark": start_benchmark_request.benchmark_name,
            "Id": str(benchmark_id),
            # CBS resolves VolumeMount's {run_id} placeholder from the
            # "run-id" label and hard-fails if this isolation identity is absent.
            "run-id": str(benchmark_id),
            "Task": task_row.task_id,
        }

        if queue_context is None and not execution_is_current(TaskStatus.BUILDING):
            return {task_id: None}

        identity = {
            "benchmark_name": benchmark_name,
            "agent_name": benchmark_agent_name,
        }
        if benchmark_started_by_email:
            identity["email"] = benchmark_started_by_email

        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            resolved_secrets = await resolve_secrets(start_benchmark_request.contract.secrets, runtime.secrets)

        # Ruff cannot trace this binding through the nested sandbox context.
        deadline_controller: ExternalServiceDeadlineController

        attempt_id = task_attempt_id(task_row.id, attempt_started_at)
        env_vars = {
            **resolved_secrets,
            "RUN_ID": str(benchmark_id),
            "TASK_ID": task_row.task_id,
            "ATTEMPT_ID": attempt_id,
            **_attested_inference_settings(start_benchmark_request.contract),
            "IDENTITY": json.dumps(identity),
            # Tags sandbox-internal OTel telemetry with our IDs + environment so traces/logs/metrics
            # are filterable per benchmark run and separable from other environments sharing the
            # same Daytona account (sandbox OTLP export is account-level).
            "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS": (
                f"benchmark_id={benchmark_id},task_id={task_row.task_id},environment={ENVIRONMENT}"
            ),
            **recovery_attempt.environment,
        }
        redact_error: Callable[[str], str] | None = None
        # Mask both sets together so a provider value cannot hide part of a
        # longer contract value from the later Tracker-side pass.
        secret_env_names = (
            (*start_benchmark_request.contract.secrets, *task_data.sandbox_secrets) if task_data.sandbox_secrets else ()
        )

        # We don't want to track the task until the sandbox is actually created.
        task_breakdown = TaskBreakdown()

        async def persist_generation_accounting_summary(
            summary: GenerationAccountingSummary,
        ) -> None:
            _persist_generation_accounting_summary(
                summary,
                task_breakdown=task_breakdown,
                task_row_id=task_row.id,
                org=org,
                authority=authority,
                expected_started_at=attempt_started_at,
                open_task_session=open_task_session,
            )

        start_sandbox_build_time = time.perf_counter()
        object_store = runtime.objects
        creation_state = SandboxCreationState(cleanup_pending=False)

        @asynccontextmanager
        async def sandbox_context() -> AsyncGenerator[Sandbox]:
            nonlocal deadline_controller, start_sandbox_build_time, redact_error, phase
            start_sandbox_build_time = time.perf_counter()
            sandbox_name = (
                task_row.task_id if queue_context is None else queued_sandbox_name(task_row.id, attempt_started_at)
            )
            # Do not mint while the queued attempt waits for admission.
            contract = start_benchmark_request.contract
            wall_deadline_at = ensure_wall_deadline()
            # Enter the key before the wall exits, but revoke it afterward so slow revocation cannot mask the task error.
            async with AsyncExitStack() as key_stack, enforce_wall_deadline(wall_deadline_at) as wall_timeout:
                scoped_env_vars = await key_stack.enter_async_context(
                    task_scoped_gateway_key(
                        env_vars,
                        run_id=str(benchmark_id),
                        task_id=task_row.task_id,
                        attempt_id=attempt_id,
                        attested_model=contract.model if contract.inference_settings_attested else None,
                        companion_models=contract.kwargs.get("companion_models"),
                        identity=identity,
                        org_name=org.name,
                    )
                )
                deadline_controller = await _create_generation_deadline_controller(
                    contract, plan, resolved_secrets, scoped_env_vars
                )
                sandbox_env_vars = scoped_env_vars
                # Mask the values actually supplied to this sandbox, including
                # its newly minted gateway token. Provider-managed values are
                # masked inside the sandbox before the optional report returns.
                redact_error = create_agent_error_redactor(scoped_env_vars[name] for name in contract.secrets)
                phase = FailurePhase.SANDBOX
                async with create_sandbox(
                    provider=sandbox_provider,
                    sandbox_name=sandbox_name,
                    source=task_data.source,
                    labels=labels,
                    env_vars=sandbox_env_vars,
                    sandbox_secrets=task_data.sandbox_secrets,
                    resources=task_data.resources,
                    volumes=task_data.volumes,
                    creation_semaphore=creation_semaphore,
                    creation_state=creation_state,
                    unique_name=queue_context is None,
                    sandbox_link=SandboxLink(
                        benchmark_id=benchmark_id,
                        provider=sandbox_provider_config.type,
                    ),
                ) as sandbox:
                    try:
                        yield sandbox
                    finally:
                        # The two-hour margin is for teardown; a finished result must not be
                        # overwritten by cancellation while deleting the sandbox.
                        if wall_timeout is not None and not wall_timeout.expired():
                            wall_timeout.reschedule(None)

        async with AsyncExitStack() as sandbox_stack:
            if queue_context is None:
                sandbox = await sandbox_stack.enter_async_context(sandbox_context())
            else:
                async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
                    sandbox = await enter_queued_sandbox(
                        stack=sandbox_stack,
                        context=queue_context,
                        task_row_id=task_row.id,
                        expected_started_at=attempt_started_at,
                        authority=authority,
                        source=task_data.source,
                        resources=task_data.resources,
                        creation_state=creation_state,
                        create=sandbox_context,
                        preparation_stack=preparation_stack,
                    )
                if sandbox is None:
                    return {task_id: None}
            sandbox_id_for_recovery = sandbox.id
            phase = FailurePhase.SETUP
            if start_benchmark_request.contract.final_output is not None:
                descriptor = ActiveNativeOutput(
                    attempt_started_at=attempt_started_at,
                    sandbox_id=sandbox.id,
                    runtime_source=task_data.source,
                    # Reconnected providers must read relative output in the agent's cwd.
                    output_path=str(PurePosixPath(task_data.cwd) / start_benchmark_request.contract.final_output),
                )
                try:
                    with open_task_session() as task_session:
                        publish_native_output(task_session, task_row.id, descriptor, authority)
                except ExecutionAuthorityRevoked:
                    raise
                except Exception:
                    logger.exception("Failed to publish native output", extra={"task_id": str(task_row.id)})

                def revoke_native_output() -> None:
                    try:
                        with open_task_session() as task_session:
                            clear_native_output(task_session, task_row.id, descriptor)
                    except Exception:
                        logger.exception("Failed to clear native output", extra={"task_id": str(task_row.id)})

                sandbox_stack.callback(revoke_native_output)
            task_breakdown.sandbox_build_duration = time.perf_counter() - start_sandbox_build_time
            start_sandbox_run_time = time.perf_counter()

            try:
                if queue_context is None:
                    with Session(bind=engine) as task_session:
                        if not commit_task_status_transition(
                            task_row.id,
                            task_session,
                            org,
                            TaskStatus.IN_PROGRESS,
                            expected_started_at=attempt_started_at,
                            authority=authority,
                        ):
                            return {task_id: None}

                # Upload the contract before applying any stage-specific network policy.
                await upload_agent_artifacts(
                    sandbox,
                    start_benchmark_request.contract,
                    str(benchmark_id),
                    object_store,
                )

                agent_sandbox = runtime_sandbox(sandbox, task_data.source)

                async def install_agent() -> None:
                    if not unrestricted_docker:
                        await apply_egress_policy(
                            agent_sandbox,
                            start_benchmark_request.contract.install_egress_policy,
                        )
                    try:
                        await install_agent_dependencies(
                            agent_sandbox,
                            start_benchmark_request.contract,
                            log_output,
                            dependency_setup_recovery.mode,
                            redact_error=redact_error,
                            secret_env_names=secret_env_names,
                        )
                    except DependencySetupExhaustedError:
                        dependency_setup_recovery.mode = DependencySetupMode.FINAL_FRESH_SANDBOX
                        raise

                # Compose setup bootstraps the service that receives agent commands,
                # so it must run before installation regardless of the declared order.
                install_after_setup = (
                    isinstance(task_data.source, ComposeSource) or task_data.agent_install_order == "after_setup"
                )
                if not install_after_setup:
                    await install_agent()

                if not unrestricted_docker:
                    await apply_egress_policy(sandbox, task_data.egress.setup_task)

                # Reset timer to keep the last received message from the benchmarks service accurate
                task_logs.last_log_time = time.monotonic()
                _ = await _run_benchmark_service_websocket(
                    benchmark_service.setup_task(
                        task_row.task_id,
                        sandbox.id,
                        on_message=log_output,
                        dataset=start_benchmark_request.dataset,
                        sandbox_provider=sandbox_provider_config,
                    )
                )
                # The benchmark has now had an opportunity to persist the
                # outage metadata in its restored volume. A later loss is a
                # distinct outage and must receive a new identity.
                recovery_attempt.mark_replacement_ready()

                if install_after_setup:
                    await install_agent()

                # Force flush the logs if anything has been buffered
                task_logs.buffer_logs(force_flush=True)

                # Compute the S3 key for the agent's output archive
                agent_output_s3_key = None
                if plan.episode is not None or start_benchmark_request.contract.final_output:
                    agent_output_s3_key = task_artifact_key(str(benchmark_id), task_id, "agent_output.tar.gz")

                if not unrestricted_docker:
                    await apply_egress_policy(agent_sandbox, run_egress_policy)
                agent_attempt_started = True
                phase = FailurePhase.AGENT
                generation_gateway_url = (
                    EXTERNAL_SERVICE_GATEWAY_URL if deadline_controller.snapshot is not None else None  # noqa: F821
                )
                try:
                    exit_reason, agent_run_time = await run_agent(
                        agent_sandbox,
                        start_benchmark_request.contract,
                        task_data.problem_path,
                        task_id,
                        log_output,
                        task_data.cwd,
                        object_store=object_store,
                        agent_output_s3_key=agent_output_s3_key,
                        plan=plan,
                        benchmark_id=str(benchmark_id),
                        execution_is_current=execution_is_current,
                        deadline_controller=deadline_controller,  # noqa: F821
                        generation_gateway_url=generation_gateway_url,
                        on_external_service_finalized=persist_generation_accounting_summary,
                        redact_error=redact_error,
                        secret_env_names=secret_env_names,
                    )
                except AgentRunFailedError as agent_error:
                    gateway_url = generation_gateway_url or resolved_secrets.get("MODEL_GATEWAY_URL")
                    gateway_host = urlsplit(gateway_url).hostname if gateway_url else None
                    if gateway_host and not await sandbox_resolves(agent_sandbox, gateway_host):
                        raise SandboxDNSResolutionError(
                            f"Sandbox could not resolve the model gateway {gateway_host} after the agent failed"
                        ) from agent_error
                    raise
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
                phase = FailurePhase.EVALUATION
                with Session(bind=engine) as task_session:
                    task_session.add(task_breakdown)
                    if not commit_task_status_transition(
                        task_row.id,
                        task_session,
                        org,
                        TaskStatus.EVALUATING,
                        expected_started_at=attempt_started_at,
                        authority=authority,
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
                if not unrestricted_docker:
                    await apply_egress_policy(sandbox, task_data.egress.evaluation)
                # Reset timer to keep the last received message from the benchmarks service accurate
                task_logs.last_log_time = time.monotonic()
                evaluation_result = await _run_benchmark_service_websocket(
                    benchmark_service.evaluate_instance(
                        task_row.task_id,
                        sandbox.id,
                        on_message=log_output,
                        on_eval_resume_state=on_eval_resume_state,
                        on_sandbox_created=on_sandbox_created,
                        dataset=start_benchmark_request.dataset,
                        sandbox_provider=sandbox_provider_config,
                    )
                )
                task_breakdown.evaluation_run_duration = time.perf_counter() - evaluation_start_time

                assert start_sandbox_run_time is not None
                task_breakdown.sandbox_run_duration = time.perf_counter() - start_sandbox_run_time

                # Force flush the logs, maybe redundant since we have the one in finally:
                task_logs.buffer_logs(force_flush=True)

                # Save the evaluation result to the database with the task row
                # Record the termination reason if the agent did not exit cleanly (timeout / OS kill)
                evaluation_result_row = EvaluationResult(
                    org_id=org.id,
                    task=task_row.id,
                    instance_id=sandbox.id,
                    result=cast(dict[str, Any], evaluation_result),
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
                        expected_status=(TaskStatus.EVALUATING, TaskStatus.ERROR),
                        authority=authority,
                    ):
                        return {task_id: None}

                    return {task_id: evaluation_result_row.result}
            except Exception:
                with Session(bind=engine) as task_session:
                    task = fetch_task_row(task_row.id, task_session, org)
                    if task.status == TaskStatus.STOPPED:
                        return {task_id: None}

                raise

    except GenerationTerminationUnconfirmedError as e:
        if task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        log_output(f"\n[ERROR] {error_message}")

        failure = replace(
            describe_failure(e, error_message, phase=phase),
            producer="tracker",
            operation="generation_termination",
            category=FailureCategory.INFRASTRUCTURE,
            cause_code="deadline_expired_termination_unconfirmed",
        )
        return commit_terminal_error(failure)
    except SandboxSetupError as e:
        if task_is_stopped():
            return {task_id: None}
        if agent_attempt_started:
            error_message = _exception_message(e)
            log_output(f"\n[ERROR] {error_message}")
            failure = replace(
                describe_failure(e, error_message, phase=phase),
                producer="sandbox_provider",
                operation="sandbox_recovery",
                category=FailureCategory.INFRASTRUCTURE,
            )
            return commit_terminal_error(failure)
        if not return_queued_task_to_pending():
            return {task_id: None}
        e.phase = phase
        log_output(f"\n[ERROR] {_exception_message(e)}")
        raise
    except SandboxNotFoundError as e:
        if task_is_stopped():
            return {task_id: None}
        if recovery_attempt.sandbox_loss_retry_available and not agent_attempt_started:
            message = (
                "Sandbox disappeared; restoring the task from its durable volume "
                f"(attempt {recovery_attempt.number + 1}/{recovery_attempt.max_attempts})"
            )
            logger.warning(message)
            log_output(f"\n[WARNING] {message}\n")
            raise

        failure = replace(
            describe_failure(e, _exception_message(e), phase=phase),
            producer="sandbox_provider",
            operation="sandbox_recovery",
            category=FailureCategory.INFRASTRUCTURE,
        )
        log_output(f"\n[ERROR] {failure.message}")
        return commit_terminal_error(failure)
    except CreditedTaskWallTimeExceeded as wall_error:
        if task_is_stopped():
            return {task_id: None}
        log_output(f"\n[ERROR] {wall_error}")
        return commit_wall_error(wall_error)
    except Exception as e:
        if task_is_stopped():
            return {task_id: None}
        failure = describe_failure(
            e, _exception_message(e), phase=phase, idle_seconds=int(time.monotonic() - task_logs.last_log_time)
        )
        disposition = decide(failure)
        if disposition is Disposition.RECREATE_SANDBOX:
            return recreate_sandbox(failure)
        if disposition is Disposition.RESUME_EVALUATION:
            recovered = await recover_evaluation_stream_failure(failure.message)
            if recovered is not None:
                return recovered
        if failure.category is FailureCategory.UNKNOWN:
            logfire.exception("process_task failed")
        log_output(f"\n[ERROR] {failure.message}")
        return commit_terminal_error(failure)
    finally:
        try:
            await drain_sandbox_writes()
        finally:
            await preparation_stack.aclose()
            if evaluation_lock is not None:
                await evaluation_lock.__aexit__(None, None, None)


def commit_task_error(
    task_row: Task,
    session: Session,
    error_message: str,
    *,
    producer: str,
    operation: str,
    error_type: str,
    category: FailureCategory,
    authority: ExecutionAuthority,
    cause_code: str | None = None,
    phase: FailurePhase | None = None,
    transient: bool | None = None,
    expected_started_at: datetime | None = None,
    expected_status: TaskStatus | None = None,
) -> bool:
    session.add(
        ErrorResult(
            org_id=task_row.org_id,
            task=task_row.id,
            error_message=error_message,
            producer=producer,
            operation=operation,
            error_type=error_type,
            cause_code=cause_code,
            category=category,
            phase=phase,
            transient=transient,
        )
    )
    return _commit_task_status(
        task_row,
        session,
        TaskStatus.ERROR,
        error_message=error_message,
        expected_started_at=expected_started_at,
        expected_status=expected_status,
        authority=authority,
    )
