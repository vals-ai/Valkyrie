"""Logic for executing, tracking, and transitioning the status of a single task."""

import asyncio
import hashlib
import ipaddress
import json
import math
import re
import time
import traceback
from asyncio import Semaphore
from collections.abc import Coroutine
from contextlib import suppress
from datetime import datetime
from enum import Enum
from typing import Any, Literal, cast
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import logfire
import sentry_sdk
from benchmark_service import (
    SandboxProviderConfig,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlmodel import Session, col, select, update
from tenacity import retry as tenacity_retry
from tenacity import retry_if_exception_type, stop_after_attempt, wait_fixed
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tracker.aws.cloudwatch_logs import write_benchmark_log_event
from tracker.aws.s3 import (
    download_from_s3,
    get_agent_result_s3_key,
    upload_to_s3,
)
from tracker.aws.secrets import resolve_secrets
from tracker.config import ENVIRONMENT
from tracker.database.models import (
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Org,
    RetryPolicy,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.session import engine
from tracker.exceptions import OutputArtifactError, SandboxSetupError, TrackerServiceError
from tracker.logging import get_logger, task_id_var
from tracker.model_gateway import (
    CapabilityEvalResumeState,
    CapabilityMintRequest,
    CapabilityUsageSummary,
    ModelGatewayAdminClient,
    capability_expires_at,
    finalize_capability_uninterruptibly,
    mint_capability_uninterruptibly,
)
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.observability import elapsed_ms, retry_callback
from tracker.sandbox import (
    EgressReadiness,
    create_sandbox,
    execute_agent,
    install_agent,
    restricted_agent_egress,
    run_agent,
    runtime_sandbox,
    upload_agent_artifacts,
    upload_agent_outputs,
)
from tracker.types import (
    AWSCredentials,
    HarnessConfig,
    StartBenchmarkRequest,
)

from tracker.utils.resources import fetch_benchmark_row, fetch_task_row

logger = get_logger(__name__)

_PTY_TASK_RETRY_LIMIT: int = 1
_MODEL_GATEWAY_USAGE_DIRECTORY = "model_gateway_usage"
_MODEL_GATEWAY_USAGE_RESULT_KEY = "_valkyrie_model_gateway_usage"
_AGENT_EGRESS_DIRECTORY = "agent_egress"
_AGENT_EGRESS_RESULT_KEY = "_valkyrie_agent_egress"
_AGENT_ENV_VAR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_AGENT_ENV_VAR_NAMES = frozenset(
    {
        "LANG",
        "RUN_ID",
        "TASK_ID",
        "TERM",
        "IDENTITY",
        "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS",
        "VALKYRIE_AGENT_SECRET_NAMES",
        "VALKYRIE_AGENT_SECRET_SCOPE",
    }
)


class EgressEvalResumeState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["agent_egress_eval_resume"]
    egress_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    benchmark_state: dict[str, Any]


class CapabilityEgressEvalResumeState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["model_gateway_egress_eval_resume"]
    capability_id: str = Field(pattern=r"^cap_[A-Za-z0-9_-]+$")
    egress_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    benchmark_state: dict[str, Any]


def _canonical_egress_origin(address: str) -> str:
    value = address.strip()
    if not value:
        raise SandboxSetupError("Egress addresses cannot be empty")
    try:
        return str(ipaddress.ip_network(value, strict=False))
    except ValueError:
        pass

    parsed = urlsplit(value if "://" in value else f"https://{value}")
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        raise SandboxSetupError(f"Unsupported egress address: {address}")
    port = parsed.port
    default_port = 443 if parsed.scheme == "https" else 80
    suffix = f":{port}" if port is not None and port != default_port else ""
    return f"{parsed.scheme}://{parsed.hostname.lower()}{suffix}"


def _merge_egress_addresses(*sources: list[str]) -> list[str]:
    merged: dict[str, str] = {}
    for source in sources:
        for address in source:
            merged.setdefault(_canonical_egress_origin(address), address)
    return list(merged.values())


def _egress_origin_set_sha256(addresses: list[str]) -> str:
    origins = sorted({_canonical_egress_origin(address) for address in addresses})
    content = json.dumps(origins, separators=(",", ":")).encode()
    return hashlib.sha256(content).hexdigest()


def _egress_readiness_receipt(
    benchmark_id: UUID,
    task_id: str,
    sandbox_id: str,
    contract_addresses: list[str],
    setup_addresses: list[str],
    gateway_addresses: list[str],
    readiness: EgressReadiness,
) -> dict[str, Any]:
    effective = _merge_egress_addresses(contract_addresses, setup_addresses, gateway_addresses)
    return {
        "schema_version": 1,
        "run_id": str(benchmark_id),
        "task_id": task_id,
        "sandbox_id": sandbox_id,
        "allowed_origin_set_sha256": {
            "contract": _egress_origin_set_sha256(contract_addresses),
            "setup": _egress_origin_set_sha256(setup_addresses),
            "model_gateway": _egress_origin_set_sha256(gateway_addresses),
            "effective": _egress_origin_set_sha256(effective),
        },
        "sentinel_sha256": sorted(
            hashlib.sha256(f"{address}:443".encode()).hexdigest() for address in readiness.sentinel_addresses
        ),
        "update_returned_at": readiness.update_returned_at,
        "ready_at": readiness.ready_at,
        "readiness_attempts": readiness.attempts,
        "required_consecutive_observations": 2,
    }


async def _upload_egress_readiness_receipt(
    receipt: dict[str, Any],
    benchmark_id: UUID,
    task_id: str,
    harness_config: HarnessConfig,
) -> str:
    content = (json.dumps(receipt, sort_keys=True) + "\n").encode()
    await upload_to_s3(
        content,
        get_agent_result_s3_key(
            str(benchmark_id),
            task_id,
            f"{_AGENT_EGRESS_DIRECTORY}/readiness-v1.json",
        ),
        harness_config.aws,
        harness_config.s3_bucket,
    )
    return hashlib.sha256(content).hexdigest()


def _parse_egress_readiness_receipt(content: bytes, benchmark_id: UUID, task_id: str) -> dict[str, Any]:
    try:
        raw: object = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TrackerServiceError("Agent egress readiness artifact is invalid JSON") from exc
    if not isinstance(raw, dict):
        raise TrackerServiceError("Agent egress readiness artifact is invalid")
    receipt = cast(dict[str, Any], raw)
    origins = receipt.get("allowed_origin_set_sha256")
    sentinels = receipt.get("sentinel_sha256")
    update_returned_at = receipt.get("update_returned_at")
    ready_at = receipt.get("ready_at")
    attempts = receipt.get("readiness_attempts")
    schema_version = receipt.get("schema_version")
    required_observations = receipt.get("required_consecutive_observations")
    if not isinstance(origins, dict) or not isinstance(sentinels, list):
        raise TrackerServiceError("Agent egress readiness artifact is invalid")
    origin_values = cast(dict[str, object], origins)
    sentinel_values = cast(list[object], sentinels)
    expected = {
        "schema_version",
        "run_id",
        "task_id",
        "sandbox_id",
        "allowed_origin_set_sha256",
        "sentinel_sha256",
        "update_returned_at",
        "ready_at",
        "readiness_attempts",
        "required_consecutive_observations",
    }
    if (
        set(receipt) != expected
        or type(schema_version) is not int
        or schema_version != 1
        or receipt.get("run_id") != str(benchmark_id)
        or receipt.get("task_id") != task_id
        or not isinstance(receipt.get("sandbox_id"), str)
        or set(origin_values) != {"contract", "setup", "model_gateway", "effective"}
        or any(
            not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in origin_values.values()
        )
        or len(sentinel_values) != 2
        or any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in sentinel_values)
        or sentinel_values != sorted(set(cast(list[str], sentinel_values)))
        or isinstance(update_returned_at, bool)
        or not isinstance(update_returned_at, int | float)
        or not math.isfinite(update_returned_at)
        or isinstance(ready_at, bool)
        or not isinstance(ready_at, int | float)
        or not math.isfinite(ready_at)
        or ready_at < update_returned_at
        or type(attempts) is not int
        or attempts < 2
        or type(required_observations) is not int
        or required_observations != 2
    ):
        raise TrackerServiceError("Agent egress readiness artifact is invalid")
    return receipt


def _validate_agent_env_vars(env_vars: dict[str, str]) -> None:
    invalid_names = sorted(name for name in env_vars if _AGENT_ENV_VAR_NAME.fullmatch(name) is None)
    if invalid_names:
        raise SandboxSetupError(f"Invalid agent secret environment variable names: {', '.join(invalid_names)}")

    reserved_names = sorted(env_vars.keys() & _RESERVED_AGENT_ENV_VAR_NAMES)
    if reserved_names:
        raise SandboxSetupError(f"Agent secret environment variables use reserved names: {', '.join(reserved_names)}")


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
    expected_status: TaskStatus | None = None,
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
        values: dict[str, TaskStatus | datetime | None] = {"status": to_status}
        if to_status in [TaskStatus.FINISHED, TaskStatus.ERROR]:
            values["finished_at"] = datetime.now(ZoneInfo("UTC"))
        if to_status == TaskStatus.FINISHED:
            values["eval_resume_state"] = None

        task_update = update(Task).where(col(Task.id) == task.id).where(col(Task.org_id) == task.org_id)
        if to_status != TaskStatus.STOPPED:
            task_update = task_update.where(col(Task.status) != TaskStatus.STOPPED)
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)
        if expected_status is not None:
            task_update = task_update.where(col(Task.status) == expected_status)
        if to_status == TaskStatus.FINISHED:
            task_update = task_update.where(col(Task.status) == TaskStatus.EVALUATING)

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
    expected_status: TaskStatus | None = None,
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
        retry_policy = benchmark_row.arguments.retry_policy
        if retry_policy == RetryPolicy.FORBID and task_row.status != TaskStatus.PENDING:
            return {task_id: None}
        if retry_policy != start_benchmark_request.retry_policy:
            commit_task_error(
                task_row,
                task_session,
                "Start request retry_policy does not match the stored run",
                expected_started_at=attempt_started_at,
                expected_status=TaskStatus.PENDING,
            )
            return {task_id: None}
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
    capability_usage: CapabilityUsageSummary | None = None
    egress_readiness_receipt: dict[str, Any] | None = None
    egress_receipt_sha256: str | None = None

    def on_eval_resume_state(state: dict[str, Any]) -> None:
        persisted_state = state
        if start_benchmark_request.contract.model_gateway_policy is not None:
            assert capability_usage is not None
            if egress_receipt_sha256 is None:
                persisted_state = CapabilityEvalResumeState(
                    kind="model_gateway_eval_resume",
                    capability_id=capability_usage.capability_id,
                    benchmark_state=state,
                ).model_dump(mode="json")
            else:
                persisted_state = CapabilityEgressEvalResumeState(
                    kind="model_gateway_egress_eval_resume",
                    capability_id=capability_usage.capability_id,
                    egress_receipt_sha256=egress_receipt_sha256,
                    benchmark_state=state,
                ).model_dump(mode="json")
        elif egress_receipt_sha256 is not None:
            persisted_state = EgressEvalResumeState(
                kind="agent_egress_eval_resume",
                egress_receipt_sha256=egress_receipt_sha256,
                benchmark_state=state,
            ).model_dump(mode="json")
        save_eval_resume_state(task_row.id, org, persisted_state, expected_started_at=attempt_started_at)

    def task_is_stopped() -> bool:
        with Session(bind=engine) as task_session:
            task = fetch_task_row(task_row.id, task_session, org)
            return task.status == TaskStatus.STOPPED or task.started_at != attempt_started_at

    try:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                benchmark_eval_resume_state = task_row.eval_resume_state
                capability_id: str | None = None
                kind = task_row.eval_resume_state.get("kind")
                if kind == "model_gateway_egress_eval_resume":
                    if start_benchmark_request.contract.model_gateway_policy is None:
                        raise TrackerServiceError("Model gateway evaluation resume state has no model policy")
                    try:
                        resume_state = CapabilityEgressEvalResumeState.model_validate(task_row.eval_resume_state)
                    except ValidationError:
                        raise TrackerServiceError("Model gateway egress evaluation resume state is invalid") from None
                    capability_id = resume_state.capability_id
                    egress_receipt_sha256 = resume_state.egress_receipt_sha256
                    benchmark_eval_resume_state = resume_state.benchmark_state
                elif kind == "model_gateway_eval_resume":
                    if start_benchmark_request.contract.model_gateway_policy is None:
                        raise TrackerServiceError("Model gateway evaluation resume state has no model policy")
                    try:
                        legacy_state = CapabilityEvalResumeState.model_validate(task_row.eval_resume_state)
                    except ValidationError:
                        raise TrackerServiceError("Model gateway evaluation resume state is invalid") from None
                    capability_id = legacy_state.capability_id
                    benchmark_eval_resume_state = legacy_state.benchmark_state
                elif kind == "agent_egress_eval_resume":
                    if start_benchmark_request.contract.model_gateway_policy is not None:
                        raise TrackerServiceError("Agent egress evaluation resume state is missing model capability")
                    try:
                        egress_state = EgressEvalResumeState.model_validate(task_row.eval_resume_state)
                    except ValidationError:
                        raise TrackerServiceError("Agent egress evaluation resume state is invalid") from None
                    egress_receipt_sha256 = egress_state.egress_receipt_sha256
                    benchmark_eval_resume_state = egress_state.benchmark_state
                elif start_benchmark_request.contract.model_gateway_policy is not None:
                    raise TrackerServiceError("Model gateway evaluation resume state is invalid")

                if capability_id is not None:
                    usage_artifact = await download_from_s3(
                        get_agent_result_s3_key(
                            str(benchmark_id),
                            task_id,
                            f"{_MODEL_GATEWAY_USAGE_DIRECTORY}/{capability_id}.json",
                        ),
                        harness_config.aws,
                        harness_config.s3_bucket,
                    )
                    try:
                        capability_usage = CapabilityUsageSummary.model_validate_json(usage_artifact)
                    except ValidationError:
                        raise TrackerServiceError("Model gateway usage artifact is invalid") from None
                    if capability_usage.capability_id != capability_id:
                        raise TrackerServiceError("Model gateway usage artifact does not match resume state")
                    if capability_usage.state != "revoked" or not capability_usage.drained:
                        raise TrackerServiceError("Model gateway usage artifact is not finalized")

                if egress_receipt_sha256 is not None:
                    egress_artifact = await download_from_s3(
                        get_agent_result_s3_key(
                            str(benchmark_id),
                            task_id,
                            f"{_AGENT_EGRESS_DIRECTORY}/readiness-v1.json",
                        ),
                        harness_config.aws,
                        harness_config.s3_bucket,
                    )
                    if hashlib.sha256(egress_artifact).hexdigest() != egress_receipt_sha256:
                        raise TrackerServiceError("Agent egress readiness artifact does not match resume state")
                    egress_readiness_receipt = _parse_egress_readiness_receipt(
                        egress_artifact,
                        benchmark_id,
                        task_id,
                    )

                resume_eval_start_time = time.perf_counter()
                # Reset timer to keep the last received message from the benchmarks service accurate
                last_log_time = time.monotonic()
                evaluation_result = await benchmark_service.resume_evaluation(
                    task_row.task_id,
                    eval_resume_state=benchmark_eval_resume_state,
                    on_message=log_output,
                    on_eval_resume_state=on_eval_resume_state,
                    dataset=start_benchmark_request.dataset,
                    sandbox_provider=sandbox_provider_config,
                )
                if capability_usage is not None:
                    if _MODEL_GATEWAY_USAGE_RESULT_KEY in evaluation_result:
                        raise TrackerServiceError(
                            f"Benchmark result uses reserved key {_MODEL_GATEWAY_USAGE_RESULT_KEY!r}"
                        )
                    evaluation_result[_MODEL_GATEWAY_USAGE_RESULT_KEY] = capability_usage.model_dump(mode="json")
                if egress_readiness_receipt is not None:
                    if _AGENT_EGRESS_RESULT_KEY in evaluation_result:
                        raise TrackerServiceError(f"Benchmark result uses reserved key {_AGENT_EGRESS_RESULT_KEY!r}")
                    evaluation_result[_AGENT_EGRESS_RESULT_KEY] = egress_readiness_receipt
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
                expected_status=TaskStatus.PENDING if retry_policy == RetryPolicy.FORBID else None,
            ):
                return {task_id: None}

        identity = {
            "benchmark_name": benchmark_name,
            "agent_name": benchmark_agent_name,
        }
        if benchmark_started_by_email:
            identity["email"] = benchmark_started_by_email

        sandbox_env_vars = {
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
        _validate_agent_env_vars(start_benchmark_request.contract.secrets)
        agent_env_vars: dict[str, str] = {}

        # We don't want to track the task until the sandbox is actually created.
        task_breakdown = TaskBreakdown()

        start_sandbox_build_time = time.perf_counter()
        async with create_sandbox(
            provider=sandbox_provider,
            sandbox_name=task_row.task_id,
            source=task_data.source,
            labels=labels,
            env_vars=sandbox_env_vars,
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
                        expected_status=TaskStatus.BUILDING if retry_policy == RetryPolicy.FORBID else None,
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
                setup_response = await benchmark_service.setup_task(
                    task_row.task_id,
                    sandbox.id,
                    on_message=log_output,
                    dataset=start_benchmark_request.dataset,
                    sandbox_provider=sandbox_provider_config,
                )

                # Force flush the logs if anything has been buffered
                buffer_logs(log_queue, stream_key, harness_config.aws, harness_config.log_group, force_flush=True)

                setup_addresses = setup_response.egress_allowlist
                contract_with_setup_egress = start_benchmark_request.contract.model_copy(
                    update={
                        "egress_allowlist": _merge_egress_addresses(
                            start_benchmark_request.contract.egress_allowlist,
                            setup_addresses,
                        )
                    }
                )

                # Compute the S3 key for the agent's output archive
                agent_output_s3_key = None
                if start_benchmark_request.contract.final_output:
                    agent_output_s3_key = get_agent_result_s3_key(str(benchmark_id), task_id, "agent_output.tar.gz")

                policy = start_benchmark_request.contract.model_gateway_policy
                if policy is None:
                    agent_env_vars = resolve_secrets(start_benchmark_request.contract.secrets, harness_config.aws)
                    try:
                        if not setup_addresses:
                            exit_reason, agent_run_time = await run_agent(
                                sandbox,
                                contract_with_setup_egress,
                                task_data.problem_path,
                                task_id,
                                log_output,
                                task_data.cwd,
                                agent_env_vars=agent_env_vars,
                                retry_policy=retry_policy,
                                aws=harness_config.aws,
                                s3_bucket=harness_config.s3_bucket,
                                agent_output_s3_key=agent_output_s3_key,
                                agent_timeout=task_data.agent_timeout,
                                benchmark_id=str(benchmark_id),
                                runtime_source=task_data.source,
                            )
                        else:
                            await install_agent(
                                sandbox,
                                contract_with_setup_egress,
                                log_output,
                                agent_env_vars,
                                task_data.source,
                                retry_policy=retry_policy,
                            )
                            command_contract = contract_with_setup_egress.model_copy(update={"egress_allowlist": []})
                            agent_sandbox = runtime_sandbox(sandbox, task_data.source)
                            async with restricted_agent_egress(
                                agent_sandbox, contract_with_setup_egress.egress_allowlist
                            ) as readiness:
                                egress_readiness_receipt = _egress_readiness_receipt(
                                    benchmark_id,
                                    task_id,
                                    sandbox.id,
                                    start_benchmark_request.contract.egress_allowlist,
                                    setup_addresses,
                                    [],
                                    readiness,
                                )
                                egress_receipt_sha256 = await _upload_egress_readiness_receipt(
                                    egress_readiness_receipt,
                                    benchmark_id,
                                    task_id,
                                    harness_config,
                                )
                                exit_reason, agent_run_time = await execute_agent(
                                    sandbox,
                                    command_contract,
                                    task_data.problem_path,
                                    task_id,
                                    log_output,
                                    task_data.cwd,
                                    agent_env_vars,
                                    task_data.agent_timeout,
                                    task_data.source,
                                )
                            await upload_agent_outputs(
                                sandbox,
                                command_contract,
                                task_id,
                                harness_config.aws,
                                harness_config.s3_bucket,
                                agent_output_s3_key,
                                str(benchmark_id),
                                task_data.source,
                            )
                    finally:
                        agent_env_vars.clear()
                else:
                    if task_data.agent_timeout is None:
                        raise TrackerServiceError("Task capability requires a finite positive agent_timeout")

                    agent_env_vars = resolve_secrets(start_benchmark_request.contract.secrets, harness_config.aws)
                    try:
                        await install_agent(
                            sandbox,
                            contract_with_setup_egress,
                            log_output,
                            agent_env_vars,
                            task_data.source,
                            retry_policy=retry_policy,
                        )
                    finally:
                        agent_env_vars.clear()

                    async with ModelGatewayAdminClient.from_environment() as gateway:

                        async def write_capability_usage(usage: CapabilityUsageSummary) -> None:
                            content = (json.dumps(usage.model_dump(mode="json"), sort_keys=True) + "\n").encode()
                            await upload_to_s3(
                                content,
                                get_agent_result_s3_key(
                                    str(benchmark_id),
                                    task_id,
                                    f"{_MODEL_GATEWAY_USAGE_DIRECTORY}/{usage.capability_id}.json",
                                ),
                                harness_config.aws,
                                harness_config.s3_bucket,
                            )
                            logger.info(
                                "model_gateway.capability.finalized",
                                extra={
                                    "benchmark_id": str(benchmark_id),
                                    "task_id": task_id,
                                    "sandbox_id": sandbox.id,
                                    **usage.model_dump(mode="json"),
                                },
                            )

                        effective_addresses = _merge_egress_addresses(
                            contract_with_setup_egress.egress_allowlist,
                            [gateway.gateway_url],
                        )
                        command_contract = contract_with_setup_egress.model_copy(update={"egress_allowlist": []})
                        agent_sandbox = runtime_sandbox(sandbox, task_data.source)
                        async with restricted_agent_egress(agent_sandbox, effective_addresses) as readiness:
                            egress_readiness_receipt = _egress_readiness_receipt(
                                benchmark_id,
                                task_id,
                                sandbox.id,
                                start_benchmark_request.contract.egress_allowlist,
                                setup_addresses,
                                [gateway.gateway_url],
                                readiness,
                            )
                            egress_receipt_sha256 = await _upload_egress_readiness_receipt(
                                egress_readiness_receipt,
                                benchmark_id,
                                task_id,
                                harness_config,
                            )
                            expires_at = capability_expires_at(task_data.agent_timeout, time.time())
                            minted = await mint_capability_uninterruptibly(
                                gateway,
                                CapabilityMintRequest(
                                    run_id=str(benchmark_id),
                                    task_id=task_id,
                                    model=policy.model,
                                    config=policy.config.model_dump(mode="json"),
                                    sandbox_id=sandbox.id,
                                    identity={"org_id": str(org.id), **identity},
                                    expires_at=expires_at,
                                    max_queries=policy.max_queries,
                                    max_sessions=policy.max_sessions,
                                ),
                                write_capability_usage,
                            )
                            runtime_env = {
                                "MODEL_GATEWAY_URL": gateway.gateway_url,
                                "MODEL_GATEWAY_API_KEY": minted.token,
                            }
                            capability_id = minted.capability_id
                            del minted
                            try:
                                exit_reason, agent_run_time = await execute_agent(
                                    sandbox,
                                    command_contract,
                                    task_data.problem_path,
                                    task_id,
                                    log_output,
                                    task_data.cwd,
                                    runtime_env,
                                    task_data.agent_timeout,
                                    task_data.source,
                                )
                            finally:
                                runtime_env.clear()
                                capability_usage = await finalize_capability_uninterruptibly(
                                    gateway,
                                    capability_id,
                                    write_capability_usage,
                                )

                    await upload_agent_outputs(
                        sandbox,
                        start_benchmark_request.contract,
                        task_id,
                        harness_config.aws,
                        harness_config.s3_bucket,
                        agent_output_s3_key,
                        str(benchmark_id),
                        task_data.source,
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
                        expected_status=TaskStatus.IN_PROGRESS if retry_policy == RetryPolicy.FORBID else None,
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
                if capability_usage is not None:
                    if _MODEL_GATEWAY_USAGE_RESULT_KEY in evaluation_result:
                        raise TrackerServiceError(
                            f"Benchmark result uses reserved key {_MODEL_GATEWAY_USAGE_RESULT_KEY!r}"
                        )
                    evaluation_result[_MODEL_GATEWAY_USAGE_RESULT_KEY] = capability_usage.model_dump(mode="json")
                if egress_readiness_receipt is not None:
                    if _AGENT_EGRESS_RESULT_KEY in evaluation_result:
                        raise TrackerServiceError(f"Benchmark result uses reserved key {_AGENT_EGRESS_RESULT_KEY!r}")
                    evaluation_result[_AGENT_EGRESS_RESULT_KEY] = egress_readiness_receipt
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
            finally:
                agent_env_vars.clear()

    except SandboxSetupError as e:
        if task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        log_output(f"\n[ERROR] {error_message}")
        if retry_policy == RetryPolicy.FORBID:
            with Session(bind=engine) as task_session:
                task = fetch_task_row(task_row.id, task_session, org)
                commit_task_error(task, task_session, error_message, expected_started_at=attempt_started_at)
            return {task_id: None}
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
    except ConnectionClosedError:
        if task_is_stopped():
            return {task_id: None}
        seconds = int(time.monotonic() - last_log_time)
        error_message = (
            f"Benchmark service has not sent a message, causing the connection to disconnect: "
            f"last message received {seconds}s ago"
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
    expected_status: TaskStatus | None = None,
) -> bool:
    session.add(ErrorResult(org_id=task_row.org_id, task=task_row.id, error_message=error_message))
    return _commit_task_status(
        task_row,
        session,
        TaskStatus.ERROR,
        error_message=error_message,
        expected_started_at=expected_started_at,
        expected_status=expected_status,
    )
