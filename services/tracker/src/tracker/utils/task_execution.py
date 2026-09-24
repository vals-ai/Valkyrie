"""Logic for executing, tracking, and transitioning the status of a single task."""

import asyncio
import json
import socket
import time
from asyncio import Semaphore
from collections.abc import Coroutine
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

import logfire
import sentry_sdk
from benchmark_service import (
    Sandbox,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxRecoveryAttempt,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceStreamError
from pydantic import ValidationError
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tracker.aws.cloudwatch_logs import (
    task_log_stream_name,
)
from tracker.runtime.services import RuntimeServices
from tracker.runtime.artifacts import task_artifact_key
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.config import ENVIRONMENT
from tracker.database.models import (
    AgentCausedExitReason,
    AgentContractRequest,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.exceptions import (
    DependencySetupExhaustedError,
    ExecutionAuthorityRevoked,
    OutputArtifactError,
    SandboxSetupError,
)
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.checkpoints import run_with_checkpoints
from tracker.executor.task_persistence import (
    ApiTaskPersistence,
)
from tracker.executor.queue_execution import ApiSandboxQueueContext
from tracker.executor_api.v1.schemas import RunStatus
from tracker.executor_api.v1.task_schemas import (
    BuildTask,
    RunTask,
    EvaluateTask,
    SaveCheckpoint,
    CompleteTask,
    FailTask,
    RetryTask,
    PendingTask,
    StopTask,
)
from tracker.logging import get_logger
from tracker.observability import error_span, incr
from tracker.observability.sentry import capture_exception, clear_sandbox_context
from tracker.observability.tracing import observability_span
from tracker.sandbox import DependencySetupMode, create_sandbox, run_agent, upload_agent_artifacts
from tracker.types import (
    StartBenchmarkRequest,
)


logger = get_logger(__name__)

_PTY_TASK_RETRY_LIMIT: int = 1
_SANDBOX_RETRY_DELAY_SECONDS: float = 2


class BenchmarkServiceWebSocketDNSResolutionError(BenchmarkServiceError):
    """A benchmark-service WebSocket could not resolve its destination host."""


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


def _exception_message(exc: BaseException) -> str:
    return str(exc).strip() or type(exc).__name__


_TASK_RETRY_METRIC = "valkyrie.task"


def _observe_task_retry(attempt: SandboxRecoveryAttempt, exc: BaseException) -> None:
    """Emit the retry telemetry the Tenacity before_sleep hook owned before recovery
    moved into the benchmark-service client."""
    error_class = type(exc).__name__
    with observability_span("task.retry", attempt=attempt.number, error_class=error_class):
        logger.warning(
            "retry.before_sleep",
            extra={
                "metric": _TASK_RETRY_METRIC,
                "fn": "_process_task_attempt",
                "attempt": attempt.number,
                "idle_for": _SANDBOX_RETRY_DELAY_SECONDS,
                "error_class": error_class,
            },
        )
        incr(f"{_TASK_RETRY_METRIC}.retry", tags={"error_class": error_class})


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
    queue_context: ApiSandboxQueueContext | None = None,
    persistence: ApiTaskPersistence,
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
                persistence=persistence,
            )

    async def record_attempt_failure(attempt: SandboxRecoveryAttempt, exc: Exception) -> None:
        _observe_task_retry(attempt, exc)
        if isinstance(exc, SandboxSetupError):
            await persistence.write(
                RetryTask(
                    error_message=_exception_message(exc),
                    producer="sandbox_provider",
                    operation_name="setup",
                    error_type=type(exc).__name__,
                    failed_attempt_number=attempt.number,
                )
            )

    result = await benchmark_service.run_with_sandbox_recovery(
        task_id=task_id,
        run_id=str(benchmark_id),
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
    queue_context: ApiSandboxQueueContext | None = None,
    persistence: ApiTaskPersistence,
) -> dict[str, dict[str, Any] | None]:
    """
    Processes a task and returns the evaluation result

    NOTE: When we close the sandbox the agent process will be killed and we will instantly go to evaluating,
    the evaluation will fail since the instance no longer exists. We handle this inside of the exception caught.
    """
    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)

    snapshot = await persistence.load()
    if snapshot is None:
        return {task_id: None}
    task_state = snapshot.task
    attempt_started_at = task_state.started_at
    if snapshot.run_status == RunStatus.STOPPING or task_state.status == TaskStatus.STOPPED:
        await persistence.write(StopTask())
        return {task_id: None}

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

    evaluation_resume_state = task_state.eval_resume_state
    sandbox_id_for_recovery: str | None = None
    exit_reason: AgentCausedExitReason | None = None
    evaluation_start_time: float | None = None
    start_sandbox_run_time: float | None = None

    async def on_eval_resume_state(state: dict[str, Any]) -> None:
        nonlocal evaluation_resume_state
        evaluation_resume_state = state
        if not await persistence.write(SaveCheckpoint(checkpoint=state)):
            raise ExecutionAuthorityRevoked("Task checkpoint authority was revoked")

    async def task_is_stopped() -> bool:
        return not await persistence.current()

    async def return_queued_task_to_pending() -> bool:
        """Release a queued task so a fresh-sandbox retry can re-admit it."""
        if queue_context is None:
            return True
        return await persistence.write(PendingTask())

    async def commit_terminal_error(
        exc: BaseException,
        error_message: str,
        *,
        producer: str,
        operation: str,
        cause_code: str | None = None,
    ) -> dict[str, dict[str, Any] | None]:
        with error_span(
            "task.error",
            exc,
            benchmark_id=str(benchmark_id),
            task_id=task_id,
            producer=producer,
            operation=operation,
            error_type=type(exc).__name__,
            cause_code=cause_code or "",
        ):
            logger.error(
                "Task execution failed",
                exc_info=(type(exc), exc, exc.__traceback__),
                extra={
                    "benchmark_id": str(benchmark_id),
                    "task_id": task_id,
                    "producer": producer,
                    "operation": operation,
                    "error_type": type(exc).__name__,
                    "cause_code": cause_code or "",
                },
            )
            capture_exception(exc)
        await persistence.write(
            FailTask(
                error_message=error_message,
                producer=producer,
                operation_name=operation,
                error_type=type(exc).__name__,
                cause_code=cause_code,
            )
        )
        return {task_id: None}

    async def recover_evaluation_stream_failure(error_message: str) -> dict[str, dict[str, Any] | None] | None:
        """Resume an interrupted evaluation when the service has persisted continuation state."""
        if evaluation_resume_state is None:
            return None
        resume_state = evaluation_resume_state
        if await task_is_stopped():
            return {task_id: None}

        recovery_message = f"{error_message}; resuming evaluation from durable benchmark state"
        logger.warning(recovery_message)
        log_output(f"\n[WARN] {recovery_message}\n")
        resume_eval_start_time = time.perf_counter()
        try:
            task_logs.last_log_time = time.monotonic()
            evaluation_result = await _run_benchmark_service_websocket(
                run_with_checkpoints(
                    lambda checkpoint: benchmark_service.resume_evaluation(
                        task_row.task_id,
                        eval_resume_state=resume_state,
                        on_message=log_output,
                        on_eval_resume_state=checkpoint,
                        dataset=start_benchmark_request.dataset,
                        sandbox_provider=sandbox_provider_config,
                    ),
                    on_eval_resume_state,
                )
            )
        except BenchmarkServiceWebSocketDNSResolutionError as resume_error:
            if await task_is_stopped():
                return {task_id: None}
            terminal_error = (
                f"{recovery_message}; WebSocket DNS resolution failed during resume: {_exception_message(resume_error)}"
            )
            logger.warning(terminal_error)
            log_output(f"\n[ERROR] {terminal_error}")
            return await commit_terminal_error(
                resume_error,
                terminal_error,
                producer="benchmark_service",
                operation="websocket_connect",
                cause_code="websocket_dns_resolution",
            )
        except Exception as resume_error:
            if await task_is_stopped():
                return {task_id: None}
            terminal_error = f"{recovery_message}; resume failed: {_exception_message(resume_error)}"
            logger.warning(terminal_error)
            log_output(f"\n[ERROR] {terminal_error}")
            return await commit_terminal_error(
                resume_error,
                terminal_error,
                producer="benchmark_service",
                operation="resume_evaluation",
            )

        finished_at = time.perf_counter()
        evaluation_run_duration = finished_at - (evaluation_start_time or resume_eval_start_time)
        sandbox_run_duration = finished_at - start_sandbox_run_time if start_sandbox_run_time is not None else None
        evaluation_result_value = cast(dict[str, Any], evaluation_result)
        if not await persistence.write(
            CompleteTask(
                instance_id=sandbox_id_for_recovery,
                result=evaluation_result_value,
                exit_reason=exit_reason.value if exit_reason else None,
                evaluation_run_duration=evaluation_run_duration,
                sandbox_run_duration=sandbox_run_duration,
            )
        ):
            return {task_id: None}

        return {task_id: evaluation_result_value}

    try:
        if task_state.status == TaskStatus.EVALUATING and evaluation_resume_state is not None:
            evaluation_resume_state = await persistence.resume()
            if evaluation_resume_state is None:
                return {task_id: None}
            resume_state = evaluation_resume_state

            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                resume_eval_start_time = time.perf_counter()
                # Reset timer to keep the last received message from the benchmarks service accurate
                task_logs.last_log_time = time.monotonic()
                evaluation_result = await _run_benchmark_service_websocket(
                    run_with_checkpoints(
                        lambda checkpoint: benchmark_service.resume_evaluation(
                            task_row.task_id,
                            eval_resume_state=resume_state,
                            on_message=log_output,
                            on_eval_resume_state=checkpoint,
                            dataset=start_benchmark_request.dataset,
                            sandbox_provider=sandbox_provider_config,
                        ),
                        on_eval_resume_state,
                    )
                )
                resume_eval_duration = time.perf_counter() - resume_eval_start_time
                evaluation_result_value = cast(dict[str, Any], evaluation_result)
                if not await persistence.write(
                    CompleteTask(
                        result=evaluation_result_value,
                        evaluation_run_duration=resume_eval_duration,
                    )
                ):
                    return {task_id: None}

                return {task_id: evaluation_result_value}
            except SandboxNotFoundError:
                if await task_is_stopped():
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
                if await task_is_stopped():
                    return {task_id: None}

                raise e from e

        task_data = await recovery_attempt.retrieve_task()
        if sandbox_provider is None:
            sandbox_provider = benchmark_service.get_sandbox_provider(sandbox_provider_config)

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

        if queue_context is None:
            if not await persistence.write(BuildTask()):
                return {task_id: None}

        env_vars = {
            **(await runtime.resolve_secrets(start_benchmark_request.contract.secrets)),
            "RUN_ID": str(benchmark_id),
            "TASK_ID": task_row.task_id,
            **_attested_inference_settings(start_benchmark_request.contract),
            "IDENTITY": json.dumps(snapshot.identity),
            # Tags sandbox-internal OTel telemetry with our IDs + environment so traces/logs/metrics
            # are filterable per benchmark run and separable from other environments sharing the
            # same Daytona account (sandbox OTLP export is account-level).
            "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS": (
                f"benchmark_id={benchmark_id},task_id={task_row.task_id},environment={ENVIRONMENT}"
            ),
            **recovery_attempt.environment,
        }

        # We don't want to track the task until the sandbox is actually created.
        task_breakdown = TaskBreakdown()

        start_sandbox_build_time = time.perf_counter()
        object_store = runtime.objects

        def sandbox_context() -> AbstractAsyncContextManager[Sandbox]:
            nonlocal start_sandbox_build_time
            start_sandbox_build_time = time.perf_counter()
            sandbox_name = (
                task_row.task_id
                if queue_context is None
                else f"queued-{task_row.id.hex}-{int(_normalized_attempt_time(attempt_started_at).replace(tzinfo=UTC).timestamp() * 1_000_000):x}"
            )
            return create_sandbox(
                provider=sandbox_provider,
                sandbox_name=sandbox_name,
                source=task_data.source,
                labels=labels,
                env_vars=env_vars,
                sandbox_secrets=task_data.sandbox_secrets,
                resources=task_data.resources,
                volumes=task_data.volumes,
                creation_semaphore=creation_semaphore,
                unique_name=queue_context is None,
            )

        async with AsyncExitStack() as sandbox_stack:
            if queue_context is None:
                sandbox = await sandbox_stack.enter_async_context(sandbox_context())
            else:
                sandbox = await queue_context.enter(
                    stack=sandbox_stack,
                    persistence=persistence,
                    source=task_data.source,
                    resources=task_data.resources,
                    create=sandbox_context,
                )
                if sandbox is None:
                    return {task_id: None}
            sandbox_id_for_recovery = sandbox.id
            task_breakdown.sandbox_build_duration = time.perf_counter() - start_sandbox_build_time
            start_sandbox_run_time = time.perf_counter()

            try:
                if queue_context is None:
                    if not await persistence.write(RunTask()):
                        return {task_id: None}

                # Upload the contract to the sandbox after creating and install the dependencies
                await upload_agent_artifacts(
                    sandbox,
                    start_benchmark_request.contract,
                    str(benchmark_id),
                    object_store,
                )

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

                # Force flush the logs if anything has been buffered
                task_logs.buffer_logs(force_flush=True)

                # Compute the S3 key for the agent's output archive
                agent_output_s3_key = None
                if start_benchmark_request.contract.final_output:
                    agent_output_s3_key = task_artifact_key(str(benchmark_id), task_id, "agent_output.tar.gz")

                try:
                    exit_reason, agent_run_time = await run_agent(
                        sandbox,
                        start_benchmark_request.contract,
                        task_data.problem_path,
                        task_id,
                        log_output,
                        task_data.cwd,
                        object_store=object_store,
                        agent_output_s3_key=agent_output_s3_key,
                        agent_timeout=task_data.agent_timeout,
                        benchmark_id=str(benchmark_id),
                        runtime_source=task_data.source,
                        dependency_setup_mode=dependency_setup_recovery.mode,
                        execution_is_current=persistence.current,
                    )
                except DependencySetupExhaustedError:
                    dependency_setup_recovery.mode = DependencySetupMode.FINAL_FRESH_SANDBOX
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
                if not await persistence.write(
                    EvaluateTask(
                        sandbox_build_duration=task_breakdown.sandbox_build_duration,
                        agent_run_duration=task_breakdown.agent_run_duration,
                    )
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
                task_logs.last_log_time = time.monotonic()
                evaluation_result = await _run_benchmark_service_websocket(
                    run_with_checkpoints(
                        lambda checkpoint: benchmark_service.evaluate_instance(
                            task_row.task_id,
                            sandbox.id,
                            on_message=log_output,
                            on_eval_resume_state=checkpoint,
                            dataset=start_benchmark_request.dataset,
                            sandbox_provider=sandbox_provider_config,
                        ),
                        on_eval_resume_state,
                    )
                )
                task_breakdown.evaluation_run_duration = time.perf_counter() - evaluation_start_time

                assert start_sandbox_run_time is not None
                task_breakdown.sandbox_run_duration = time.perf_counter() - start_sandbox_run_time

                # Force flush the logs, maybe redundant since we have the one in finally:
                task_logs.buffer_logs(force_flush=True)

                evaluation_result_value = cast(dict[str, Any], evaluation_result)
                if not await persistence.write(
                    CompleteTask(
                        instance_id=sandbox.id,
                        result=evaluation_result_value,
                        exit_reason=exit_reason.value if exit_reason else None,
                        evaluation_run_duration=task_breakdown.evaluation_run_duration,
                        sandbox_run_duration=task_breakdown.sandbox_run_duration,
                    )
                ):
                    return {task_id: None}

                return {task_id: evaluation_result_value}
            except Exception:
                if await task_is_stopped():
                    return {task_id: None}

                raise

    except SandboxSetupError as e:
        if await task_is_stopped():
            return {task_id: None}
        if not await return_queued_task_to_pending():
            return {task_id: None}
        log_output(f"\n[ERROR] {_exception_message(e)}")
        raise
    except SandboxNotFoundError as e:
        if await task_is_stopped():
            return {task_id: None}
        if recovery_attempt.sandbox_loss_retry_available:
            message = (
                "Sandbox disappeared; restoring the task from its durable volume "
                f"(attempt {recovery_attempt.number + 1}/{recovery_attempt.max_attempts})"
            )
            logger.warning(message)
            log_output(f"\n[WARNING] {message}\n")
            raise

        error_message = _exception_message(e)
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="sandbox_provider",
            operation="sandbox_recovery",
        )
    except OutputArtifactError as e:
        if await task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        logger.warning(error_message)
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="output_artifact",
            operation="upload_output_artifacts",
        )
    except BenchmarkServiceWebSocketDNSResolutionError as e:
        if await task_is_stopped():
            return {task_id: None}
        error_message = f"Benchmark service WebSocket connection failed during DNS resolution: {_exception_message(e)}"
        logger.warning(error_message)
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="benchmark_service",
            operation="websocket_connect",
            cause_code="websocket_dns_resolution",
        )
    except ConnectionClosedError as e:
        if await task_is_stopped():
            return {task_id: None}
        seconds = int(time.monotonic() - task_logs.last_log_time)
        error_message = (
            f"Benchmark service WebSocket disconnected: {e}; last application message received {seconds}s ago"
        )
        recovered = await recover_evaluation_stream_failure(error_message)
        if recovered is not None:
            return recovered
        logger.warning(error_message)
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="benchmark_service",
            operation="websocket",
            cause_code="websocket_connection_closed",
        )
    except BenchmarkServiceStreamError as e:
        if await task_is_stopped():
            return {task_id: None}
        error_message = f"Benchmark service WebSocket stream failed: {_exception_message(e)}"
        recovered = await recover_evaluation_stream_failure(error_message)
        if recovered is not None:
            return recovered
        logger.warning(error_message)
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="benchmark_service",
            operation="websocket",
            cause_code="websocket_connection_closed",
        )
    except ValidationError as e:
        if await task_is_stopped():
            return {task_id: None}
        field_names = ", ".join(".".join(str(loc) for loc in err["loc"]) for err in e.errors())
        error_message = (
            f"Benchmark service returned an incompatible task response. Missing or invalid fields: {field_names}"
        )
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="benchmark_service",
            operation="decode_task_response",
            cause_code="incompatible_response",
        )
    except InvalidStatus as e:
        if await task_is_stopped():
            return {task_id: None}
        error_message = f"Benchmark service rejected the WebSocket connection (HTTP {e.response.status_code})"
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="benchmark_service",
            operation="websocket_connect",
            cause_code="websocket_http_rejected",
        )
    except BenchmarkServiceError as e:
        if await task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        # This is necessary because Daytona routes tasks to bad nodes. We should
        # remove this when Daytona fixes their infrastructure.
        if "docker daemon is not ready inside the sandbox" in error_message:
            if not await return_queued_task_to_pending():
                return {task_id: None}
            log_output(f"\n[ERROR] {error_message}")
            raise SandboxSetupError(error_message) from e
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="benchmark_service",
            operation="request",
        )
    except Exception as e:
        if await task_is_stopped():
            return {task_id: None}
        logfire.exception("process_task failed")
        error_message = _exception_message(e)

        # include the error message
        log_output(f"\n[ERROR] {error_message}")

        return await commit_terminal_error(
            e,
            error_message,
            producer="tracker",
            operation="process_task",
        )
    finally:
        await persistence.close()
