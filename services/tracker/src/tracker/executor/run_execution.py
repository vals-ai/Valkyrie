"""Run tasks, monitor attempts, and finalize through the packaged version-one client."""

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any
from uuid import UUID, uuid4

import httpx
from benchmark_service import SandboxProvider, SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient

from tracker.database.models import BenchmarkStatus, Org, Task
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.queue_execution import ApiSandboxQueueContext
from tracker.executor.task_persistence import ApiTaskPersistence, attempt_time
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.finalization_schemas import CompleteRun, FailRun, Finalization, StopRun
from tracker.executor_api.v1.schemas import RunInfo, RunStateResponse, RunStatus, TaskState, TaskStatus
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.runtime.services import RuntimeServices
from tracker.scheduler.store import queue_pool_id
from tracker.types import FinalViewResponse, StartBenchmarkRequest
from tracker.utils.reporting import upload_final_view
from tracker.utils.task_error_summary import summarize_task_errors
from tracker.utils.task_execution import ResizableLimiter, process_task

logger = logging.getLogger(__name__)
_TERMINAL_TASK_STATUSES = (TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED)
_STATE_BATCH_SIZE = 1000


def _notification_context(state: RunStateResponse) -> NotificationContext:
    return NotificationContext(
        benchmark_name=state.run.benchmark_name,
        agent_name=state.run.agent_name,
        benchmark_id=state.run.benchmark_id,
        started_at=state.run.started_at,
        total_tasks=sum(state.task_counts.values()),
        finished_tasks=sum(state.task_counts.get(status, 0) for status in _TERMINAL_TASK_STATUSES),
        model=state.run.model,
    )


async def _run_tasks(
    api: ExecutorClient,
    initial: RunStateResponse,
    request: StartBenchmarkRequest,
    runtime: RuntimeServices,
    benchmark_service: BenchmarkServiceClient,
    sandbox_provider_config: SandboxProviderConfig,
    sandbox_provider: SandboxProvider,
    dispatch_id: UUID,
    notifier: SlackNotifier | None,
) -> None:
    run = initial.run
    queue_context = None
    if run.queue_pool_id is not None:
        provider_pool_id = sandbox_provider.admission_pool_id
        if provider_pool_id is None or queue_pool_id(provider_pool_id) != run.queue_pool_id:
            raise TrackerServiceError("Configured sandbox provider does not match the run's queued provider pool")
        queue_context = ApiSandboxQueueContext(sandbox_provider)
    limiter = ResizableLimiter(run.concurrency)
    creation_semaphore = asyncio.Semaphore(10)
    org = Org(id=run.org_id, name=run.org_name)
    authority = ExecutionAuthority(benchmark_id=run.benchmark_id, dispatch_id=dispatch_id)
    active: dict[str, tuple[TaskState, asyncio.Task[None]]] = {}
    cancelled: set[str] = set()

    async def execute(task: TaskState) -> None:
        async def process() -> dict[str, dict[str, Any] | None]:
            task_row = Task(
                id=task.id,
                task_id=task.task_id,
                benchmark=run.benchmark_id,
                org_id=run.org_id,
                started_at=task.started_at,
            )

            return await process_task(
                task_row,
                request,
                benchmark_service,
                run.benchmark_id,
                task.task_id,
                runtime,
                org,
                sandbox_provider_config,
                creation_semaphore,
                authority,
                sandbox_provider=sandbox_provider,
                queue_context=queue_context,
                persistence=ApiTaskPersistence(api, task),
            )

        # The queue API applies the current global ordering and per-run concurrency limit.
        if queue_context is not None or task.status == TaskStatus.EVALUATING:
            await process()
        else:
            async with limiter:
                await process()

    async with asyncio.TaskGroup() as group:
        for task in initial.tasks:
            if task.status not in _TERMINAL_TASK_STATUSES:
                active[task.task_id] = (task, group.create_task(execute(task)))

        while active:
            task_ids = list(active)
            for offset in range(0, len(task_ids), _STATE_BATCH_SIZE):
                state = await api.run_state(task_ids[offset : offset + _STATE_BATCH_SIZE])
                if not state.current:
                    raise ExecutionAuthorityRevoked("Tracker revoked this executor dispatch")
                await limiter.resize(state.run.concurrency)
                for task in state.tasks:
                    original, work = active[task.task_id]
                    if work.done():
                        del active[task.task_id]
                        continue
                    invalid = (
                        task.id != original.id
                        or attempt_time(task.started_at) != attempt_time(original.started_at)
                        or task.status == TaskStatus.STOPPED
                        or state.run.status in (RunStatus.ERROR, RunStatus.STOPPED)
                    )
                    if invalid and task.task_id not in cancelled:
                        cancelled.add(task.task_id)
                        work.cancel()
                if notifier is not None:
                    await notifier.check_and_notify(_notification_context(state))
            if active:
                await asyncio.sleep(2)


async def _finalize_run(
    api: ExecutorClient,
    request: StartBenchmarkRequest,
    runtime: RuntimeServices,
    benchmark_service: BenchmarkServiceClient,
    notifier: SlackNotifier | None,
) -> None:
    snapshot = await api.finalization_state()
    if not snapshot.current or snapshot.snapshot_digest is None:
        return
    finalization: Finalization
    if snapshot.operation == "complete":
        score = await benchmark_service.final_score(
            evaluation_results=snapshot.evaluation_results, dataset=request.dataset
        )
        finalization = CompleteRun(final_score=score.final_score, metadata=score.metadata)
    elif snapshot.operation == "fail":
        summary = await asyncio.to_thread(summarize_task_errors, snapshot.task_errors)
        finalization = FailRun(error_message=summary)
    else:
        finalization = StopRun()

    command_id = uuid4()
    try:
        completed = await api.finalize_run(snapshot.snapshot_digest, finalization, command_id=command_id)
    except httpx.HTTPStatusError as error:
        if error.response.status_code != 409:
            raise
        # A retry or sibling finalizer changed the snapshot while the service calculated the score.
        return

    if notifier is not None:
        state = await api.run_state([])
        await notifier.send_terminal_notification(
            _notification_context(state),
            BenchmarkStatus(completed.status),
            final_score=finalization.final_score if isinstance(finalization, CompleteRun) else None,
            error_message=finalization.error_message if isinstance(finalization, FailRun) else None,
        )
    if (
        not isinstance(finalization, CompleteRun)
        or completed.status == "STOPPED"
        or not (await api.authority()).current
    ):
        return
    report = await api.run_report(command_id)
    final_view = FinalViewResponse.model_validate(report.report.model_dump(mode="json"))
    await upload_final_view(final_view, runtime.objects)
    if (await api.authority()).current:
        await runtime.run_completion_callback(final_view)


async def process_benchmark_v1(
    api: ExecutorClient,
    request: StartBenchmarkRequest,
    runtime: RuntimeServices,
    verified_task_ids: list[str],
    dispatch_id: UUID,
    *,
    run: RunInfo,
) -> None:
    """Execute one assigned batch; the entrypoint retains the dispatch lease around this operation."""
    tasks: list[TaskState] = []
    initial: RunStateResponse | None = None
    for offset in range(0, len(verified_task_ids), _STATE_BATCH_SIZE):
        initial = await api.initialize_run_tasks(verified_task_ids[offset : offset + _STATE_BATCH_SIZE])
        if not initial.current:
            raise ExecutionAuthorityRevoked("Tracker revoked this executor dispatch")
        tasks.extend(initial.tasks)
    if initial is None:
        initial = await api.run_state([])
    if initial.run.benchmark_id != run.benchmark_id:
        raise TrackerServiceError("Executor dispatch returned a different run")
    initial = initial.model_copy(update={"tasks": tasks})
    notifier = None
    if request.webhook_secret_name and request.webhook_intervals:
        notifier = SlackNotifier(
            secret_name=request.webhook_secret_name,
            secret_store=runtime.secrets,
            intervals=request.webhook_intervals,
        )

    async with AsyncExitStack() as stack:
        benchmark_service = await stack.enter_async_context(request.benchmark_service)
        sandbox_provider_config = await runtime.get_sandbox_provider_config()
        sandbox_provider = await stack.enter_async_context(runtime.get_sandbox_provider(sandbox_provider_config))
        await _run_tasks(
            api,
            initial,
            request,
            runtime,
            benchmark_service,
            sandbox_provider_config,
            sandbox_provider,
            dispatch_id,
            notifier,
        )
        await _finalize_run(api, request, runtime, benchmark_service, notifier)
