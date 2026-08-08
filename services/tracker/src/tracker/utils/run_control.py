"""Operations that stop, resume, or retry a run and tear down its sandboxes."""

import traceback
from collections.abc import AsyncGenerator

from benchmark_service import (
    Sandbox,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from sqlmodel import Session

from tracker.database.models import Benchmark, Org, RetryMode
from tracker.database.repositories import BenchmarkRepository, RunControlRepository
from tracker.executor.dispatch_control import terminalize_active_dispatches
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.sandbox import delete_sandbox
from tracker.types import (
    AWSCredentials,
)

from tracker.utils.resources import fetch_benchmark_row, fetch_sandbox_provider_config

logger = get_logger(__name__)


async def initiate_stop_benchmark(
    benchmark_row: Benchmark,
    session: Session,
    force: bool,
    org: Org,
    task_ids: list[str] | None = None,
    *,
    repository: RunControlRepository,
) -> None:
    """Initiate Stop without interrupting work that already started unless forced."""
    try:
        repository.apply_stop(
            benchmark_row,
            org.id,
            force=force,
            task_ids=task_ids,
        )
        session.commit()
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error stopping run {benchmark_row.id}: {str(e)}") from e


async def stop_sandbox(sandbox: Sandbox, provider: SandboxProvider) -> str | None:
    try:
        await delete_sandbox(sandbox, provider)
        return None
    except SandboxNotFoundError:
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
        return None
    except Exception as e:
        return f"{str(e)}: {traceback.format_exc()}"


async def sandbox_generator(
    benchmark_row: Benchmark,
    provider: SandboxProvider,
    task_ids: list[str] | None = None,
) -> AsyncGenerator[Sandbox, None]:
    """
    Generator that yields all sandboxes for a given benchmark.
    """
    labels = {"Benchmark": benchmark_row.name, "Id": str(benchmark_row.id)}
    queries = (
        [SandboxQuery(labels={**labels, "Task": task_id}) for task_id in task_ids]
        if task_ids
        else [SandboxQuery(labels=labels)]
    )
    seen_sandbox_ids: set[str] = set()
    for query in queries:
        async for sandbox in provider.list_sandboxes(query):
            if sandbox.id in seen_sandbox_ids:
                continue
            seen_sandbox_ids.add(sandbox.id)
            yield sandbox


async def force_stop_sandboxes(
    benchmark_row: Benchmark,
    session: Session,
    sandbox_provider_secret_name: str,
    aws: AWSCredentials,
    org: Org,
    sandbox_provider: str = "daytona",
    task_ids: list[str] | None = None,
    *,
    repository: RunControlRepository,
    benchmark_repository: BenchmarkRepository,
) -> None:
    """
    Stops and deletes all sandboxes which are in progress or evaluating.
    NOTE: If task is not in progress but sandbox exists, we kill it and leave the task status as is.

    Raises:
        TrackerServiceError: If there are any errors stopping the sandboxes
    """
    # Persist active-task STOPPED statuses before touching provider sandboxes.
    repository.stop_active_tasks(benchmark_row.id, org.id, task_ids=task_ids)
    session.commit()

    # Construct the service only after the stop phase is committed. Provider setup
    # belongs in the cleanup try/finally so the service closes on setup failures too.
    benchmark_service = benchmark_row.benchmark_service()
    results: dict[str, str | None] = {}
    try:
        provider = benchmark_service.get_sandbox_provider(
            fetch_sandbox_provider_config(sandbox_provider_secret_name, aws, sandbox_provider)
        )
        async for sandbox in sandbox_generator(benchmark_row, provider, task_ids=task_ids):
            result = await stop_sandbox(sandbox, provider)
            results[sandbox.name] = result
    finally:
        await benchmark_service.close()

    error_message: str = "\n".join(
        f"{task_alias}: {error_message}" for task_alias, error_message in results.items() if error_message
    )

    # Sandbox teardown releases the request's original benchmark lock. Reacquire
    # it so Retry admission cannot land between the runnable check and revocation.
    locked_benchmark = repository.lock_benchmark(benchmark_row.id, org.id)
    if locked_benchmark is None:
        # Preserve the legacy distinction between a missing run and a wrong-org run.
        locked_benchmark = fetch_benchmark_row(benchmark_row.id, benchmark_repository, org, for_update=True)
    if repository.count_nonterminal_tasks(locked_benchmark.id, org.id) == 0:
        repository.mark_stopped(locked_benchmark)
        terminalize_active_dispatches(session, locked_benchmark.id)
    session.commit()

    if error_message:
        raise TrackerServiceError(f"Unexpected errors stopping sandboxes:\n{error_message}")


async def reset_to_in_progress_status(
    benchmark_row: Benchmark,
    benchmark_service: BenchmarkServiceClient,
    retry: bool,
    retry_mode: RetryMode,
    rerun_task_ids: list[str],
    org: Org,
    *,
    repository: RunControlRepository,
    benchmark_repository: BenchmarkRepository,
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
        # Serialize retries with final-score persistence for this benchmark and keep
        # the row lock while the service verifies IDs and the caller admits dispatch.
        locked_benchmark = repository.lock_benchmark(benchmark_row.id, org.id)
        if locked_benchmark is None:
            # Preserve the legacy distinction between a missing run and a wrong-org run.
            locked_benchmark = fetch_benchmark_row(benchmark_row.id, benchmark_repository, org, for_update=True)

        selection = repository.select_retryable(
            locked_benchmark,
            org.id,
            retry=retry,
            rerun_task_ids=rerun_task_ids,
        )

        # Allow re-running the end of the benchmark without running any tasks.
        if not selection.existing_tasks and not selection.new_task_ids:
            return []

        # Verify the task ids are still valid before priming to resume.
        verify_response = await benchmark_service.verify_task_ids(
            task_ids=[task.task_id for task in selection.existing_tasks] + selection.new_task_ids,
            slice_str=None,
            dataset=locked_benchmark.arguments.dataset,
        )
        repository.apply_retry(
            selection,
            org.id,
            retry_mode=retry_mode,
        )
        return verify_response.task_ids
    except (TrackerServiceError, BenchmarkServiceError):
        raise
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error resuming run {benchmark_row.id}: {str(e)}") from e
