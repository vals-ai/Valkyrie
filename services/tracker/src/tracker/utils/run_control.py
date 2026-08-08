"""Operations that stop, resume, or retry a run and tear down its sandboxes."""

import traceback
from collections.abc import AsyncGenerator, Callable

from benchmark_service import (
    Sandbox,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, Org, RetryMode
from tracker.database.repositories import BenchmarkRepository, ExecutorControlRepository, RunControlRepository
from tracker.database.transaction import TrackerTransaction
from tracker.exceptions import ReleaseControlError, TrackerServiceError
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
    executor_control_repository: ExecutorControlRepository,
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
    # it in a fresh transaction so Retry admission cannot land between the runnable
    # check and revocation.
    with Session(bind=session.bind) as post_session:
        transaction = TrackerTransaction.from_session(post_session)
        locked_benchmark = transaction.run_control.lock_benchmark(benchmark_row.id, org.id)
        if locked_benchmark is None:
            # Preserve the legacy distinction between a missing run and a wrong-org run.
            locked_benchmark = fetch_benchmark_row(benchmark_row.id, transaction.benchmarks, org, for_update=True)
        if transaction.run_control.count_nonterminal_tasks(locked_benchmark.id, org.id) == 0:
            transaction.run_control.mark_stopped(locked_benchmark)
            transaction.executor_control.terminalize_active_dispatches(locked_benchmark.id)
        post_session.commit()

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
    executor_control_repository: ExecutorControlRepository | None = None,
    phase_status: list[BenchmarkStatus] | None = None,
    phase_session_factory: Callable[[], Session] | None = None,
    phase_session_commit: Callable[[Session], None] | None = None,
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
    session = repository.session
    try:
        # Phase one serializes selection with final-score persistence, then releases
        # the lock before contacting the benchmark service.
        if executor_control_repository is not None:
            executor_control_repository.lock_executor_admission()
        locked_benchmark = repository.lock_benchmark(benchmark_row.id, org.id)
        if locked_benchmark is None:
            # Preserve the legacy distinction between a missing run and a wrong-org run.
            locked_benchmark = fetch_benchmark_row(benchmark_row.id, benchmark_repository, org, for_update=True)

        if phase_status is not None:
            phase_status[:] = [locked_benchmark.status]
        if locked_benchmark.status == BenchmarkStatus.STOPPING:
            session.rollback()
            raise ReleaseControlError(
                f"Run {benchmark_row.id} is in the {locked_benchmark.status} state. Cannot continue a run that is stopping."
            )
        selection = repository.select_retryable(
            locked_benchmark,
            org.id,
            retry=retry,
            rerun_task_ids=rerun_task_ids,
        )
        selected_task_ids = [task.task_id for task in selection.existing_tasks] + selection.new_task_ids
        dataset = locked_benchmark.arguments.dataset

        # Allow re-running the end of the benchmark without running any tasks.
        if not selected_task_ids:
            session.rollback()
            return []
        session.rollback()

        # Verify task IDs outside the database transaction.
        verify_response = await benchmark_service.verify_task_ids(
            task_ids=selected_task_ids,
            slice_str=None,
            dataset=dataset,
        )

        # Phase two reacquires the locks and reselects the rows. If another writer
        # changed the selection while verification ran, refuse to apply stale results.
        phase_session = phase_session_factory() if phase_session_factory is not None else session
        try:
            transaction = TrackerTransaction.from_session(phase_session)
            if executor_control_repository is not None:
                transaction.executor_control.lock_executor_admission()
            locked_benchmark = transaction.run_control.lock_benchmark(benchmark_row.id, org.id)
            if locked_benchmark is None:
                locked_benchmark = fetch_benchmark_row(benchmark_row.id, transaction.benchmarks, org, for_update=True)
            benchmark_row.status = locked_benchmark.status
            if phase_status is not None:
                phase_status[:] = [locked_benchmark.status]
            if locked_benchmark.status == BenchmarkStatus.STOPPING:
                phase_session.rollback()
                raise ReleaseControlError(
                    f"Run {benchmark_row.id} is in the {locked_benchmark.status} state. Cannot continue a run that is stopping."
                )
            current_selection = transaction.run_control.select_retryable(
                locked_benchmark,
                org.id,
                retry=retry,
                rerun_task_ids=rerun_task_ids,
            )
            current_task_ids = [
                task.task_id for task in current_selection.existing_tasks
            ] + current_selection.new_task_ids
            if current_task_ids != selected_task_ids:
                phase_session.rollback()
                raise TrackerServiceError("Run changed while task IDs were being verified; please retry")
            transaction.run_control.apply_retry(
                current_selection,
                org.id,
                retry_mode=retry_mode,
            )
            if phase_session_commit is not None:
                phase_session_commit(phase_session)
            return verify_response.task_ids
        except Exception:
            phase_session.rollback()
            raise
        finally:
            if phase_session is not session:
                phase_session.close()
    except (TrackerServiceError, BenchmarkServiceError, ReleaseControlError):
        raise
    except Exception as e:
        session.rollback()
        raise TrackerServiceError(f"Unexpected error resuming run {benchmark_row.id}: {str(e)}") from e
