"""Run with `uv run pytest tests/integration/live/sandbox/test_force_stop.py`.

Exercise force-stop behavior against real sandbox infrastructure.
"""

import asyncio
from typing import Any, Optional

import pytest
from benchmark_service import ImageSource, Resources, Sandbox, SandboxNotFoundError, SandboxProvider, SandboxQuery
from benchmark_service.client import BenchmarkServiceClient
from fastapi.testclient import TestClient
from sqlmodel import Session, col, select

import tracker.utils as tracker_utils
import tracker.utils.run_control as run_control
from tests.utils import TEST_ORG_ID, random_task_id
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.logging import get_logger
from tracker.sandbox import create_sandbox
from tracker.types import HarnessConfig
from tracker.utils import fetch_sandbox_provider_config, force_stop_sandboxes

process_benchmark = getattr(tracker_utils, "process_benchmark")

logger = get_logger(__name__)

pytestmark = pytest.mark.usefixtures("tracker_database")

_ACTIVE_TASK_STATUSES = [TaskStatus.BUILDING, TaskStatus.PENDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
_TERMINAL_TASK_STATUSES = [TaskStatus.STOPPED, TaskStatus.FINISHED]


async def _sandboxes_for_benchmark(benchmark: Benchmark, provider: SandboxProvider) -> list[Sandbox]:
    query = SandboxQuery(labels={"Benchmark": benchmark.name, "Id": str(benchmark.id)})
    return [sandbox async for sandbox in provider.list_sandboxes(query)]


async def _wait_for_running_benchmark(
    benchmark: Benchmark,
    database_session: Session,
    provider: SandboxProvider,
) -> None:
    for _ in range(60):
        database_session.expire_all()
        task_statuses = database_session.exec(select(Task.status).where(Task.benchmark == benchmark.id)).all()
        sandboxes = await _sandboxes_for_benchmark(benchmark, provider)

        if TaskStatus.IN_PROGRESS in task_statuses and sandboxes:
            return
        if task_statuses and all(
            status in _TERMINAL_TASK_STATUSES or status == TaskStatus.ERROR for status in task_statuses
        ):
            pytest.fail("Benchmark finished before force stop could interrupt a running sandbox")

        await asyncio.sleep(2)

    pytest.fail("Benchmark did not start running before force stop timeout")


async def _wait_until_no_sandboxes(benchmark: Benchmark, provider: SandboxProvider) -> None:
    sandboxes = []
    for _ in range(30):
        sandboxes = await _sandboxes_for_benchmark(benchmark, provider)
        if not sandboxes:
            return
        await asyncio.sleep(2)

    remaining = ", ".join(f"{sandbox.name} ({sandbox.state})" for sandbox in sandboxes)
    pytest.fail(f"Sandboxes still existed after force stop: {remaining}")


async def _wait_for_sandbox_setup(
    all_sandboxes_created: asyncio.Event,
    sandbox_creation_failed: asyncio.Event,
) -> None:
    """Wait until every sandbox is ready, failing promptly if creation fails."""
    ready_waiter = asyncio.create_task(all_sandboxes_created.wait())
    failure_waiter = asyncio.create_task(sandbox_creation_failed.wait())
    waiters = {ready_waiter, failure_waiter}

    try:
        completed, _ = await asyncio.wait(waiters, timeout=120, return_when=asyncio.FIRST_COMPLETED)
        if not completed:
            pytest.fail("Sandboxes were not created before the force-stop timeout")
        if failure_waiter in completed:
            pytest.fail("A sandbox failed during force-stop test setup")
    finally:
        for waiter in waiters:
            if not waiter.done():
                waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


def _assert_no_task_errors(benchmark: Benchmark, database_session: Session) -> None:
    database_session.expire_all()
    assert benchmark.fetch_tasks_with_errors(database_session) is None


class TestForceStop:
    """Live graceful and forced sandbox stop flows."""

    async def test_force_stop_active_sandbox(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        benchmark_service: BenchmarkServiceClient,
        test_resources: Resources,
        daytona_secret_name: str,
        harness_config: HarnessConfig,
        test_image: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify force_stop_sandboxes stops a sandbox whose context is still active.

        Test cases:
        - A task already marked in progress is moved to STOPPED while its sandbox context is active.
        - The created sandbox is deleted and provider lookup raises SandboxNotFoundError.
        """
        database_session.add(example_benchmark_object)
        database_session.commit()

        task = Task(
            org_id=TEST_ORG_ID,
            benchmark=example_benchmark_object.id,
            task_id=random_task_id(),
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(task)
        database_session.commit()

        aws_runtime = AWSRuntime.from_harness_config(harness_config)
        provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_runtime.clients, "daytona")
        provider = benchmark_service.get_sandbox_provider(provider_config)
        labels = {
            "Benchmark": example_benchmark_object.name,
            "Id": str(example_benchmark_object.id),
            "Task": task.task_id,
        }

        sandbox_created = asyncio.Event()
        release_sandbox = asyncio.Event()

        async def force_stop_sandbox() -> None:
            await asyncio.wait_for(sandbox_created.wait(), timeout=120)

            try:
                await force_stop_sandboxes(
                    example_benchmark_object,
                    database_session,
                    daytona_secret_name,
                    aws_runtime,
                    Org(id=TEST_ORG_ID, name="default"),
                    sandbox_provider="daytona",
                )
            finally:
                release_sandbox.set()

        created_sandbox_name: list[str] = []

        async def create_sandbox_until_stopped() -> None:
            async with create_sandbox(
                provider,
                task.task_id,
                ImageSource(image=test_image),
                resources=test_resources,
                creation_semaphore=creation_semaphore,
                labels=labels,
            ) as sandbox:
                created_sandbox_name.append(sandbox.name)
                sandbox_created.set()
                await release_sandbox.wait()

        sandbox_tasks = [
            asyncio.create_task(create_sandbox_until_stopped()),
            asyncio.create_task(force_stop_sandbox()),
        ]
        try:
            await asyncio.gather(*sandbox_tasks)
        finally:
            for sandbox_task in sandbox_tasks:
                if not sandbox_task.done():
                    sandbox_task.cancel()
            await asyncio.gather(*sandbox_tasks, return_exceptions=True)

        persisted_task = database_session.exec(select(Task).where(Task.id == task.id)).one()
        assert persisted_task.status == TaskStatus.STOPPED

        with pytest.raises(SandboxNotFoundError):
            await provider.get_sandbox(created_sandbox_name[0])

    async def test_force_stop_sandboxes(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        harness_config: HarnessConfig,
        daytona_secret_name: str,
        benchmark_service: BenchmarkServiceClient,
        test_image: str,
        test_resources: Resources,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify force_stop_sandboxes handles multiple active sandbox contexts.

        Test cases:
        - In-progress and evaluating task sandboxes are stopped while their contexts are still open.
        - Deleted sandbox races surface only SandboxNotFoundError and no task error messages are recorded.
        - No benchmark-labeled sandboxes remain after force stop completes.
        """
        database_session.add(example_benchmark_object)
        database_session.commit()

        aws_runtime = AWSRuntime.from_harness_config(harness_config)
        provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_runtime.clients, "daytona")
        provider = benchmark_service.get_sandbox_provider(provider_config)

        labels = {"Benchmark": example_benchmark_object.name, "Id": str(example_benchmark_object.id)}
        release_sandboxes = asyncio.Event()
        all_sandboxes_created = asyncio.Event()
        sandbox_creation_failed = asyncio.Event()
        created_sandbox_count = 0

        async def create_sandbox_with_delay(sandbox_name: str) -> None:
            """Keep a created sandbox open until force-stop completes."""
            nonlocal created_sandbox_count

            try:
                async with create_sandbox(
                    provider=provider,
                    sandbox_name=sandbox_name,
                    source=ImageSource(image=test_image),
                    resources=test_resources,
                    creation_semaphore=creation_semaphore,
                    labels=labels,
                ) as sandbox:
                    result = await sandbox.exec("true")
                    assert result.exit_code == 0
                    created_sandbox_count += 1
                    if created_sandbox_count == 12:
                        all_sandboxes_created.set()

                    await release_sandboxes.wait()
            except Exception:
                sandbox_creation_failed.set()
                raise

        tasks: list[Task] = []
        for task_index in range(12):
            status = TaskStatus.IN_PROGRESS if task_index < 6 else TaskStatus.EVALUATING
            task = Task(
                org_id=TEST_ORG_ID, benchmark=example_benchmark_object.id, task_id=random_task_id(), status=status
            )
            tasks.append(task)
            database_session.add(task)

        database_session.commit()

        sandbox_tasks: list[asyncio.Task[None]] = []
        created_results: list[Optional[BaseException]] = []
        try:
            sandbox_tasks = [asyncio.create_task(create_sandbox_with_delay(task.task_id)) for task in tasks]
            await _wait_for_sandbox_setup(all_sandboxes_created, sandbox_creation_failed)
            await force_stop_sandboxes(
                example_benchmark_object,
                database_session,
                daytona_secret_name,
                aws_runtime,
                Org(id=TEST_ORG_ID, name="default"),
                sandbox_provider="daytona",
            )
        finally:
            release_sandboxes.set()
            if sandbox_tasks:
                created_results = await asyncio.wait_for(
                    asyncio.gather(*sandbox_tasks, return_exceptions=True),
                    timeout=30,
                )

        unexpected_errors = [
            result
            for result in created_results
            if isinstance(result, Exception) and not isinstance(result, SandboxNotFoundError)
        ]
        assert unexpected_errors == []

        _assert_no_task_errors(example_benchmark_object, database_session)

        await _wait_until_no_sandboxes(example_benchmark_object, provider)

    async def test_force_stop_waits_for_the_executor_to_release_its_sandboxes(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        daytona_secret_name: str,
        harness_config: HarnessConfig,
        service_headers: dict[str, str],
        executor_authority_kwargs: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify force stop lets a live executor tear down its own sandboxes instead of racing it.

        One task, so the only sandbox in play belongs to a task that has finished building. A task
        still inside `create_sandbox` holds a shielded creation that cannot be released until it
        completes, which is the case the drain window deliberately does not wait for.

        Test cases:
        - Force stop runs while the executor is still working and deletes no sandbox itself.
        - The run still reaches STOPPED with no task errors and no sandboxes left behind.
        """
        example_benchmark_object.arguments.slice_str = ":1"
        example_benchmark_object.arguments.concurrency = 1
        database_session.add(example_benchmark_object)
        database_session.commit()
        aws_runtime = AWSRuntime.from_harness_config(harness_config)

        benchmark_service = example_benchmark_object.benchmark_service(service_headers=service_headers)
        benchmark_task: Optional[asyncio.Task[None]] = None
        provider: Optional[SandboxProvider] = None
        reaped: list[str] = []
        release_sandbox = run_control.stop_sandbox

        async def record_reaped_sandbox(sandbox: Sandbox, sandbox_provider: SandboxProvider, org: Org) -> str | None:
            reaped.append(sandbox.name)
            return await release_sandbox(sandbox, sandbox_provider, org)

        monkeypatch.setattr(run_control, "stop_sandbox", record_reaped_sandbox)

        try:
            verify_response = await benchmark_service.verify_task_ids(
                task_ids=example_benchmark_object.arguments.task_ids,
                slice_str=example_benchmark_object.arguments.slice_str,
            )

            authority_kwargs = executor_authority_kwargs(example_benchmark_object)
            benchmark_task = asyncio.create_task(
                process_benchmark(
                    start_benchmark_request_json=example_benchmark_object.start_benchmark_request(
                        harness_config, service_headers=service_headers
                    ).model_dump(),
                    benchmark_id_str=str(example_benchmark_object.id),
                    verified_task_ids=verify_response.task_ids,
                    **authority_kwargs,
                )
            )

            provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_runtime.clients, "daytona")
            provider = benchmark_service.get_sandbox_provider(provider_config)
            await _wait_for_running_benchmark(example_benchmark_object, database_session, provider)

            await force_stop_sandboxes(
                example_benchmark_object,
                database_session,
                daytona_secret_name,
                aws_runtime,
                Org(id=TEST_ORG_ID, name="default"),
                sandbox_provider="daytona",
            )

            assert reaped == [], f"Force stop deleted sandboxes the executor still owned: {', '.join(reaped)}"

            await benchmark_task
            _assert_no_task_errors(example_benchmark_object, database_session)

            database_session.refresh(example_benchmark_object)
            assert example_benchmark_object.status == BenchmarkStatus.STOPPED

            await _wait_until_no_sandboxes(example_benchmark_object, provider)
        finally:
            monkeypatch.setattr(run_control, "stop_sandbox", release_sandbox)
            if benchmark_task is not None and not benchmark_task.done():
                benchmark_task.cancel()
                await asyncio.gather(benchmark_task, return_exceptions=True)

            try:
                if provider is not None and await _sandboxes_for_benchmark(example_benchmark_object, provider):
                    await force_stop_sandboxes(
                        example_benchmark_object,
                        database_session,
                        daytona_secret_name,
                        aws_runtime,
                        Org(id=TEST_ORG_ID, name="default"),
                        sandbox_provider="daytona",
                    )
                    await _wait_until_no_sandboxes(example_benchmark_object, provider)
            finally:
                await benchmark_service.close()

    async def test_force_stop_end_to_end(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        daytona_secret_name: str,
        harness_config: HarnessConfig,
        harness_headers: dict[str, str],
        service_headers: dict[str, str],
        live_api_client: TestClient,
        executor_authority_kwargs: Any,
    ) -> None:
        """Verify the HTTP force-stop path interrupts a live benchmark without task errors.

        Test cases:
        - A five-task benchmark reaches an in-progress sandbox before the stop endpoint is called.
        - The force stop endpoint returns success and leaves no active or error tasks.
        - The benchmark status becomes STOPPED and no benchmark-labeled sandboxes remain.
        """
        example_benchmark_object.arguments.slice_str = ":5"
        example_benchmark_object.arguments.concurrency = 2
        database_session.add(example_benchmark_object)
        database_session.commit()

        benchmark_service = example_benchmark_object.benchmark_service(service_headers=service_headers)
        benchmark_task: Optional[asyncio.Task[None]] = None
        provider: Optional[SandboxProvider] = None
        aws_runtime = AWSRuntime.from_harness_config(harness_config)

        try:
            verify_response = await benchmark_service.verify_task_ids(
                task_ids=example_benchmark_object.arguments.task_ids,
                slice_str=example_benchmark_object.arguments.slice_str,
            )

            authority_kwargs = executor_authority_kwargs(example_benchmark_object)
            benchmark_task = asyncio.create_task(
                process_benchmark(
                    start_benchmark_request_json=example_benchmark_object.start_benchmark_request(
                        harness_config, service_headers=service_headers
                    ).model_dump(),
                    benchmark_id_str=str(example_benchmark_object.id),
                    verified_task_ids=verify_response.task_ids,
                    **authority_kwargs,
                )
            )

            provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_runtime.clients, "daytona")
            provider = benchmark_service.get_sandbox_provider(provider_config)
            await _wait_for_running_benchmark(example_benchmark_object, database_session, provider)

            response = live_api_client.post(
                f"/stop-benchmark/{example_benchmark_object.id}?force=true",
                headers=harness_headers,
            )
            assert response.status_code == 200
            assert response.json() == {"status": "success"}

            await benchmark_task

            active_tasks = database_session.exec(
                select(Task)
                .where(Task.benchmark == example_benchmark_object.id)
                .where(col(Task.status).in_(_ACTIVE_TASK_STATUSES))
            ).all()
            assert active_tasks == []

            error_tasks = database_session.exec(
                select(Task).where(Task.benchmark == example_benchmark_object.id).where(Task.status == TaskStatus.ERROR)
            ).all()

            assert error_tasks == [], f"Tasks have error status: {', '.join(task.task_id for task in error_tasks)}"
            _assert_no_task_errors(example_benchmark_object, database_session)

            terminal_tasks = database_session.exec(
                select(Task)
                .where(Task.benchmark == example_benchmark_object.id)
                .where(col(Task.status).in_([TaskStatus.STOPPED, TaskStatus.FINISHED]))
            ).all()

            assert len(terminal_tasks) == 5

            database_session.refresh(example_benchmark_object)
            assert example_benchmark_object.status == BenchmarkStatus.STOPPED

            await _wait_until_no_sandboxes(example_benchmark_object, provider)
        finally:
            if benchmark_task is not None and not benchmark_task.done():
                benchmark_task.cancel()
                await asyncio.gather(benchmark_task, return_exceptions=True)

            try:
                if provider is not None and await _sandboxes_for_benchmark(example_benchmark_object, provider):
                    await force_stop_sandboxes(
                        example_benchmark_object,
                        database_session,
                        daytona_secret_name,
                        aws_runtime,
                        Org(id=TEST_ORG_ID, name="default"),
                        sandbox_provider="daytona",
                    )
                    await _wait_until_no_sandboxes(example_benchmark_object, provider)
            finally:
                await benchmark_service.close()
