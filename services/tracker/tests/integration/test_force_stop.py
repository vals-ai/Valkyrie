import asyncio

import pytest
from benchmark_service import ImageSource, Resources, SandboxNotFoundError, SandboxProvider, SandboxQuery
from benchmark_service.client import BenchmarkServiceClient
from fastapi.testclient import TestClient
from sqlmodel import Session, col, select

from main import app
from tests.conftest import TEST_ORG_ID
from tests.utils import random_task_id
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.logging import get_logger
from tracker.sandbox import create_sandbox
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import fetch_harness_config, force_stop_sandboxes, process_benchmark
from tracker.utils import fetch_sandbox_provider_config

logger = get_logger(__name__)

_ACTIVE_TASK_STATUSES = [TaskStatus.BUILDING, TaskStatus.PENDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
_TERMINAL_TASK_STATUSES = [TaskStatus.STOPPED, TaskStatus.FINISHED]


async def _sandboxes_for_benchmark(benchmark: Benchmark, provider: SandboxProvider):
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


def _assert_no_task_errors(benchmark: Benchmark, database_session: Session) -> None:
    database_session.expire_all()
    assert benchmark.fetch_tasks_with_errors(database_session) is None


class TestForceStop:
    async def test_force_stop_sandbox(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        benchmark_service: BenchmarkServiceClient,
        test_resources: Resources,
        daytona_secret_name: str,
        aws_credentials: AWSCredentials,
        test_image: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify force_stop_sandboxes can stop a sandbox while creation is racing.

        Test cases:
        - A task already marked in progress is moved to STOPPED while its sandbox context is active.
        - The created sandbox is deleted and provider lookup raises SandboxNotFoundError.
        """
        database_session.add(example_benchmark_object)
        database_session.commit()

        # Create a single task
        task = Task(
            org_id=TEST_ORG_ID,
            benchmark=example_benchmark_object.id,
            task_id=random_task_id(),
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(task)
        database_session.commit()

        provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_credentials, "daytona")
        provider = benchmark_service.get_sandbox_provider(provider_config)
        labels = {
            "Benchmark": example_benchmark_object.name,
            "Id": str(example_benchmark_object.id),
            "Task": task.task_id,
        }

        async def force_stop_sandbox() -> None:
            await asyncio.sleep(0.5)

            await force_stop_sandboxes(
                example_benchmark_object,
                database_session,
                daytona_secret_name,
                aws_credentials,
                Org(id=TEST_ORG_ID, name="default"),
                sandbox_provider="daytona",
            )

        created_sandbox_name: list[str] = []

        async def _generator_to_courtine():
            async with create_sandbox(
                provider,
                task.task_id,
                ImageSource(image=test_image),
                resources=test_resources,
                creation_semaphore=creation_semaphore,
                labels=labels,
            ) as sandbox:
                created_sandbox_name.append(sandbox.name)

        # Start sandbox and immediately try to stop it, expected to wait until its started to delete
        await asyncio.gather(
            _generator_to_courtine(),
            force_stop_sandbox(),
        )

        # Ensure that the task is in the stopped state
        task = database_session.exec(select(Task).where(Task.id == task.id)).one()
        assert task.status == TaskStatus.STOPPED

        # Ensure that the sandbox does not exist anymore
        with pytest.raises(SandboxNotFoundError):
            await provider.get_sandbox(created_sandbox_name[0])

    async def test_force_stop_sandboxes(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        aws_credentials: AWSCredentials,
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

        provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_credentials, "daytona")
        provider = benchmark_service.get_sandbox_provider(provider_config)

        labels = {"Benchmark": example_benchmark_object.name, "Id": str(example_benchmark_object.id)}
        release_sandboxes = asyncio.Event()

        async def create_sandbox_with_delay(sandbox_name: str) -> None:
            """Create sandbox that will not be closed automatically"""
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
                await release_sandboxes.wait()

        # Create 12 tasks that are in progress and evaluating
        tasks: list[Task] = []
        for i in range(12):
            status = TaskStatus.IN_PROGRESS if i < 6 else TaskStatus.EVALUATING
            task = Task(
                org_id=TEST_ORG_ID, benchmark=example_benchmark_object.id, task_id=random_task_id(), status=status
            )
            tasks.append(task)
            database_session.add(task)

        database_session.commit()

        # Test force_stop_sandboxes util by running all 12 sandboxes in parallel and
        # See if we can close them all
        created_sandboxes = asyncio.gather(
            *[asyncio.create_task(create_sandbox_with_delay(task.task_id)) for task in tasks],
            return_exceptions=True,
        )

        # Pause for 2 seconds to ensure that the sandboxes are being created
        await asyncio.sleep(2)

        try:
            # Force stop the benchmark run with all sandboxes
            await force_stop_sandboxes(
                example_benchmark_object,
                database_session,
                daytona_secret_name,
                aws_credentials,
                Org(id=TEST_ORG_ID, name="default"),
                sandbox_provider="daytona",
            )
        finally:
            release_sandboxes.set()

        created_results = await asyncio.wait_for(created_sandboxes, timeout=30)
        unexpected_errors = [
            result
            for result in created_results
            if isinstance(result, Exception) and not isinstance(result, SandboxNotFoundError)
        ]
        assert unexpected_errors == []

        _assert_no_task_errors(example_benchmark_object, database_session)

        # Ensure that there are no more sandboxes left running
        await _wait_until_no_sandboxes(example_benchmark_object, provider)

    @pytest.mark.slow
    async def test_force_stop_end_to_end(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        aws_credentials: AWSCredentials,
        daytona_secret_name: str,
        harness_config: HarnessConfig,
        service_headers: dict[str, str],
    ) -> None:
        """Verify the HTTP force-stop path interrupts a live benchmark without task errors.

        Test cases:
        - A five-task benchmark reaches an in-progress sandbox before the stop endpoint is called.
        - The force stop endpoint returns success and leaves no active or error tasks.
        - The benchmark status becomes STOPPED and no benchmark-labeled sandboxes remain.
        """
        # Max concurrency at 2 with 5 tasks
        example_benchmark_object.arguments.slice_str = ":5"
        example_benchmark_object.arguments.concurrency = 2
        database_session.add(example_benchmark_object)
        database_session.commit()

        client = TestClient(app)
        # This end-to-end test drives the real force-stop endpoint against real AWS,
        # so the endpoint needs the real harness config — not the autouse FAKE one.
        app.dependency_overrides[fetch_harness_config] = lambda: harness_config

        benchmark_service = example_benchmark_object.benchmark_service(service_headers=service_headers)

        try:
            verify_response = await benchmark_service.verify_task_ids(
                task_ids=example_benchmark_object.arguments.task_ids,
                slice_str=example_benchmark_object.arguments.slice_str,
            )

            # Start the benchmark run with just 5 tasks
            benchmark_task = asyncio.create_task(
                process_benchmark(
                    start_benchmark_request_json=example_benchmark_object.start_benchmark_request(
                        harness_config, service_headers=service_headers
                    ).model_dump(),
                    benchmark_id_str=str(example_benchmark_object.id),
                    verified_task_ids=verify_response.task_ids,
                )
            )

            provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_credentials, "daytona")
            provider = benchmark_service.get_sandbox_provider(provider_config)
            await _wait_for_running_benchmark(example_benchmark_object, database_session, provider)

            # Force stop the benchmark run with all sandboxes
            response = client.post(f"/stop-benchmark/{example_benchmark_object.id}?force=true")
            assert response.status_code == 200
            assert response.json() == {"status": "success"}

            await benchmark_task

            # All tasks are stopped
            pending_tasks = database_session.exec(
                select(Task)
                .where(Task.benchmark == example_benchmark_object.id)
                .where(col(Task.status).in_(_ACTIVE_TASK_STATUSES))
            ).all()
            assert len(pending_tasks) == 0

            # No tasks have error status or captured error messages
            error_tasks = database_session.exec(
                select(Task).where(Task.benchmark == example_benchmark_object.id).where(Task.status == TaskStatus.ERROR)
            ).all()

            assert len(error_tasks) == 0, f"Tasks have error status: {', '.join(task.task_id for task in error_tasks)}"
            _assert_no_task_errors(example_benchmark_object, database_session)

            # All tasks should be in a finished state (STOPPED or FINISHED)
            # Some tasks will finish quickly since the agent is a dummy model
            terminal_tasks = database_session.exec(
                select(Task)
                .where(Task.benchmark == example_benchmark_object.id)
                .where(col(Task.status).in_([TaskStatus.STOPPED, TaskStatus.FINISHED]))
            ).all()

            assert len(terminal_tasks) == 5

            # Fetch the benchmark and ensure that it is in the stopped state
            database_session.refresh(example_benchmark_object)
            assert example_benchmark_object.status == BenchmarkStatus.STOPPED

            # Try to fetch the sandboxes and see if any of them are still running
            await _wait_until_no_sandboxes(example_benchmark_object, provider)
        finally:
            await benchmark_service.close()
