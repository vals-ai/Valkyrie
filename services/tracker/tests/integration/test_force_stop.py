import asyncio

import pytest
from benchmark_service import ImageSource, Resources, SandboxProvider, SandboxQuery
from benchmark_service.client import BenchmarkServiceClient
from fastapi.testclient import TestClient
from sqlmodel import Session, col, select

from main import app
from tests.utils import random_task_id
from tests.conftest import TEST_ORG_ID
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.logging import get_logger
from tracker.sandbox import create_sandbox
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import force_stop_sandboxes, process_benchmark

logger = get_logger(__name__)


async def _sandboxes_for_benchmark(benchmark: Benchmark, provider: SandboxProvider):
    query = SandboxQuery(labels={"Benchmark": benchmark.name, "Id": str(benchmark.id)})
    return [sandbox async for sandbox in provider.list_sandboxes(query)]


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
        """
        Test force stopping a single sandbox that is building

        Test Cases:
        - Force stop can be ran while a sandbox is being built
        - After finished running, no sandbox exists anymore
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

        provider = benchmark_service.get_sandbox_provider()
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
            )

        async def _generator_to_courtine():
            async with create_sandbox(
                provider,
                task.alias,
                ImageSource(image=test_image),
                resources=test_resources,
                creation_semaphore=creation_semaphore,
                labels=labels,
            ) as _:
                pass

        # Start sandbox and immediately try to stop it, expected to wait until its started to delete
        await asyncio.gather(
            _generator_to_courtine(),
            force_stop_sandbox(),
        )

        # Ensure that the task is in the stopped state
        task = database_session.exec(select(Task).where(Task.id == task.id)).one()
        assert task.status == TaskStatus.STOPPED

        # Ensure that the sandbox does not exist anymore
        with pytest.raises(Exception):
            await provider.get_sandbox(task.alias)

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
        """
        Test force stopping multiple sandboxes that are being built / already built

        Test Cases:
        - Create 12 sandboxes all delayed to exist for 20 seconds, force stopping after they are built
        - 0 Sandboxes exist from the group we created after have finished stopping them
        """
        database_session.add(example_benchmark_object)
        database_session.commit()

        provider = benchmark_service.get_sandbox_provider()

        labels = {"Benchmark": example_benchmark_object.name, "Id": str(example_benchmark_object.id)}

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
            *[asyncio.create_task(create_sandbox_with_delay(task.alias)) for task in tasks]
        )

        # Pause for 2 seconds to ensure that the sandboxes are being created
        await asyncio.sleep(2)

        # Force stop the benchmark run with all sandboxes
        await force_stop_sandboxes(
            example_benchmark_object,
            database_session,
            daytona_secret_name,
            aws_credentials,
            Org(id=TEST_ORG_ID, name="default"),
        )

        await created_sandboxes

        # Ensure that there are no more sandboxes left running
        sandboxes = await _sandboxes_for_benchmark(example_benchmark_object, provider)
        assert len(sandboxes) == 0

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
        """
        Test end to end with a benchmark service -- Starting a slice of tasks and force stopping them

        Test Cases:
        - Start the run using 5 tasks
        - Force stop the benchmark
        - Task statuses are set to stopped
        - No tasks have any errors after force stopping
        - Benchmark status is STOPPED
        - No sandboxes exist in daytona from this run
        """
        # Max concurrency at 2 with 5 tasks
        example_benchmark_object.arguments.slice_str = ":5"
        example_benchmark_object.arguments.concurrency = 2
        database_session.add(example_benchmark_object)
        database_session.commit()

        client = TestClient(app)

        benchmark_service = example_benchmark_object.benchmark_service(
            daytona_secret_name, aws_credentials, service_headers=service_headers
        )

        verify_response = await benchmark_service.verify_task_ids(
            task_ids=example_benchmark_object.arguments.task_ids, slice_str=example_benchmark_object.arguments.slice_str
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

        # Wait a few seconds for all the tasks to start
        await asyncio.sleep(5)

        # Force stop the benchmark run with all sandboxes
        response = client.post(f"/stop-benchmark/{example_benchmark_object.id}?force=true")
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        await benchmark_task

        # All tasks are stopped
        pending_tasks = database_session.exec(
            select(Task)
            .where(Task.benchmark == example_benchmark_object.id)
            .where(
                col(Task.status).in_(
                    [TaskStatus.BUILDING, TaskStatus.PENDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
                )
            )
        ).all()
        assert len(pending_tasks) == 0

        # No tasks have error status
        error_tasks = database_session.exec(
            select(Task).where(Task.benchmark == example_benchmark_object.id).where(Task.status == TaskStatus.ERROR)
        ).all()

        assert len(error_tasks) == 0, (
            f"Tasks have error status: {', '.join([task.error_message or 'No error message' for task in error_tasks])}"
        )

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

        # Create daytona client from the current benchmark service
        provider = benchmark_service.get_sandbox_provider()

        # Try to fetch the sandboxes and see if any of them are still running
        sandboxes = await _sandboxes_for_benchmark(example_benchmark_object, provider)
        assert len(sandboxes) == 0

        await benchmark_service.close()
