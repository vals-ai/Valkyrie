import asyncio
from asyncio import Semaphore

import pytest
from benchmark_service.schemas import Resources
from daytona import AsyncDaytona
from daytona.common.errors import DaytonaNotFoundError

from tests.utils import random_task_id
from tracker.sandbox import create_sandbox


class TestSandboxLifecycle:
    async def test_parallel_create_and_immediate_delete(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        test_image: str,
    ) -> None:
        """
        Create and delete sandboxes at the same time to ensure that the set_autostop_interval inside of the delete_sandbox method works

        Test Cases:
        - Sandboxes do not exist after we exit
        - Able to delete all sandboxes with no errors
        """
        TASKS_TO_CREATE = 10
        semaphore = Semaphore(TASKS_TO_CREATE)
        names = [f"test-parallel-delete-{random_task_id()}" for _ in range(TASKS_TO_CREATE)]

        async def _create_and_delete(name: str) -> None:
            async with create_sandbox(daytona_client, name, test_image, test_resources, semaphore):
                await asyncio.sleep(2)

        await asyncio.gather(*[_create_and_delete(name) for name in names])

        for name in names:
            with pytest.raises(DaytonaNotFoundError):
                await daytona_client.get(name)
