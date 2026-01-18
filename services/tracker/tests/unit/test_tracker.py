import asyncio
from asyncio import Semaphore, gather
from typing import Any

import pytest

from tracker.utils import TrackedTask, TrackedTaskStatus


class TestTracker:
    async def _mock_coro(self, task_id: str) -> dict[str, dict[str, Any] | None]:
        """Blank coro that returns the same format as process_task method"""
        await asyncio.sleep(5)

        return {task_id: {"result": task_id}}

    async def _validate_task_state_before_run(
        self, tracked_task: TrackedTask, task_id: str
    ) -> dict[str, dict[str, Any] | None]:
        """
        Validates the state of the tracked task while its running.

        the task running needs to start before the timer ends.
        """
        await asyncio.sleep(1)

        assert tracked_task.status == TrackedTaskStatus.RUNNING
        assert tracked_task.task

        return {task_id: None}

    def test_task_monitor(self) -> None: ...

    async def test_tracked_task(self) -> None:
        """
        Test functionality of the TrackedTask class

        Test Cases:
            - When first created, it is in the waiting state
            - When waiting to be aquired by the semaphore, it is still waiting
            - When picked up by the semaphore, it is in the running state
            - When cancelled or finished, it is in the done state
        """

        # Pass in a blank method to replace process_task
        mock_coro = self._mock_coro(task_id="task_id_1")
        tracked_task = TrackedTask(coro=mock_coro)

        # Test case 1. When first created, it is in the waiting state
        assert tracked_task.status == TrackedTaskStatus.WAITING
        assert tracked_task.task is None

        # Create another task that tracks the status of the first task
        # Allows us to test that the task status changes to running once the semaphore is aquired.
        tracked_task_2 = TrackedTask(coro=self._validate_task_state_before_run(tracked_task, "task_id_2"))

        semaphore = Semaphore(value=2)
        tasks = [tracked_task.run(semaphore, "task_id_1"), tracked_task_2.run(semaphore, "task_id_2")]
        results = await gather(*tasks)
        assert results == [{"task_id_1": {"result": "task_id_1"}}, {"task_id_2": None}]

        # Test case 2. After the tasks have finished running, we can validate that the task status changes to done
        assert tracked_task.status == TrackedTaskStatus.DONE
        assert tracked_task.task is not None
        assert tracked_task.task.result() == {"task_id_1": {"result": "task_id_1"}}

        assert tracked_task_2.status == TrackedTaskStatus.DONE
        assert tracked_task_2.task is not None
        assert tracked_task_2.task.result() == {"task_id_2": None}

        # Test case 3. When the task is cancelled, it is in the done state and default response is returned
        tracked_task = TrackedTask(coro=self._mock_coro(task_id="task_id_3"))

        # Create semaphore to run task instantly
        semaphore = Semaphore(value=1)
        run_task = asyncio.create_task(tracked_task.run(semaphore, "task_id_3"))

        # Wait for the task to start running and ensure that the status is running
        await asyncio.sleep(1)
        assert tracked_task.status == TrackedTaskStatus.RUNNING

        # Once the task is cancelled, the tracker should go to the done state
        run_task.cancel()

        result = await run_task

        assert tracked_task.status == TrackedTaskStatus.DONE

        # The response of the tracked result should be None since the task was cancelled
        assert tracked_task.task is not None

        # Ensurance of cancellation
        with pytest.raises(asyncio.CancelledError):
            tracked_task.task.result()

        assert result == {"task_id_3": None}

        # Test case 4. When a task is waiting to be aquired it is kept inside of the waiting state
        running_task = TrackedTask(coro=self._mock_coro(task_id="task_id_4"))
        waiting_task = TrackedTask(coro=self._mock_coro(task_id="task_id_5"))

        semaphore = Semaphore(value=1)
        running_task_coro = running_task.run(semaphore, "task_id_4")
        waiting_task_coro = waiting_task.run(semaphore, "task_id_5")

        results = asyncio.gather(running_task_coro, waiting_task_coro)

        # Give the first task time to acquire semaphore
        await asyncio.sleep(1)

        # First task aquired is running and the second task is still waiting
        assert running_task.status == TrackedTaskStatus.RUNNING
        assert waiting_task.status == TrackedTaskStatus.WAITING

        await results
