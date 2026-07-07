import asyncio
from asyncio import Semaphore, gather
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import Benchmark, Org, Task, TaskStatus
from tracker.utils import TaskMonitor, TrackedTask, TrackedTaskStatus


class TestTracker:
    _test_org = Org(id=TEST_ORG_ID, name="default")

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

    async def test_task_monitor(
        self, database_session: Session, example_benchmark_object: Benchmark, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Test functionality of the TaskMonitor class

        NOTE: Most of the functionality is closed off so we used the private variables and methods.
        had to use type: ignore to satisfy the type checker.

        Test Cases:
            - _validate_task fetches from updates database
            - _validate_task returns false if the task status has been set to stopped
            - running tasks remain tracked until they finish
        """
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        tasks_to_track: list[str] = ["task_id_1"]
        for task_id in tasks_to_track:
            task_row = Task(org_id=TEST_ORG_ID, task_id=task_id, benchmark=benchmark_row.id)
            database_session.add(task_row)
            database_session.commit()

        task_tracking: dict[str, TrackedTask] = {
            task_id: TrackedTask(coro=self._mock_coro(task_id=task_id), org=self._test_org)
            for task_id in tasks_to_track
        }

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monitor = TaskMonitor(benchmark_row.id, task_tracking.copy(), org=self._test_org)

        # Change task status to running and add a task to the object
        task_tracking["task_id_1"]._status = TrackedTaskStatus.RUNNING  # type: ignore
        cancel_mock = Mock()

        def _cancel(*_args: Any, **_kwargs: Any) -> None:
            task_tracking["task_id_1"]._status = TrackedTaskStatus.DONE  # type: ignore

        cancel_mock.side_effect = _cancel
        task_tracking["task_id_1"]._task = Mock(cancel=cancel_mock, done=lambda: False)  # type: ignore

        # Test case 1. Validate task returns true if the task is not stopped
        assert monitor._validate_task("task_id_1")  # type: ignore

        # Change the task status to stopped to make sure that it gets invalidated inside of the validate task method
        task_row = monitor._fetch_task_row("task_id_1")  # type: ignore

        # Commit the changes to the database, will be available from any session
        task_row.status = TaskStatus.STOPPED
        database_session.add(task_row)
        database_session.commit()

        # Test case 2. Validate task returns false if the task status has been set to stopped
        # NOTE: ensures that the database change gets picked up by the session
        assert not monitor._validate_task("task_id_1")  # type: ignore

        # Test case 3. Running tasks stay tracked until they are done
        await monitor.track_tasks()
        assert task_tracking["task_id_1"].task
        cancel_mock.assert_called_once()

        assert monitor._task_tracking == {}
        task_tracking["task_id_1"]._coro.close()  # type: ignore[attr-defined]

    async def test_tracked_task(self) -> None:
        """
        Test functionality of the TrackedTask class

        Test Cases:
            - When first created, it is in the waiting state
            - When waiting to be aquired by the semaphore, it is still waiting
            - When picked up by the semaphore, it is in the running state
            - When cancelled or finished, it is in the done state
        """

        # Create mock task_row and session for the new TrackedTask.run() signature
        mock_task_row = MagicMock(spec=Task)
        mock_task_row.task_id = "task_id_1"

        # Pass in a blank method to replace process_task
        mock_coro = self._mock_coro(task_id="task_id_1")

        mock_task_row = MagicMock(spec=Task)
        mock_task_row.task_id = "task_id_1"
        mock_task_row_2 = MagicMock(spec=Task)
        mock_task_row_2.task_id = "task_id_2"

        tracked_task = TrackedTask(coro=mock_coro, org=self._test_org)

        # Test case 1. When first created, it is in the waiting state
        assert tracked_task.status == TrackedTaskStatus.WAITING
        assert tracked_task.task is None

        # Create another task that tracks the status of the first task
        # Allows us to test that the task status changes to running once the semaphore is aquired.
        tracked_task_2 = TrackedTask(
            coro=self._validate_task_state_before_run(tracked_task, "task_id_2"), org=self._test_org
        )

        mock_task_row_2 = MagicMock(spec=Task)
        mock_task_row_2.task_id = "task_id_2"

        semaphore = Semaphore(value=2)
        tasks = [
            tracked_task.run(semaphore, mock_task_row),
            tracked_task_2.run(semaphore, mock_task_row_2),
        ]
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
        mock_task_row_3 = MagicMock(spec=Task)
        mock_task_row_3.task_id = "task_id_3"

        tracked_task = TrackedTask(coro=self._mock_coro(task_id="task_id_3"), org=self._test_org)
        semaphore = Semaphore(value=1)
        run_task = asyncio.create_task(tracked_task.run(semaphore, mock_task_row_3))

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
        mock_task_row_4 = MagicMock(spec=Task)
        mock_task_row_4.task_id = "task_id_4"
        mock_task_row_5 = MagicMock(spec=Task)
        mock_task_row_5.task_id = "task_id_5"

        running_task = TrackedTask(coro=self._mock_coro(task_id="task_id_4"), org=self._test_org)
        waiting_task = TrackedTask(coro=self._mock_coro(task_id="task_id_5"), org=self._test_org)

        semaphore = Semaphore(value=1)

        running_task_coro = running_task.run(semaphore, mock_task_row_4)
        waiting_task_coro = waiting_task.run(semaphore, mock_task_row_5)

        results = asyncio.gather(running_task_coro, waiting_task_coro)

        # Give the first task time to acquire semaphore
        await asyncio.sleep(1)

        # First task aquired is running and the second task is still waiting
        assert running_task.status == TrackedTaskStatus.RUNNING
        assert waiting_task.status == TrackedTaskStatus.WAITING

        # Cancel the waiting task
        assert waiting_task.task is not None
        waiting_task.task.cancel()

        task_results = await results

        # All results are now done
        assert running_task.status == TrackedTaskStatus.DONE
        assert waiting_task.status == TrackedTaskStatus.DONE

        # Waiting task returns an empty response since we cancelled it
        assert task_results[1] == {"task_id_5": None}

        # Running task was completed so the result is full
        assert task_results[0] == {"task_id_4": {"result": "task_id_4"}}
