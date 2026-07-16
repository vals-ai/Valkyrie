"""Unit tests for tracked task scheduling and monitoring."""

import asyncio
from asyncio import Semaphore
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import Benchmark, Org, Task, TaskStatus
from tracker.utils import TaskMonitor, TrackedTask, TrackedTaskStatus


class TestTracker:
    _test_org = Org(id=TEST_ORG_ID, name="default")

    async def test_task_monitor(
        self, database_session: Session, example_benchmark_object: Benchmark, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The monitor must notice persisted stops and cancel active work.

        Test cases:
        - A pending database task remains valid.
        - A stopped database task cancels its active asyncio task.
        - Completed tasks are removed from the monitor.
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
            task_id: TrackedTask(coro=asyncio.sleep(0), org=self._test_org) for task_id in tasks_to_track
        }

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
        monitor = TaskMonitor(benchmark_row.id, task_tracking.copy(), org=self._test_org)
        monkeypatch.setattr(monitor, "_TRACK_INTERVAL", 0)

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

        assert getattr(monitor, "_task_tracking") == {}
        task_tracking["task_id_1"]._coro.close()  # type: ignore[attr-defined]

    async def test_tracked_task(self) -> None:
        """Tracked task states must match semaphore scheduling and cancellation.

        Test cases:
        - A new task stays waiting until its semaphore slot is available.
        - A running task returns its result and becomes done.
        - Cancellation returns the task-shaped empty result and becomes done.
        """

        async def controlled_result(
            task_id: str,
            started: asyncio.Event,
            release: asyncio.Event,
        ) -> dict[str, dict[str, str]]:
            started.set()
            await release.wait()
            return {task_id: {"result": task_id}}

        semaphore = Semaphore(value=1)
        running_started = asyncio.Event()
        release_running = asyncio.Event()
        waiting_started = asyncio.Event()
        release_waiting = asyncio.Event()
        running_row = MagicMock(spec=Task, task_id="task_id_1")
        waiting_row = MagicMock(spec=Task, task_id="task_id_2")
        running = TrackedTask(controlled_result("task_id_1", running_started, release_running), self._test_org)
        waiting = TrackedTask(controlled_result("task_id_2", waiting_started, release_waiting), self._test_org)

        assert running.status == TrackedTaskStatus.WAITING
        assert running.task is None

        running_call = asyncio.create_task(running.run(semaphore, running_row))
        await running_started.wait()
        waiting_call = asyncio.create_task(waiting.run(semaphore, waiting_row))
        await asyncio.sleep(0)

        assert running.status == TrackedTaskStatus.RUNNING
        assert waiting.status == TrackedTaskStatus.WAITING
        assert not waiting_started.is_set()

        assert waiting.task is not None
        waiting.task.cancel()
        release_running.set()
        results = await asyncio.gather(running_call, waiting_call)

        assert results == [{"task_id_1": {"result": "task_id_1"}}, {"task_id_2": None}]
        assert running.status == TrackedTaskStatus.DONE
        assert waiting.status == TrackedTaskStatus.DONE
        assert running.task is not None
        assert running.task.result() == {"task_id_1": {"result": "task_id_1"}}
        assert waiting.task is not None
        with pytest.raises(asyncio.CancelledError):
            waiting.task.result()
