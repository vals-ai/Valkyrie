"""Unit tests for tracked task scheduling and monitoring.

Run: uv run pytest tests/unit/utils/test_task_execution.py
"""

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import Benchmark, Org, Task, TaskStatus
from tracker.utils import ResizableLimiter, TaskMonitor, TrackedTask, TrackedTaskStatus


class TestTaskExecution:
    """Task monitoring and tracked task state transitions."""

    _test_org = Org(id=TEST_ORG_ID, name="default")

    async def test_resizable_limiter_increase_wakes_waiting_admission(self) -> None:
        limiter = ResizableLimiter(limit=1)
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_attempted = asyncio.Event()
        second_started = asyncio.Event()

        async def worker(
            started: asyncio.Event,
            release: asyncio.Event | None = None,
            attempting: asyncio.Event | None = None,
        ) -> None:
            if attempting is not None:
                attempting.set()
            async with limiter:
                started.set()
                if release is not None:
                    await release.wait()

        first = asyncio.create_task(worker(first_started, release_first))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        second = asyncio.create_task(worker(second_started, attempting=second_attempted))
        await asyncio.wait_for(second_attempted.wait(), timeout=1)
        assert not second_started.is_set()

        await limiter.resize(2)
        await asyncio.wait_for(second_started.wait(), timeout=1)

        release_first.set()
        await asyncio.gather(first, second)

    async def test_resizable_limiter_decrease_is_non_preemptive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        limiter = ResizableLimiter(limit=2)
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        third_started = asyncio.Event()
        release_first = asyncio.Event()
        release_second = asyncio.Event()
        third_wait_attempted = asyncio.Event()
        third_rewait_attempted = asyncio.Event()
        condition = getattr(limiter, "_condition")
        condition_wait = getattr(condition, "wait")
        wait_attempts = 0

        async def observed_condition_wait() -> bool:
            nonlocal wait_attempts
            wait_attempts += 1
            if wait_attempts == 1:
                third_wait_attempted.set()
            elif wait_attempts == 2:
                third_rewait_attempted.set()
            return await condition_wait()

        monkeypatch.setattr(condition, "wait", observed_condition_wait)

        async def worker(started: asyncio.Event, release: asyncio.Event | None = None) -> None:
            async with limiter:
                started.set()
                if release is not None:
                    await release.wait()

        first = asyncio.create_task(worker(first_started, release_first))
        second = asyncio.create_task(worker(second_started, release_second))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await asyncio.wait_for(second_started.wait(), timeout=1)

        await limiter.resize(1)
        third = asyncio.create_task(worker(third_started))
        await asyncio.wait_for(third_wait_attempted.wait(), timeout=1)
        assert not first.done()
        assert not second.done()
        assert not third_started.is_set()

        release_first.set()
        await first
        await asyncio.wait_for(third_rewait_attempted.wait(), timeout=1)
        assert not third_started.is_set()

        release_second.set()
        await second
        await asyncio.wait_for(third_started.wait(), timeout=1)
        await third

    async def test_task_monitor(
        self, database_session: Session, example_benchmark_object: Benchmark, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The monitor removes completed work and cancels invalid attempts once."""
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        stopped_row = Task(
            org_id=TEST_ORG_ID,
            task_id="stopped",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
        )
        superseded_row = Task(org_id=TEST_ORG_ID, task_id="superseded", benchmark=benchmark_row.id)
        database_session.add_all([stopped_row, superseded_row])
        database_session.commit()

        done = TrackedTask(asyncio.sleep(0), self._test_org, stopped_row.started_at)
        stopped = TrackedTask(asyncio.sleep(0), self._test_org, stopped_row.started_at)
        superseded = TrackedTask(asyncio.sleep(0), self._test_org, superseded_row.started_at)
        setattr(done, "_status", TrackedTaskStatus.DONE)
        setattr(stopped, "_status", TrackedTaskStatus.RUNNING)
        setattr(superseded, "_status", TrackedTaskStatus.RUNNING)

        stopped_cancel = Mock()
        superseded_cancel = Mock()
        setattr(stopped, "_task", Mock(cancel=stopped_cancel, done=lambda: False))
        setattr(superseded, "_task", Mock(cancel=superseded_cancel, done=lambda: False))

        superseded_row.started_at += timedelta(seconds=1)
        database_session.add(superseded_row)
        database_session.commit()

        task_tracking = {"done": done, "stopped": stopped, "superseded": superseded}

        monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
        monitor = TaskMonitor(
            benchmark_row.id,
            task_tracking,
            org=self._test_org,
            limiter=ResizableLimiter(limit=1),
        )
        sleep_count = 0

        async def complete_cancelled_tasks(_delay: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 2:
                setattr(stopped, "_status", TrackedTaskStatus.DONE)
                setattr(superseded, "_status", TrackedTaskStatus.DONE)

        monkeypatch.setattr("tracker.utils.task_execution.asyncio.sleep", complete_cancelled_tasks)

        await monitor.track_tasks()

        stopped_cancel.assert_called_once()
        superseded_cancel.assert_called_once()
        assert task_tracking == {}
        for tracked_task in (done, stopped, superseded):
            getattr(tracked_task, "_coro").close()

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

        limiter = ResizableLimiter(limit=1)
        running_started = asyncio.Event()
        release_running = asyncio.Event()
        waiting_attempted = asyncio.Event()
        waiting_started = asyncio.Event()
        release_waiting = asyncio.Event()
        running_row = MagicMock(spec=Task, task_id="task_id_1")
        waiting_row = MagicMock(spec=Task, task_id="task_id_2")
        running = TrackedTask(
            controlled_result("task_id_1", running_started, release_running),
            self._test_org,
            running_row.started_at,
        )
        waiting = TrackedTask(
            controlled_result("task_id_2", waiting_started, release_waiting),
            self._test_org,
            waiting_row.started_at,
        )

        assert running.status == TrackedTaskStatus.WAITING
        assert running.task is None

        running_call = asyncio.create_task(running.run(limiter, running_row))
        await running_started.wait()

        async def run_waiting() -> dict[str, dict[str, Any] | None]:
            waiting_attempted.set()
            return await waiting.run(limiter, waiting_row)

        waiting_call = asyncio.create_task(run_waiting())
        await waiting_attempted.wait()

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
