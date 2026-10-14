"""Unit tests for tracked task scheduling and monitoring.

Run: uv run pytest tests/unit/utils/test_task_execution.py
"""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from tests.utils import TEST_ORG_ID
from tracker.database.models import Benchmark, ExecutorDispatch, ExecutorDispatchStatus, Org, Task, TaskStatus
from tracker.exceptions import SandboxError, SandboxSetupError
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.utils import ResizableLimiter, TaskMonitor, TrackedTask, TrackedTaskStatus
from tracker.utils import task_execution
from tracker.utils.run_control import apply_shed_benchmark

_exception_message = getattr(task_execution, "_exception_message")


class TestExceptionMessage:
    """Persisted task error text keeps the original error behind tracker wrappers."""

    def test_direct_error_keeps_type_and_message(self) -> None:
        assert _exception_message(ValueError("bad task input")) == "ValueError: bad task input"

    def test_empty_message_keeps_type(self) -> None:
        assert _exception_message(TimeoutError()) == "TimeoutError"

    def test_wrapper_with_new_text_appends_original_type_and_message(self) -> None:
        try:
            try:
                raise TimeoutError("egress rule update exceeded 30s")
            except TimeoutError as e:
                raise SandboxSetupError("Failed to clear egress rules") from e
        except SandboxSetupError as wrapped:
            message = _exception_message(wrapped)
        assert message == (
            "SandboxSetupError: Sandbox error: Failed to clear egress rules "
            "(caused by TimeoutError: egress rule update exceeded 30s)"
        )

    def test_cause_type_survives_when_wrapper_contains_its_message(self) -> None:
        try:
            try:
                raise TimeoutError("provider unavailable")
            except TimeoutError as e:
                raise SandboxError(str(e)) from e
        except SandboxError as wrapped:
            message = _exception_message(wrapped)
        assert message == (
            "SandboxError: Sandbox error: provider unavailable (caused by TimeoutError: provider unavailable)"
        )

    def test_implicit_context_survives(self) -> None:
        try:
            try:
                raise ValueError("bad task input")
            except ValueError:
                raise RuntimeError("setup failed")
        except RuntimeError as wrapped:
            message = _exception_message(wrapped)
        assert message == "RuntimeError: setup failed (during handling of ValueError: bad task input)"

    def test_suppressed_context_stays_suppressed(self) -> None:
        try:
            try:
                raise ValueError("hidden detail")
            except ValueError:
                raise RuntimeError("public detail") from None
        except RuntimeError as wrapped:
            message = _exception_message(wrapped)
        assert message == "RuntimeError: public detail"

    def test_explicit_cause_takes_precedence_over_context(self) -> None:
        exc = RuntimeError("setup failed")
        exc.__context__ = ValueError("unrelated context")
        exc.__cause__ = TimeoutError("provider unavailable")
        assert _exception_message(exc) == "RuntimeError: setup failed (caused by TimeoutError: provider unavailable)"

    def test_self_referential_cause_chain_terminates(self) -> None:
        exc = RuntimeError("stream closed")
        exc.__cause__ = exc
        assert _exception_message(exc) == "RuntimeError: stream closed"

    def test_context_cycle_terminates(self) -> None:
        exc = RuntimeError("outer")
        cause = ValueError("inner")
        exc.__context__ = cause
        cause.__context__ = exc
        assert _exception_message(exc) == "RuntimeError: outer (during handling of ValueError: inner)"


class TestTaskExecution:
    """Task monitoring and tracked task state transitions."""

    _test_org = Org(id=TEST_ORG_ID, name="default")

    @pytest.mark.parametrize("shed_first", [False, True])
    async def test_shed_preserves_waiting_direct_tasks(
        self,
        shed_first: bool,
        database_session: Session,
        example_benchmark_object: Benchmark,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority: Any,
    ) -> None:
        benchmark = example_benchmark_object
        benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": 3})
        database_session.add(benchmark)
        database_session.commit()
        authority = executor_authority(benchmark, session=database_session)
        tasks = [
            Task(
                org_id=TEST_ORG_ID,
                benchmark=benchmark.id,
                task_id=f"task-{index}",
                started_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index),
            )
            for index in range(4)
        ]
        database_session.add_all(tasks)
        database_session.commit()
        monkeypatch.setattr(task_execution, "engine", database_session.get_bind())
        started: list[str] = []
        ready = asyncio.Event()

        async def inline_admission(function: Callable[..., Any], *args: Any) -> Any:
            return function(*args)

        monkeypatch.setattr(task_execution, "to_thread", inline_admission)

        async def prepare(task: Task) -> dict[str, dict[str, Any] | None]:
            async def retrieve() -> None:
                started.append(task.task_id)
                if len(started) == (1 if shed_first else 3):
                    ready.set()
                await asyncio.Event().wait()

            recovery = MagicMock(retrieve_task=retrieve)
            return await getattr(task_execution, "_process_task_attempt")(
                task,
                MagicMock(),
                MagicMock(),
                benchmark.id,
                task.task_id,
                MagicMock(),
                MagicMock(),
                self._test_org,
                MagicMock(),
                asyncio.Semaphore(1),
                getattr(task_execution, "_DependencySetupRecoveryState")(),
                recovery,
                authority,
            )

        if shed_first:
            assert apply_shed_benchmark(benchmark, 1, database_session, self._test_org) == []
            database_session.commit()

        runners = [asyncio.create_task(prepare(task)) for task in tasks]
        try:
            await asyncio.wait_for(ready.wait(), timeout=3)
            database_session.expire_all()
            statuses = {task.task_id: task.status for task in database_session.exec(select(Task)).all()}
            assert list(statuses.values()).count(TaskStatus.BUILDING) == (1 if shed_first else 3)
            assert statuses["task-3"] == TaskStatus.PENDING
            if not shed_first:
                assert apply_shed_benchmark(benchmark, 1, database_session, self._test_org) == ["task-2", "task-1"]
                database_session.commit()
        finally:
            for runner in runners:
                runner.cancel()
            await asyncio.gather(*runners, return_exceptions=True)

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
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority: Any,
    ) -> None:
        """The monitor removes completed work and cancels invalid attempts once."""
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        stopped_row = Task(
            org_id=TEST_ORG_ID,
            task_id="stopped",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
        )
        superseded_row = Task(org_id=TEST_ORG_ID, task_id="superseded", benchmark=benchmark_row.id)
        database_session.add_all([stopped_row, superseded_row])
        database_session.commit()

        done = TrackedTask(asyncio.sleep(0), self._test_org, authority, stopped_row.started_at)
        stopped = TrackedTask(asyncio.sleep(0), self._test_org, authority, stopped_row.started_at)
        superseded = TrackedTask(asyncio.sleep(0), self._test_org, authority, superseded_row.started_at)
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
            authority=authority,
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

    def _monitor_for_task(
        self,
        database_session: Session,
        benchmark_row: Benchmark,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
        *,
        status: TaskStatus,
    ) -> tuple[TaskMonitor, TrackedTask, Mock, ExecutionAuthority]:
        database_session.add(benchmark_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)
        task_row = Task(org_id=TEST_ORG_ID, task_id="monitored", benchmark=benchmark_row.id, status=status)
        database_session.add(task_row)
        database_session.commit()

        tracked = TrackedTask(asyncio.sleep(0), self._test_org, authority, task_row.started_at)
        setattr(tracked, "_status", TrackedTaskStatus.RUNNING)
        cancellation = Mock()
        setattr(tracked, "_task", Mock(cancel=cancellation, done=lambda: False))
        monkeypatch.setattr(task_execution, "engine", database_session.bind)
        monitor = TaskMonitor(
            benchmark_row.id,
            {"monitored": tracked},
            org=self._test_org,
            limiter=None,
            authority=authority,
        )

        return monitor, tracked, cancellation, authority

    @pytest.mark.parametrize("failure_stage", ["authority", "state"])
    async def test_task_monitor_retries_operational_error_without_cancelling(
        self,
        failure_stage: str,
        database_session: Session,
        example_benchmark_object: Benchmark,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monitor, tracked, cancellation, _authority = self._monitor_for_task(
            database_session,
            example_benchmark_object,
            executor_authority,
            monkeypatch,
            status=TaskStatus.STOPPED,
        )
        owner: Any = task_execution
        name = "lock_execution_authority" if failure_stage == "authority" else "fetch_benchmark_row"
        original = getattr(owner, name)
        failed = False

        def fail_once(*args: Any, **kwargs: Any) -> Any:
            nonlocal failed
            if not failed:
                failed = True
                raise OperationalError("SELECT", {}, Exception("connection lost"))

            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, fail_once)
        sleep_count = 0

        async def finish_after_recovery(_delay: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                cancellation.assert_not_called()
            elif sleep_count == 2:
                cancellation.assert_called_once()
                setattr(tracked, "_status", TrackedTaskStatus.DONE)

        monkeypatch.setattr(task_execution.asyncio, "sleep", finish_after_recovery)

        try:
            await monitor.track_tasks()
        finally:
            getattr(tracked, "_coro").close()

        assert failed
        assert sleep_count == 3
        cancellation.assert_called_once()

    async def test_task_monitor_revoked_authority_cancels_once(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monitor, tracked, cancellation, authority = self._monitor_for_task(
            database_session,
            example_benchmark_object,
            executor_authority,
            monkeypatch,
            status=TaskStatus.IN_PROGRESS,
        )
        dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FINISHED
        database_session.add(dispatch)
        database_session.commit()
        sleep_count = 0

        async def finish_after_cancellation(_delay: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            cancellation.assert_called_once()
            if sleep_count == 2:
                setattr(tracked, "_status", TrackedTaskStatus.DONE)

        monkeypatch.setattr(task_execution.asyncio, "sleep", finish_after_cancellation)

        try:
            await monitor.track_tasks()
        finally:
            getattr(tracked, "_coro").close()

        assert sleep_count == 3
        cancellation.assert_called_once()

    async def test_task_monitor_unexpected_db_error_propagates(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monitor, tracked, cancellation, _authority = self._monitor_for_task(
            database_session,
            example_benchmark_object,
            executor_authority,
            monkeypatch,
            status=TaskStatus.STOPPED,
        )

        def fail_read(_benchmark_id: Any, _session: Session, _org: Org) -> None:
            raise ValueError("invalid benchmark state")

        monkeypatch.setattr(task_execution, "fetch_benchmark_row", fail_read)

        try:
            with pytest.raises(ValueError, match="invalid benchmark state"):
                await monitor.track_tasks()
        finally:
            getattr(tracked, "_coro").close()

        cancellation.assert_not_called()

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
        authority = ExecutionAuthority(benchmark_id=uuid4(), dispatch_id=uuid4())
        running = TrackedTask(
            controlled_result("task_id_1", running_started, release_running),
            self._test_org,
            authority,
            running_row.started_at,
        )
        waiting = TrackedTask(
            controlled_result("task_id_2", waiting_started, release_waiting),
            self._test_org,
            authority,
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
