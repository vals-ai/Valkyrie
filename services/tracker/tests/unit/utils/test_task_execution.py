"""Unit tests for tracked task scheduling and monitoring.

Run: uv run pytest tests/unit/utils/test_task_execution.py
"""

import asyncio
from typing import Any

import pytest

from tests.utils import TEST_ORG_ID
from tracker.database.models import Org
from tracker.executor.checkpoints import CheckpointCallback, run_with_checkpoints
from tracker.utils import ResizableLimiter


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


class TestEvaluationCheckpoints:
    """Async persistence of the benchmark client's synchronous checkpoint callbacks."""

    async def test_coalesces_burst_to_latest_pending_snapshot(self) -> None:
        """Keep only the in-flight write and newest pending replacement snapshot."""
        writing = asyncio.Event()
        release = asyncio.Event()
        burst_sent = asyncio.Event()
        saved: list[dict[str, Any]] = []

        async def evaluate(checkpoint: CheckpointCallback) -> int:
            checkpoint({"cursor": 0})
            await writing.wait()
            for position in range(1, 101):
                checkpoint({"cursor": position})
            burst_sent.set()
            return 42

        async def persist(state: dict[str, Any]) -> None:
            writing.set()
            await release.wait()
            saved.append(state)

        runner = asyncio.create_task(run_with_checkpoints(evaluate, persist))
        async with asyncio.timeout(5):
            await writing.wait()
            await burst_sent.wait()
            assert not runner.done()
            release.set()
            assert await runner == 42

        assert saved == [{"cursor": 0}, {"cursor": 100}]

    @pytest.mark.parametrize("stream_fails", [False, True])
    async def test_flushes_ordered_snapshots_before_returning(self, stream_fails: bool) -> None:
        """Persist snapshots in order before exposing completion or a stream error.

        Test cases:
        - Later mutation of the callback's dictionary cannot change a queued snapshot.
        - Completion waits for slow persistence without blocking the event loop.
        - A stream failure still flushes checkpoints before recovery begins.
        """
        writing = asyncio.Event()
        release = asyncio.Event()
        second_checkpoint_sent = asyncio.Event()
        saved: list[dict[str, Any]] = []

        async def evaluate(checkpoint: CheckpointCallback) -> int:
            state = {"cursor": {"position": 1}}
            checkpoint(state)
            await writing.wait()
            state["cursor"]["position"] = 2
            checkpoint(state)
            state["cursor"]["position"] = 3
            second_checkpoint_sent.set()
            if stream_fails:
                raise ConnectionError("Evaluation stream disconnected")
            return 42

        async def persist(state: dict[str, Any]) -> None:
            writing.set()
            await release.wait()
            saved.append(state)

        runner = asyncio.create_task(run_with_checkpoints(evaluate, persist))
        async with asyncio.timeout(5):
            await writing.wait()
            await second_checkpoint_sent.wait()
            assert not runner.done()
            release.set()
            if stream_fails:
                with pytest.raises(ConnectionError, match="Evaluation stream disconnected"):
                    await runner
            else:
                assert await runner == 42

        assert saved == [{"cursor": {"position": 1}}, {"cursor": {"position": 2}}]

    @pytest.mark.parametrize("stream_completes", [False, True])
    async def test_repeated_cancellation_flushes_accepted_checkpoints(self, stream_completes: bool) -> None:
        """Do not leave accepted checkpoints writing after task cleanup returns.

        Test cases:
        - Repeated cancellation settles the stream and the pending writer.
        - A completed stream still retains its pending checkpoint during cancellation.
        - The original cancellation remains visible after checkpoint persistence.
        """
        writing = asyncio.Event()
        stopped = asyncio.Event()
        release = asyncio.Event()
        saved: list[dict[str, Any]] = []

        async def evaluate(checkpoint: CheckpointCallback) -> None:
            checkpoint({"job_id": "durable-job"})
            try:
                if not stream_completes:
                    await asyncio.Event().wait()
            finally:
                stopped.set()

        async def persist(state: dict[str, Any]) -> None:
            writing.set()
            await release.wait()
            saved.append(state)

        runner = asyncio.create_task(run_with_checkpoints(evaluate, persist))
        async with asyncio.timeout(5):
            await writing.wait()
            if stream_completes:
                # Let the completed stream wake the runner while persistence remains blocked.
                await asyncio.sleep(0)
                await asyncio.sleep(0)
            runner.cancel()
            await stopped.wait()
            assert not runner.done()
            runner.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await runner

        assert saved == [{"job_id": "durable-job"}]

    async def test_persistence_failure_cancels_the_evaluation(self) -> None:
        """A failed checkpoint cannot silently turn into successful evaluation completion.

        Test cases:
        - Persistence failure interrupts the running stream.
        - Stream cleanup settles before the original persistence error propagates.
        """
        stopped = asyncio.Event()

        async def evaluate(checkpoint: CheckpointCallback) -> None:
            checkpoint({"job_id": "durable-job"})
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def persist(_state: dict[str, Any]) -> None:
            raise OSError("Checkpoint storage unavailable")

        async with asyncio.timeout(5):
            with pytest.raises(OSError, match="Checkpoint storage unavailable"):
                await run_with_checkpoints(evaluate, persist)

        assert stopped.is_set()
