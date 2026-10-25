"""Admission leases share controlled polling and recover after cancellation or errors.

Run: pytest tests/unit/test_admission_turns.py
"""

import asyncio
from collections.abc import Mapping
from datetime import datetime
from typing import Protocol
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from tracker.scheduler import admission

_ATTEMPT = datetime(2026, 10, 16)
_DISPATCH = UUID(int=1)


class PreparedWaiter(Protocol):
    """Readiness exposed to the database-selection stub."""

    ready: bool


class PollClock:
    """Advance one dispatcher interval only when the test requests it."""

    def __init__(self) -> None:
        self.ticks: asyncio.Queue[None] = asyncio.Queue()
        self.rounds: asyncio.Queue[None] = asyncio.Queue()

    async def sleep(self, _context: admission.SandboxQueueContext) -> None:
        self.rounds.put_nowait(None)
        await self.ticks.get()

    async def wait(self) -> None:
        await asyncio.wait_for(self.rounds.get(), timeout=5)

    async def advance(self) -> None:
        self.ticks.put_nowait(None)
        await self.wait()


@pytest.fixture
def poll_clock(monkeypatch: pytest.MonkeyPatch) -> PollClock:
    clock = PollClock()
    monkeypatch.setattr(admission, "_poll_sleep", clock.sleep)

    return clock


@pytest.fixture
def turn_context(monkeypatch: pytest.MonkeyPatch) -> admission.SandboxQueueContext:
    snapshot = admission.CapacitySnapshot(capacities={}, read_at=_ATTEMPT, fetched_at=0)
    context = admission.SandboxQueueContext(
        provider=Mock(),
        pool_id="pool.reserved",
        engine=Mock(),
        capacity_cache=Mock(get=AsyncMock(return_value=snapshot)),
    )

    # Database selection is covered by the PostgreSQL tests; these tests own leases.
    def waiting(
        _context: admission.SandboxQueueContext,
        waiters: Mapping[UUID, object],
    ) -> tuple[set[UUID], set[UUID], int | None]:
        return set(waiters), set(waiters), None

    def ready(
        _context: admission.SandboxQueueContext,
        waiters: Mapping[UUID, PreparedWaiter],
        _snapshot: admission.CapacitySnapshot,
        _budget: int | None,
        slots: int,
    ) -> list[UUID]:
        return [task_id for task_id, waiter in waiters.items() if waiter.ready][:slots]

    monkeypatch.setattr(context.admission_turns, "_waiting", waiting)
    monkeypatch.setattr(context.admission_turns, "_ready", ready)

    return context


class TestAdmissionTurns:
    """Bounded lease ownership, fair retries, and observable termination."""

    async def test_stalled_leases_bound_dispatch_and_cancellation_restores_slots(
        self,
        turn_context: admission.SandboxQueueContext,
        poll_clock: PollClock,
    ) -> None:
        existing_tasks = asyncio.all_tasks()
        manager = turn_context.admission_turns
        entered: asyncio.Queue[UUID] = asyncio.Queue()
        hold = asyncio.Event()

        async def contender(task_id: UUID) -> None:
            async with manager.turn(turn_context, task_id, _ATTEMPT, _DISPATCH, ready=True) as turn:
                assert turn is not None
                assert turn.snapshot is not None
                entered.put_nowait(task_id)
                await hold.wait()

        runners = {UUID(int=index): asyncio.create_task(contender(UUID(int=index))) for index in range(1, 22)}
        try:
            await poll_clock.wait()
            first = {await asyncio.wait_for(entered.get(), timeout=5) for _ in range(10)}
            assert len(first) == 10
            assert entered.empty()

            for _ in range(3):
                await poll_clock.advance()
                assert entered.empty()

            for task_id in first:
                runners[task_id].cancel()
            await asyncio.gather(*(runners[task_id] for task_id in first), return_exceptions=True)
            await poll_clock.advance()

            second = {await asyncio.wait_for(entered.get(), timeout=5) for _ in range(10)}
            assert len(second) == 10
            assert first.isdisjoint(second)
            assert entered.empty()
        finally:
            for runner in runners.values():
                runner.cancel()
            await asyncio.gather(*runners.values(), return_exceptions=True)

        # Cancelling both leased and parked contenders must leave the manager reusable.
        retry = asyncio.create_task(contender(UUID(int=1)))
        try:
            await poll_clock.wait()
            poll_clock.ticks.put_nowait(None)
            assert await asyncio.wait_for(entered.get(), timeout=5) == UUID(int=1)
        finally:
            retry.cancel()
            await asyncio.gather(retry, return_exceptions=True)

        assert asyncio.all_tasks() == existing_tasks

    async def test_rejected_rechecks_rotate_while_ready_leases_are_held(
        self,
        turn_context: admission.SandboxQueueContext,
        poll_clock: PollClock,
    ) -> None:
        manager = turn_context.admission_turns
        entered: asyncio.Queue[tuple[UUID, bool]] = asyncio.Queue()
        retried: asyncio.Queue[None] = asyncio.Queue()
        hold = asyncio.Event()
        ready_ids = [UUID(int=index) for index in range(1, 21)]
        rejected_ids = [UUID(int=index) for index in range(21, 46)]

        async def contender(task_id: UUID, ready: bool) -> None:
            while True:
                async with manager.turn(turn_context, task_id, _ATTEMPT, _DISPATCH, ready=ready) as turn:
                    assert turn is not None
                    assert (turn.snapshot is not None) is ready
                    entered.put_nowait((task_id, ready))
                    if ready:
                        await hold.wait()
                retried.put_nowait(None)

        runners = [asyncio.create_task(contender(task_id, True)) for task_id in ready_ids]
        runners += [asyncio.create_task(contender(task_id, False)) for task_id in rejected_ids]
        try:
            await poll_clock.wait()
            initial = [await asyncio.wait_for(entered.get(), timeout=5) for _ in range(10)]
            assert all(ready for _, ready in initial)

            rejected_seen: set[UUID] = set()
            for _ in range(3):
                await poll_clock.advance()
                batch = [await asyncio.wait_for(entered.get(), timeout=5) for _ in range(10)]
                assert not any(ready for _, ready in batch)
                rejected_seen.update(task_id for task_id, _ in batch)
                for _ in batch:
                    await asyncio.wait_for(retried.get(), timeout=5)
            assert rejected_seen == set(rejected_ids)
            assert entered.empty()
        finally:
            for runner in runners:
                runner.cancel()
            await asyncio.gather(*runners, return_exceptions=True)

    async def test_single_contender_retry_waits_for_the_next_tick(
        self,
        turn_context: admission.SandboxQueueContext,
        poll_clock: PollClock,
    ) -> None:
        async def attempt() -> bool:
            async with turn_context.admission_turns.turn(
                turn_context,
                UUID(int=1),
                _ATTEMPT,
                _DISPATCH,
                ready=True,
            ) as turn:
                return turn is not None

        assert await asyncio.wait_for(attempt(), timeout=5)

        retry = asyncio.create_task(attempt())
        try:
            await poll_clock.wait()
            assert not retry.done()

            poll_clock.ticks.put_nowait(None)
            assert await asyncio.wait_for(retry, timeout=5)
        finally:
            retry.cancel()
            await asyncio.gather(retry, return_exceptions=True)

    @pytest.mark.parametrize("outcome", ["revoked", "error"])
    async def test_invalid_waiters_finish_and_errors_propagate_unchanged(
        self,
        outcome: str,
        turn_context: admission.SandboxQueueContext,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = RuntimeError("database unavailable")
        waiting = Mock(return_value=(set(), set(), None), side_effect=error if outcome == "error" else None)
        monkeypatch.setattr(turn_context.admission_turns, "_waiting", waiting)

        async def contender(index: int) -> bool:
            async with turn_context.admission_turns.turn(
                turn_context,
                UUID(int=index),
                _ATTEMPT,
                _DISPATCH,
                ready=True,
            ) as turn:
                return turn is not None

        results = await asyncio.wait_for(
            asyncio.gather(*(contender(index) for index in range(1, 4)), return_exceptions=True),
            timeout=5,
        )
        if outcome == "error":
            assert all(result is error for result in results)
        else:
            assert results == [False, False, False]
