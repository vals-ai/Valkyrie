from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import cast

import pytest
from benchmark_service import DaytonaProviderConfig, Sandbox, SandboxConnectionError
from redis.asyncio import Redis

from tracker.scheduler.admission import DaytonaQueueContext, create_daytona_queue_context, enter_queued_sandbox
from tracker.scheduler.capacity import (
    CapacityObservationUnavailableError,
    CapacitySnapshot,
    ImpossibleResourceDemandError,
    InvalidCapacityObservationError,
    ResourceVector,
)
from tracker.scheduler.gate import QueueTicket, RedisQueueGate


class FakeGate:
    def __init__(self) -> None:
        self.waited = 0
        self.left = 0
        self.locked = False

    async def join(self, _ticket: QueueTicket) -> None:
        pass

    async def touch(self, _ticket: QueueTicket) -> None:
        pass

    async def leave(self, _ticket: QueueTicket) -> None:
        assert not self.locked
        self.left += 1

    async def wait(self) -> None:
        self.waited += 1

    @asynccontextmanager
    async def start_turn(self, _ticket: QueueTicket) -> AsyncGenerator[bool]:
        self.locked = True
        try:
            yield True
        finally:
            self.locked = False


class FakeSandbox:
    id = "sandbox-id"
    name = "sandbox-name"


def _provider_config() -> DaytonaProviderConfig:
    return DaytonaProviderConfig(
        DAYTONA_API_KEY="secret",
        DAYTONA_API_URL="https://daytona.example",
        DAYTONA_TARGET="us",
    )


def _context(gate: FakeGate) -> DaytonaQueueContext:
    return DaytonaQueueContext(
        gate=cast(RedisQueueGate, gate),
        pool_key="pool",
        organization_id="org",
        priority=3,
        provider_config=_provider_config(),
    )


def _ticket() -> QueueTicket:
    return QueueTicket(pool_key="pool", task_key="attempt", priority=3, enqueued_at=datetime(2026, 1, 1, tzinfo=UTC))


def _snapshot(cpu_available: int, *, cpu_total: int | None = None) -> CapacitySnapshot:
    total = cpu_available if cpu_total is None else cpu_total
    return CapacitySnapshot(
        total=ResourceVector(cpu_millis=total),
        used=ResourceVector(cpu_millis=total - cpu_available),
    )


def test_builds_context_from_existing_provider_config_without_exposing_key() -> None:
    context = create_daytona_queue_context(
        redis=cast(Redis, object()),
        provider_config=_provider_config(),
        organization_id="org",
        priority=3,
    )

    assert context.pool_key.startswith("daytona:")
    assert context.organization_id == "org"
    assert context.priority == 3
    assert "secret" not in repr(context)


def _factory(
    gate: FakeGate,
    events: list[str],
    failure: Exception | None = None,
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def sandbox() -> AsyncGenerator[Sandbox]:
        assert gate.locked
        events.append("create")
        if failure:
            raise failure
        try:
            yield cast(Sandbox, FakeSandbox())
        finally:
            events.append("cleanup")

    return sandbox


async def _enter(
    stack: AsyncExitStack,
    gate: FakeGate,
    events: list[str],
    *,
    demand: ResourceVector,
    stopped: Callable[[], bool],
    mark_building: Callable[[], bool] = lambda: True,
    failure: Exception | None = None,
) -> Sandbox | None:
    return await enter_queued_sandbox(
        stack=stack,
        context=_context(gate),
        ticket=_ticket(),
        demand=demand,
        create=_factory(gate, events, failure),
        stopped=stopped,
        mark_building=mark_building,
    )


@pytest.mark.asyncio
async def test_waits_for_capacity_and_releases_lock_after_create(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = FakeGate()
    observations = iter([_snapshot(0, cpu_total=1000), _snapshot(1000)])
    events: list[str] = []

    async def observe(**_kwargs: str) -> CapacitySnapshot:
        return next(observations)

    monkeypatch.setattr("tracker.scheduler.admission.observe_daytona_capacity", observe)
    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, gate, events, demand=ResourceVector(cpu_millis=1000), stopped=lambda: False)
        assert sandbox is not None
        assert not gate.locked
        assert events == ["create"]

    assert events == ["create", "cleanup"]
    assert gate.waited == 1
    assert gate.left == 1


@pytest.mark.asyncio
async def test_impossible_demand_fails_without_blocking_or_creating(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = FakeGate()
    events: list[str] = []

    async def observe(**_kwargs: str) -> CapacitySnapshot:
        return _snapshot(1000)

    monkeypatch.setattr("tracker.scheduler.admission.observe_daytona_capacity", observe)
    with pytest.raises(ImpossibleResourceDemandError, match="exceeds total Daytona capacity"):
        async with AsyncExitStack() as stack:
            await _enter(stack, gate, events, demand=ResourceVector(cpu_millis=2000), stopped=lambda: gate.waited > 0)

    assert events == []
    assert gate.waited == 0
    assert gate.left == 1


@pytest.mark.asyncio
async def test_transient_observation_falls_back_to_create_and_cleans_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = FakeGate()
    events: list[str] = []

    async def unavailable(**_kwargs: str) -> CapacitySnapshot:
        raise CapacityObservationUnavailableError

    monkeypatch.setattr("tracker.scheduler.admission.observe_daytona_capacity", unavailable)
    with pytest.raises(SandboxConnectionError, match="capacity"):
        async with AsyncExitStack() as stack:
            await _enter(
                stack,
                gate,
                events,
                demand=ResourceVector(cpu_millis=1000),
                stopped=lambda: False,
                failure=SandboxConnectionError("capacity"),
            )

    assert events == ["create"]
    assert gate.waited == 0
    assert gate.left == 1


@pytest.mark.asyncio
async def test_rechecks_task_ownership_after_acquiring_the_start_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = FakeGate()
    ownership_checks = iter([False, True])

    async def unexpected_observation(**_kwargs: str) -> CapacitySnapshot:
        raise AssertionError("lost task ownership must prevent capacity observation")

    monkeypatch.setattr("tracker.scheduler.admission.observe_daytona_capacity", unexpected_observation)
    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            gate,
            [],
            demand=ResourceVector(cpu_millis=1000),
            stopped=lambda: next(ownership_checks),
        )

    assert sandbox is None
    assert gate.left == 1


@pytest.mark.asyncio
async def test_cleans_created_sandbox_when_building_transition_loses_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = FakeGate()
    events: list[str] = []

    async def observe(**_kwargs: str) -> CapacitySnapshot:
        return _snapshot(1000)

    monkeypatch.setattr("tracker.scheduler.admission.observe_daytona_capacity", observe)
    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            gate,
            events,
            demand=ResourceVector(cpu_millis=1000),
            stopped=lambda: False,
            mark_building=lambda: False,
        )
        assert sandbox is None
        assert events == ["create"]

    assert events == ["create", "cleanup"]
    assert gate.left == 1
    assert gate.waited == 0


@pytest.mark.asyncio
async def test_stop_and_invalid_capacity_leave_the_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = FakeGate()

    async def invalid(**_kwargs: str) -> CapacitySnapshot:
        raise InvalidCapacityObservationError

    monkeypatch.setattr("tracker.scheduler.admission.observe_daytona_capacity", invalid)
    async with AsyncExitStack() as stack:
        assert await _enter(stack, gate, [], demand=ResourceVector(), stopped=lambda: True) is None
    assert gate.left == 1

    with pytest.raises(InvalidCapacityObservationError):
        async with AsyncExitStack() as stack:
            await _enter(stack, gate, [], demand=ResourceVector(), stopped=lambda: False)
    assert gate.left == 2
