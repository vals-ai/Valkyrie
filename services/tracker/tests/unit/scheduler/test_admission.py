"""Tests for sandbox queue admission.

Run: pytest tests/unit/scheduler/test_admission.py

Covers provider selection and queued sandbox lifecycle behavior.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import cast

import pytest
from benchmark_service import ImageSource, Resources, Sandbox, SandboxError, SandboxProvider, SandboxSource
from redis.asyncio import Redis

from tracker.scheduler.admission import SandboxQueueContext, create_queue_context, enter_queued_sandbox
from tracker.scheduler.gate import QueueTicket, RedisQueueGate


class MockGate:
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
        assert not self.locked
        self.waited += 1

    @asynccontextmanager
    async def start_turn(self, _ticket: QueueTicket) -> AsyncGenerator[bool]:
        self.locked = True
        try:
            yield True
        finally:
            self.locked = False


class MockProvider:
    def __init__(
        self,
        responses: list[bool | Exception],
        events: list[str],
        *,
        pool_id: str | None = "pool",
    ) -> None:
        self._responses = iter(responses)
        self._events = events
        self._pool_id = pool_id
        self.requests: list[tuple[SandboxSource, Resources]] = []

    @property
    def admission_pool_id(self) -> str | None:
        return self._pool_id

    async def check_admission(
        self,
        source: SandboxSource,
        resources: Resources,
    ) -> bool:
        response = next(self._responses)
        self.requests.append((source, resources))
        if isinstance(response, Exception):
            self._events.append("check:error")
            raise response
        self._events.append(f"check:{response}")

        return response


def _source() -> SandboxSource:
    return ImageSource(image="sandbox-image")


def _resources() -> Resources:
    return Resources(vcpu=1, memory=2, disk=3)


def _ticket() -> QueueTicket:
    return QueueTicket(pool_key="pool", task_key="attempt", priority=3, enqueued_at=datetime(2026, 1, 1, tzinfo=UTC))


def _context(gate: MockGate, provider: MockProvider) -> SandboxQueueContext:
    return SandboxQueueContext(
        gate=cast(RedisQueueGate, gate),
        pool_id="pool",
        priority=3,
        provider=cast(SandboxProvider, provider),
    )


def _factory(
    gate: MockGate,
    events: list[str],
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def sandbox() -> AsyncGenerator[Sandbox]:
        assert gate.locked
        events.append("create")
        try:
            yield cast(Sandbox, object())
        finally:
            events.append("cleanup")

    return sandbox


async def _enter(
    stack: AsyncExitStack,
    gate: MockGate,
    provider: MockProvider,
    events: list[str],
    *,
    mark_building: Callable[[], bool] = lambda: True,
) -> Sandbox | None:
    return await enter_queued_sandbox(
        stack=stack,
        context=_context(gate, provider),
        ticket=_ticket(),
        source=_source(),
        resources=_resources(),
        create=_factory(gate, events),
        mark_building=mark_building,
    )


class TestCreateQueueContext:
    """Provider-specific queue setup."""

    def test_selects_provider_pool_and_rejects_queue_disabled_provider(self) -> None:
        provider = MockProvider([], [])

        context = create_queue_context(
            redis=cast(Redis, object()),
            provider=cast(SandboxProvider, provider),
            priority=3,
        )

        assert context.pool_id == "pool"
        assert context.provider is provider

        with pytest.raises(ValueError, match="does not support queued admission"):
            create_queue_context(
                redis=cast(Redis, object()),
                provider=cast(SandboxProvider, MockProvider([], [], pool_id=None)),
                priority=3,
            )


class TestEnterQueuedSandbox:
    """Queued sandbox lifecycle behavior."""

    async def test_waits_then_marks_building_before_create_and_defers_cleanup(self) -> None:
        gate = MockGate()
        events: list[str] = []
        provider = MockProvider([False, True], events)

        def mark_building() -> bool:
            events.append("mark_building")

            return True

        async with AsyncExitStack() as stack:
            sandbox = await _enter(stack, gate, provider, events, mark_building=mark_building)

            assert sandbox is not None
            assert events == ["check:False", "check:True", "mark_building", "create"]
            assert not gate.locked

        assert events == ["check:False", "check:True", "mark_building", "create", "cleanup"]
        assert provider.requests == [(_source(), _resources()), (_source(), _resources())]
        assert gate.waited == 1
        assert gate.left == 1

    async def test_provider_error_creates_nothing_and_removes_ticket(self) -> None:
        gate = MockGate()
        events: list[str] = []
        provider = MockProvider([SandboxError("capacity request failed")], events)

        with pytest.raises(SandboxError, match="capacity request failed"):
            async with AsyncExitStack() as stack:
                await _enter(stack, gate, provider, events)

        assert events == ["check:error"]
        assert gate.left == 1

    async def test_failed_building_transition_creates_nothing_and_removes_ticket(self) -> None:
        gate = MockGate()
        events: list[str] = []
        provider = MockProvider([True], events)

        def mark_building() -> bool:
            events.append("mark_building")

            return False

        async with AsyncExitStack() as stack:
            sandbox = await _enter(stack, gate, provider, events, mark_building=mark_building)

        assert sandbox is None
        assert events == ["check:True", "mark_building"]
        assert gate.left == 1
