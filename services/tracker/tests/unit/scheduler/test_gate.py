from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from redis.asyncio import Redis

from tracker.scheduler.gate import QueueTicket, RedisQueueGate


class FakeLock:
    def __init__(self, locks: set[str], name: str, extensions: list[tuple[float, bool]]) -> None:
        self._locks = locks
        self._name = name
        self._extensions = extensions
        self._owned = False

    async def acquire(self, *, blocking: bool = True) -> bool:
        del blocking
        if self._name in self._locks:
            return False
        self._locks.add(self._name)
        self._owned = True
        return True

    async def release(self) -> None:
        if self._owned:
            self._locks.remove(self._name)
            self._owned = False

    async def extend(self, additional_time: float, *, replace_ttl: bool = False) -> bool:
        assert self._owned
        self._extensions.append((additional_time, replace_ttl))
        return True


class FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._operations: list[tuple[str, str]] = []

    def exists(self, name: str) -> FakePipeline:
        self._operations.append(("exists", name))
        return self

    async def execute(self) -> list[str | int | None]:
        results: list[str | int | None] = []
        for _operation, name in self._operations:
            results.append(await self._redis.exists(name))
        return results


class FakeRedis:
    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {}
        self.values: set[str] = set()
        self.strings: dict[str, str] = {}
        self.expirations: dict[str, int] = {}
        self.locks: set[str] = set()
        self.lock_extensions: list[tuple[float, bool]] = []
        self.on_missing_exists: Callable[[], Awaitable[None]] | None = None

    async def exists(self, name: str) -> int:
        exists = name in self.values
        if not exists and self.on_missing_exists is not None:
            callback = self.on_missing_exists
            self.on_missing_exists = None
            await callback()
        return int(exists)

    async def delete(self, *names: str) -> int:
        removed = 0
        for name in names:
            removed += int(name in self.values or name in self.strings)
            self.values.discard(name)
            self.strings.pop(name, None)
            self.expirations.pop(name, None)
        return removed

    async def zrem(self, name: str, *members: str | bytes) -> int:
        target = self.zsets.setdefault(name, {})
        removed = 0
        for raw_member in members:
            member = raw_member.decode() if isinstance(raw_member, bytes) else raw_member
            removed += int(member in target)
            target.pop(member, None)
        return removed

    async def zrange(
        self,
        name: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[str] | list[tuple[str, float]]:
        ordered = sorted(self.zsets.setdefault(name, {}).items(), key=lambda item: (item[1], item[0]))
        stop = None if end == -1 else end + 1
        selected = ordered[start:stop]
        return selected if withscores else [member for member, _score in selected]

    async def zscore(self, name: str, member: str) -> float | None:
        return self.zsets.setdefault(name, {}).get(member)

    async def get(self, name: str) -> str | None:
        return self.strings.get(name)

    async def scan_iter(self, *, match: str) -> AsyncIterator[str]:
        del match
        for name in self.zsets:
            yield name

    async def eval(
        self,
        _script: str,
        number_of_keys: int,
        *values: str | bytes,
    ) -> int:
        if number_of_keys == 4:
            queue, heartbeat, member_key, sequence_key = cast(tuple[str, str, str, str], values[:4])
            task_key, score, heartbeat_ttl, queue_ttl, member_ttl, sequence_ttl = cast(
                tuple[str, str, str, str, str, str],
                values[4:],
            )
            member = self.strings.get(member_key)
            if member is None:
                if await self.zscore(queue, task_key) is not None:
                    member = task_key
                else:
                    order = int(self.strings.get(sequence_key, "0")) + 1
                    self.strings[sequence_key] = str(order)
                    member = f"~{order:020d}|{task_key}"
                self.strings[member_key] = member
            was_member = await self.zscore(queue, member) is not None
            if not was_member:
                self.zsets.setdefault(queue, {})[member] = float(score)
            self.values.add(heartbeat)
            self.expirations[heartbeat] = int(heartbeat_ttl)
            self.expirations[queue] = int(queue_ttl)
            self.expirations[member_key] = int(member_ttl)
            self.expirations[sequence_key] = int(sequence_ttl)
            return int(was_member)
        queue, heartbeat = cast(tuple[str, str], values[:2])
        (member,) = values[2:]
        if heartbeat in self.values:
            return 0
        return await self.zrem(queue, member)

    def pipeline(self, *, transaction: bool) -> FakePipeline:
        del transaction
        return FakePipeline(self)

    def lock(self, name: str, *, timeout: int, blocking: bool) -> FakeLock:
        del timeout, blocking
        return FakeLock(self.locks, name, self.lock_extensions)

    def lose_heartbeats(self) -> None:
        self.values.clear()


NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _ticket(task: str, priority: int, seconds: int = 0) -> QueueTicket:
    return QueueTicket(pool_key="pool", task_key=task, priority=priority, enqueued_at=NOW + timedelta(seconds=seconds))


def _gate(
    redis: FakeRedis,
    *,
    ticket_ttl_seconds: int = 120,
    poll_interval_seconds: float = 1.0,
    start_lock_seconds: int = 480,
) -> RedisQueueGate:
    return RedisQueueGate(
        cast(Redis, redis),
        ticket_ttl_seconds=ticket_ttl_seconds,
        poll_interval_seconds=poll_interval_seconds,
        start_lock_seconds=start_lock_seconds,
    )


@pytest.mark.asyncio
async def test_orders_strictly_by_priority_then_fifo() -> None:
    redis = FakeRedis()
    gate = _gate(redis)
    tickets = [_ticket("p4", 4), _ticket("later-p0", 0, 2), _ticket("p3", 3), _ticket("first-p0", 0, 1)]
    for ticket in tickets:
        await gate.join(ticket)

    expected = [tickets[3], tickets[1], tickets[2], tickets[0]]
    for ticket in expected:
        assert await gate.is_next(ticket)
        await gate.leave(ticket)


@pytest.mark.asyncio
async def test_preserves_join_order_when_fifo_timestamps_tie() -> None:
    redis = FakeRedis()
    gate = _gate(redis)
    first = _ticket("z-task", 2)
    second = _ticket("a-task", 2)

    await gate.join(first)
    await gate.join(second)

    assert await gate.is_next(first)
    await gate.leave(first)
    assert await gate.is_next(second)


@pytest.mark.asyncio
async def test_join_touch_and_leave_preserve_score_and_expiry() -> None:
    redis = FakeRedis()
    gate = _gate(redis, ticket_ttl_seconds=90)
    original = _ticket("z-task", 1, 7)
    follower = _ticket("a-task", 1, 7)
    duplicate = _ticket("z-task", 1, 60)
    await gate.join(original)
    await gate.join(follower)
    await gate.join(duplicate)

    queue_key = next(
        key for key, members in redis.zsets.items() if any(member.endswith("|z-task") for member in members)
    )
    heartbeat_key = next(key for key in redis.values if ":ticket:" in key)
    original_member_key = next(
        key for key, member in redis.strings.items() if ":member:" in key and member.endswith("|z-task")
    )
    original_member = redis.strings[original_member_key]
    assert redis.zsets[queue_key][original_member] == original.enqueued_at.timestamp()
    assert redis.expirations[queue_key] > redis.expirations[heartbeat_key]
    await gate.touch(original)

    redis.lose_heartbeats()
    await gate.touch(follower)
    redis.zsets[queue_key].pop(original_member)
    await gate.touch(original)
    assert redis.zsets[queue_key][original_member] == original.enqueued_at.timestamp()
    assert await gate.is_next(original)
    assert sum(len(items) for items in redis.zsets.values()) == 2

    await gate.leave(original)
    await gate.leave(original)
    assert not await gate.is_next(original)
    assert await gate.is_next(follower)
    assert original_member_key not in redis.strings


@pytest.mark.asyncio
async def test_snapshot_reports_live_tickets_in_priority_fifo_order() -> None:
    redis = FakeRedis()
    gate = _gate(redis)
    stale = _ticket("stale", 0, -1)
    urgent = _ticket("urgent", 0, 1)
    first = _ticket("z-task", 2)
    second = _ticket("a-task", 2)

    await gate.join(stale)
    stale_heartbeat = next(iter(redis.values))
    for ticket in (first, second, urgent):
        await gate.join(ticket)
    redis.values.remove(stale_heartbeat)

    snapshot = await gate.snapshot()

    assert [(entry.task_key, entry.priority, entry.enqueued_at) for entry in snapshot] == [
        (urgent.task_key, urgent.priority, urgent.enqueued_at),
        (first.task_key, first.priority, first.enqueued_at),
        (second.task_key, second.priority, second.enqueued_at),
    ]
    pool_ids = {entry.pool_id for entry in snapshot}
    assert len(pool_ids) == 1
    assert next(iter(pool_ids)).startswith("pool_")


@pytest.mark.asyncio
async def test_discards_stale_heads() -> None:
    redis = FakeRedis()
    gate = _gate(redis)
    stale = _ticket("stale", 0)
    live = _ticket("live", 1)
    await gate.join(stale)
    redis.lose_heartbeats()
    await gate.join(live)

    assert await gate.is_next(live)
    assert not await gate.is_next(stale)


@pytest.mark.asyncio
async def test_stale_cleanup_does_not_remove_a_concurrent_rejoin() -> None:
    redis = FakeRedis()
    gate = _gate(redis)
    ticket = _ticket("renewed", 0)
    await gate.join(ticket)
    redis.lose_heartbeats()

    async def rejoin() -> None:
        await gate.join(ticket)

    redis.on_missing_exists = rejoin

    assert await gate.is_next(ticket)
    assert sum(len(items) for items in redis.zsets.values()) == 1


@pytest.mark.asyncio
async def test_start_lock_requires_head_and_releases_on_every_exit() -> None:
    redis = FakeRedis()
    gate = _gate(redis)
    first = _ticket("first", 1)
    second = _ticket("second", 1, 1)
    await gate.join(first)
    await gate.join(second)

    async with gate.start_turn(second) as admitted:
        assert not admitted
    async with gate.start_turn(first) as admitted:
        assert admitted
        async with gate.start_turn(first) as duplicate:
            assert not duplicate
    async with gate.start_turn(first) as admitted_again:
        assert admitted_again

    with pytest.raises(RuntimeError, match="boom"):
        async with gate.start_turn(first) as admitted:
            assert admitted
            raise RuntimeError("boom")

    async with gate.start_turn(first) as admitted_again:
        assert admitted_again
    assert not redis.locks


@pytest.mark.asyncio
async def test_start_turn_renews_a_ticket_removed_while_create_is_running() -> None:
    redis = FakeRedis()
    gate = _gate(redis, ticket_ttl_seconds=1, start_lock_seconds=2)
    ticket = _ticket("creating", 0)
    follower = _ticket("follower", 1)
    await gate.join(ticket)

    async with gate.start_turn(ticket) as admitted:
        assert admitted
        redis.lose_heartbeats()
        await gate.join(follower)
        assert await gate.is_next(follower)
        await asyncio.sleep(0.4)
        assert await gate.is_next(ticket)


@pytest.mark.asyncio
async def test_start_turn_renews_lock_during_cancellation_resistant_work() -> None:
    redis = FakeRedis()
    gate = _gate(redis, ticket_ttl_seconds=1, start_lock_seconds=1)
    ticket = _ticket("slow-cleanup", 0)
    await gate.join(ticket)

    with pytest.raises(TimeoutError):
        async with gate.start_turn(ticket) as admitted:
            assert admitted
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(0.5)
                raise

    assert redis.lock_extensions
    assert set(redis.lock_extensions) == {(1, True)}
    assert not redis.locks
