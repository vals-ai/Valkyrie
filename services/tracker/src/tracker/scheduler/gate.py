"""Small Redis-backed priority gate for sandbox creation."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import cast

from redis.asyncio import Redis
from redis.asyncio.lock import Lock
from redis.exceptions import LockNotOwnedError

_REMOVE_STALE_HEAD = """
if redis.call('EXISTS', KEYS[2]) == 0 then
    return redis.call('ZREM', KEYS[1], ARGV[1])
end
return 0
"""

_RENEW_TICKET = """
local member = redis.call('GET', KEYS[3])
if not member then
    if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
        member = ARGV[1]
    else
        local order = redis.call('INCR', KEYS[4])
        member = string.format('~%020d|%s', order, ARGV[1])
    end
    redis.call('SET', KEYS[3], member)
end
local was_member = redis.call('ZSCORE', KEYS[1], member)
if not was_member then
    redis.call('ZADD', KEYS[1], ARGV[2], member)
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[3], ARGV[5])
redis.call('EXPIRE', KEYS[4], ARGV[6])
"""

_QUEUE_KEY = re.compile(r"^sandbox-queue:([0-9a-f]{24}):p([0-4])$")


@dataclass(frozen=True, slots=True)
class QueueTicket:
    pool_key: str
    task_key: str
    priority: int
    enqueued_at: datetime


@dataclass(frozen=True, slots=True)
class QueueSnapshotEntry:
    pool_id: str
    task_key: str
    priority: int
    enqueued_at: datetime


class RedisQueueGate:
    def __init__(
        self,
        redis: Redis,
        *,
        ticket_ttl_seconds: int = 120,
        poll_interval_seconds: float = 1.0,
        start_lock_seconds: int = 480,
    ) -> None:
        self._redis = redis
        self._ticket_ttl_seconds = ticket_ttl_seconds
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._start_lock_seconds = start_lock_seconds

    async def join(self, ticket: QueueTicket) -> None:
        await self.touch(ticket)

    async def touch(self, ticket: QueueTicket) -> None:
        """Atomically renew a ticket, restoring lost membership at its original score."""
        prefix = self._prefix(ticket.pool_key)
        queue_key = self._queue_key(prefix, ticket.priority)
        await cast(
            Awaitable[object],
            self._redis.eval(
                _RENEW_TICKET,
                4,
                queue_key,
                self._heartbeat_key_for(prefix, ticket.task_key),
                self._member_key_for(queue_key, ticket.task_key),
                self._sequence_key(queue_key),
                ticket.task_key,
                repr(ticket.enqueued_at.timestamp()),
                str(self._ticket_ttl_seconds),
                str(self._ticket_ttl_seconds * 2),
                str(self._ticket_ttl_seconds * 3),
                str(self._ticket_ttl_seconds * 4),
            ),
        )

    async def is_next(self, ticket: QueueTicket) -> bool:
        prefix = self._prefix(ticket.pool_key)
        for priority in range(5):
            queue_key = self._queue_key(prefix, priority)
            while raw_heads := await cast(
                Awaitable[list[bytes | str]],
                self._redis.zrange(queue_key, 0, 0),
            ):
                raw_head = raw_heads[0]
                head = self._decode_member(raw_head)
                heartbeat = self._heartbeat_key_for(prefix, head)
                if await self._redis.exists(heartbeat):
                    return head == ticket.task_key
                await cast(
                    Awaitable[object],
                    self._redis.eval(_REMOVE_STALE_HEAD, 2, queue_key, heartbeat, raw_head),
                )
        return False

    async def leave(self, ticket: QueueTicket) -> None:
        prefix = self._prefix(ticket.pool_key)
        queue_key = self._queue_key(prefix, ticket.priority)
        member_key = self._member_key_for(queue_key, ticket.task_key)
        raw_member = await cast(
            Awaitable[bytes | str | None],
            self._redis.get(member_key),
        )
        member = ticket.task_key if raw_member is None else self._decode(raw_member)
        await self._redis.zrem(queue_key, member, ticket.task_key)
        await self._redis.delete(
            self._heartbeat_key_for(prefix, ticket.task_key),
            member_key,
        )

    async def snapshot(self) -> list[QueueSnapshotEntry]:
        """Return live tickets ordered exactly as the gate sees them."""
        candidates: list[tuple[str, str, int, str, str, float]] = []
        keys = cast(
            AsyncIterator[bytes | str],
            self._redis.scan_iter(match="sandbox-queue:*:p[0-4]"),
        )
        async for raw_key in keys:
            queue_key = self._decode(raw_key)
            match = _QUEUE_KEY.fullmatch(queue_key)
            if match is None:
                continue
            prefix = queue_key.rsplit(":", 1)[0]
            pool_id = f"pool_{match.group(1)}"
            priority = int(match.group(2))
            raw_entries = await cast(
                Awaitable[list[tuple[bytes | str, float]]],
                self._redis.zrange(queue_key, 0, -1, withscores=True),
            )
            for raw_member, raw_score in raw_entries:
                score = float(raw_score)
                if math.isfinite(score):
                    member = self._decode(raw_member)
                    candidates.append(
                        (
                            prefix,
                            pool_id,
                            priority,
                            member,
                            self._decode_member(member),
                            score,
                        )
                    )
        if not candidates:
            return []

        pipe = self._redis.pipeline(transaction=False)
        for prefix, _pool_id, _priority, _member, task_key, _score in candidates:
            pipe.exists(self._heartbeat_key_for(prefix, task_key))
        states = await pipe.execute()

        ranked: list[tuple[tuple[str, int, float, str], QueueSnapshotEntry]] = []
        for index, (_prefix, pool_id, priority, member, task_key, score) in enumerate(candidates):
            heartbeat_exists = states[index]
            if not heartbeat_exists:
                continue
            try:
                enqueued_at = datetime.fromtimestamp(score, UTC)
            except (OSError, OverflowError, ValueError):
                continue
            ranked.append(
                (
                    (pool_id, priority, score, member),
                    QueueSnapshotEntry(
                        pool_id=pool_id,
                        task_key=task_key,
                        priority=priority,
                        enqueued_at=enqueued_at,
                    ),
                )
            )

        seen: set[tuple[str, str]] = set()
        snapshot: list[QueueSnapshotEntry] = []
        for _rank, entry in sorted(ranked, key=lambda item: item[0]):
            identity = (entry.pool_id, entry.task_key)
            if identity not in seen:
                seen.add(identity)
                snapshot.append(entry)
        return snapshot

    @asynccontextmanager
    async def start_turn(self, ticket: QueueTicket) -> AsyncGenerator[bool]:
        if not await self.is_next(ticket):
            yield False
            return

        lock = self._redis.lock(
            f"{self._prefix(ticket.pool_key)}:start",
            timeout=self._start_lock_seconds,
            blocking=False,
        )
        acquired = await lock.acquire(blocking=False)
        if not acquired:
            yield False
            return

        try:
            if not await self.is_next(ticket):
                yield False
                return

            owner = cast(asyncio.Task[object], asyncio.current_task())
            maintenance = asyncio.create_task(self._maintain_ticket(ticket, lock, owner))
            try:
                async with asyncio.timeout(self._start_lock_seconds * 0.9):
                    yield True
            finally:
                maintenance.cancel()
                with suppress(asyncio.CancelledError):
                    await maintenance
        finally:
            try:
                await lock.release()
            except LockNotOwnedError:
                pass

    async def wait(self) -> None:
        await asyncio.sleep(self._poll_interval_seconds)

    async def _maintain_ticket(self, ticket: QueueTicket, lock: Lock, owner: asyncio.Task[object]) -> None:
        try:
            while True:
                await asyncio.sleep(min(self._ticket_ttl_seconds, self._start_lock_seconds) / 3)
                await lock.extend(self._start_lock_seconds, replace_ttl=True)
                await self.touch(ticket)
        except Exception:
            owner.cancel()
            raise

    @staticmethod
    def _prefix(pool_key: str) -> str:
        return f"sandbox-queue:{sha256(pool_key.encode()).hexdigest()[:24]}"

    @staticmethod
    def _queue_key(prefix: str, priority: int) -> str:
        return f"{prefix}:p{priority}"

    @staticmethod
    def _sequence_key(queue_key: str) -> str:
        return f"{queue_key}:sequence"

    @classmethod
    def _member_key_for(cls, queue_key: str, task_key: str) -> str:
        return f"{queue_key}:member:{cls._task_hash(task_key)}"

    @classmethod
    def _heartbeat_key_for(cls, prefix: str, task_key: str) -> str:
        return f"{prefix}:ticket:{cls._task_hash(task_key)}"

    @staticmethod
    def _task_hash(task_key: str) -> str:
        return sha256(task_key.encode()).hexdigest()[:24]

    @staticmethod
    def _decode(value: bytes | str) -> str:
        return value.decode() if isinstance(value, bytes) else value

    @classmethod
    def _decode_member(cls, value: bytes | str) -> str:
        member = cls._decode(value)
        prefix, separator, task_key = member.partition("|")
        if separator and len(prefix) == 21 and prefix.startswith("~") and prefix[1:].isdigit():
            return task_key
        return member
