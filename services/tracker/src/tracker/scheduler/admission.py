"""Provider-neutral sandbox admission built on the Redis queue gate."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field

from benchmark_service import (
    Resources,
    Sandbox,
    SandboxProvider,
    SandboxSource,
)
from redis.asyncio import Redis

from tracker.scheduler.gate import QueueTicket, RedisQueueGate

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]


@dataclass(frozen=True, slots=True)
class SandboxQueueContext:
    gate: RedisQueueGate
    pool_id: str
    priority: int
    provider: SandboxProvider = field(repr=False)


def create_queue_context(
    *,
    redis: Redis,
    provider: SandboxProvider,
    priority: int,
) -> SandboxQueueContext:
    pool_id = provider.admission_pool_id
    if pool_id is None:
        raise ValueError("Sandbox provider does not support queued admission")
    return SandboxQueueContext(
        gate=RedisQueueGate(redis),
        pool_id=pool_id,
        priority=priority,
        provider=provider,
    )


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    ticket: QueueTicket,
    source: SandboxSource,
    resources: Resources,
    create: SandboxFactory,
    mark_building: Callable[[], bool],
) -> Sandbox | None:
    """Wait for one serialized start turn and enter the sandbox cleanup context."""
    gate = context.gate
    await gate.join(ticket)
    try:
        while True:
            await gate.touch(ticket)
            async with gate.start_turn(ticket) as admitted:
                if admitted:
                    if await context.provider.check_admission(source, resources):
                        if not mark_building():
                            return None
                        return await stack.enter_async_context(create())
            await gate.wait()
    finally:
        await gate.leave(ticket)
