"""Capacity-aware Daytona sandbox admission built on the Redis queue gate."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field

from benchmark_service import DaytonaProviderConfig, Sandbox
from redis.asyncio import Redis

from tracker.scheduler.capacity import (
    CapacityObservationUnavailableError,
    ImpossibleResourceDemandError,
    ResourceVector,
    daytona_pool_key,
    observe_daytona_capacity,
)
from tracker.scheduler.gate import QueueTicket, RedisQueueGate

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]


@dataclass(frozen=True, slots=True)
class DaytonaQueueContext:
    gate: RedisQueueGate
    pool_key: str
    organization_id: str
    priority: int
    provider_config: DaytonaProviderConfig = field(repr=False)


def create_daytona_queue_context(
    *,
    redis: Redis,
    provider_config: DaytonaProviderConfig,
    organization_id: str,
    priority: int,
) -> DaytonaQueueContext:
    return DaytonaQueueContext(
        gate=RedisQueueGate(redis),
        pool_key=daytona_pool_key(
            organization_id=organization_id,
            target=provider_config.DAYTONA_TARGET,
            api_url=provider_config.DAYTONA_API_URL,
        ),
        organization_id=organization_id,
        priority=priority,
        provider_config=provider_config,
    )


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: DaytonaQueueContext,
    ticket: QueueTicket,
    demand: ResourceVector,
    create: SandboxFactory,
    stopped: Callable[[], bool],
    mark_building: Callable[[], bool],
) -> Sandbox | None:
    """Wait for one serialized start turn and enter the sandbox cleanup context."""
    gate = context.gate
    await gate.join(ticket)
    try:
        while not stopped():
            await gate.touch(ticket)
            async with gate.start_turn(ticket) as admitted:
                if admitted:
                    if stopped():
                        return None
                    can_create = False
                    try:
                        capacity = await observe_daytona_capacity(
                            organization_id=context.organization_id,
                            target=context.provider_config.DAYTONA_TARGET,
                            api_url=context.provider_config.DAYTONA_API_URL,
                            api_key=context.provider_config.DAYTONA_API_KEY,
                        )
                    except CapacityObservationUnavailableError:
                        can_create = True
                    else:
                        if not capacity.fits_total(demand):
                            raise ImpossibleResourceDemandError("Sandbox demand exceeds total Daytona capacity")
                        can_create = capacity.fits(demand)

                    if can_create:
                        sandbox = await stack.enter_async_context(create())
                        if not mark_building():
                            return None
                        return sandbox
            await gate.wait()
        return None
    finally:
        await gate.leave(ticket)
