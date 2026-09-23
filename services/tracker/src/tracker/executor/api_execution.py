"""Hold dispatch authority inside the executor process without database access."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

import httpx

from executor_protocol import DEFAULT_EXECUTOR_DISPATCH_HEARTBEAT_INTERVAL_SECONDS
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.schemas import ClaimRequest, LeaseResponse

logger = logging.getLogger(__name__)


def _lease_deadline(lease: LeaseResponse, request_started_at: float) -> float:
    # Count response latency against the lease; never rely on the executor's wall clock.
    remaining = (lease.lease_expires_at - lease.server_time).total_seconds()

    return request_started_at + max(remaining, 0)


async def _renew_lease(api: ExecutorClient, deadline: float, interval_seconds: float) -> None:
    loop = asyncio.get_running_loop()
    delay = interval_seconds
    while True:
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.sleep(delay)
                started_at = loop.time()
                lease = await api.heartbeat()
        except (httpx.TransportError, TimeoutError):
            if loop.time() >= deadline:
                raise ExecutionAuthorityRevoked("Executor dispatch lease expired during Tracker outage") from None
            logger.warning("Tracker heartbeat unavailable; retaining the last confirmed lease")
            delay = min(interval_seconds, 1)
            continue
        except httpx.HTTPStatusError as error:
            if error.response.status_code in (502, 503, 504):
                delay = min(interval_seconds, 1)
                continue
            if error.response.status_code in (401, 403, 409):
                raise ExecutionAuthorityRevoked("Tracker revoked this executor dispatch") from error
            raise

        if loop.time() >= deadline:
            raise ExecutionAuthorityRevoked("Executor dispatch lease expired before renewal was confirmed")
        deadline = _lease_deadline(lease, started_at)
        delay = interval_seconds


async def _settle_task(task: asyncio.Task[None]) -> None:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except Exception:
            logger.exception("Executor task failed while settling cancellation")
            break
    # Retrieve failures without masking the original execution or cancellation exception.
    await asyncio.gather(task, return_exceptions=True)


async def run_with_dispatch_lease(
    api: ExecutorClient,
    claim: ClaimRequest,
    operation: Callable[[], Awaitable[None]],
    *,
    heartbeat_interval_seconds: float = DEFAULT_EXECUTOR_DISPATCH_HEARTBEAT_INTERVAL_SECONDS,
) -> None:
    """Claim before starting work, retain it through transient outages, and settle cleanup on cancellation.

    The caller finishes the dispatch after this returns. Revocation never triggers a failure write here.
    """
    if heartbeat_interval_seconds <= 0:
        raise ValueError("Heartbeat interval must be positive")
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    lease = await api.claim(claim)
    deadline = _lease_deadline(lease, started_at)
    if loop.time() >= deadline:
        raise ExecutionAuthorityRevoked("Executor dispatch lease expired before execution could start")

    async def execute() -> None:
        await operation()

    work = asyncio.create_task(execute())
    renewal = asyncio.create_task(_renew_lease(api, deadline, heartbeat_interval_seconds))
    try:
        done, _ = await asyncio.wait((work, renewal), return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            await work
        else:
            await renewal
    finally:
        if not work.done():
            work.cancel()
        await _settle_task(work)
        renewal.cancel()
        await _settle_task(renewal)
