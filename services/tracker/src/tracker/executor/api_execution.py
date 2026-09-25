"""Hold dispatch authority inside the executor process without database access."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

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


@dataclass
class _Lease:
    deadline: float


async def _observe_lease(api: ExecutorClient, lease: _Lease, interval_seconds: float) -> None:
    loop = asyncio.get_running_loop()
    delay = interval_seconds
    while True:
        try:
            async with asyncio.timeout_at(lease.deadline):
                await asyncio.sleep(delay)
                started_at = loop.time()
                authority = await api.authority()
                if not authority.current:
                    raise ExecutionAuthorityRevoked("Tracker revoked this executor dispatch")

                if authority.lease_expires_at is None or authority.server_time is None:
                    # Older Tracker versions expose only the boolean authority response.
                    renewed = await api.heartbeat()
                    lease.deadline = _lease_deadline(renewed, started_at)
                    delay = interval_seconds
                    continue

                lease.deadline = started_at + max(
                    (authority.lease_expires_at - authority.server_time).total_seconds(),
                    0,
                )
                if loop.time() >= lease.deadline:
                    raise ExecutionAuthorityRevoked("Executor dispatch lease expired before authority was confirmed")
                delay = interval_seconds
        except (httpx.TransportError, TimeoutError):
            if loop.time() >= lease.deadline:
                raise ExecutionAuthorityRevoked("Executor dispatch lease expired during Tracker outage") from None
            logger.warning("Tracker authority unavailable; retaining the last confirmed lease")
            delay = min(interval_seconds, 1)
        except httpx.HTTPStatusError as error:
            if error.response.status_code in (502, 503, 504):
                delay = min(interval_seconds, 1)
                continue
            if error.response.status_code in (401, 403, 409):
                raise ExecutionAuthorityRevoked("Tracker revoked this executor dispatch") from error
            raise


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
    claimed = await api.claim(claim)
    lease = _Lease(_lease_deadline(claimed, started_at))
    if loop.time() >= lease.deadline:
        raise ExecutionAuthorityRevoked("Executor dispatch lease expired before execution could start")

    async def execute() -> None:
        await operation()

    with api.retry_until(lambda: lease.deadline):
        work = asyncio.create_task(execute())
        renewal = asyncio.create_task(_observe_lease(api, lease, heartbeat_interval_seconds))
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
