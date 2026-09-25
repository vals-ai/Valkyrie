"""Executor-owned lease and cancellation tests with an HTTP boundary.

Run: uv run pytest tests/unit/executor/test_api_execution.py
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr
from tenacity import wait_none

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.api_execution import run_with_dispatch_lease
from tracker.executor_api import transport
from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.schemas import ClaimRequest

_DISPATCH = UUID("11111111-1111-1111-1111-111111111111")
_CLAIM = ClaimRequest(
    claimant_id=UUID("22222222-2222-2222-2222-222222222222"),
    benchmark_id=UUID("33333333-3333-3333-3333-333333333333"),
    executor_release_id="lease-test",
    executor_artifact_uri="s3://artifacts/lease-test.pex",
    executor_artifact_digest="a" * 64,
    executor_protocol_version="4",
)


def _lease(seconds: float = 60) -> httpx.Response:
    server_time = datetime(2000, 1, 1, tzinfo=UTC)

    return httpx.Response(
        200,
        json={
            "dispatch_id": str(_DISPATCH),
            "claimant_id": str(_CLAIM.claimant_id),
            "server_time": server_time.isoformat(),
            "lease_expires_at": (server_time + timedelta(seconds=seconds)).isoformat(),
        },
    )


def _authority(seconds: float | None = None) -> httpx.Response:
    server_time = datetime(2000, 1, 1, tzinfo=UTC)
    body: dict[str, object] = {"current": True}
    if seconds is not None:
        body.update(
            {
                "server_time": server_time.isoformat(),
                "lease_expires_at": (server_time + timedelta(seconds=seconds)).isoformat(),
            }
        )

    return httpx.Response(200, json=body)


@pytest.mark.parametrize("status", [401, 409, 200])
async def test_rejected_or_expired_claim_never_starts_work(status: int) -> None:
    """Do not launch after duplicate delivery, invalid credentials, or an exhausted claim.

    Test cases:
    - Rejected claims propagate without invoking the operation.
    - An expired lease cannot start work even when the claim returned HTTP 200.
    """
    started = False

    async def execute() -> None:
        nonlocal started
        started = True

    boundary = httpx.MockTransport(lambda _request: _lease(0) if status == 200 else httpx.Response(status))
    async with httpx.AsyncClient(transport=boundary, base_url="http://tracker.test") as http:
        api = ExecutorClient(ExecutorTransport(http, SecretStr("test")), _DISPATCH, _CLAIM.claimant_id)
        with pytest.raises((httpx.HTTPStatusError, ExecutionAuthorityRevoked)):
            await run_with_dispatch_lease(api, _CLAIM, execute)

    assert not started


@pytest.mark.parametrize("failure", ["unavailable", "connection", "timeout"])
async def test_transient_heartbeat_outage_preserves_running_work(failure: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the same operation alive through failed authority observations and recovery.

    Test cases:
    - HTTP failures, connection failures, and request timeouts retain the last confirmed lease.
    - Database timestamps far from the executor clock still permit execution.
    """

    def no_retry_delay(**_kwargs: float) -> wait_none:
        return wait_none()

    monkeypatch.setattr(transport, "wait_random_exponential", no_retry_delay)
    renewed = asyncio.Event()
    failures = 0
    completed = False

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal failures
        if request.url.path.endswith("claim"):
            return _lease()
        if failures < 5:
            failures += 1
            if failure == "connection":
                raise httpx.ConnectError("Tracker restarting", request=request)
            if failure == "timeout":
                raise TimeoutError("Tracker request timed out")
            return httpx.Response(503)
        renewed.set()
        return _authority(60)

    async def execute() -> None:
        nonlocal completed
        await renewed.wait()
        completed = True

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://tracker.test") as http:
        api = ExecutorClient(ExecutorTransport(http, SecretStr("test")), _DISPATCH, _CLAIM.claimant_id)
        async with asyncio.timeout(5):
            await run_with_dispatch_lease(api, _CLAIM, execute, heartbeat_interval_seconds=0.01)

    assert completed and failures == 5


async def test_authority_observation_uses_host_renewal_without_pex_heartbeat() -> None:
    """Let the stable host renew a lease and the PEX observe its server deadline.

    Test cases:
    - A current authority response extends the short claim lease.
    - The PEX does not send a competing heartbeat when timestamps are available.
    """
    authority_checks = 0
    heartbeat_checks = 0
    observation = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal authority_checks, heartbeat_checks
        if request.url.path.endswith("claim"):
            return _lease(0.1)
        if request.url.path.endswith("authority"):
            authority_checks += 1
            observation.set()
            return _authority(60)
        if request.url.path.endswith("heartbeat"):
            heartbeat_checks += 1
            return _lease()
        return httpx.Response(404)

    async def execute() -> None:
        await observation.wait()
        await asyncio.sleep(0.15)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://tracker.test") as http:
        api = ExecutorClient(ExecutorTransport(http, SecretStr("test")), _DISPATCH, _CLAIM.claimant_id)
        async with asyncio.timeout(5):
            await run_with_dispatch_lease(api, _CLAIM, execute, heartbeat_interval_seconds=0.01)

    assert authority_checks >= 1
    assert heartbeat_checks == 0


async def test_authority_without_lease_timestamps_keeps_legacy_heartbeat() -> None:
    """Retain lease renewal when an older Tracker returns only boolean authority.

    Test cases:
    - A timestamp-free authority response triggers the compatibility heartbeat.
    - The operation continues after that heartbeat confirms the lease.
    """
    heartbeat_seen = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("claim"):
            return _lease()
        if request.url.path.endswith("authority"):
            return _authority()
        if request.url.path.endswith("heartbeat"):
            heartbeat_seen.set()
            return _lease()
        return httpx.Response(404)

    async def execute() -> None:
        await heartbeat_seen.wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://tracker.test") as http:
        api = ExecutorClient(ExecutorTransport(http, SecretStr("test")), _DISPATCH, _CLAIM.claimant_id)
        async with asyncio.timeout(5):
            await run_with_dispatch_lease(api, _CLAIM, execute, heartbeat_interval_seconds=0.01)

    assert heartbeat_seen.is_set()


@pytest.mark.parametrize("failure", ["revoked", "hung", "unavailable", "late", "unexpected"])
async def test_revocation_or_lease_expiry_settles_cleanup(failure: str) -> None:
    """Cancel execution when a lease expires, even while heartbeat HTTP is hung.

    Test cases:
    - An explicit revocation cancels the active operation immediately.
    - A hung or unavailable Tracker cannot extend the last confirmed lease.
    - A response arriving after expiry cannot restore authority.
    - An unexpected server error remains visible instead of being treated as renewal.
    - The caller sees revocation only after operation cleanup settles.
    """
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("claim"):
            return _lease(0.2)
        await started.wait()
        if failure in ("hung", "late"):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if failure == "late":
                    return _lease()
                raise
        if failure == "unexpected":
            return httpx.Response(500)
        return httpx.Response(409 if failure == "revoked" else 503)

    async def execute() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://tracker.test") as http:
        api = ExecutorClient(ExecutorTransport(http, SecretStr("test")), _DISPATCH, _CLAIM.claimant_id)
        async with asyncio.timeout(5):
            expected = httpx.HTTPStatusError if failure == "unexpected" else ExecutionAuthorityRevoked
            with pytest.raises(expected):
                await run_with_dispatch_lease(api, _CLAIM, execute, heartbeat_interval_seconds=0.01)

    assert started.is_set() and cleaned.is_set()


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_repeated_cancellation_waits_for_cleanup(cleanup_fails: bool) -> None:
    """Repeated shutdown requests must not abandon operation cleanup.

    Test cases:
    - A second cancellation leaves cleanup running until it finishes.
    - Cancellation remains visible to the caller after cleanup.
    - A cleanup failure does not replace the original cancellation.
    """
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleaned = asyncio.Event()

    async def execute() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release_cleanup.wait()
            cleaned.set()
            if cleanup_fails:
                raise OSError("Provider cleanup failed")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: _lease()), base_url="http://tracker.test"
    ) as http:
        api = ExecutorClient(ExecutorTransport(http, SecretStr("test")), _DISPATCH, _CLAIM.claimant_id)
        runner = asyncio.create_task(run_with_dispatch_lease(api, _CLAIM, execute))
        async with asyncio.timeout(5):
            await started.wait()
            runner.cancel()
            await cleaning.wait()
            runner.cancel()
            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await runner

    assert cleaned.is_set()
