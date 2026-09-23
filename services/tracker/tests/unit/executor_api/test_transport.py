"""Exercise executor HTTP retry and credential forwarding rules.

Run: uv run pytest tests/unit/executor_api/test_transport.py
Only the external HTTP transport is mocked.
"""

import asyncio

import httpx
import pytest
from pydantic import SecretStr
from tenacity import wait_none

from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api import transport


@pytest.mark.parametrize("status", [502, 503, 504])
async def test_retries_temporary_tracker_unavailability(status: int) -> None:
    """Recover a replay-safe request after a temporary gateway failure.

    Test cases:
    - A transient response is retried with the same body and dispatch credential.
    - The successful response reaches the caller.
    """
    responses = iter([status, 200])

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer dispatch-test-token"
        assert request.content == b'{"claimant_id":"test-process"}'

        return httpx.Response(next(responses), json={"current": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="https://tracker.test") as client:
        response = await ExecutorTransport(client, SecretStr("dispatch-test-token")).post(
            "/internal/executor/v1/dispatches/test/authority", {"claimant_id": "test-process"}
        )

    assert response.json() == {"current": True}


@pytest.mark.parametrize("status", [401, 403, 409, 422, 307])
async def test_does_not_retry_rejections_or_follow_redirects(status: int) -> None:
    """Keep rejection final and never forward credentials to a redirect destination.

    Test cases:
    - Auth, ownership, and contract errors reach the caller immediately.
    - Redirects are rejected even when the supplied HTTP client follows redirects.
    """
    responses = iter([status, 200])

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "tracker.test"

        return httpx.Response(next(responses), headers={"Location": "https://other.test/"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), base_url="https://tracker.test", follow_redirects=True
    ) as client:
        with pytest.raises(httpx.HTTPStatusError) as failure:
            await ExecutorTransport(client, SecretStr("dispatch-test-token")).post("/claim", {})

    assert failure.value.response.status_code == status


async def test_cancellation_interrupts_the_request() -> None:
    """Propagate process cancellation without retrying it as a network failure.

    Test cases:
    - A cancelled HTTP request stays cancelled at the executor boundary.
    """

    async def handle(_request: httpx.Request) -> httpx.Response:
        raise asyncio.CancelledError

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="https://tracker.test") as client:
        with pytest.raises(asyncio.CancelledError):
            await ExecutorTransport(client, SecretStr("dispatch-test-token")).post("/claim", {})


@pytest.fixture
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    def immediate_retry(**_kwargs: float) -> wait_none:
        return wait_none()

    monkeypatch.setattr(transport, "wait_random_exponential", immediate_retry)


@pytest.mark.usefixtures("no_retry_delay")
async def test_live_lease_retries_beyond_the_bootstrap_limit() -> None:
    """Keep a task write pending through an outage without changing its command payload.

    Test cases:
    - A lease-scoped write recovers after more than five failed attempts.
    - Leaving the lease scope restores the bounded bootstrap behavior.
    """
    responses = iter([503] * 7 + [200] + [503] * 5)

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.content == b'{"command_id":"original-command"}'
        return httpx.Response(next(responses), json={"revision": 1})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://tracker.test") as http:
        client = ExecutorTransport(http, SecretStr("test"))
        deadline = asyncio.get_running_loop().time() + 10
        with client.retry_until(lambda: deadline):
            result = await client.post("/write", {"command_id": "original-command"})
        assert result.json()["revision"] == 1
        with pytest.raises(httpx.HTTPStatusError):
            await client.post("/write", {"command_id": "original-command"})


@pytest.mark.usefixtures("no_retry_delay")
@pytest.mark.parametrize("expire", ["before", "retry", "response"])
async def test_expired_lease_cannot_send_or_accept_another_write(expire: str) -> None:
    """Enforce a changing lease deadline before retries and after delayed responses.

    Test cases:
    - An already expired lease sends nothing.
    - Expiry during failure handling prevents another request.
    - A late successful response does not grant further execution authority.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() - 1 if expire == "before" else loop.time() + 10

    def handle(_request: httpx.Request) -> httpx.Response:
        nonlocal deadline
        assert expire != "before"
        assert loop.time() < deadline
        deadline = loop.time() - 1
        return httpx.Response(503 if expire == "retry" else 200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://tracker.test") as http:
        client = ExecutorTransport(http, SecretStr("test"))
        with client.retry_until(lambda: deadline):
            with pytest.raises(TimeoutError):
                await client.post("/write", {})


@pytest.mark.usefixtures("no_retry_delay")
async def test_renewed_lease_extends_a_pending_command() -> None:
    """Retry a pending command when another heartbeat extended its original deadline.

    Test cases:
    - The command does not fail just because its original lease timer elapsed.
    - Retrying preserves the same command identity after renewal.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.1
    renewed = False

    async def handle(request: httpx.Request) -> httpx.Response:
        nonlocal deadline, renewed
        assert request.content == b'{"command_id":"pending-command"}'
        if not renewed:
            renewed = True
            deadline = loop.time() + 10
            await asyncio.Event().wait()
        return httpx.Response(200, json={"revision": 1})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://tracker.test") as http:
        client = ExecutorTransport(http, SecretStr("test"))
        with client.retry_until(lambda: deadline):
            async with asyncio.timeout(5):
                result = await client.post("/write", {"command_id": "pending-command"})

    assert result.json()["revision"] == 1
