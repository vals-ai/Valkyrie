"""Exercise executor HTTP retry and credential forwarding rules.

Run: uv run pytest tests/unit/executor_api/test_transport.py
Only the external HTTP transport is mocked.
"""

import asyncio

import httpx
import pytest
from pydantic import SecretStr

from tracker.executor_api.transport import ExecutorTransport


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
