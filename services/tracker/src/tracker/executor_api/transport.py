"""Bounded HTTP retries shared by executor API versions."""

import asyncio

import httpx
from pydantic import SecretStr
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_random_exponential


def _retryable(error: BaseException) -> bool:
    if isinstance(error, httpx.TransportError):
        return True

    return isinstance(error, httpx.HTTPStatusError) and error.response.status_code in (502, 503, 504)


class ExecutorTransport:
    """Retry only replay-safe requests; caller supplies a scoped credential and client.

    The client must use the configured Tracker origin. Redirects never receive executor credentials.
    """

    def __init__(self, client: httpx.AsyncClient, token: SecretStr) -> None:
        self._client = client
        self._token = token

    async def post(self, path: str, payload: dict[str, object]) -> httpx.Response:
        async with asyncio.timeout(30):
            async for attempt in AsyncRetrying(
                retry=retry_if_exception(_retryable),
                wait=wait_random_exponential(multiplier=0.1, max=2),
                stop=stop_after_attempt(5),
                reraise=True,
            ):
                with attempt:
                    response = await self._client.post(
                        path,
                        json=payload,
                        headers={"Authorization": f"Bearer {self._token.get_secret_value()}"},
                        timeout=10,
                        follow_redirects=False,
                    )
                    response.raise_for_status()

                    return response

        raise AssertionError("Retry loop exited without a response or exception")
