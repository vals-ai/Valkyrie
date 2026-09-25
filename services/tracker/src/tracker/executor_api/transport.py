"""Bounded HTTP retries shared by executor API versions."""

import asyncio
from collections.abc import Callable, Generator
from contextlib import contextmanager

import httpx
from pydantic import SecretStr
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, stop_never, wait_random_exponential


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
        self._retry_deadline: Callable[[], float] | None = None

    @contextmanager
    def retry_until(self, deadline: Callable[[], float]) -> Generator[None]:
        """Retry transient failures within a confirmed monotonic lease; settle callers before leaving."""
        previous = self._retry_deadline
        self._retry_deadline = deadline
        try:
            yield
        finally:
            self._retry_deadline = previous

    async def post(self, path: str, payload: dict[str, object]) -> httpx.Response:
        retry_deadline = self._retry_deadline
        loop = asyncio.get_running_loop()

        def can_retry(error: BaseException) -> bool:
            return _retryable(error) or (
                isinstance(error, TimeoutError) and retry_deadline is not None and loop.time() < retry_deadline()
            )

        async with asyncio.timeout(30 if retry_deadline is None else None):
            async for attempt in AsyncRetrying(
                retry=retry_if_exception(can_retry),
                wait=wait_random_exponential(multiplier=0.1, max=2),
                stop=stop_after_attempt(5) if retry_deadline is None else stop_never,
                reraise=True,
            ):
                with attempt:
                    deadline = retry_deadline() if retry_deadline is not None else None
                    if deadline is not None and loop.time() >= deadline:
                        raise TimeoutError("Executor lease expired before the Tracker request")
                    async with asyncio.timeout_at(deadline):
                        response = await self._client.post(
                            path,
                            json=payload,
                            headers={"Authorization": f"Bearer {self._token.get_secret_value()}"},
                            timeout=10,
                            follow_redirects=False,
                        )
                    if retry_deadline is not None and loop.time() >= retry_deadline():
                        raise TimeoutError("Executor lease expired before the Tracker response was confirmed")
                    response.raise_for_status()

                    return response

        raise AssertionError("Retry loop exited without a response or exception")
