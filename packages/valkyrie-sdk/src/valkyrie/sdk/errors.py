"""Exceptions raised by the Valkyrie SDK."""

import logging
from collections.abc import AsyncIterator, Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

import httpx

logger = logging.getLogger(__name__)
Params = ParamSpec("Params")
StreamItem = TypeVar("StreamItem")


class ValkyrieSDKError(Exception):
    """Base class for all SDK errors."""


class ValkyrieConfigError(ValkyrieSDKError):
    """The supplied Valkyrie configuration is missing or invalid."""


class ValkyrieRunError(ValkyrieSDKError):
    """A run operation received invalid client-side input."""


class ValkyrieTransportError(ValkyrieSDKError):
    """A request could not reach the Valkyrie service."""


class ValkyrieAPIError(ValkyrieSDKError):
    """The Valkyrie API returned a non-success response."""

    def __init__(self, status_code: int, detail: Any):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Valkyrie API request failed ({status_code}): {detail}")


class ValkyrieStreamError(ValkyrieSDKError):
    """A run update stream returned an error or invalid event."""


def handle_httpx_stream_errors(
    message: str,
) -> Callable[[Callable[Params, AsyncIterator[StreamItem]]], Callable[Params, AsyncIterator[StreamItem]]]:
    """Log and translate HTTPX failures from an async stream."""

    def decorator(
        function: Callable[Params, AsyncIterator[StreamItem]],
    ) -> Callable[Params, AsyncIterator[StreamItem]]:
        @wraps(function)
        async def wrapped(*args: Params.args, **kwargs: Params.kwargs) -> AsyncIterator[StreamItem]:
            try:
                async for item in function(*args, **kwargs):
                    yield item
            except httpx.HTTPError as exc:
                logger.warning("%s: %s", message, exc)
                raise ValkyrieTransportError(f"{message}: {exc}") from exc

        return wrapped

    return decorator
