"""Shared exceptions and error-handling decorators used by the CLI and tracker service."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar


class TrackerServiceError(Exception):
    """Base exception for all tracker service errors."""

    pass


class S3Error(TrackerServiceError):
    """Exception raised for S3 storage operation errors."""

    def __str__(self) -> str:
        return "S3 error: " + super().__str__()


_F = TypeVar("_F", bound=Callable[..., Any])


def handle_s3_error(message: str) -> Callable[[_F], _F]:
    """Decorator that catches botocore S3 errors and re-raises as S3Error.

    Handles both sync and async functions. The botocore import is deferred
    so that tracker-shared does not require botocore as a declared dependency.
    """

    def decorator(func: _F) -> _F:
        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                from botocore.exceptions import BotoCoreError, ClientError

                try:
                    return await func(*args, **kwargs)
                except (ClientError, BotoCoreError) as e:
                    raise S3Error(f"{message}: {e}") from e

            return async_wrapper  # type: ignore[return-value]
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                from botocore.exceptions import BotoCoreError, ClientError

                try:
                    return func(*args, **kwargs)
                except (ClientError, BotoCoreError) as e:
                    raise S3Error(f"{message}: {e}") from e

            return sync_wrapper  # type: ignore[return-value]

    return decorator
