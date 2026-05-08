"""Daytona retry helpers."""

import logging
from collections.abc import Callable, Mapping

from daytona.common.errors import DaytonaRateLimitError
from tenacity import RetryCallState
from tenacity.wait import wait_base

from tracker.observability.metrics import distribution, incr
from tracker.observability.retry import retry_callback

logger = logging.getLogger(__name__)

_RETRY_AFTER_PREFIX = "retry-after-"
_RATE_LIMIT_REMAINING_PREFIX = "x-ratelimit-remaining-"
_KNOWN_THROTTLERS = ("sandbox-create", "sandbox-lifecycle", "authenticated", "anonymous")


def _parse_retry_after_seconds(value: object) -> float | None:
    try:
        seconds = float(str(value))
    except (TypeError, ValueError):
        return None

    if seconds < 0:
        return None

    return seconds


def _get_header(headers: Mapping[str, object], header_name: str) -> object | None:
    header_name_lower = header_name.lower()
    for key, value in headers.items():
        if str(key).lower() == header_name_lower:
            return value

    return None


def daytona_rate_limit_throttler(exc: DaytonaRateLimitError) -> str:
    for key in exc.headers:
        lower_key = str(key).lower()
        if lower_key.startswith(_RETRY_AFTER_PREFIX):
            return lower_key.removeprefix(_RETRY_AFTER_PREFIX)
        if lower_key.startswith(_RATE_LIMIT_REMAINING_PREFIX):
            return lower_key.removeprefix(_RATE_LIMIT_REMAINING_PREFIX)

    return "unknown"


def _daytona_rate_limit_header(exc: DaytonaRateLimitError, prefix: str, throttler: str) -> object | None:
    if throttler == "unknown":
        return None

    return _get_header(exc.headers, f"{prefix}{throttler}")


def daytona_retry_after_seconds(exc: DaytonaRateLimitError) -> float | None:
    headers: Mapping[str, object] = exc.headers

    for throttler in _KNOWN_THROTTLERS:
        value = _get_header(headers, f"retry-after-{throttler}")
        seconds = _parse_retry_after_seconds(value)
        if seconds is not None:
            return seconds

    value = _get_header(headers, "retry-after")
    seconds = _parse_retry_after_seconds(value)
    if seconds is not None:
        return seconds

    for key, value in headers.items():
        if str(key).lower().startswith(_RETRY_AFTER_PREFIX):
            seconds = _parse_retry_after_seconds(value)
            if seconds is not None:
                return seconds

    return None


class wait_daytona_rate_limit(wait_base):
    def __init__(self, fallback: wait_base, *, max_retry_after_seconds: float = 60.0) -> None:
        self._fallback = fallback
        self._max_retry_after_seconds = max_retry_after_seconds

    def __call__(self, retry_state: RetryCallState) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, DaytonaRateLimitError):
            seconds = daytona_retry_after_seconds(exc)
            if seconds is not None:
                return min(seconds, self._max_retry_after_seconds)

        return self._fallback(retry_state)


def daytona_retry_callback(
    metric_name: str,
    *,
    op: str,
    log_level: int = logging.WARNING,
) -> Callable[[RetryCallState], None]:
    base_callback = retry_callback(metric_name, log_level=log_level)

    def _hook(state: RetryCallState) -> None:
        base_callback(state)

        exc = state.outcome.exception() if state.outcome else None
        if not isinstance(exc, DaytonaRateLimitError):
            return

        throttler = daytona_rate_limit_throttler(exc)
        sleep_seconds = state.next_action.sleep if state.next_action else None
        logger.warning(
            "daytona.rate_limit_retry",
            extra={
                "op": op,
                "throttler": throttler,
                "attempt": state.attempt_number,
                "sleep_seconds": sleep_seconds,
                "rate_limit_remaining": _daytona_rate_limit_header(exc, "x-ratelimit-remaining-", throttler),
                "rate_limit_reset": _daytona_rate_limit_header(exc, "x-ratelimit-reset-", throttler),
            },
        )
        tags = {"op": op, "throttler": throttler}
        incr("valkyrie.daytona.rate_limit.retry", tags=tags)
        if sleep_seconds is not None:
            distribution("valkyrie.daytona.rate_limit.retry_sleep", sleep_seconds, tags=tags)

    return _hook
