"""Daytona retry helpers."""

import logging
from collections.abc import Callable, Mapping

from daytona.common.errors import DaytonaRateLimitError
from tenacity import RetryCallState
from tenacity import wait_exponential
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
            throttler = lower_key.removeprefix(_RETRY_AFTER_PREFIX)
            return throttler if throttler in _KNOWN_THROTTLERS else "unknown"
        if lower_key.startswith(_RATE_LIMIT_REMAINING_PREFIX):
            throttler = lower_key.removeprefix(_RATE_LIMIT_REMAINING_PREFIX)
            return throttler if throttler in _KNOWN_THROTTLERS else "unknown"

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
    def __init__(self, *, non_rate_limit_wait: wait_base, rate_limit_wait: wait_base | None = None) -> None:
        self._non_rate_limit_wait = non_rate_limit_wait
        self._rate_limit_wait = rate_limit_wait or wait_exponential(multiplier=1, min=1, max=30)

    def __call__(self, retry_state: RetryCallState) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, DaytonaRateLimitError):
            seconds = daytona_retry_after_seconds(exc)
            if seconds is not None:
                return seconds

            return self._rate_limit_wait(retry_state)

        return self._non_rate_limit_wait(retry_state)


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
