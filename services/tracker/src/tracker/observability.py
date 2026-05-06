"""Shared observability helpers for metrics, retry logs, and Sentry context."""

import logging
from collections.abc import Callable, Mapping
from typing import Any

import sentry_sdk
from sentry_sdk import metrics as _sentry_metrics
from tenacity import RetryCallState

logger = logging.getLogger(__name__)

# High-cardinality diagnostic fields belong in logs, spans, or Sentry context,
# not metric attributes where they create one time series per task/session/etc.
_BANNED_TAG_KEYS = {
    "command",
    "error_message",
    "request_id",
    "sandbox_id",
    "session_id",
    "task_id",
}


def _normalize(tags: Mapping[str, Any] | None) -> dict[str, str]:
    """Convert metric tags to low-cardinality Sentry metric attributes."""
    if not tags:
        return {}

    attributes: dict[str, str] = {}
    for key, value in tags.items():
        if value is None:
            continue

        key_str = str(key)
        if key_str in _BANNED_TAG_KEYS:
            logger.debug("dropping high-cardinality metric tag %s", key_str)
            continue

        attributes[key_str] = str(value)

    return attributes


def incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
    """Increment a Sentry counter metric without letting metric failures escape."""
    try:
        _sentry_metrics.count(name, value, attributes=_normalize(tags))
    except Exception as e:
        logger.warning("metric incr(%s) failed: %s: %s", name, type(e).__name__, e)


def distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
    """Record a Sentry distribution metric without letting metric failures escape."""
    try:
        _sentry_metrics.distribution(name, value, attributes=_normalize(tags))
    except Exception as e:
        logger.warning("metric distribution(%s) failed: %s: %s", name, type(e).__name__, e)


def gauge(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
    """Record a Sentry gauge metric without letting metric failures escape."""
    try:
        _sentry_metrics.gauge(name, value, attributes=_normalize(tags))
    except Exception as e:
        logger.warning("metric gauge(%s) failed: %s: %s", name, type(e).__name__, e)


def set_sandbox_context(sandbox: Any, *, image: str | None = None) -> None:
    """Attach sandbox identifiers to Sentry tags/context, not metric attributes."""
    try:
        sentry_sdk.set_tag("sandbox_id", sandbox.id)
        sentry_sdk.set_tag("sandbox_name", sandbox.name)

        state = getattr(sandbox, "state", None)
        context = {
            "id": sandbox.id,
            "name": sandbox.name,
            "state": str(state) if state is not None else None,
        }
        if image is not None:
            context["image"] = image

        sentry_sdk.set_context("sandbox", context)
    except Exception as e:
        logger.warning("set_sandbox_context failed: %s: %s", type(e).__name__, e)


def set_pty_context(*, session_id: str, attempt: int | None = None) -> None:
    """Attach PTY identifiers to Sentry tags, not metric attributes."""
    try:
        sentry_sdk.set_tag("pty_session_id", session_id)
        if attempt is not None:
            sentry_sdk.set_tag("pty_attempt", str(attempt))
    except Exception as e:
        logger.warning("set_pty_context failed: %s: %s", type(e).__name__, e)


def retry_callback(metric_name: str, *, log_level: int = logging.WARNING) -> Callable[[RetryCallState], None]:
    """Create a Tenacity before_sleep hook with structured logging and retry metrics."""

    def _hook(state: RetryCallState) -> None:
        exc = state.outcome.exception() if state.outcome else None
        error_class = type(exc).__name__ if exc else "unknown"
        logger.log(
            log_level,
            "retry.before_sleep",
            extra={
                "metric": metric_name,
                "fn": state.fn.__name__ if state.fn else None,
                "attempt": state.attempt_number,
                "idle_for": state.idle_for,
                "error_class": error_class,
            },
        )
        incr(f"{metric_name}.retry", tags={"error_class": error_class})

    return _hook


def tag_daytona_error(exc: Exception, *, op: str) -> None:
    """Tag the active Sentry scope and emit a low-cardinality Daytona error metric."""
    error_class = type(exc).__name__
    try:
        sentry_sdk.set_tag("daytona.op", op)
        sentry_sdk.set_tag("error_class", error_class)
    except Exception as e:
        logger.warning("tag_daytona_error failed: %s: %s", type(e).__name__, e)

    incr("valkyrie.daytona.error", tags={"op": op, "error_class": error_class})
