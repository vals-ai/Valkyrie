"""Sentry metric helpers with low-cardinality attribute normalization."""

import logging
from collections.abc import Mapping
from typing import Any

from sentry_sdk import metrics as _sentry_metrics

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
