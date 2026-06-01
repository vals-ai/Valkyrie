"""Retry telemetry helpers for Tenacity retry sites."""

import logging
from collections.abc import Callable

from tenacity import RetryCallState

from tracker.observability.metrics import incr

logger = logging.getLogger(__name__)


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
