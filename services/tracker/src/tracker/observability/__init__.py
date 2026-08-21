"""Observability setup and runtime helpers for the tracker service."""

import logging
import time

from tracker.observability.metrics import distribution, gauge, incr
from tracker.observability.retry import retry_callback
from tracker.observability.sentry import init_sentry, set_sandbox_context
from tracker.observability.tracing import configure_tracing, error_span

logger = logging.getLogger(__name__)


def configure_observability(service_name: str, environment: str) -> None:
    """Configure Sentry first, then OTel/Logfire tracing for this process."""
    try:
        init_sentry(service_name, environment=environment)
    except Exception as error:
        logger.warning("Failed to initialize Sentry: %s: %s", type(error).__name__, error)

    try:
        configure_tracing(service_name, environment=environment)
    except Exception as error:
        logger.warning("Failed to initialize tracing: %s: %s", type(error).__name__, error)


def elapsed_ms(start: float) -> float:
    """Milliseconds since `start` (a `time.monotonic()` reading), rounded to 2dp for log-friendly output."""
    return round((time.monotonic() - start) * 1000, 2)


__all__ = [
    "configure_observability",
    "configure_tracing",
    "distribution",
    "elapsed_ms",
    "error_span",
    "gauge",
    "incr",
    "init_sentry",
    "retry_callback",
    "set_sandbox_context",
]
