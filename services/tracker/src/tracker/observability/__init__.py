"""Observability setup and runtime helpers for the tracker service."""

import time

from tracker.observability.metrics import distribution, gauge, incr
from tracker.observability.retry import retry_callback
from tracker.observability.sentry import init_sentry, set_sandbox_context
from tracker.observability.tracing import configure_tracing


def configure_observability(service_name: str, environment: str) -> None:
    """Configure Sentry first, then OTel/Logfire tracing for this process."""
    init_sentry(service_name, environment=environment)
    configure_tracing(service_name, environment=environment)


def elapsed_ms(start: float) -> float:
    """Milliseconds since `start` (a `time.monotonic()` reading), rounded to 2dp for log-friendly output."""
    return round((time.monotonic() - start) * 1000, 2)


__all__ = [
    "configure_observability",
    "configure_tracing",
    "distribution",
    "elapsed_ms",
    "gauge",
    "incr",
    "init_sentry",
    "retry_callback",
    "set_sandbox_context",
]
