"""Observability setup and runtime helpers for the tracker service."""

from tracker.observability.metrics import distribution, gauge, incr
from tracker.observability.retry import retry_callback
from tracker.observability.sentry import init_sentry, set_pty_context, set_sandbox_context, tag_daytona_error
from tracker.observability.tracing import configure_tracing


def configure_observability(service_name: str, environment: str) -> None:
    """Configure Sentry first, then OTel/Logfire tracing for this process."""
    init_sentry(service_name, environment=environment)
    configure_tracing(service_name, environment=environment)


__all__ = [
    "configure_observability",
    "configure_tracing",
    "distribution",
    "gauge",
    "incr",
    "init_sentry",
    "retry_callback",
    "set_pty_context",
    "set_sandbox_context",
    "tag_daytona_error",
]
