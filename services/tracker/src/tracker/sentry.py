"""Sentry SDK initialization shared by the Tracker API and Worker processes."""

import os

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint

from tracker.logging.context import benchmark_id_var, request_id_var, task_id_var


def _before_send(
    event: Event,
    hint: Hint,
) -> Event | None:
    """Inject structured logging context vars as Sentry tags on every event."""
    if "tags" not in event:
        event["tags"] = {}

    request_id = request_id_var.get("")
    benchmark_id = benchmark_id_var.get("")
    task_id = task_id_var.get("")

    if request_id:
        event["tags"]["request_id"] = request_id
    if benchmark_id:
        event["tags"]["benchmark_id"] = benchmark_id
    if task_id:
        event["tags"]["task_id"] = task_id

    return event


def init_sentry(service_name: str) -> None:
    """Initialize Sentry SDK. No-op if SENTRY_DSN is not set.

    Args:
        service_name: Identifies the process in Sentry (e.g. "valkyrie-tracker", "valkyrie-worker").
    """
    dsn = os.environ.get("SENTRY_DSN", "")
    if not dsn:
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=os.environ.get("ENVIRONMENT", "development"),
        release=os.environ.get("SENTRY_RELEASE", ""),
        server_name=service_name,
        traces_sample_rate=0,
        send_default_pii=False,
        before_send=_before_send,
        integrations=[
            LoggingIntegration(
                level=None,
                event_level=None,
            ),
        ],
    )
