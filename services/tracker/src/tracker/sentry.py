"""Sentry SDK initialization shared by the Tracker API and Worker processes."""

import logging
import os

import sentry_sdk
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint

from tracker.logging.context import get_context_tags


def _before_send(
    event: Event,
    hint: Hint,
) -> Event | None:
    """Attach structured logging context vars as Sentry tags on every event."""
    tags = event.setdefault("tags", {})
    for key, value in get_context_tags().items():
        if value:
            tags[key] = value
    return event


def init_sentry(service_name: str, environment: str) -> None:
    """Initialize Sentry SDK. No-op if SENTRY_DSN is not set.

    Args:
        service_name: Identifies the process in Sentry (e.g. "valkyrie-tracker", "valkyrie-worker").
        environment: Deployment environment tag (e.g. "development", "production").
    """
    dsn = os.environ.get("SENTRY_DSN", "")
    if not dsn:
        return

    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            release=os.environ.get("SENTRY_RELEASE", ""),
            server_name=service_name,
            # Sampling happens upstream in OTel; pass everything through.
            traces_sample_rate=1.0,
            instrumenter=INSTRUMENTER.OTEL,
            enable_logs=True,
            send_default_pii=False,
            before_send=_before_send,
            integrations=[
                # level=None / event_level=None: spans carry context and we capture_exception explicitly,
                # so we only want LoggingIntegration for shipping log records to Sentry Logs.
                LoggingIntegration(
                    level=None,
                    event_level=None,
                    sentry_logs_level=logging.INFO,
                ),
            ],
        )
    except Exception as e:
        # A malformed SENTRY_DSN or invalid integration shouldn't crash service startup.
        logging.getLogger(__name__).warning("Failed to initialize Sentry: %s: %s", type(e).__name__, e)
