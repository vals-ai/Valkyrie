"""Sentry SDK initialization shared by the Tracker API and Worker processes."""

import logging
import os
from typing import Any

import daytona
import sentry_sdk
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.types import Event, Hint

from tracker.exceptions import PtyCreationError, SSLConnectionError, SandboxError
from tracker.logging.context import get_context_tags
from tracker.observability.metrics import incr

logger = logging.getLogger(__name__)


def _before_send(
    event: Event,
    hint: Hint,
) -> Event | None:
    """Attach structured logging context vars as Sentry tags on every event."""
    exc_info = hint.get("exc_info")
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, PtyCreationError):
            event["fingerprint"] = ["{{ default }}", "PtyCreationError"]
        elif isinstance(exc, SandboxError) and "PTY reconnect failed" in str(exc):
            event["fingerprint"] = ["{{ default }}", "pty_reconnect_failed"]
        elif isinstance(exc, SSLConnectionError):
            event["fingerprint"] = ["{{ default }}", "SSLConnectionError"]

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
        sentry_sdk.set_tag("daytona.sdk_version", getattr(daytona, "__version__", "unknown"))
    except Exception as e:
        # A malformed SENTRY_DSN or invalid integration shouldn't crash service startup.
        logger.warning("Failed to initialize Sentry: %s: %s", type(e).__name__, e)


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


def tag_daytona_error(exc: Exception, *, op: str) -> None:
    """Tag the active Sentry scope and emit a low-cardinality Daytona error metric."""
    error_class = type(exc).__name__
    try:
        sentry_sdk.set_tag("daytona.op", op)
        sentry_sdk.set_tag("error_class", error_class)
    except Exception as e:
        logger.warning("tag_daytona_error failed: %s: %s", type(e).__name__, e)

    incr("valkyrie.daytona.error", tags={"op": op, "error_class": error_class})
