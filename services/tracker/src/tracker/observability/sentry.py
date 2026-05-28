"""Sentry SDK initialization shared by the Tracker API and Worker processes."""

import logging
import os
from typing import Any, cast

import sentry_sdk
from opentelemetry.trace import get_current_span
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.otlp import OTLPIntegration
from sentry_sdk.types import Event, Hint, Log

from tracker.exceptions import SSLConnectionError
from tracker.logging.context import get_context_tags

logger = logging.getLogger(__name__)


def _before_send(
    event: Event,
    hint: Hint,
) -> Event | None:
    """Attach structured logging context vars as Sentry tags on every event."""
    exc_info = hint.get("exc_info")
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, SSLConnectionError):
            event["fingerprint"] = ["{{ default }}", "SSLConnectionError"]

    tags = event.setdefault("tags", {})
    for key, value in get_context_tags().items():
        if value:
            tags[key] = value
    return event


def _apply_current_otel_trace_context(telemetry: dict[str, Any]) -> None:
    span_context = get_current_span().get_span_context()
    if not span_context.is_valid:
        return
    telemetry["trace_id"] = f"{span_context.trace_id:032x}"
    telemetry["span_id"] = f"{span_context.span_id:016x}"


def _before_send_log(log: Log, _hint: Hint) -> Log | None:
    _apply_current_otel_trace_context(cast(dict[str, Any], log))
    return log


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
            before_send_log=_before_send_log,
            integrations=[
                # level=None / event_level=None: spans carry context and we capture_exception explicitly,
                # so we only want LoggingIntegration for shipping log records to Sentry Logs.
                LoggingIntegration(
                    level=None,
                    event_level=None,
                    sentry_logs_level=logging.INFO,
                ),
                # Bridges the active OpenTelemetry span context into Sentry's scope.
                # setup_otlp_traces_exporter=False: SentrySpanProcessor in tracing.py already ships
                # spans to Sentry; the default would double-publish via an OTLP exporter.
                # setup_propagator=False: tracing.py installs a CompositePropagator that supports
                # both W3C traceparent (Daytona / benchmark_service) and sentry-trace; the default
                # would replace it with SentryOTLPPropagator only.
                OTLPIntegration(
                    setup_otlp_traces_exporter=False,
                    setup_propagator=False,
                ),
            ],
        )
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
