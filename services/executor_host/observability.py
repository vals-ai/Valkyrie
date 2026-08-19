"""Structured logging and Sentry correlation for the stable executor host."""

from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Generator

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from executor_protocol import ExecutorTelemetryContext

request_id_var = contextvars.ContextVar("request_id", default="")
benchmark_id_var = contextvars.ContextVar("benchmark_id", default="")
dispatch_id_var = contextvars.ContextVar("executor_dispatch_id", default="")
release_id_var = contextvars.ContextVar("executor_release_id", default="")


def _context_fields() -> dict[str, str]:
    return {
        "request_id": request_id_var.get(),
        "benchmark_id": benchmark_id_var.get(),
        "executor_dispatch_id": dispatch_id_var.get(),
        "executor_release_id": release_id_var.get(),
    }


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _context_fields().items():
            if not getattr(record, key, None):
                setattr(record, key, value)
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **{key: getattr(record, key, "") for key in _context_fields()},
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_observability() -> None:
    """Configure CloudWatch JSON logs and Sentry for the host process."""
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(_ContextFilter())
    handler.setFormatter(_JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

    dsn = os.environ.get("SENTRY_DSN", "")
    if not dsn:
        return
    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.environ.get("ENVIRONMENT", "development"),
            release=os.environ.get("SENTRY_RELEASE") or None,
            server_name="valkyrie-executor-host",
            enable_logs=True,
            send_default_pii=False,
            traces_sample_rate=1.0,
            integrations=[
                LoggingIntegration(
                    level=logging.INFO,
                    event_level=None,
                    sentry_logs_level=logging.INFO,
                )
            ],
        )
    except Exception as error:
        if os.environ.get("ENVIRONMENT") == "production":
            raise RuntimeError("Sentry could not start. Check the production DSN secret and deploy again.") from None
        logging.getLogger(__name__).warning(
            "Failed to initialize Sentry: %s: %s",
            type(error).__name__,
            error,
        )


@contextmanager
def dispatch_observability_context(
    benchmark_id: str,
    dispatch_id: str,
    release_id: str,
    telemetry_context: ExecutorTelemetryContext,
) -> Generator[ExecutorTelemetryContext, None, None]:
    """Bind one dispatch and continue its trace into the executor artifact."""
    tokens = [
        request_id_var.set(telemetry_context["request_id"]),
        benchmark_id_var.set(benchmark_id),
        dispatch_id_var.set(dispatch_id),
        release_id_var.set(release_id),
    ]
    try:
        with sentry_sdk.new_scope() as scope:
            scope.set_tags({key: value for key, value in _context_fields().items() if value})
            transaction = sentry_sdk.continue_trace(
                telemetry_context["trace_headers"],
                op="queue.process",
                name="executor_host.dispatch",
            )
            with sentry_sdk.start_transaction(transaction):
                trace_headers = dict(telemetry_context["trace_headers"])
                if sentry_trace := sentry_sdk.get_traceparent():
                    trace_headers.pop("traceparent", None)
                    trace_headers.pop("tracestate", None)
                    trace_headers["sentry-trace"] = sentry_trace
                if baggage := sentry_sdk.get_baggage():
                    trace_headers["baggage"] = baggage
                yield {
                    "request_id": telemetry_context["request_id"],
                    "trace_headers": trace_headers,
                }
    finally:
        for token in reversed(tokens):
            token.var.reset(token)
