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
from sentry_sdk.consts import SPANSTATUS
from sentry_sdk.integrations.logging import LoggingIntegration

from executor_protocol import ExecutorTelemetryContext

request_id_var = contextvars.ContextVar("request_id", default="")
benchmark_id_var = contextvars.ContextVar("benchmark_id", default="")
dispatch_id_var = contextvars.ContextVar("executor_dispatch_id", default="")
release_id_var = contextvars.ContextVar("executor_release_id", default="")
logger = logging.getLogger(__name__)


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
        logger.warning(
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
    """Bind one dispatch and create the child executor trace context."""
    tokens = [
        request_id_var.set(telemetry_context["request_id"]),
        benchmark_id_var.set(benchmark_id),
        dispatch_id_var.set(dispatch_id),
        release_id_var.set(release_id),
    ]
    try:
        with _dispatch_sentry_scope():
            child_context = _accepted_telemetry_context(telemetry_context)
            yield child_context
    finally:
        for token in reversed(tokens):
            token.var.reset(token)


@contextmanager
def _dispatch_sentry_scope() -> Generator[None, None, None]:
    scope_manager = None
    scope_entered = False
    try:
        scope_manager = sentry_sdk.new_scope()
        scope = scope_manager.__enter__()
        scope_entered = True
        scope.set_tags({key: value for key, value in _context_fields().items() if value})
    except Exception as error:
        logger.warning("Failed to bind executor dispatch telemetry: %s: %s", type(error).__name__, error)

    try:
        yield
    except BaseException as error:
        if scope_manager is not None and scope_entered:
            try:
                scope_manager.__exit__(type(error), error, error.__traceback__)
            except Exception as telemetry_error:
                logger.warning(
                    "Failed to release executor dispatch telemetry: %s: %s",
                    type(telemetry_error).__name__,
                    telemetry_error,
                )
        raise
    else:
        if scope_manager is not None and scope_entered:
            try:
                scope_manager.__exit__(None, None, None)
            except Exception as error:
                logger.warning("Failed to release executor dispatch telemetry: %s: %s", type(error).__name__, error)


def _accepted_telemetry_context(telemetry_context: ExecutorTelemetryContext) -> ExecutorTelemetryContext:
    try:
        transaction = sentry_sdk.continue_trace(
            telemetry_context["trace_headers"],
            op="queue.process",
            name="executor_host.dispatch.accepted",
        )
        with sentry_sdk.start_transaction(transaction):
            trace_headers = dict(telemetry_context["trace_headers"])
            if sentry_trace := sentry_sdk.get_traceparent():
                trace_headers.pop("traceparent", None)
                trace_headers.pop("tracestate", None)
                trace_headers["sentry-trace"] = sentry_trace
            if baggage := sentry_sdk.get_baggage():
                trace_headers["baggage"] = baggage
            child_context: ExecutorTelemetryContext = {
                "request_id": telemetry_context["request_id"],
                "trace_headers": trace_headers,
            }
        return child_context
    except Exception as error:
        logger.warning(
            "Failed to record executor dispatch acceptance: %s: %s",
            type(error).__name__,
            error,
        )
        return {
            "request_id": telemetry_context["request_id"],
            "trace_headers": dict(telemetry_context["trace_headers"]),
        }


def _record_terminal_transaction(
    telemetry_context: ExecutorTelemetryContext,
    *,
    name: str,
    status: str | None = None,
    error: BaseException | None = None,
) -> None:
    try:
        with sentry_sdk.new_scope() as scope:
            scope.set_tags({key: value for key, value in _context_fields().items() if value})
            transaction = sentry_sdk.continue_trace(
                telemetry_context["trace_headers"],
                op="queue.process",
                name=name,
            )
            with sentry_sdk.start_transaction(transaction) as span:
                if status is not None:
                    span.set_status(status)
                if error is not None:
                    sentry_sdk.capture_exception(error)
    except Exception as telemetry_error:
        logger.warning(
            "Failed to record executor dispatch telemetry: %s: %s",
            type(telemetry_error).__name__,
            telemetry_error,
        )


def record_dispatch_completion(telemetry_context: ExecutorTelemetryContext) -> None:
    """Record the terminal host signal without holding a transaction across execution."""
    _record_terminal_transaction(
        telemetry_context,
        name="executor_host.dispatch.completed",
    )


def record_dispatch_cancellation(telemetry_context: ExecutorTelemetryContext) -> None:
    """Record a cancelled host dispatch without creating an error issue."""
    logger.info("Executor dispatch cancelled")
    _record_terminal_transaction(
        telemetry_context,
        name="executor_host.dispatch.cancelled",
        status=SPANSTATUS.CANCELLED,
    )


def capture_dispatch_error(error: BaseException, telemetry_context: ExecutorTelemetryContext) -> None:
    """Capture a host dispatch error on a bounded trace segment."""
    logger.error(
        "Executor dispatch failed",
        exc_info=(type(error), error, error.__traceback__),
    )
    _record_terminal_transaction(
        telemetry_context,
        name="executor_host.dispatch.failed",
        status=SPANSTATUS.INTERNAL_ERROR,
        error=error,
    )
