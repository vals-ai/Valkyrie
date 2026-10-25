"""Dispatch log context and Sentry correlation for the executor task."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import sentry_sdk
from opentelemetry.context import attach, detach
from opentelemetry.propagate import extract
from sentry_sdk.consts import SPANSTATUS

from executor_protocol import ExecutorTelemetryContext
from tracker.logging import benchmark_id_var, request_id_var, task_id_var
from tracker.logging.context import attempt_started_at_var, executor_dispatch_id_var, get_context_tags

logger = logging.getLogger(__name__)
_captured_dispatch_error: BaseException | None = None


def _context_fields() -> dict[str, str]:
    return {key: value for key, value in get_context_tags().items() if value}


@contextmanager
def dispatch_observability_context(
    benchmark_id: str,
    dispatch_id: str,
    release_id: str,
    telemetry_context: ExecutorTelemetryContext,
) -> Generator[ExecutorTelemetryContext, None, None]:
    """Bind one dispatch to the log context, the parent trace, and a Sentry scope; yield the child trace context."""
    tokens = [
        request_id_var.set(telemetry_context["request_id"]),
        benchmark_id_var.set(benchmark_id),
        task_id_var.set(""),
        executor_dispatch_id_var.set(dispatch_id),
        attempt_started_at_var.set(""),
    ]
    trace_headers = telemetry_context["trace_headers"]
    otel_token = attach(extract(trace_headers)) if trace_headers else None
    try:
        with _dispatch_sentry_scope(release_id):
            yield _accepted_telemetry_context(telemetry_context)
    finally:
        if otel_token is not None:
            detach(otel_token)
        for token in reversed(tokens):
            token.var.reset(token)


@contextmanager
def _dispatch_sentry_scope(release_id: str) -> Generator[None, None, None]:
    scope_manager = None
    scope_entered = False
    try:
        scope_manager = sentry_sdk.new_scope()
        scope = scope_manager.__enter__()
        scope_entered = True
        scope.set_tags({**_context_fields(), "executor_release_id": release_id})
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
            name="executor_runner.dispatch.accepted",
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
) -> bool:
    try:
        with sentry_sdk.new_scope() as scope:
            scope.set_tags(_context_fields())
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
        return True
    except Exception as telemetry_error:
        logger.warning(
            "Failed to record executor dispatch telemetry: %s: %s",
            type(telemetry_error).__name__,
            telemetry_error,
        )
        return False


def record_dispatch_completion(telemetry_context: ExecutorTelemetryContext) -> None:
    """Record the terminal runner signal without holding a transaction across execution."""
    _record_terminal_transaction(
        telemetry_context,
        name="executor_runner.dispatch.completed",
    )


def record_dispatch_cancellation(telemetry_context: ExecutorTelemetryContext) -> None:
    """Record a cancelled runner dispatch without creating an error issue."""
    logger.info("Executor dispatch cancelled")
    _record_terminal_transaction(
        telemetry_context,
        name="executor_runner.dispatch.cancelled",
        status=SPANSTATUS.CANCELLED,
    )


def capture_dispatch_error(error: BaseException, telemetry_context: ExecutorTelemetryContext) -> None:
    """Capture a runner dispatch error on a bounded trace segment."""
    global _captured_dispatch_error
    logger.error(
        "Executor dispatch failed",
        exc_info=(type(error), error, error.__traceback__),
    )
    if _record_terminal_transaction(
        telemetry_context,
        name="executor_runner.dispatch.failed",
        status=SPANSTATUS.INTERNAL_ERROR,
        error=error,
    ):
        _captured_dispatch_error = error


def capture_runner_failure(error: BaseException, dispatch_id: str) -> None:
    """Capture a runner failure that the dispatch context has not already reported."""
    if error is _captured_dispatch_error:
        return
    with sentry_sdk.new_scope() as scope:
        scope.set_tag("executor_dispatch_id", dispatch_id)
        sentry_sdk.capture_exception(error)
