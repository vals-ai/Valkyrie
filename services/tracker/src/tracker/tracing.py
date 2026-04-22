"""Logfire tracing configuration for the tracker service."""

import os

import logfire
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor

from tracker.logging.context import benchmark_id_var, request_id_var, task_id_var


class _ContextVarSpanProcessor(SpanProcessor):
    """Attaches request/benchmark/task context vars to every span, mirroring sentry._before_send."""

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        existing = span.attributes or {}
        if "request_id" not in existing and (rid := request_id_var.get("")):
            span.set_attribute("request_id", rid)
        if "benchmark_id" not in existing and (bid := benchmark_id_var.get("")):
            span.set_attribute("benchmark_id", bid)
        if "task_id" not in existing and (tid := task_id_var.get("")):
            span.set_attribute("task_id", tid)

    def on_end(self, span: ReadableSpan) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def configure_logfire(service_name: str) -> None:
    """Configure Logfire tracing. Must run before any instrumentation hooks (instrument_fastapi, etc.)."""
    environment = os.environ.get("ENVIRONMENT", "development")

    logfire.configure(
        service_name=service_name,
        environment=environment,
        send_to_logfire="if-token-present",
        # Opt in: trace context is propagated via TracingContextMiddleware.
        distributed_tracing=True,
        # configure_logging() owns stdout; LogfireLoggingHandler ships to the cloud.
        console=False,
        additional_span_processors=[_ContextVarSpanProcessor()],
    )

    logfire.instrument_httpx()
