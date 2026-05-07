"""Tracing configuration: OTel instrumentation (via logfire) with Sentry as the backend."""

import logfire
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from sentry_sdk.integrations.opentelemetry import SentryPropagator, SentrySpanProcessor
from sentry_sdk.integrations.opentelemetry import span_processor as _sentry_span_processor

from tracker.logging.context import get_context_tags

# SentrySpanProcessor drops spans from its in-memory map after 10 minutes by default, which
# silently loses long-running parents like process_benchmark / process_task. Bump to 4 hours.
_sentry_span_processor.SPAN_MAX_TIME_OPEN_MINUTES = 240


class _ContextVarSpanProcessor(SpanProcessor):
    """Attaches request/benchmark/task context vars to every span as attributes."""

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        existing = span.attributes or {}
        for key, value in get_context_tags().items():
            if value and key not in existing:
                span.set_attribute(key, value)

    def on_end(self, span: ReadableSpan) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def configure_tracing(service_name: str, environment: str) -> None:
    """Configure OTel tracing. Call after init_sentry() and before any instrument_*() hooks."""
    logfire.configure(
        service_name=service_name,
        environment=environment,
        send_to_logfire=False,
        # Opt in: trace context is propagated via TracingContextMiddleware.
        distributed_tracing=True,
        # configure_logging() owns stdout.
        console=False,
        additional_span_processors=[_ContextVarSpanProcessor(), SentrySpanProcessor()],
    )
    # Composite: inject both W3C traceparent/tracestate (for non-Sentry peers like Daytona /
    # benchmark_service) and Sentry's sentry-trace/baggage. Extract honors whichever headers arrive.
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator(), SentryPropagator()])
    )

    logfire.instrument_httpx()
