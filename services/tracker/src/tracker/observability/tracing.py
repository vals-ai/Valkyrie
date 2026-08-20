"""Tracing configuration: OTel instrumentation (via logfire) with Sentry as the backend."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import logfire
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from sentry_sdk.integrations.opentelemetry import SentryPropagator, SentrySpanProcessor

from tracker.logging.context import get_context_tags
from tracker.logging.logger import get_logger

logger = get_logger(__name__)

_EXCLUDED_SPAN_NAMES = frozenset(
    {
        "AsyncProcess.exec",
        "AsyncSandbox.refresh_data",
    }
)


@contextmanager
def observability_span(name: str, **attributes: Any) -> Generator[None, None, None]:
    """Create a span without allowing telemetry failures to escape."""
    span_manager = None
    span_entered = False
    try:
        span_manager = logfire.span(name, **attributes)
        span_manager.__enter__()
        span_entered = True
    except Exception as error:
        logger.warning("Failed to start observability span %s: %s: %s", name, type(error).__name__, error)

    try:
        yield
    except BaseException as error:
        if span_manager is not None and span_entered:
            try:
                span_manager.__exit__(type(error), error, error.__traceback__)
            except Exception as telemetry_error:
                logger.warning(
                    "Failed to finish observability span %s: %s: %s",
                    name,
                    type(telemetry_error).__name__,
                    telemetry_error,
                )
        raise
    else:
        if span_manager is not None and span_entered:
            try:
                span_manager.__exit__(None, None, None)
            except Exception as error:
                logger.warning("Failed to finish observability span %s: %s: %s", name, type(error).__name__, error)


@contextmanager
def error_span(name: str, exc: BaseException, **attributes: Any) -> Generator[None, None, None]:
    """Create a bounded error span for a handled exception."""
    with observability_span(name, **attributes):
        span = trace.get_current_span()
        try:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
        except Exception as error:
            logger.warning("Failed to annotate observability span %s: %s: %s", name, type(error).__name__, error)
        yield


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


class _FilteredSentrySpanProcessor(SpanProcessor):
    """Drop low-level polling span subtrees before exporting them to Sentry."""

    def __init__(self) -> None:
        self._delegate = SentrySpanProcessor()
        self._excluded_span_ids: set[int] = set()

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        parent_span_id = span.parent.span_id if span.parent is not None else None
        span_id = span.get_span_context().span_id
        if span.name in _EXCLUDED_SPAN_NAMES or parent_span_id in self._excluded_span_ids:
            self._excluded_span_ids.add(span_id)
            return
        self._delegate.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        span_id = span.get_span_context().span_id
        if span_id in self._excluded_span_ids:
            self._excluded_span_ids.discard(span_id)
            return
        self._delegate.on_end(span)

    def shutdown(self) -> None:
        self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._delegate.force_flush(timeout_millis)


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
        additional_span_processors=[_ContextVarSpanProcessor(), _FilteredSentrySpanProcessor()],
    )
    # Composite: inject both W3C traceparent/tracestate (for non-Sentry peers like Daytona /
    # benchmark_service) and Sentry's sentry-trace/baggage. Extract honors whichever headers arrive.
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator(), SentryPropagator()])
    )

    logfire.instrument_httpx()
