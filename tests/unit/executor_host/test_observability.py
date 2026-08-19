from __future__ import annotations

import io
import json
import logging

import pytest
import sentry_sdk

from services.executor_host import observability


def test_dispatch_context_correlates_cloudwatch_logs_and_child_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sentry_sdk, "get_traceparent", lambda: "sentry-child-trace")
    monkeypatch.setattr(sentry_sdk, "get_baggage", lambda: "sentry-child-baggage")
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.addFilter(observability._ContextFilter())  # pyright: ignore[reportPrivateUsage]
    handler.setFormatter(observability._JsonFormatter())  # pyright: ignore[reportPrivateUsage]
    logger = logging.Logger("executor-host-test")
    logger.addHandler(handler)

    with observability.dispatch_observability_context(
        "benchmark-123",
        "dispatch-456",
        "release-789",
        {
            "request_id": "request-abc",
            "trace_headers": {
                "traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
                "tracestate": "vendor=value",
            },
        },
    ) as child_context:
        logger.info("Launching executor")

    record = json.loads(output.getvalue())
    assert record == {
        "timestamp": record["timestamp"],
        "level": "INFO",
        "logger": "executor-host-test",
        "message": "Launching executor",
        "request_id": "request-abc",
        "benchmark_id": "benchmark-123",
        "executor_dispatch_id": "dispatch-456",
        "executor_release_id": "release-789",
    }
    assert child_context == {
        "request_id": "request-abc",
        "trace_headers": {
            "sentry-trace": "sentry-child-trace",
            "baggage": "sentry-child-baggage",
        },
    }
