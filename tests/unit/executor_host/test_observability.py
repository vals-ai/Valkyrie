"""Tests for ExecutorHost structured logging and Sentry correlation.

Run: uv run pytest tests/unit/executor_host/test_observability.py
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
import io
import json
import logging
from unittest.mock import Mock

import pytest
import sentry_sdk

from services.executor_host import observability


def test_dispatch_transaction_finishes_before_executor_work(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    transaction = Mock()

    @contextmanager
    def record_transaction(_transaction: object) -> Generator[Mock, None, None]:
        events.append("transaction-started")
        yield transaction
        events.append("transaction-finished")

    monkeypatch.setattr(sentry_sdk, "continue_trace", Mock(return_value=transaction))
    monkeypatch.setattr(sentry_sdk, "start_transaction", record_transaction)
    monkeypatch.setattr(sentry_sdk, "get_traceparent", lambda: "child-trace")
    monkeypatch.setattr(sentry_sdk, "get_baggage", lambda: None)

    with observability.dispatch_observability_context(
        "benchmark-123",
        "dispatch-456",
        "release-789",
        {"request_id": "request-abc", "trace_headers": {}},
    ):
        events.append("executor-running")

    assert events == ["transaction-started", "transaction-finished", "executor-running"]


def test_invalid_production_sentry_configuration_fails_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setattr(sentry_sdk, "init", Mock(side_effect=RuntimeError("bad dsn")))

    with pytest.raises(RuntimeError, match="Check the production DSN secret") as exc_info:
        observability.configure_observability()

    assert exc_info.value.__cause__ is None


def test_missing_production_sentry_configuration_fails_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "production")

    with pytest.raises(RuntimeError, match="Check the production DSN secret"):
        observability.configure_observability()


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


def test_dispatch_error_logs_before_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    span = Mock()

    @contextmanager
    def record_transaction(_transaction: object) -> Generator[Mock, None, None]:
        yield span

    def record_log(*_args: object, **_kwargs: object) -> None:
        events.append("logged")

    def record_capture(_error: BaseException) -> None:
        events.append("captured")

    monkeypatch.setattr(sentry_sdk, "continue_trace", Mock(return_value=Mock()))
    monkeypatch.setattr(sentry_sdk, "start_transaction", record_transaction)
    monkeypatch.setattr(observability.logger, "error", record_log)
    monkeypatch.setattr(sentry_sdk, "capture_exception", record_capture)
    error = RuntimeError("executor failed")

    observability.capture_dispatch_error(error, {"request_id": "request-abc", "trace_headers": {}})

    span.set_status.assert_called_once_with(observability.SPANSTATUS.INTERNAL_ERROR)
    assert events == ["logged", "captured"]
