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

from executor_protocol import ExecutorTelemetryContext
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


def test_dispatch_acceptance_telemetry_failure_preserves_context(monkeypatch: pytest.MonkeyPatch) -> None:
    telemetry_context: ExecutorTelemetryContext = {
        "request_id": "request-abc",
        "trace_headers": {"traceparent": "parent-trace"},
    }
    monkeypatch.setattr(sentry_sdk, "continue_trace", Mock(side_effect=RuntimeError("sentry unavailable")))
    monkeypatch.setattr(observability.logger, "warning", Mock())

    with observability.dispatch_observability_context(
        "benchmark-123",
        "dispatch-456",
        "release-789",
        telemetry_context,
    ) as child_context:
        assert observability.benchmark_id_var.get() == "benchmark-123"

    assert child_context == telemetry_context


def test_dispatch_scope_is_released_when_tag_binding_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = Mock()
    scope.set_tags.side_effect = RuntimeError("sentry unavailable")
    scope_manager = Mock()
    scope_manager.__enter__ = Mock(return_value=scope)
    scope_manager.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(sentry_sdk, "new_scope", Mock(return_value=scope_manager))
    monkeypatch.setattr(observability.logger, "warning", Mock())

    with observability.dispatch_observability_context(
        "benchmark-123",
        "dispatch-456",
        "release-789",
        {"request_id": "request-abc", "trace_headers": {}},
    ):
        pass

    scope_manager.__exit__.assert_called_once_with(None, None, None)


def test_terminal_sentry_failure_does_not_escape(monkeypatch: pytest.MonkeyPatch) -> None:
    telemetry_context: ExecutorTelemetryContext = {"request_id": "request-abc", "trace_headers": {}}
    monkeypatch.setattr(sentry_sdk, "new_scope", Mock(side_effect=RuntimeError("sentry unavailable")))
    monkeypatch.setattr(observability.logger, "info", Mock())
    monkeypatch.setattr(observability.logger, "error", Mock())
    warning = Mock()
    monkeypatch.setattr(observability.logger, "warning", warning)

    observability.record_dispatch_completion(telemetry_context)
    observability.record_dispatch_cancellation(telemetry_context)
    observability.capture_dispatch_error(RuntimeError("dispatch failed"), telemetry_context)

    assert warning.call_count == 3


def test_invalid_production_sentry_configuration_logs_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    error = RuntimeError("bad dsn")
    warning = Mock()
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setattr(sentry_sdk, "init", Mock(side_effect=error))
    monkeypatch.setattr(observability.logger, "warning", warning)

    observability.configure_observability()

    warning.assert_called_once_with("Failed to initialize Sentry: %s: %s", "RuntimeError", error)


def test_missing_production_sentry_configuration_skips_sentry(monkeypatch: pytest.MonkeyPatch) -> None:
    init_mock = Mock()
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setattr(sentry_sdk, "init", init_mock)

    observability.configure_observability()

    init_mock.assert_not_called()


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
