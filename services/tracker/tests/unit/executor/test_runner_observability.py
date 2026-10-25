"""Tests for executor dispatch log context and Sentry correlation.

Run: uv run pytest services/tracker/tests/unit/executor/test_runner_observability.py
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import Mock

import pytest
import sentry_sdk
from opentelemetry import trace

from executor_protocol import ExecutorTelemetryContext
from tracker.logging import request_id_var
from tracker.logging.context import executor_dispatch_id_var, get_context_tags
from tracker.executor import runner_observability as observability


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


def test_dispatch_context_correlates_cloudwatch_logs_and_child_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sentry_sdk, "get_traceparent", lambda: "sentry-child-trace")
    monkeypatch.setattr(sentry_sdk, "get_baggage", lambda: "sentry-child-baggage")
    outer = request_id_var.set("outer-request")
    try:
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
            assert get_context_tags() == {
                "request_id": "request-abc",
                "benchmark_id": "benchmark-123",
                "task_id": "",
                "executor_dispatch_id": "dispatch-456",
                "attempt_started_at": "",
            }
            assert trace.get_current_span().get_span_context().trace_id == 0x1234567890ABCDEF1234567890ABCDEF
        assert request_id_var.get() == "outer-request"
        assert executor_dispatch_id_var.get("") == ""
        assert not trace.get_current_span().get_span_context().is_valid
    finally:
        request_id_var.reset(outer)

    assert child_context == {
        "request_id": "request-abc",
        "trace_headers": {
            "sentry-trace": "sentry-child-trace",
            "baggage": "sentry-child-baggage",
        },
    }
