"""Unit tests for tracker metrics, tracing, and retry observability.

Run: uv run pytest tests/unit/observability/test_observability.py
"""

import logging
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from sentry_sdk import metrics as sentry_metrics
from tenacity import RetryCallState, Retrying

import tracker.observability.metrics as metrics_module
import tracker.observability.retry as retry_module
import tracker.observability.sentry as sentry_module


class TestMetrics:
    """Metric emission and failure handling."""

    def test_incr_uses_sentry_count_attributes_and_drops_high_cardinality_tags(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Counters must exclude identifiers that create one time series per request.

        Test cases:
        - Low-cardinality values are normalized and sent to Sentry.
        - Task, session, and null values are excluded from metric attributes.
        """
        calls: list[tuple[str, float, dict[str, str]]] = []

        def fake_count(name: str, value: float, *, attributes: dict[str, str]) -> None:
            calls.append((name, value, attributes))

        monkeypatch.setattr(sentry_metrics, "count", fake_count)

        metrics_module.incr(
            "valkyrie.test.count",
            value=2,
            tags={
                "operation": "create",
                "attempt": 3,
                "task_id": "task-123",
                "session_id": "pty-123",
                "ignored_none": None,
            },
        )

        assert calls == [
            (
                "valkyrie.test.count",
                2,
                {
                    "operation": "create",
                    "attempt": "3",
                },
            )
        ]

    def test_distribution_and_gauge_use_sentry_attributes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Distribution and gauge helpers must preserve normalized attributes.

        Test cases:
        - Distribution and gauge values reach the matching Sentry APIs.
        """
        distribution_calls: list[tuple[str, float, dict[str, str]]] = []
        gauge_calls: list[tuple[str, float, dict[str, str]]] = []

        def fake_distribution(name: str, value: float, *, attributes: dict[str, str]) -> None:
            distribution_calls.append((name, value, attributes))

        def fake_gauge(name: str, value: float, *, attributes: dict[str, str]) -> None:
            gauge_calls.append((name, value, attributes))

        monkeypatch.setattr(sentry_metrics, "distribution", fake_distribution)
        monkeypatch.setattr(sentry_metrics, "gauge", fake_gauge)

        metrics_module.distribution("valkyrie.test.duration", 1.5, tags={"outcome": "success"})
        metrics_module.gauge("valkyrie.test.in_flight", 4, tags={"worker": "worker-1"})

        assert distribution_calls == [("valkyrie.test.duration", 1.5, {"outcome": "success"})]
        assert gauge_calls == [("valkyrie.test.in_flight", 4, {"worker": "worker-1"})]

    def test_metric_failures_are_logged_without_propagating(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Counter failures must not interrupt tracker work.

        Test cases:
        - A Sentry error is converted into a warning with useful context.
        """
        warnings: list[tuple[str, tuple[object, ...]]] = []

        def fake_warning(message: str, *args: object) -> None:
            warnings.append((message, args))

        monkeypatch.setattr(sentry_metrics, "count", Mock(side_effect=RuntimeError("boom")))
        monkeypatch.setattr(metrics_module.logger, "warning", fake_warning)

        metrics_module.incr("valkyrie.test.count")

        assert len(warnings) == 1
        assert warnings[0][0] == "metric incr(%s) failed: %s: %s"
        assert warnings[0][1][:2] == ("valkyrie.test.count", "RuntimeError")
        assert str(warnings[0][1][2]) == "boom"

    def test_distribution_and_gauge_failures_are_logged_without_propagating(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Distribution and gauge failures must not interrupt tracker work.

        Test cases:
        - Each Sentry failure produces a warning and the next metric still runs.
        """
        warnings: list[tuple[str, tuple[object, ...]]] = []

        def fake_warning(message: str, *args: object) -> None:
            warnings.append((message, args))

        monkeypatch.setattr(sentry_metrics, "distribution", Mock(side_effect=RuntimeError("dist boom")))
        monkeypatch.setattr(sentry_metrics, "gauge", Mock(side_effect=RuntimeError("gauge boom")))
        monkeypatch.setattr(metrics_module.logger, "warning", fake_warning)

        metrics_module.distribution("valkyrie.test.duration", 1.5)
        metrics_module.gauge("valkyrie.test.in_flight", 4)

        assert warnings[0][0] == "metric distribution(%s) failed: %s: %s"
        assert warnings[0][1][:2] == ("valkyrie.test.duration", "RuntimeError")
        assert str(warnings[0][1][2]) == "dist boom"
        assert warnings[1][0] == "metric gauge(%s) failed: %s: %s"
        assert warnings[1][1][:2] == ("valkyrie.test.in_flight", "RuntimeError")
        assert str(warnings[1][1][2]) == "gauge boom"


class TestSandboxContext:
    """Sandbox tags and diagnostic context."""

    def test_set_sandbox_context_sets_tags_and_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Sandbox failures must carry identifiers and runtime details in Sentry.

        Test cases:
        - Sandbox tags and context include the ID, name, state, and image.
        """
        tags: dict[str, str] = {}
        contexts: dict[str, dict[str, Any]] = {}

        def fake_set_tag(key: str, value: str) -> None:
            tags[key] = value

        def fake_set_context(key: str, value: dict[str, Any]) -> None:
            contexts[key] = value

        monkeypatch.setattr(sentry_module.sentry_sdk, "set_tag", fake_set_tag)
        monkeypatch.setattr(sentry_module.sentry_sdk, "set_context", fake_set_context)

        sandbox = SimpleNamespace(id="sandbox-123", name="bench-task-1", state="STARTED")

        sentry_module.set_sandbox_context(sandbox, image="ghcr.io/example/image:latest")

        assert tags == {
            "sandbox_id": "sandbox-123",
            "sandbox_name": "bench-task-1",
        }
        assert contexts == {
            "sandbox": {
                "id": "sandbox-123",
                "name": "bench-task-1",
                "state": "STARTED",
                "image": "ghcr.io/example/image:latest",
            }
        }

    def test_set_sandbox_context_omits_optional_image_and_missing_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Partial sandbox objects must still produce useful Sentry context.

        Test cases:
        - Missing state is recorded as null and an absent image is omitted.
        """
        contexts: dict[str, dict[str, Any]] = {}

        def fake_set_context(key: str, value: dict[str, Any]) -> None:
            contexts[key] = value

        monkeypatch.setattr(sentry_module.sentry_sdk, "set_tag", Mock())
        monkeypatch.setattr(sentry_module.sentry_sdk, "set_context", fake_set_context)

        sandbox = SimpleNamespace(id="sandbox-123", name="bench-task-1")

        sentry_module.set_sandbox_context(sandbox)

        assert contexts == {
            "sandbox": {
                "id": "sandbox-123",
                "name": "bench-task-1",
                "state": None,
            }
        }

    def test_set_sandbox_context_failures_are_logged_without_propagating(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Sentry context failures must not block sandbox execution.

        Test cases:
        - A closed Sentry scope produces a warning instead of escaping.
        """
        warnings: list[tuple[str, tuple[object, ...]]] = []

        def fake_warning(message: str, *args: object) -> None:
            warnings.append((message, args))

        monkeypatch.setattr(sentry_module.sentry_sdk, "set_tag", Mock(side_effect=RuntimeError("scope closed")))
        monkeypatch.setattr(sentry_module.logger, "warning", fake_warning)

        sentry_module.set_sandbox_context(SimpleNamespace(id="sandbox-123", name="bench-task-1"))

        assert warnings[0][0] == "set_sandbox_context failed: %s: %s"
        assert warnings[0][1][:1] == ("RuntimeError",)
        assert str(warnings[0][1][1]) == "scope closed"


def test_retry_callback_logs_attempt_and_emits_retry_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry callbacks must emit both operator context and a low-cardinality metric.

    Test cases:
    - A timed-out retry records its function, attempt, delay, and error class.
    """
    increments: list[tuple[str, dict[str, str]]] = []
    log_records: list[dict[str, Any]] = []

    def fake_incr(name: str, _value: float = 1, tags: dict[str, str] | None = None) -> None:
        increments.append((name, tags or {}))

    def retried_function() -> None:
        raise RuntimeError("unused")

    state = RetryCallState(Retrying(), retried_function, (), {})
    state.attempt_number = 2
    state.idle_for = 1.25
    retry_error = TimeoutError("timed out")
    state.set_exception((type(retry_error), retry_error, retry_error.__traceback__))

    monkeypatch.setattr(retry_module, "incr", fake_incr)

    def fake_log(
        level: int,
        message: str,
        *_args: object,
        extra: Mapping[str, Any] | None = None,
        **_kwargs: Any,
    ) -> None:
        log_records.append(
            {
                "level": level,
                "message": message,
                **(extra or {}),
            }
        )

    monkeypatch.setattr(retry_module.logger, "log", fake_log)

    retry_module.retry_callback("valkyrie.pty.create")(state)

    assert increments == [("valkyrie.pty.create.retry", {"error_class": "TimeoutError"})]
    assert log_records == [
        {
            "level": logging.WARNING,
            "message": "retry.before_sleep",
            "metric": "valkyrie.pty.create",
            "fn": "retried_function",
            "attempt": 2,
            "idle_for": 1.25,
            "error_class": "TimeoutError",
        }
    ]
