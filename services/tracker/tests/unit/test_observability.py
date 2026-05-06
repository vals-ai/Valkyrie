import logging
import importlib
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest


def _observability() -> Any:
    try:
        return importlib.import_module("tracker.observability")
    except ModuleNotFoundError as e:
        raise AssertionError("tracker.observability module should exist") from e


def test_incr_uses_sentry_count_attributes_and_drops_high_cardinality_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observability = _observability()
    calls: list[tuple[str, float, dict[str, str]]] = []

    def fake_count(name: str, value: float, *, attributes: dict[str, str]) -> None:
        calls.append((name, value, attributes))

    monkeypatch.setattr(observability._sentry_metrics, "count", fake_count)

    observability.incr(
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


def test_distribution_and_gauge_use_sentry_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    distribution_calls: list[tuple[str, float, dict[str, str]]] = []
    gauge_calls: list[tuple[str, float, dict[str, str]]] = []

    def fake_distribution(name: str, value: float, *, attributes: dict[str, str]) -> None:
        distribution_calls.append((name, value, attributes))

    def fake_gauge(name: str, value: float, *, attributes: dict[str, str]) -> None:
        gauge_calls.append((name, value, attributes))

    monkeypatch.setattr(observability._sentry_metrics, "distribution", fake_distribution)
    monkeypatch.setattr(observability._sentry_metrics, "gauge", fake_gauge)

    observability.distribution("valkyrie.test.duration", 1.5, tags={"outcome": "success"})
    observability.gauge("valkyrie.test.in_flight", 4, tags={"worker": "worker-1"})

    assert distribution_calls == [("valkyrie.test.duration", 1.5, {"outcome": "success"})]
    assert gauge_calls == [("valkyrie.test.in_flight", 4, {"worker": "worker-1"})]


def test_metric_failures_are_logged_without_propagating(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    warnings: list[tuple[str, tuple[object, ...]]] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append((message, args))

    monkeypatch.setattr(observability._sentry_metrics, "count", Mock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(observability.logger, "warning", fake_warning)

    observability.incr("valkyrie.test.count")

    assert len(warnings) == 1
    assert warnings[0][0] == "metric incr(%s) failed: %s: %s"
    assert warnings[0][1][:2] == ("valkyrie.test.count", "RuntimeError")
    assert str(warnings[0][1][2]) == "boom"


def test_distribution_and_gauge_failures_are_logged_without_propagating(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    warnings: list[tuple[str, tuple[object, ...]]] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append((message, args))

    monkeypatch.setattr(observability._sentry_metrics, "distribution", Mock(side_effect=RuntimeError("dist boom")))
    monkeypatch.setattr(observability._sentry_metrics, "gauge", Mock(side_effect=RuntimeError("gauge boom")))
    monkeypatch.setattr(observability.logger, "warning", fake_warning)

    observability.distribution("valkyrie.test.duration", 1.5)
    observability.gauge("valkyrie.test.in_flight", 4)

    assert warnings[0][0] == "metric distribution(%s) failed: %s: %s"
    assert warnings[0][1][:2] == ("valkyrie.test.duration", "RuntimeError")
    assert str(warnings[0][1][2]) == "dist boom"
    assert warnings[1][0] == "metric gauge(%s) failed: %s: %s"
    assert warnings[1][1][:2] == ("valkyrie.test.in_flight", "RuntimeError")
    assert str(warnings[1][1][2]) == "gauge boom"


def test_set_sandbox_context_sets_tags_and_context(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    tags: dict[str, str] = {}
    contexts: dict[str, dict[str, Any]] = {}

    def fake_set_tag(key: str, value: str) -> None:
        tags[key] = value

    def fake_set_context(key: str, value: dict[str, Any]) -> None:
        contexts[key] = value

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", fake_set_tag)
    monkeypatch.setattr(observability.sentry_sdk, "set_context", fake_set_context)

    sandbox = SimpleNamespace(id="sandbox-123", name="bench-task-1", state="STARTED")

    observability.set_sandbox_context(sandbox, image="ghcr.io/example/image:latest")

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


def test_set_sandbox_context_omits_optional_image_and_missing_state(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    contexts: dict[str, dict[str, Any]] = {}

    def fake_set_context(key: str, value: dict[str, Any]) -> None:
        contexts[key] = value

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", Mock())
    monkeypatch.setattr(observability.sentry_sdk, "set_context", fake_set_context)

    sandbox = SimpleNamespace(id="sandbox-123", name="bench-task-1")

    observability.set_sandbox_context(sandbox)

    assert contexts == {
        "sandbox": {
            "id": "sandbox-123",
            "name": "bench-task-1",
            "state": None,
        }
    }


def test_set_sandbox_context_failures_are_logged_without_propagating(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    warnings: list[tuple[str, tuple[object, ...]]] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append((message, args))

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", Mock(side_effect=RuntimeError("scope closed")))
    monkeypatch.setattr(observability.logger, "warning", fake_warning)

    observability.set_sandbox_context(SimpleNamespace(id="sandbox-123", name="bench-task-1"))

    assert warnings[0][0] == "set_sandbox_context failed: %s: %s"
    assert warnings[0][1][:1] == ("RuntimeError",)
    assert str(warnings[0][1][1]) == "scope closed"


def test_set_pty_context_sets_session_and_attempt_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    tags: dict[str, str] = {}

    def fake_set_tag(key: str, value: str) -> None:
        tags[key] = value

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", fake_set_tag)

    observability.set_pty_context(session_id="sandbox:pty-123", attempt=3)

    assert tags == {
        "pty_session_id": "sandbox:pty-123",
        "pty_attempt": "3",
    }


def test_set_pty_context_omits_attempt_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    tags: dict[str, str] = {}

    def fake_set_tag(key: str, value: str) -> None:
        tags[key] = value

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", fake_set_tag)

    observability.set_pty_context(session_id="sandbox:pty-123")

    assert tags == {"pty_session_id": "sandbox:pty-123"}


def test_set_pty_context_failures_are_logged_without_propagating(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    warnings: list[tuple[str, tuple[object, ...]]] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append((message, args))

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", Mock(side_effect=RuntimeError("scope closed")))
    monkeypatch.setattr(observability.logger, "warning", fake_warning)

    observability.set_pty_context(session_id="sandbox:pty-123")

    assert warnings[0][0] == "set_pty_context failed: %s: %s"
    assert warnings[0][1][:1] == ("RuntimeError",)
    assert str(warnings[0][1][1]) == "scope closed"


def test_retry_callback_logs_attempt_and_emits_retry_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    increments: list[tuple[str, dict[str, str]]] = []
    log_records: list[dict[str, Any]] = []

    def fake_incr(name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        increments.append((name, tags or {}))

    def retried_function() -> None:
        raise RuntimeError("unused")

    class FakeOutcome:
        def exception(self) -> Exception:
            return TimeoutError("timed out")

    state = SimpleNamespace(
        outcome=FakeOutcome(),
        fn=retried_function,
        attempt_number=2,
        idle_for=1.25,
    )

    monkeypatch.setattr(observability, "incr", fake_incr)

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

    monkeypatch.setattr(observability.logger, "log", fake_log)

    observability.retry_callback("valkyrie.pty.create")(state)

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


def test_tag_daytona_error_sets_tags_and_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    tags: dict[str, str] = {}
    increments: list[tuple[str, dict[str, str]]] = []

    def fake_set_tag(key: str, value: str) -> None:
        tags[key] = value

    def fake_incr(name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        increments.append((name, tags or {}))

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", fake_set_tag)
    monkeypatch.setattr(observability, "incr", fake_incr)

    observability.tag_daytona_error(TimeoutError("timed out"), op="sandbox.create")

    assert tags == {
        "daytona.op": "sandbox.create",
        "error_class": "TimeoutError",
    }
    assert increments == [
        (
            "valkyrie.daytona.error",
            {
                "op": "sandbox.create",
                "error_class": "TimeoutError",
            },
        )
    ]


def test_tag_daytona_error_logs_tag_failures_and_still_emits_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    observability = _observability()
    warnings: list[tuple[str, tuple[object, ...]]] = []
    increments: list[tuple[str, dict[str, str]]] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append((message, args))

    def fake_incr(name: str, value: float = 1, tags: dict[str, str] | None = None) -> None:
        increments.append((name, tags or {}))

    monkeypatch.setattr(observability.sentry_sdk, "set_tag", Mock(side_effect=RuntimeError("scope closed")))
    monkeypatch.setattr(observability.logger, "warning", fake_warning)
    monkeypatch.setattr(observability, "incr", fake_incr)

    observability.tag_daytona_error(TimeoutError("timed out"), op="sandbox.create")

    assert warnings[0][0] == "tag_daytona_error failed: %s: %s"
    assert warnings[0][1][:1] == ("RuntimeError",)
    assert str(warnings[0][1][1]) == "scope closed"
    assert increments == [
        (
            "valkyrie.daytona.error",
            {
                "op": "sandbox.create",
                "error_class": "TimeoutError",
            },
        )
    ]
