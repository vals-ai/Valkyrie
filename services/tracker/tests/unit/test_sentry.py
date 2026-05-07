from collections.abc import Callable
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
import sentry_sdk
from sentry_sdk.integrations.otlp import OTLPIntegration
from sentry_sdk.types import Event, Hint

import tracker.observability.sentry as sentry_module
from tracker.exceptions import PtyCreationError, SSLConnectionError, SandboxError


BeforeSend = Callable[[Event, Hint], Event | None]


def _before_send() -> BeforeSend:
    return cast(BeforeSend, getattr(sentry_module, "_before_send"))


def test_before_send_fingerprints_pty_creation_errors() -> None:
    exc = PtyCreationError("Failed to create PTY session after 5 attempts: sandbox abc")
    event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

    assert event is not None
    assert event.get("fingerprint") == ["{{ default }}", "PtyCreationError"]


def test_before_send_fingerprints_pty_reconnect_sandbox_errors() -> None:
    exc = SandboxError("PTY reconnect failed after 10 attempts for sandbox abc")
    event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

    assert event is not None
    assert event.get("fingerprint") == ["{{ default }}", "pty_reconnect_failed"]


def test_before_send_fingerprints_ssl_connection_errors() -> None:
    exc = SSLConnectionError("curl failed with exit code 35")
    event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

    assert event is not None
    assert event.get("fingerprint") == ["{{ default }}", "SSLConnectionError"]


def test_before_send_preserves_non_grouped_errors_and_filters_empty_context_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exc = SandboxError("ordinary sandbox setup failure")

    def fake_context_tags() -> dict[str, str]:
        return {
            "benchmark_id": "benchmark-123",
            "task_id": "",
        }

    monkeypatch.setattr(sentry_module, "get_context_tags", fake_context_tags)

    event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

    assert event is not None
    assert "fingerprint" not in event
    assert event.get("tags") == {"benchmark_id": "benchmark-123"}


def test_before_send_handles_events_without_exception_info(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_context_tags() -> dict[str, str]:
        return {}

    monkeypatch.setattr(sentry_module, "get_context_tags", fake_context_tags)

    event = _before_send()({"message": "log event"}, {})

    assert event == {"message": "log event", "tags": {}}


def test_init_sentry_sets_daytona_sdk_version_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    tags: dict[str, str] = {}

    def fake_set_tag(key: str, value: str) -> None:
        tags[key] = value

    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setattr(sentry_sdk, "init", Mock())
    monkeypatch.setattr(sentry_sdk, "set_tag", fake_set_tag)
    monkeypatch.setattr(sentry_module.daytona, "__version__", "0.169.0a2", raising=False)

    sentry_module.init_sentry("valkyrie-worker", environment="test")

    assert tags["daytona.sdk_version"] == "0.169.0a2"


def test_init_sentry_registers_otlp_integration_without_exporter_or_propagator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_mock = Mock()
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setattr(sentry_sdk, "init", init_mock)
    monkeypatch.setattr(sentry_sdk, "set_tag", Mock())

    sentry_module.init_sentry("valkyrie-worker", environment="test")

    integrations = init_mock.call_args.kwargs["integrations"]
    otlp_integrations = [i for i in integrations if isinstance(i, OTLPIntegration)]
    assert len(otlp_integrations) == 1, "expected exactly one OTLPIntegration in integrations="
    otlp = otlp_integrations[0]
    # Both flags must be False; defaults would double-publish spans and replace the global propagator.
    assert otlp.setup_otlp_traces_exporter is False
    assert otlp.setup_propagator is False


def test_before_send_log_prefers_current_otel_trace_context(monkeypatch: pytest.MonkeyPatch) -> None:
    trace_id = "019e04c6fbf0397e32a8d9601f98e45c"
    span_id = "a1f0f4fc15b83e82"
    span_context = SimpleNamespace(
        trace_id=int(trace_id, 16),
        span_id=int(span_id, 16),
        is_valid=True,
    )
    span = SimpleNamespace(get_span_context=lambda: span_context)
    log = {
        "body": "created sandbox",
        "trace_id": "00000000-0000-0000-0000-000000000000",
        "span_id": None,
    }
    monkeypatch.setattr(sentry_module, "get_current_span", lambda: span)

    result = sentry_module._before_send_log(log, {})

    assert result is log
    assert log["trace_id"] == trace_id
    assert log["span_id"] == span_id


def test_init_sentry_logs_warning_when_initialization_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[tuple[str, tuple[object, ...]]] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append((message, args))

    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setattr(sentry_sdk, "init", Mock(side_effect=RuntimeError("bad dsn")))
    monkeypatch.setattr(sentry_module.logger, "warning", fake_warning)

    sentry_module.init_sentry("valkyrie-worker", environment="test")

    assert len(warnings) == 1
    assert warnings[0][0] == "Failed to initialize Sentry: %s: %s"
    assert warnings[0][1][:1] == ("RuntimeError",)
    assert str(warnings[0][1][1]) == "bad dsn"
