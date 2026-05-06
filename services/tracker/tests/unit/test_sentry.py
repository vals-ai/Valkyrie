from collections.abc import Callable
from typing import cast
from unittest.mock import Mock

import pytest
import sentry_sdk
from sentry_sdk.types import Event, Hint

import tracker.sentry as sentry_module
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
