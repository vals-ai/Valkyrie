"""Unit tests for tracker Sentry configuration.

Run: uv run pytest tests/unit/observability/test_sentry.py
"""

import asyncio

from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
import sentry_sdk
from sentry_sdk.integrations.otlp import OTLPIntegration
from sentry_sdk.types import Event, Hint, Log

import tracker.observability.sentry as sentry_module
import tracker.utils.task_execution as task_execution
from tracker.database.models import Org, Task
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.exceptions import SandboxError, SSLConnectionError

BeforeSend = Callable[[Event, Hint], Event | None]
BeforeSendLog = Callable[[Log, Hint], Log | None]


def _before_send() -> BeforeSend:
    return cast(BeforeSend, getattr(sentry_module, "_before_send"))


def _before_send_log() -> BeforeSendLog:
    return cast(BeforeSendLog, getattr(sentry_module, "_before_send_log"))


class TestBeforeSend:
    """Sentry event grouping and context filtering."""

    def test_before_send_fingerprints_ssl_connection_errors(self) -> None:
        exc = SSLConnectionError("curl failed with exit code 35")
        event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

        assert event is not None
        assert event.get("fingerprint") == ["{{ default }}", "SSLConnectionError"]

    def test_before_send_preserves_non_grouped_errors_and_filters_empty_context_tags(
        self,
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

    def test_before_send_handles_events_without_exception_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_context_tags() -> dict[str, str]:
            return {}

        monkeypatch.setattr(sentry_module, "get_context_tags", fake_context_tags)

        event = _before_send()({"message": "log event"}, {})

        assert event == {"message": "log event", "tags": {}}


class TestSentrySetup:
    """Sentry initialization and log context behavior."""

    def test_init_sentry_registers_otlp_integration_without_exporter_or_propagator(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        init_mock = Mock()
        monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
        monkeypatch.setenv("SENTRY_ENVIRONMENT", "bench")
        monkeypatch.setattr(sentry_sdk, "init", init_mock)
        monkeypatch.setattr(sentry_sdk, "set_tag", Mock())

        sentry_module.init_sentry("valkyrie-worker", environment="test")

        assert init_mock.call_args.kwargs["environment"] == "bench"
        integrations = init_mock.call_args.kwargs["integrations"]
        otlp_integrations = [i for i in integrations if isinstance(i, OTLPIntegration)]
        assert len(otlp_integrations) == 1, "expected exactly one OTLPIntegration in integrations="
        otlp = otlp_integrations[0]
        # Both flags must be False; defaults would double-publish spans and replace the global propagator.
        assert otlp.setup_otlp_traces_exporter is False
        assert otlp.setup_propagator is False

    def test_before_send_log_prefers_current_otel_trace_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        trace_id = "019e04c6fbf0397e32a8d9601f98e45c"
        span_id = "a1f0f4fc15b83e82"
        span_context = SimpleNamespace(
            trace_id=int(trace_id, 16),
            span_id=int(span_id, 16),
            is_valid=True,
        )
        span = SimpleNamespace(get_span_context=lambda: span_context)
        log = cast(
            Log,
            {
                "body": "created sandbox",
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "span_id": None,
            },
        )
        monkeypatch.setattr(sentry_module, "get_current_span", lambda: span)
        monkeypatch.setattr(
            sentry_module,
            "get_context_tags",
            lambda: {"request_id": "request-123", "benchmark_id": "benchmark-123", "task_id": ""},
        )

        result = _before_send_log()(log, {})

        assert result is log
        assert log["trace_id"] == trace_id
        assert log["span_id"] == span_id
        assert log["attributes"] == {
            "request_id": "request-123",
            "benchmark_id": "benchmark-123",
        }

    def test_init_sentry_logs_warning_when_initialization_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        warnings: list[tuple[str, tuple[object, ...]]] = []

        def fake_warning(message: str, *args: object) -> None:
            warnings.append((message, args))

        monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
        monkeypatch.setattr(sentry_sdk, "init", Mock(side_effect=RuntimeError("bad dsn")))
        monkeypatch.setattr(sentry_module.logger, "warning", fake_warning)

        sentry_module.init_sentry("valkyrie-worker", environment="production")

        assert len(warnings) == 1
        assert warnings[0][0] == "Failed to initialize Sentry: %s: %s"
        assert warnings[0][1][:1] == ("RuntimeError",)
        assert str(warnings[0][1][1]) == "bad dsn"

    def test_init_sentry_skips_missing_production_dsn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        init_mock = Mock()
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        monkeypatch.setattr(sentry_sdk, "init", init_mock)

        sentry_module.init_sentry("valkyrie-worker", environment="production")

        init_mock.assert_not_called()


@pytest.mark.asyncio
async def test_task_scope_isolates_concurrent_sandbox_events_and_outer_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[Event] = []
    rows: dict[str, SimpleNamespace] = {}

    class FakeSession:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> "FakeSession":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    def fetch_task(task_id: object, _session: object, _org: object) -> SimpleNamespace:
        return rows[str(task_id)]

    monkeypatch.setattr(task_execution, "Session", FakeSession)
    monkeypatch.setattr(task_execution, "fetch_task_row", fetch_task)
    monkeypatch.setattr(task_execution, "commit_task_error", lambda *_args, **_kwargs: None)

    async def run_tasks() -> None:
        ready = 0
        release = asyncio.Event()

        async def body(task_id: str, sandbox_id: str, *, fail: bool) -> dict[str, dict[str, object] | None]:
            nonlocal ready
            sentry_module.set_sandbox_context(SimpleNamespace(id=sandbox_id, name=f"{sandbox_id}-name"))
            sentry_sdk.capture_message("sandbox event")
            ready += 1
            if ready == 2:
                release.set()
            await release.wait()
            if fail:
                raise RuntimeError("outer failure")
            return {task_id: {"ok": True}}

        for task_id in ("task-a", "task-b"):
            rows[task_id] = SimpleNamespace(
                id=task_id,
                task_id=task_id,
                started_at=datetime.now(UTC),
            )

        task_a = task_execution.TrackedTask(
            body("task-a", "sandbox-a", fail=False),
            cast(Org, object()),
            cast(ExecutionAuthority, object()),
            datetime.now(UTC),
        )
        task_b = task_execution.TrackedTask(
            body("task-b", "sandbox-b", fail=True),
            cast(Org, object()),
            cast(ExecutionAuthority, object()),
            datetime.now(UTC),
        )
        await asyncio.gather(task_a.run(None, cast(Task, rows["task-a"])), task_b.run(None, cast(Task, rows["task-b"])))

    with sentry_sdk.init(
        dsn="https://public@example.com/1",
        transport=events.append,
        default_integrations=False,
        before_send=sentry_module._before_send,
    ):
        await run_tasks()
        sentry_sdk.capture_message("after tasks")

    task_events = [event for event in events if event.get("tags", {}).get("task_id")]
    assert len(task_events) == 3
    task_tags = [cast(dict[str, str], event.get("tags", {})) for event in task_events]
    assert {(tags["task_id"], tags["sandbox_id"]) for tags in task_tags} == {
        ("task-a", "sandbox-a"),
        ("task-b", "sandbox-b"),
    }
    exception_events = [event for event in task_events if "exception" in event]
    assert len(exception_events) == 1
    assert cast(dict[str, str], exception_events[0].get("tags")) == {
        "task_id": "task-b",
        "sandbox_id": "sandbox-b",
        "sandbox_name": "sandbox-b-name",
    }
    assert events[-1].get("tags") == {}
