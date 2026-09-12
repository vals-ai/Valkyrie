"""Unit tests for tracker Sentry configuration.

Run: uv run pytest tests/unit/observability/test_sentry.py
"""

import asyncio
import json
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest
import sentry_sdk
import sentry_sdk.scope as sentry_scope
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.otlp import OTLPIntegration
from sentry_sdk.transport import Transport
from sentry_sdk.types import Event, Hint, Log

import tracker.observability.sentry as sentry_module
import tracker.observability.tracing as tracing_module
import tracker.utils.task_execution as task_execution
from tracker.database.models import Org, Task
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.exceptions import SandboxError, SandboxSetupError, SSLConnectionError
from tracker.logging.context import benchmark_id_var, request_id_var

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
@pytest.mark.parametrize("client_is_root", [True, False], ids=["client-root", "client-child"])
async def test_sentry_export_captures_task_identities_on_roots_and_children(
    monkeypatch: pytest.MonkeyPatch,
    client_is_root: bool,
) -> None:
    """Characterize SDK serialization, not Logfire registration or backend indexing."""
    transactions: list[dict[str, Any]] = []

    class CaptureTransport(Transport):
        def capture_envelope(self, envelope: Envelope) -> None:
            for item in envelope.items:
                if item.type == "transaction":
                    transactions.append(json.loads(item.get_bytes()))

    # SentrySpanProcessor registers a global error processor on construction.
    # Keep that registration local to this test; all test-owned spans finish normally.
    monkeypatch.setattr(sentry_scope, "global_event_processors", list(sentry_scope.global_event_processors))
    provider = TracerProvider()
    provider.add_span_processor(tracing_module._ContextVarSpanProcessor())
    provider.add_span_processor(tracing_module._FilteredSentrySpanProcessor())
    tracer = provider.get_tracer(__name__)
    client = sentry_sdk.Client(
        dsn="https://public@example.com/1",
        # Let the SDK pass DSN options into Transport.__init__; the processor checks parsed_dsn.
        transport=CaptureTransport,
        default_integrations=False,
        traces_sample_rate=1.0,
        instrumenter=INSTRUMENTER.OTEL,
        before_send=sentry_module._before_send,
    )
    identities = {
        "task-a": {"request_id": "request-a", "benchmark_id": "benchmark-a", "task_id": "task-a"},
        "task-b": {"request_id": "request-b", "benchmark_id": "benchmark-b", "task_id": "task-b"},
    }
    client_ids: dict[str, str] = {}
    response_ids: dict[str, str] = {}
    ready = 0
    release = asyncio.Event()

    async def request(task_id: str) -> None:
        nonlocal ready
        identity = identities[task_id]
        request_token = request_id_var.set(identity["request_id"])
        benchmark_token = benchmark_id_var.set(identity["benchmark_id"])
        try:
            with sentry_module.task_scope(task_id, attempt_started_at="2026-04-01T12:00:00+00:00"):
                with tracer.start_as_current_span(
                    "POST /execute",
                    context=Context() if client_is_root else None,
                    kind=trace.SpanKind.CLIENT,
                    attributes={"http.method": "POST", "http.url": "https://benchmark.example/execute"},
                ) as span:
                    client_ids[task_id] = f"{span.get_span_context().span_id:016x}"
                    ready += 1
                    if ready == 2:
                        release.set()
                    await release.wait()
                    with tracer.start_as_current_span("response.process") as response:
                        response_ids[task_id] = f"{response.get_span_context().span_id:016x}"
        finally:
            benchmark_id_var.reset(benchmark_token)
            request_id_var.reset(request_token)

    try:
        with sentry_sdk.new_scope() as scope:
            scope.set_client(client)
            parent = nullcontext() if client_is_root else tracer.start_as_current_span("dispatch", context=Context())
            with parent:
                await asyncio.gather(request("task-a"), request("task-b"))
                # A shared transaction finishes after both isolated tasks have left.
                # Its finishing scope must not replace the identities captured on children.
                scope.set_tag("task_id", "finishing-scope-task")
    finally:
        provider.shutdown()
        client.close()

    assert len(transactions) == (2 if client_is_root else 1)
    children = {span["span_id"]: span for event in transactions for span in event["spans"]}
    assert len(children) == (2 if client_is_root else 4)
    for task_id, identity in identities.items():
        if client_is_root:
            event = next(
                event for event in transactions if event["contexts"]["trace"]["span_id"] == client_ids[task_id]
            )
            captured_client = event["contexts"]["otel"]["attributes"]
            assert event["contexts"]["trace"]["op"] == "http.client"
            assert set(identity).isdisjoint(event.get("tags", {}))
            trace_id = event["contexts"]["trace"]["trace_id"]
        else:
            event = transactions[0]
            client_span = children[client_ids[task_id]]
            captured_client = client_span["data"]
            assert client_span["parent_span_id"] == event["contexts"]["trace"]["span_id"]
            assert client_span["op"] == "http.client"
            assert set(identity).isdisjoint(client_span.get("tags", {}))
            trace_id = client_span["trace_id"]
            assert trace_id == event["contexts"]["trace"]["trace_id"]

        response = children[response_ids[task_id]]
        assert response["parent_span_id"] == client_ids[task_id]
        assert response["trace_id"] == trace_id
        for key, value in identity.items():
            assert captured_client[key] == value
            assert response["data"][key] == value
        assert set(identity).isdisjoint(response.get("tags", {}))

    if not client_is_root:
        assert transactions[0]["tags"]["task_id"] == "finishing-scope-task"


@pytest.mark.asyncio
async def test_task_scope_isolates_concurrent_sandbox_events_and_outer_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[Event] = []
    rows: dict[str, SimpleNamespace] = {}

    attempt_starts = {
        "task-a": datetime(2026, 4, 1, 12, tzinfo=UTC),
        "task-b": datetime(2026, 4, 1, 13, tzinfo=UTC),
    }

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
                started_at=attempt_starts[task_id],
            )

        task_a = task_execution.TrackedTask(
            body("task-a", "sandbox-a", fail=False),
            cast(Org, object()),
            cast(ExecutionAuthority, object()),
            attempt_starts["task-a"],
        )
        task_b = task_execution.TrackedTask(
            body("task-b", "sandbox-b", fail=True),
            cast(Org, object()),
            cast(ExecutionAuthority, object()),
            attempt_starts["task-b"],
        )
        await asyncio.gather(
            task_a.run(None, cast(Task, rows["task-a"])),
            task_b.run(None, cast(Task, rows["task-b"])),
        )

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
    assert {(tags["task_id"], tags["sandbox_id"], tags["attempt_started_at"]) for tags in task_tags} == {
        ("task-a", "sandbox-a", "2026-04-01T12:00:00"),
        ("task-b", "sandbox-b", "2026-04-01T13:00:00"),
    }
    exception_events = [event for event in task_events if "exception" in event]
    assert len(exception_events) == 1
    assert cast(dict[str, str], exception_events[0].get("tags")) == {
        "task_id": "task-b",
        "attempt_started_at": "2026-04-01T13:00:00",
        "sandbox_id": "sandbox-b",
        "sandbox_name": "sandbox-b-name",
    }
    assert events[-1].get("tags") == {}


@pytest.mark.asyncio
async def test_retry_attempt_clears_previous_sandbox_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[Event] = []

    class FakeBenchmarkService:
        async def run_with_sandbox_recovery(
            self,
            *,
            operation: Callable[[Any], Any],
            **_kwargs: object,
        ) -> dict[str, dict[str, object] | None]:
            try:
                await operation(SimpleNamespace(number=1))
            except SandboxSetupError:
                pass
            return await operation(SimpleNamespace(number=2))

    async def process_attempt(**kwargs: Any) -> dict[str, dict[str, object] | None]:
        if kwargs["recovery_attempt"].number == 1:
            sentry_module.set_sandbox_context(SimpleNamespace(id="sandbox-first", name="sandbox-first-name"))
            raise SandboxSetupError("retry after first sandbox")

        sentry_sdk.capture_exception(RuntimeError("second attempt failed before sandbox assignment"))
        return {"task-0": {"ok": True}}

    monkeypatch.setattr(task_execution, "_process_task_attempt", process_attempt)

    with sentry_sdk.init(
        dsn="https://public@example.com/1",
        transport=events.append,
        default_integrations=False,
        before_send=_before_send(),
    ):
        with sentry_module.task_scope("task-0", attempt_started_at="2026-04-01T12:00:00+00:00"):
            result = await task_execution.process_task(
                task_row=cast(Any, object()),
                start_benchmark_request=cast(
                    Any,
                    SimpleNamespace(
                        benchmark_name="retry-sandbox-context",
                        contract=SimpleNamespace(name="test-agent"),
                        dataset=None,
                    ),
                ),
                benchmark_service=cast(Any, FakeBenchmarkService()),
                benchmark_id=cast(Any, "benchmark-0"),
                task_id="task-0",
                aws_runtime=cast(Any, object()),
                org=cast(Any, object()),
                sandbox_provider_config=cast(Any, object()),
                creation_semaphore=cast(Any, object()),
                authority=cast(Any, object()),
            )

    assert result == {"task-0": {"ok": True}}
    retry_event = next(event for event in events if "exception" in event)
    retry_tags = cast(dict[str, str], retry_event.get("tags", {}))
    assert retry_tags == {"task_id": "task-0", "attempt_started_at": "2026-04-01T12:00:00+00:00"}
    assert "sandbox" not in retry_event.get("contexts", {})
