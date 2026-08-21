"""Credential-free cross-process transport smoke for run telemetry correlation.

Run: uv run pytest tests/integration/observability/test_run_correlation.py
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import multiprocessing
import os
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import logfire
import sentry_sdk
from sentry_sdk.envelope import Envelope, Item

from services.executor_host import supervisor as host_supervisor
from services.executor_host import observability as host_observability
from services.executor_host.supervisor import ExecutorProcessPayload
from services.tracker import main as tracker_main
from tracker.executor.entrypoint import _executor_context
from tracker.logging import benchmark_id_var, configure_logging, request_id_var, task_id_var
from tracker.observability import configure_observability, error_span, incr

_RUN_ID = "00000000-0000-4000-8000-000000000123"
_REQUEST_ID = "observability-smoke-request"
_DISPATCH_ID = "00000000-0000-4000-8000-000000000456"
_RELEASE_ID = "local-smoke-release"
_ENVIRONMENT = "local-smoke"


class _EnvelopeServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _EnvelopeHandler)
        self.envelopes: list[bytes] = []
        self.envelopes_lock = threading.Lock()

    def record(self, body: bytes) -> None:
        with self.envelopes_lock:
            self.envelopes.append(body)


class _EnvelopeHandler(BaseHTTPRequestHandler):
    server: _EnvelopeServer

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        self.server.record(body)
        self.send_response(200)
        self.end_headers()

    def log_message(self, _format: str, *args: object) -> None:
        del args


@contextmanager
def _local_sentry_receiver() -> Iterator[tuple[str, _EnvelopeServer]]:
    server = _EnvelopeServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://public@{host}:{port}/1", server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _set_sentry_environment(dsn: str) -> None:
    os.environ["SENTRY_DSN"] = dsn
    os.environ["SENTRY_RELEASE"] = _RELEASE_ID
    os.environ["LOG_LEVEL"] = "INFO"


def _capture_test_error(service_name: str, *, span_name: str | None = None) -> None:
    try:
        raise RuntimeError(f"{service_name} observability smoke error")
    except RuntimeError as error:
        if span_name is None:
            sentry_sdk.capture_exception(error)
            return
        with error_span(span_name, error, benchmark_id=_RUN_ID):
            sentry_sdk.capture_exception(error)


def _flush_telemetry() -> None:
    logfire.force_flush(timeout_millis=5000)
    sentry_sdk.flush(timeout=5)


def _run_tracker(dsn: str, context_path: str) -> None:
    _set_sentry_environment(dsn)
    os.environ["ENVIRONMENT"] = "development"
    configure_logging()
    configure_observability("valkyrie-tracker", environment=_ENVIRONMENT)
    logger = logging.getLogger("tracker.observability.smoke")
    tokens = [
        request_id_var.set(_REQUEST_ID),
        benchmark_id_var.set(_RUN_ID),
        task_id_var.set("tracker-task"),
    ]
    try:
        with logfire.span("observability.smoke.tracker"):
            logger.info("Tracker accepted observability smoke run")
            incr(
                "valkyrie.observability.smoke",
                tags={"operation": "run", "benchmark_id": _RUN_ID},
            )
            payload = {
                "start_benchmark_request_json": {},
                "benchmark_id_str": _RUN_ID,
                "verified_task_ids": [],
                "telemetry_context_json": tracker_main._executor_telemetry_context(),  # pyright: ignore[reportPrivateUsage]
                "executor_dispatch_id": _DISPATCH_ID,
                "executor_release_id": _RELEASE_ID,
                "executor_artifact_uri": "s3://artifacts/local-smoke.pex",
                "executor_artifact_digest": "a" * 64,
                "executor_protocol_version": "1",
            }
            Path(context_path).write_text(json.dumps(payload))
            _capture_test_error("valkyrie-tracker", span_name="run.error")
    finally:
        for token in reversed(tokens):
            token.var.reset(token)
        _flush_telemetry()


def _run_executor_host(dsn: str, input_path: str, output_path: str) -> None:
    _set_sentry_environment(dsn)
    os.environ["ENVIRONMENT"] = _ENVIRONMENT
    host_observability.configure_observability()
    payload = cast(dict[str, Any], json.loads(Path(input_path).read_text()))

    async def capture_dispatch(
        _executor_supervisor: object,
        _store: object,
        *,
        executor_dispatch_id: str,
        dispatch: object,
        process_payload: ExecutorProcessPayload,
    ) -> None:
        del _executor_supervisor, _store, executor_dispatch_id, dispatch
        logging.getLogger("executor-host.observability.smoke").info("ExecutorHost dispatched observability smoke run")
        Path(output_path).write_text(json.dumps(process_payload.arguments["telemetry_context_json"]))
        _capture_test_error("valkyrie-executor-host")

    host_supervisor.run_executor_dispatch = capture_dispatch
    asyncio.run(host_supervisor.launch_executor.original_func(**payload))
    _flush_telemetry()


def _run_executor(dsn: str, context_path: str) -> None:
    _set_sentry_environment(dsn)
    os.environ["ENVIRONMENT"] = "development"
    configure_logging()
    configure_observability("valkyrie-executor", environment=_ENVIRONMENT)
    telemetry_context = cast(dict[str, Any], json.loads(Path(context_path).read_text()))
    with _executor_context(
        {
            "benchmark_id_str": _RUN_ID,
            "telemetry_context_json": telemetry_context,
        }
    ):
        with logfire.span("observability.smoke.executor"):
            logging.getLogger("tracker.executor.observability.smoke").info("Executor processed observability smoke run")
            _capture_test_error("valkyrie-executor")
    _flush_telemetry()


def _run_process(target: Any, *args: str) -> None:
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        raise AssertionError(f"{target.__name__} did not finish")
    assert process.exitcode == 0


def _items(server: _EnvelopeServer) -> list[Item]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with server.envelopes_lock:
            if server.envelopes:
                bodies = list(server.envelopes)
                break
        time.sleep(0.05)
    else:
        raise AssertionError("The local Sentry receiver did not receive any envelopes")
    return [item for body in bodies for item in Envelope.deserialize(body).items]


def _json_payload(item: Item) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(item.get_bytes()))


def _log_entries(items: list[Item]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for item in items:
        if item.type != "log":
            continue
        payload = _json_payload(item)
        raw_entries = payload.get("items", [payload])
        if isinstance(raw_entries, list):
            entries.extend(cast(list[dict[str, Any]], raw_entries))
    return entries


def _metric_entries(items: list[Item]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for item in items:
        if item.type != "trace_metric":
            continue
        payload = _json_payload(item)
        raw_entries = payload.get("items", [payload])
        if isinstance(raw_entries, list):
            entries.extend(cast(list[dict[str, Any]], raw_entries))
    return entries


def _attribute_value(entry: Mapping[str, Any], key: str) -> object | None:
    attributes = entry.get("attributes")
    if not isinstance(attributes, Mapping):
        return None
    value = cast(Mapping[str, Any], attributes).get(key)
    if isinstance(value, Mapping):
        return cast(Mapping[str, Any], value).get("value")
    return value


def _breadcrumbs(event: Mapping[str, Any]) -> list[dict[str, Any]]:
    breadcrumbs = event.get("breadcrumbs", {})
    if not isinstance(breadcrumbs, Mapping):
        return []
    values = cast(Mapping[str, Any], breadcrumbs).get("values", [])
    return cast(list[dict[str, Any]], values) if isinstance(values, list) else []


def test_run_id_correlates_logs_errors_and_traces_across_processes(tmp_path: Path) -> None:
    tracker_context = tmp_path / "tracker-context.json"
    executor_context = tmp_path / "executor-context.json"

    with _local_sentry_receiver() as (dsn, server):
        _run_process(_run_tracker, dsn, str(tracker_context))
        _run_process(_run_executor_host, dsn, str(tracker_context), str(executor_context))
        _run_process(_run_executor, dsn, str(executor_context))
        items = _items(server)

    events = [event for item in items if (event := item.get_event()) is not None]
    transactions = [event for item in items if (event := item.get_transaction_event()) is not None]
    logs = _log_entries(items)
    metrics = _metric_entries(items)

    assert {event["server_name"] for event in events} == {
        "valkyrie-tracker",
        "valkyrie-executor-host",
        "valkyrie-executor",
    }
    assert all(event["tags"]["benchmark_id"] == _RUN_ID for event in events)
    assert all(event["environment"] == _ENVIRONMENT for event in events)
    assert all(event["release"] == _RELEASE_ID for event in events)
    assert all(
        any(breadcrumb.get("message", "").endswith("observability smoke run") for breadcrumb in _breadcrumbs(event))
        for event in events
    )

    correlated_logs = [entry for entry in logs if _attribute_value(entry, "benchmark_id") == _RUN_ID]
    assert len(correlated_logs) >= 3
    assert {_attribute_value(entry, "server.address") for entry in correlated_logs} == {
        "valkyrie-tracker",
        "valkyrie-executor-host",
        "valkyrie-executor",
    }

    assert len(transactions) >= 3
    tracker_error_spans = [
        span
        for transaction in transactions
        if transaction.get("transaction") == "observability.smoke.tracker"
        for span in transaction.get("spans", [])
        if span.get("op") == "run.error"
    ]
    assert len(tracker_error_spans) == 1
    assert tracker_error_spans[0]["status"] == "internal_error"
    event_trace_ids = [cast(str, event["contexts"]["trace"]["trace_id"]) for event in events]
    transaction_trace_ids = [cast(str, transaction["contexts"]["trace"]["trace_id"]) for transaction in transactions]
    log_trace_ids = [cast(str, entry["trace_id"]) for entry in correlated_logs]
    assert len(metrics) == 1
    assert metrics[0]["name"] == "valkyrie.observability.smoke"
    assert _attribute_value(metrics[0], "operation") == "run"
    assert _attribute_value(metrics[0], "benchmark_id") is None
    metric_trace_ids = [cast(str, metric["trace_id"]) for metric in metrics]
    assert len(set([*event_trace_ids, *transaction_trace_ids, *log_trace_ids, *metric_trace_ids])) == 1
