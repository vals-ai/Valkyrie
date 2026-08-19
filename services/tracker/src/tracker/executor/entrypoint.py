"""Entrypoint packaged into immutable executor PEX artifacts."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Mapping, cast

import logfire
import sentry_sdk
from opentelemetry.context import attach, detach
from opentelemetry.propagate import extract

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tracker.config import ENVIRONMENT
from tracker.logging import benchmark_id_var, request_id_var, task_id_var
from tracker.observability import configure_observability
from tracker.utils.run_orchestration import process_benchmark

logger = logging.getLogger(__name__)


def _benchmark_id(payload: Mapping[str, object]) -> str:
    benchmark_id = payload.get("benchmark_id_str")
    if benchmark_id:
        return str(benchmark_id)
    execution_context = payload.get("execution_context_json")
    if isinstance(execution_context, Mapping):
        benchmark_id = cast(Mapping[object, object], execution_context).get("benchmark_id")
        if benchmark_id:
            return str(benchmark_id)
    return ""


@contextmanager
def _executor_context(payload: Mapping[str, object]) -> Generator[None, None, None]:
    telemetry_context = payload.get("telemetry_context_json")
    request_id = ""
    trace_headers: dict[str, str] = {}
    if isinstance(telemetry_context, Mapping):
        normalized_context = cast(Mapping[object, object], telemetry_context)
        raw_request_id = normalized_context.get("request_id")
        request_id = str(raw_request_id) if raw_request_id else ""
        raw_trace_headers = normalized_context.get("trace_headers")
        if isinstance(raw_trace_headers, Mapping):
            trace_headers = {
                str(key): str(value) for key, value in cast(Mapping[object, object], raw_trace_headers).items()
            }

    context_tokens = [
        request_id_var.set(request_id),
        benchmark_id_var.set(_benchmark_id(payload)),
        task_id_var.set(""),
    ]
    otel_token = attach(extract(trace_headers)) if trace_headers else None
    try:
        yield
    finally:
        if otel_token is not None:
            detach(otel_token)
        for token in reversed(context_tokens):
            token.var.reset(token)


async def _run_executor(payload: dict[str, Any]) -> None:
    with _executor_context(payload):
        task = asyncio.create_task(
            process_benchmark(
                start_benchmark_request_json=payload.get("start_benchmark_request_json"),
                benchmark_id_str=payload.get("benchmark_id_str"),
                verified_task_ids=payload.get("verified_task_ids"),
                execution_context_json=payload.get("execution_context_json"),
                executor_dispatch_id=payload["executor_dispatch_id"],
            )
        )
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception as error:
            sentry_sdk.capture_exception(error)
            raise
        finally:
            loop.remove_signal_handler(signal.SIGTERM)


def _flush_observability() -> None:
    try:
        logfire.force_flush(timeout_millis=3000)
    except Exception as error:
        logger.warning("Failed to flush executor traces: %s: %s", type(error).__name__, error)
    try:
        sentry_sdk.flush(timeout=3)
    except Exception as error:
        logger.warning("Failed to flush executor Sentry events: %s: %s", type(error).__name__, error)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("payload", nargs="?", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        print(json.dumps({"protocol_version": SUPPORTED_PROTOCOL_VERSION}))
        return
    if args.payload is None:
        parser.error("payload is required")

    decoded: object = json.loads(args.payload.read_text())
    if not isinstance(decoded, dict):
        raise SystemExit("Invalid executor payload: expected object")
    payload = cast(dict[str, Any], decoded)
    configure_observability("valkyrie-executor", environment=ENVIRONMENT)
    try:
        asyncio.run(_run_executor(payload))
    finally:
        _flush_observability()


if __name__ == "__main__":
    main()
