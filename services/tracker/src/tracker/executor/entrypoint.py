"""Entrypoint packaged into immutable executor PEX artifacts."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Protocol
from uuid import UUID

import logfire
import sentry_sdk
from opentelemetry.context import attach, detach
from opentelemetry.propagate import extract

from executor_protocol import SUPPORTED_PROTOCOL_VERSION, ExecutorTelemetryContext
from tracker.config import ENVIRONMENT
from tracker.logging import benchmark_id_var, request_id_var, task_id_var
from tracker.observability import configure_observability
from tracker.observability.sentry import capture_exception
from tracker.types import ExecutorProcessPayload
from tracker.utils.run_orchestration import process_benchmark

logger = logging.getLogger(__name__)


class _ExecutorContextPayload(Protocol):
    @property
    def benchmark_id(self) -> UUID: ...

    @property
    def telemetry_context(self) -> ExecutorTelemetryContext: ...


@contextmanager
def _executor_context(payload: _ExecutorContextPayload) -> Generator[None, None, None]:
    telemetry_context = payload.telemetry_context
    context_tokens = [
        request_id_var.set(telemetry_context.request_id),
        benchmark_id_var.set(str(payload.benchmark_id)),
        task_id_var.set(""),
    ]
    trace_headers = telemetry_context.trace_headers
    otel_token = attach(extract(trace_headers)) if trace_headers else None
    try:
        yield
    finally:
        if otel_token is not None:
            detach(otel_token)
        for token in reversed(context_tokens):
            token.var.reset(token)


async def _run_executor(payload: ExecutorProcessPayload) -> None:
    with _executor_context(payload):
        task = asyncio.create_task(
            process_benchmark(
                payload.execution,
                executor_dispatch_id=payload.executor_dispatch_id,
            )
        )
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception as error:
            capture_exception(error)
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

    configure_observability("valkyrie-executor", environment=ENVIRONMENT)
    try:
        decoded: object = json.loads(args.payload.read_text())
        if not isinstance(decoded, dict):
            error = ValueError("expected object")
            capture_exception(error)
            raise SystemExit("Invalid executor payload: expected object")
        try:
            payload = ExecutorProcessPayload.from_wire(decoded)
        except ValueError as error:
            capture_exception(error)
            raise SystemExit(f"Invalid executor payload: {error}") from None
        asyncio.run(_run_executor(payload))
    finally:
        _flush_observability()


if __name__ == "__main__":
    main()
