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
from uuid import UUID

import httpx
import logfire
import sentry_sdk
from opentelemetry.context import attach, detach
from opentelemetry.propagate import extract
from pydantic import SecretStr

from executor_protocol import (
    SUPPORTED_PROTOCOL_VERSION,
    executor_payload_benchmark_id,
    normalize_executor_telemetry_context,
)
from tracker.config import AUTH_REQUIRED, ENVIRONMENT
from tracker.aws.runtime import AWSResources
from tracker.aws.services import CloudRuntimeFactory
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.executor.api_execution import run_with_dispatch_lease
from tracker.executor.run_execution import process_benchmark_v1
from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.schemas import ClaimRequest
from tracker.logging import benchmark_id_var, request_id_var, task_id_var
from tracker.logging.context import attempt_started_at_var, executor_dispatch_id_var
from tracker.observability import configure_observability
from tracker.observability.sentry import capture_exception
from tracker.utils.run_orchestration import parse_queued_execution, process_benchmark
from tracker.outbound_security import validate_custom_service_destination

logger = logging.getLogger(__name__)


async def _run_api_executor(payload: dict[str, Any]) -> None:
    execution = parse_queued_execution(
        payload.get("start_benchmark_request_json"),
        payload.get("benchmark_id_str"),
        payload.get("verified_task_ids"),
        payload.get("execution_context_json"),
    )
    dispatch_id = UUID(payload["executor_dispatch_id"])
    claimant_id = UUID(payload["executor_claimant_id"])
    claim = ClaimRequest(
        claimant_id=claimant_id,
        benchmark_id=execution.benchmark_id,
        executor_release_id=payload["executor_release_id"],
        executor_artifact_uri=payload["executor_artifact_uri"],
        executor_artifact_digest=payload["executor_artifact_digest"],
        executor_protocol_version=payload["executor_protocol_version"],
    )
    async with httpx.AsyncClient(base_url=payload["executor_tracker_url"]) as http:
        api = ExecutorClient(
            ExecutorTransport(http, SecretStr(payload["executor_api_token"])), dispatch_id, claimant_id
        )

        async def execute() -> None:
            state = await api.run_state([])
            run = state.run
            if run.benchmark_id != execution.benchmark_id or run.aws_managed != execution.aws_managed:
                raise TrackerServiceError("Queued execution does not match the stored run")
            request = execution.request
            if request.custom_benchmark_service is not None:
                validate_custom_service_destination(
                    request.custom_benchmark_service, org_name=run.org_name, auth_required=AUTH_REQUIRED
                )
            resources = (
                AWSResources(
                    region=run.resources.region,
                    s3_bucket=run.resources.s3_bucket,
                    log_group=run.resources.log_group,
                    log_retention_days=run.resources.log_retention_days,
                )
                if run.resources is not None
                else None
            )
            if execution.context_version == 3 and (resources is None or resources != request.properties):
                raise TrackerServiceError("Queued managed resources differ from the saved run")
            runtime = await CloudRuntimeFactory.create_execution_runtime(
                request,
                run.org_id,
                run.benchmark_id,
                properties=resources,
                context_version=execution.context_version,
            )
            await process_benchmark_v1(api, request, runtime, execution.verified_task_ids, dispatch_id, run=run)

        try:
            await run_with_dispatch_lease(api, claim, execute)
        except ExecutionAuthorityRevoked:
            logger.info("Executor dispatch %s no longer has execution authority", dispatch_id)
        except Exception as error:
            if (await api.authority()).current:
                await api.fail(f"{type(error).__name__}: {error}"[:4096])
            raise


@contextmanager
def _executor_context(payload: Mapping[str, object]) -> Generator[None, None, None]:
    telemetry_context = normalize_executor_telemetry_context(payload.get("telemetry_context_json"))

    context_tokens = [
        request_id_var.set(telemetry_context["request_id"]),
        benchmark_id_var.set(executor_payload_benchmark_id(payload)),
        task_id_var.set(""),
        executor_dispatch_id_var.set(cast(str, payload["executor_dispatch_id"])),
        attempt_started_at_var.set(""),
    ]
    trace_headers = telemetry_context["trace_headers"]
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
        operation = (
            _run_api_executor(payload)
            if payload.get("executor_protocol_version") == "4"
            else process_benchmark(
                start_benchmark_request_json=payload.get("start_benchmark_request_json"),
                benchmark_id_str=payload.get("benchmark_id_str"),
                verified_task_ids=payload.get("verified_task_ids"),
                execution_context_json=payload.get("execution_context_json"),
                executor_dispatch_id=payload["executor_dispatch_id"],
            )
        )
        task = asyncio.create_task(operation)
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
