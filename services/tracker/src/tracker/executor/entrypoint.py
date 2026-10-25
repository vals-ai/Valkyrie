"""Run one executor dispatch inside its task: claim it, execute the benchmark, and record the outcome."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from contextlib import suppress
from uuid import UUID

import logfire
import httpx
import sentry_sdk
from botocore.exceptions import ClientError
from cryptography.exceptions import InvalidTag
from sqlmodel import Session

from executor_protocol import normalize_executor_telemetry_context
from tracker.config import ENVIRONMENT
from tracker.database.session import engine
from tracker.database.models import ExecutorDispatch, ExecutorDispatchStatus, FailureCategory
from tracker.executor.dispatch_control import (
    LOCAL_DISPATCH_LEASE,
    ClaimedDispatch,
    claim_dispatch,
    finish_dispatch,
    record_dispatch_failure,
    renew_local_dispatch,
)
from tracker.executor.runner_observability import (
    capture_dispatch_error,
    capture_runner_failure,
    dispatch_observability_context,
    record_dispatch_cancellation,
    record_dispatch_completion,
)
from tracker.logging import configure_logging
from tracker.observability import configure_observability
from tracker.utils.run_orchestration import process_benchmark
from tracker.utils.failures import classify_failure

logger = logging.getLogger(__name__)
_LOCAL_RENEW_INTERVAL_SECONDS = 10


def _claim(dispatch_id: UUID) -> ClaimedDispatch | None:
    task_arn = None
    if os.environ["EXECUTOR_LAUNCHER"] == "ecs":
        response = httpx.get(f"{os.environ['ECS_CONTAINER_METADATA_URI_V4']}/task", timeout=5)
        response.raise_for_status()
        task_arn = response.json()["TaskARN"]
    with Session(engine) as session:
        try:
            claimed = claim_dispatch(session, dispatch_id, task_arn=task_arn)
        except (InvalidTag, ClientError, ValueError) as error:
            session.rollback()
            dispatch = session.get(ExecutorDispatch, dispatch_id)
            if dispatch is not None:
                record_dispatch_failure(
                    session,
                    benchmark_id=dispatch.benchmark_id,
                    dispatch_id=dispatch.id,
                    task_ids=dispatch.assigned_task_ids or [],
                    error_message=f"Executor payload decrypt failed: {type(error).__name__}",
                    producer="executor_dispatch",
                    operation="dispatch_claim",
                    error_type=type(error).__name__,
                    category=FailureCategory.INFRASTRUCTURE,
                    cause_code="PAYLOAD_DECRYPT_FAILED",
                    failure_reason="PAYLOAD_DECRYPT_FAILED",
                    dispatch_status=ExecutorDispatchStatus.QUEUED,
                )
                session.commit()
            raise
        session.commit()
    return claimed


def _finish(claimed: ClaimedDispatch) -> None:
    with Session(engine) as session:
        finish_dispatch(session, claimed)
        session.commit()


def _renew_local(dispatch_id: UUID) -> bool:
    with Session(engine) as session:
        renewed = renew_local_dispatch(session, dispatch_id)
        session.commit()
        return renewed


async def _watch_local_dispatch(dispatch_id: UUID, work: asyncio.Task[None], lease: asyncio.Timeout) -> None:
    loop = asyncio.get_running_loop()
    while not work.done():
        await asyncio.sleep(_LOCAL_RENEW_INTERVAL_SECONDS)
        renewal_started = loop.time()
        try:
            renewed = await asyncio.to_thread(_renew_local, dispatch_id)
        except Exception:
            logger.exception("Failed to renew local executor dispatch %s", dispatch_id)
            continue
        deadline = lease.when()
        if not renewed or lease.expired() or (deadline is not None and loop.time() >= deadline):
            work.cancel()
            return
        lease.reschedule(renewal_started + LOCAL_DISPATCH_LEASE.total_seconds())


def _fail(
    claimed: ClaimedDispatch, error_message: str, error_type: str, failure_reason: str, category: FailureCategory
) -> None:
    with Session(engine) as session:
        record_dispatch_failure(
            session,
            benchmark_id=claimed.benchmark_id,
            dispatch_id=claimed.dispatch_id,
            task_ids=claimed.assigned_task_ids,
            error_message=error_message,
            producer="executor_dispatch",
            operation="dispatch_execution",
            error_type=error_type,
            category=category,
            cause_code=failure_reason,
            failure_reason=failure_reason,
        )
        session.commit()


async def run_dispatch(dispatch_id: UUID) -> None:
    loop = asyncio.get_running_loop()
    claim_started = loop.time()
    claimed = await asyncio.to_thread(_claim, dispatch_id)
    if claimed is None:
        logger.info("Executor dispatch %s is no longer claimable", dispatch_id)
        return
    local = os.environ["EXECUTOR_LAUNCHER"] == "local"
    deadline = claim_started + LOCAL_DISPATCH_LEASE.total_seconds() if local else None
    payload = claimed.payload
    telemetry_context = normalize_executor_telemetry_context(payload.get("telemetry_context_json"))
    async with asyncio.timeout_at(deadline) as lease:
        with dispatch_observability_context(
            str(claimed.benchmark_id), str(dispatch_id), claimed.release_id, telemetry_context
        ) as child_context:
            work = asyncio.create_task(
                process_benchmark(
                    start_benchmark_request_json=payload.get("start_benchmark_request_json"),
                    benchmark_id_str=payload.get("benchmark_id_str"),
                    verified_task_ids=payload.get("verified_task_ids"),
                    execution_context_json=payload.get("execution_context_json"),
                    executor_dispatch_id=str(dispatch_id),
                )
            )
            loop.add_signal_handler(signal.SIGTERM, work.cancel)
            local_watch = asyncio.create_task(_watch_local_dispatch(dispatch_id, work, lease)) if local else None
            try:
                await work
            except asyncio.CancelledError:
                await asyncio.to_thread(
                    _fail,
                    claimed,
                    "Executor dispatch was cancelled",
                    "ExecutorDispatchCancelled",
                    "EXECUTOR_CANCELLED",
                    FailureCategory.CANCELLED,
                )
                record_dispatch_cancellation(child_context)
                return
            except Exception as error:
                await asyncio.to_thread(
                    _fail,
                    claimed,
                    f"Executor dispatch failed: {error}",
                    type(error).__name__,
                    "EXECUTOR_FAILED",
                    classify_failure(error),
                )
                capture_dispatch_error(error, child_context)
                raise
            finally:
                loop.remove_signal_handler(signal.SIGTERM)
                if local_watch is not None:
                    local_watch.cancel()
                    with suppress(asyncio.CancelledError):
                        await local_watch
            await asyncio.to_thread(_finish, claimed)
            record_dispatch_completion(child_context)


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
    parser.add_argument("--dispatch-id", required=True, type=UUID)
    dispatch_id: UUID = parser.parse_args().dispatch_id
    configure_logging()
    configure_observability("valkyrie-executor", environment=ENVIRONMENT)
    try:
        asyncio.run(run_dispatch(dispatch_id))
    except Exception as error:
        capture_runner_failure(error, str(dispatch_id))
        raise
    finally:
        _flush_observability()


if __name__ == "__main__":
    main()
