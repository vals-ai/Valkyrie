"""Discard pending local credentials when dispatches expire or stop."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime

from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, ExecutorDispatch, ExecutorDispatchStatus
from tracker.database.session import engine
from tracker.local.handoff import pending_execution_secrets

logger = logging.getLogger(__name__)


def reap_execution_secrets() -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    dispatch_ids = pending_execution_secrets.pending_ids()
    if not dispatch_ids:
        return
    with Session(engine) as session:
        for dispatch_id in dispatch_ids:
            dispatch = session.get(ExecutorDispatch, dispatch_id)
            benchmark = session.get(Benchmark, dispatch.benchmark_id) if dispatch is not None else None
            deadline = None
            if dispatch is not None:
                if dispatch.status == ExecutorDispatchStatus.QUEUED:
                    deadline = dispatch.claim_deadline_at
                elif dispatch.status == ExecutorDispatchStatus.RUNNING:
                    deadline = dispatch.lease_expires_at
            if (
                benchmark is None
                or benchmark.status != BenchmarkStatus.IN_PROGRESS
                or deadline is None
                or deadline.replace(tzinfo=None) <= now
            ):
                pending_execution_secrets.discard(dispatch_id)


async def _reap_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(reap_execution_secrets)
        except Exception:
            logger.exception("Failed to reconcile pending local execution secrets")
        await asyncio.sleep(5)


@asynccontextmanager
async def local_secret_handoff_lifespan() -> AsyncGenerator[None]:
    task = asyncio.create_task(_reap_loop())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        pending_execution_secrets.close()
