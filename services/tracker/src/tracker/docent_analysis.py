"""Tracker-side Docent analyzer Lambda invocation.

Used by the SSE endpoint behind `valk run analyze` (manual trigger).
Sets docent_reading_status to RUNNING on entry, DONE on success (with
the URL), ERROR on failure. try/finally guarantees no stuck-RUNNING rows.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID

from botocore.config import Config
from sqlmodel import Session

from tracker._lambda import invoke_lambda
from tracker.aws.clients import AWSClientProvider
from tracker.database.models import Benchmark, DocentReadingStatus
from tracker.database.session import engine
from tracker.runtime.lifecycle import finish_cleanup

# Analyzer Lambdas can run up to 15 min (AWS Lambda's ceiling); retries
# disabled because the Lambda is non-idempotent (a retry would re-ingest).
_ANALYZER_CONFIG = Config(read_timeout=905, retries={"max_attempts": 1})


def _set_analyzer_status(benchmark_id: UUID, status: DocentReadingStatus, reading_plan_url: str | None = None) -> None:
    with Session(engine) as session:
        benchmark = session.get(Benchmark, benchmark_id)
        if benchmark is None:
            raise ValueError(f"benchmark {benchmark_id} not found")
        benchmark.docent_reading_status = status
        if reading_plan_url:
            benchmark.docent_reading_url = reading_plan_url
        session.add(benchmark)
        session.commit()


async def invoke_analyzer(
    *,
    benchmark_id: UUID,
    lambda_function: str,
    payload: dict[str, Any],
    clients: AWSClientProvider,
) -> dict[str, Any]:
    """Await the analyzer while keeping synchronous database sessions in worker threads."""
    status = DocentReadingStatus.ERROR
    reading_plan_url = None
    try:
        await finish_cleanup(
            asyncio.create_task(asyncio.to_thread(_set_analyzer_status, benchmark_id, DocentReadingStatus.RUNNING))
        )
        result = await invoke_lambda(clients, lambda_function, payload, config=_ANALYZER_CONFIG)
        if url := result.get("reading_plan_url"):
            reading_plan_url = str(url)
        status = DocentReadingStatus.DONE
        return result
    finally:
        await finish_cleanup(
            asyncio.create_task(asyncio.to_thread(_set_analyzer_status, benchmark_id, status, reading_plan_url))
        )


async def analyze_event_stream(
    *,
    benchmark_id: UUID,
    lambda_function: str,
    payload: dict[str, Any],
    clients: AWSClientProvider,
) -> AsyncGenerator[str, None]:
    """SSE event stream: started → heartbeats → done|error."""
    yield f"event: started\ndata: {json.dumps({'lambda_function': lambda_function})}\n\n"

    invoke_task = asyncio.create_task(
        invoke_analyzer(
            benchmark_id=benchmark_id,
            lambda_function=lambda_function,
            payload=payload,
            clients=clients,
        )
    )

    while not invoke_task.done():
        try:
            await asyncio.wait_for(asyncio.shield(invoke_task), timeout=10.0)
        except asyncio.TimeoutError:
            yield "event: heartbeat\ndata: {}\n\n"
        except Exception:
            # Real error from the invocation — break out and let the block below
            # format the `error` event. Without this, the exception escapes the
            # generator and FastAPI logs "response already started".
            break

    try:
        result = await invoke_task
        yield f"event: done\ndata: {json.dumps(result)}\n\n"
    except Exception as e:
        yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"
