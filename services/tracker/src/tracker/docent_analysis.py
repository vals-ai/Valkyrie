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

from botocore.config import Config
from sqlmodel import Session as _Session

from tracker._lambda import invoke_lambda, lambda_client
from tracker.database.models import Benchmark, DocentReadingStatus
from tracker.types import AWSCredentials

# Analyzer Lambdas can run up to 15 min (AWS Lambda's ceiling); retries
# disabled because the Lambda is non-idempotent (a retry would re-ingest).
# Hoisted to a module-level constant so lambda_client's lru_cache actually
# deduplicates across calls (Config instances aren't equal by value).
_ANALYZER_CONFIG = Config(read_timeout=905, retries={"max_attempts": 1})


def invoke_analyzer(
    *,
    benchmark: Benchmark,
    session: _Session,
    lambda_function: str,
    payload: dict[str, Any],
    aws: AWSCredentials,
) -> dict[str, Any]:
    """Invoke an analyzer Lambda and persist status/URL onto the Benchmark row.

    try/finally guarantees status is always set before returning, so no row is
    left at RUNNING on failure.
    """
    benchmark.docent_reading_status = DocentReadingStatus.RUNNING
    session.add(benchmark)
    session.commit()

    try:
        result = invoke_lambda(lambda_client(aws, _ANALYZER_CONFIG), lambda_function, payload)
        reading_plan_url = result.get("reading_plan_url")
        if reading_plan_url:
            benchmark.docent_reading_url = str(reading_plan_url)
        benchmark.docent_reading_status = DocentReadingStatus.DONE
        return result
    except Exception:
        benchmark.docent_reading_status = DocentReadingStatus.ERROR
        raise
    finally:
        session.add(benchmark)
        session.commit()


async def analyze_event_stream(
    *,
    benchmark_row: Benchmark,
    session: _Session,
    lambda_function: str,
    payload: dict[str, Any],
    aws: AWSCredentials,
) -> AsyncGenerator[str, None]:
    """SSE event stream: started → heartbeats → done|error."""
    yield f"event: started\ndata: {json.dumps({'lambda_function': lambda_function})}\n\n"

    invoke_task = asyncio.create_task(
        asyncio.to_thread(
            invoke_analyzer,
            benchmark=benchmark_row,
            session=session,
            lambda_function=lambda_function,
            payload=payload,
            aws=aws,
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
