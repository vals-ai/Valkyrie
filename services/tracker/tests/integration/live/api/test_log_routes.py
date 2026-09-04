"""Live integration tests for CloudWatch log routes.

Run: cd services/tracker && uv run pytest tests/integration/live/api/test_log_routes.py

Covers real CloudWatch writes followed by aggregate, task, time-filtered, and streaming reads.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.aws.cloudwatch_logs import (
    CloudWatchBenchmarkLogSink,
    benchmark_log_group_name,
    task_log_stream_name,
)
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Task
from tracker.types import HarnessConfig


def _wait_for_events(
    client: TestClient,
    path: str,
    *,
    headers: dict[str, str],
    params: dict[str, str],
    expected_count: int,
    timeout_seconds: float = 30.0,
) -> list[dict[str, Any]]:
    """Poll a live log route until CloudWatch exposes the expected events."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        response = client.get(path, headers=headers, params=params)
        assert response.status_code == 200, response.text

        events = response.json()["events"]
        if len(events) >= expected_count:
            return events

        time.sleep(1.0)

    raise AssertionError(f"CloudWatch did not expose {expected_count} event(s) within {timeout_seconds} seconds.")


def test_log_routes_round_trip_real_cloudwatch(
    live_api_client: TestClient,
    database_session: Session,
    harness_config: HarnessConfig,
    harness_headers: dict[str, str],
) -> None:
    """Exercise aggregate, task, bounded, filtered, and streaming reads against CloudWatch.

    Test cases:
    - Aggregate reads return events from two task streams with task attribution.
    - A slash-containing task ID round-trips through the task query parameter.
    - Literal queries and inclusive time bounds select the expected task event.
    - The SSE route reads the same event through ``GetLogEvents`` and terminates cleanly.
    """
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name="live-cloudwatch-logs",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="live-cloudwatch-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )
    first_task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="live/logs:first")
    second_task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="live-logs-second")
    database_session.add_all([benchmark, first_task, second_task])
    database_session.commit()
    database_session.refresh(first_task)
    database_session.refresh(second_task)

    runtime = AWSRuntime.from_harness_config(harness_config)
    log_group_name = benchmark_log_group_name(runtime.resources.log_group, str(benchmark.id))
    log_sink = CloudWatchBenchmarkLogSink(runtime.clients, runtime.resources.log_group)
    logs_client = runtime.clients.cloudwatch_logs_client()
    marker = f"live-cloudwatch-{uuid4().hex}"
    first_message = f"{marker} literal * first"
    second_message = f"{marker} second"
    start_time = datetime.now(timezone.utc) - timedelta(seconds=5)
    log_group_created = False

    try:
        log_sink.create_benchmark(str(benchmark.id), retention_days=1)
        log_group_created = True
        log_sink.write(
            f"{benchmark.id}:{task_log_stream_name(first_task.task_id, first_task.started_at)}",
            first_message,
        )
        log_sink.write(
            f"{benchmark.id}:{task_log_stream_name(second_task.task_id, second_task.started_at)}",
            second_message,
        )
        end_time = datetime.now(timezone.utc) + timedelta(seconds=5)
        bounds = {"start_time": start_time.isoformat(), "end_time": end_time.isoformat()}

        aggregate_events = _wait_for_events(
            live_api_client,
            f"/benchmarks/{benchmark.id}/logs",
            headers=harness_headers,
            params={"query": marker, **bounds},
            expected_count=2,
        )
        assert {event["message"]: event["task_id"] for event in aggregate_events} == {
            first_message: first_task.task_id,
            second_message: second_task.task_id,
        }

        task_events = _wait_for_events(
            live_api_client,
            f"/benchmarks/{benchmark.id}/logs/task",
            headers=harness_headers,
            params={"task_id": first_task.task_id, "query": "literal *", **bounds},
            expected_count=1,
        )
        assert [event["message"] for event in task_events] == [first_message]

        stream_response = live_api_client.get(
            f"/benchmarks/{benchmark.id}/logs/task/stream",
            headers=harness_headers,
            params={
                "task_id": first_task.task_id,
                "query": "literal *",
                "start_time": start_time.isoformat(),
                "end_time": datetime.now(timezone.utc).isoformat(),
            },
        )
        assert stream_response.status_code == 200
        assert "event: log" in stream_response.text
        assert first_message in stream_response.text
        assert "event: end" in stream_response.text
    finally:
        if log_group_created:
            logs_client.delete_log_group(logGroupName=log_group_name)
