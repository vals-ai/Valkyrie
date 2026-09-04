"""Tests for authenticated benchmark log endpoints.

Run: uv run pytest services/tracker/tests/unit/api/test_log_routes.py
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import cast
from uuid import uuid4

import pytest  # pyright: ignore[reportMissingImports]
from botocore.exceptions import ClientError  # pyright: ignore[reportMissingImports]
from fastapi.testclient import TestClient  # pyright: ignore[reportMissingImports]
from sqlmodel import Session  # pyright: ignore[reportMissingImports]

from main import app
from tests.factories import make_task
from tracker.api import logs as logs_api
from tracker.api.dependencies import get_log_provider
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchLogProvider
from tracker.database.models import Benchmark, Org
from tracker.runtime.logs import LogEvent, LogPage, LogProvider, RunLogReference, RunTaskLogReference, TaskLogReference

_client = TestClient(app)


class MockLogProvider(LogProvider):
    """Return deterministic log pages while retaining provider-neutral references."""

    def __init__(self) -> None:
        self.task_reference: TaskLogReference | None = None
        self.run_reference: RunLogReference | None = None
        self.query: str | None = None
        self.stream_release: asyncio.Event | None = None
        self.stream_closed = False

    async def fetch(
        self,
        reference: RunLogReference | TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        self.query = query
        if isinstance(reference, TaskLogReference):
            self.task_reference = reference
            return LogPage(
                events=[
                    LogEvent(
                        timestamp=start_time or reference.started_at,
                        message="task log",
                        task_id=reference.task_id,
                    )
                ],
                next_cursor=cursor,
            )

        self.run_reference = reference
        timestamp = start_time or datetime(2026, 1, 1, tzinfo=timezone.utc)
        return LogPage(
            events=[
                LogEvent(timestamp=timestamp, message="first", task_id="task-1"),
                LogEvent(timestamp=timestamp, message="second", task_id="task-2"),
            ]
        )

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        self.task_reference = reference
        self.query = query
        try:
            if self.stream_release is not None:
                await self.stream_release.wait()
            yield LogEvent(timestamp=start_time or reference.started_at, message="streamed", task_id=reference.task_id)
        finally:
            self.stream_closed = True


class FailingClients:
    """Fail while constructing the CloudWatch Logs client."""

    def cloudwatch_logs_client(self) -> object:
        raise ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "FilterLogEvents",
        )


def _override_provider(monkeypatch: pytest.MonkeyPatch, provider: LogProvider) -> None:
    monkeypatch.setitem(app.dependency_overrides, get_log_provider, lambda: provider)


def test_client_construction_failure_uses_snapshot_and_sse_error_paths(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Client construction failures must remain provider-neutral in snapshots and streams."""
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = CloudWatchLogProvider(cast(AWSClientProvider, FailingClients()), "benchmarks")
    _override_provider(monkeypatch, provider)

    snapshot_response = _client.get(f"/benchmarks/{benchmark.id}/logs", headers=harness_headers)
    stream_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs/stream",
        params={"task_id": task.task_id},
        headers=harness_headers,
    )

    assert snapshot_response.status_code == 502
    assert snapshot_response.json() == {"detail": "CloudWatch log request failed: AccessDeniedException"}
    assert stream_response.status_code == 200
    assert "event: error" in stream_response.text
    assert '"detail": "CloudWatch log request failed: AccessDeniedException"' in stream_response.text


def test_task_and_run_logs_use_scoped_provider_references(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task and aggregate endpoints must resolve scoped identities without exposing AWS locations."""
    benchmark = example_benchmark_object
    first_task = make_task(benchmark, "provider/model:fast")
    second_task = make_task(benchmark, "task-2")
    database_session.add_all([benchmark, first_task, second_task])
    database_session.commit()
    provider = MockLogProvider()
    _override_provider(monkeypatch, provider)

    task_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={
            "task_id": first_task.task_id,
            "query": "needle",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-01T01:00:00+00:00",
        },
        headers=harness_headers,
    )
    run_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={"query": "aggregate"},
        headers=harness_headers,
    )

    assert task_response.status_code == 200
    assert task_response.json()["events"][0]["task_id"] == first_task.task_id
    assert provider.task_reference == TaskLogReference(
        run_id=benchmark.id,
        task_id=first_task.task_id,
        started_at=first_task.started_at,
    )
    assert run_response.status_code == 200
    assert [event["task_id"] for event in run_response.json()["events"]] == ["task-1", "task-2"]
    assert provider.run_reference == RunLogReference(
        run_id=benchmark.id,
        tasks=(
            RunTaskLogReference(task_id=first_task.task_id, started_at=first_task.started_at),
            RunTaskLogReference(task_id=second_task.task_id, started_at=second_task.started_at),
        ),
    )
    assert task_response.request.url.params["task_id"] == first_task.task_id
    assert "log_group" not in task_response.request.url.params
    assert "log_stream" not in task_response.request.url.params


def test_log_routes_validate_time_and_prevent_cross_org_access(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Log endpoints must reject ambiguous bounds and runs outside the active organization."""
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    other_org = Org(id=uuid4(), name="other")
    other_benchmark = Benchmark(org_id=other_org.id, name=benchmark.name, arguments=benchmark.arguments)
    database_session.add_all([benchmark, task, other_org, other_benchmark])
    database_session.commit()
    provider = MockLogProvider()
    _override_provider(monkeypatch, provider)

    naive_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={"start_time": "2026-01-01T00:00:00"},
        headers=harness_headers,
    )
    reversed_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={
            "start_time": "2026-01-01T01:00:00+00:00",
            "end_time": "2026-01-01T00:00:00+00:00",
        },
        headers=harness_headers,
    )
    equal_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-01T00:00:00+00:00",
        },
        headers=harness_headers,
    )
    other_response = _client.get(f"/benchmarks/{other_benchmark.id}/logs", headers=harness_headers)

    assert naive_response.status_code == 422
    assert reversed_response.status_code == 422
    assert equal_response.status_code == 422
    assert other_response.status_code == 404
    assert provider.run_reference is None


def test_task_log_stream_returns_sse_events(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The task stream must frame typed log events and pass literal queries to the provider."""
    benchmark = example_benchmark_object
    task = make_task(benchmark, "provider/model:fast")
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = MockLogProvider()
    _override_provider(monkeypatch, provider)

    response = _client.get(
        f"/benchmarks/{benchmark.id}/logs/stream",
        params={"task_id": task.task_id, "query": "needle"},
        headers=harness_headers,
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert "event: log" in response.text
    assert '"message":"streamed"' in response.text
    assert "event: end" in response.text
    assert provider.query == "needle"
    assert response.request.url.params["task_id"] == task.task_id


async def test_task_log_stream_sends_keep_alives_without_closing_pending_provider() -> None:
    """Idle streams must emit comments and cancel the pending provider read only when closed."""
    provider = MockLogProvider()
    provider.stream_release = asyncio.Event()
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="provider/model:fast",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    events = logs_api._stream_events(  # pyright: ignore[reportPrivateUsage]
        provider,
        reference,
        query=None,
        start_time=None,
        end_time=None,
        keep_alive_interval=0,
    )

    assert await anext(events) == ": connected\n\n"
    assert await anext(events) == ": keep-alive\n\n"
    assert await anext(events) == ": keep-alive\n\n"
    assert not provider.stream_closed

    await events.aclose()

    assert provider.stream_closed
