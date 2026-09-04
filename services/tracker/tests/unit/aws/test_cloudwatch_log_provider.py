"""Tests for CloudWatch benchmark log reads.

Run: uv run pytest services/tracker/tests/unit/aws/test_cloudwatch_log_provider.py
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from uuid import uuid4

import pytest  # pyright: ignore[reportMissingImports]
from botocore.exceptions import ClientError, EndpointConnectionError  # pyright: ignore[reportMissingImports]

from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchLogProvider, task_log_stream_name
from tracker.runtime.logs import LogProviderError, RunLogReference, RunTaskLogReference, TaskLogReference


class MockLogsClient:
    """Return queued CloudWatch responses and retain request parameters."""

    def __init__(self, responses: list[dict[str, Any] | BaseException]) -> None:
        self.responses = deque(responses)
        self.filter_requests: list[dict[str, Any]] = []
        self.get_requests: list[dict[str, Any]] = []

    def filter_log_events(self, **request: Any) -> dict[str, Any]:
        self.filter_requests.append(request)
        return self._response()

    def get_log_events(self, **request: Any) -> dict[str, Any]:
        self.get_requests.append(request)
        return self._response()

    def _response(self) -> dict[str, Any]:
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


class MockClients:
    """Expose one mock CloudWatch Logs client through the provider seam."""

    def __init__(self, logs_client: MockLogsClient) -> None:
        self.logs_client = logs_client

    def cloudwatch_logs_client(self) -> MockLogsClient:
        return self.logs_client


class FailingClients:
    """Raise while constructing a CloudWatch Logs client."""

    def __init__(self, error: BaseException) -> None:
        self.error = error

    def cloudwatch_logs_client(self) -> MockLogsClient:
        raise self.error


def _provider(logs_client: MockLogsClient) -> CloudWatchLogProvider:
    return CloudWatchLogProvider(cast(AWSClientProvider, MockClients(logs_client)), "benchmarks")


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (
            ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "FilterLogEvents"),
            "AccessDeniedException",
        ),
        (EndpointConnectionError(endpoint_url="https://logs.example.com"), "CloudWatch log request failed"),
    ],
)
async def test_snapshot_translates_client_construction_errors(failure: BaseException, message: str) -> None:
    """Snapshot reads must translate client construction failures and retain their cause."""
    provider = CloudWatchLogProvider(cast(AWSClientProvider, FailingClients(failure)), "benchmarks")

    with pytest.raises(LogProviderError, match=message) as error:
        await provider.fetch_run(RunLogReference(run_id=uuid4()))

    assert error.value.__cause__ is failure


async def test_run_fetch_uses_exact_stream_metadata_and_inclusive_millisecond_bound() -> None:
    """Run reads must use exact stream identities, deterministic order, and unchanged end milliseconds."""
    run_id = uuid4()
    first_started = datetime(2026, 1, 1, 11, 59, tzinfo=timezone.utc)
    second_started = datetime(2026, 1, 1, 11, 58, tzinfo=timezone.utc)
    start = datetime(2026, 1, 1, 12, 0, 0, 123_999, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 12, 5, 0, 456_789, tzinfo=timezone.utc)
    logs_client = MockLogsClient(
        [
            {
                "events": [
                    {
                        "timestamp": 2_000,
                        "ingestionTime": 2_100,
                        "message": "second",
                        "logStreamName": task_log_stream_name("task_two", second_started),
                        "eventId": "event-2",
                    },
                    {
                        "timestamp": 1_000,
                        "ingestionTime": 1_100,
                        "message": "first",
                        "logStreamName": task_log_stream_name("task_one", first_started),
                        "eventId": "event-1",
                    },
                ],
                "nextToken": "next-page",
            }
        ]
    )

    page = await _provider(logs_client).fetch_run(
        RunLogReference(
            run_id=run_id,
            tasks=(
                RunTaskLogReference(task_id="task_one", started_at=first_started),
                RunTaskLogReference(task_id="task_two", started_at=second_started),
            ),
        ),
        start_time=start,
        end_time=end,
        cursor="current-page",
        limit=50,
    )

    assert logs_client.filter_requests[0] == {
        "logGroupName": f"benchmarks/{run_id}",
        "limit": 50,
        "startTime": int(start.timestamp() * 1_000),
        "endTime": int(end.timestamp() * 1_000),
        "nextToken": "current-page",
    }
    assert [event.message for event in page.events] == ["first", "second"]
    assert [event.task_id for event in page.events] == ["task_one", "task_two"]
    assert page.next_cursor == "next-page"


async def test_run_fetch_filters_cloudwatch_metacharacters_as_literal_substrings() -> None:
    """Queries must be applied locally so CloudWatch cannot interpret filter metacharacters."""
    run_id = uuid4()
    query = '* "quoted" \\'
    logs_client = MockLogsClient(
        [
            {
                "events": [
                    {"timestamp": 1_000, "message": f"prefix {query} suffix"},
                    {"timestamp": 2_000, "message": "prefix wildcard-like text suffix"},
                ],
                "nextToken": "next-page",
            }
        ]
    )

    page = await _provider(logs_client).fetch_run(RunLogReference(run_id=run_id), query=query)

    assert [event.message for event in page.events] == [f"prefix {query} suffix"]
    assert "filterPattern" not in logs_client.filter_requests[0]
    assert page.next_cursor == "next-page"


async def test_run_attribution_returns_none_for_sanitized_stream_collision() -> None:
    """Two task IDs resolving to one physical stream must never be guessed."""
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = RunTaskLogReference(task_id="task:one", started_at=started_at)
    second = RunTaskLogReference(task_id="task*one", started_at=started_at)
    stream_name = task_log_stream_name(first.task_id, first.started_at)
    assert stream_name == task_log_stream_name(second.task_id, second.started_at)
    logs_client = MockLogsClient(
        [{"events": [{"timestamp": 1_000, "message": "ambiguous", "logStreamName": stream_name}]}]
    )

    page = await _provider(logs_client).fetch_run(RunLogReference(run_id=run_id, tasks=(first, second)))

    assert page.events[0].task_id is None


async def test_task_fetch_keeps_empty_changing_page_and_stops_on_stable_cursor() -> None:
    """An empty page only terminates when CloudWatch stops advancing its token."""
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(run_id=run_id, task_id="provider/model:fast", started_at=started_at)
    logs_client = MockLogsClient(
        [
            {"events": [], "nextToken": "advanced"},
            {"events": [], "nextToken": "advanced"},
        ]
    )
    provider = _provider(logs_client)

    first_page = await provider.fetch_task(reference)
    final_page = await provider.fetch_task(reference, cursor=first_page.next_cursor)

    assert first_page.next_cursor == "advanced"
    assert final_page.next_cursor is None
    assert logs_client.filter_requests[0]["logStreamNames"] == [task_log_stream_name(reference.task_id, started_at)]


async def test_follow_deduplicates_poll_results_and_translates_aws_errors() -> None:
    """Follow mode must emit a repeated event once and retain AWS failures as provider causes."""
    run_id = uuid4()
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reference = TaskLogReference(run_id=run_id, task_id="task", started_at=started_at)
    repeated_event = {"timestamp": 1_000, "ingestionTime": 1_100, "message": "hello"}
    ignored_event = {"timestamp": 1_001, "ingestionTime": 1_101, "message": "noise"}
    logs_client = MockLogsClient(
        [
            {"events": [repeated_event, ignored_event], "nextForwardToken": "tail"},
            {"events": [repeated_event, ignored_event], "nextForwardToken": "tail"},
        ]
    )

    events = [
        event
        async for event in _provider(logs_client).stream_task(
            reference,
            query="hell",
            end_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
            poll_interval=0,
        )
    ]

    assert [event.message for event in events] == ["hello"]

    denied = ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "FilterLogEvents")
    failing_client = MockLogsClient([denied])
    with pytest.raises(LogProviderError, match="AccessDeniedException") as error:
        await _provider(failing_client).fetch_run(RunLogReference(run_id=run_id))
    assert error.value.__cause__ is denied


async def test_follow_deduplication_retains_only_a_recent_identity_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Follow mode must suppress recent duplicates without retaining every emitted identity."""
    monkeypatch.setattr("tracker.aws.cloudwatch_logs._FOLLOW_DEDUPLICATION_WINDOW", 2)
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="task",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    events = [
        {"timestamp": 1_000, "message": "a", "eventId": "a"},
        {"timestamp": 2_000, "message": "b", "eventId": "b"},
        {"timestamp": 1_000, "message": "a", "eventId": "a"},
        {"timestamp": 3_000, "message": "c", "eventId": "c"},
        {"timestamp": 1_000, "message": "a", "eventId": "a"},
    ]
    responses: list[dict[str, Any] | BaseException] = [
        {"events": [event], "nextForwardToken": f"page-{index}"} for index, event in enumerate(events, start=1)
    ]
    responses.append({"events": [], "nextForwardToken": "page-5"})

    messages = [
        event.message
        async for event in _provider(MockLogsClient(responses)).stream_task(
            reference,
            end_time=datetime(2020, 1, 1, tzinfo=timezone.utc),
            poll_interval=0,
        )
    ]

    assert messages == ["a", "b", "c", "a"]


def test_task_stream_name_treats_naive_started_at_as_utc() -> None:
    """Persisted naive timestamps must name the same stream as their UTC instant."""
    naive = datetime(2026, 1, 1, 12, 30, 45, 123_456)
    utc = naive.replace(tzinfo=timezone.utc)
    offset = utc.astimezone(timezone(timedelta(hours=-7)))

    assert task_log_stream_name("task", naive) == task_log_stream_name("task", utc)
    assert task_log_stream_name("task", offset) == task_log_stream_name("task", utc)
