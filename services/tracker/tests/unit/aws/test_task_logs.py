from datetime import datetime
from unittest.mock import Mock, call
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

from tracker.aws.cloudwatch_logs import (
    get_task_log_events,
    list_task_log_attempts,
    task_log_attempt_id,
)
from tracker.aws.runtime import AWSResources, AWSRuntime


def _runtime(client: Mock) -> AWSRuntime:
    provider = Mock()
    provider.cloudwatch_logs_client.return_value = client
    return AWSRuntime(
        resources=AWSResources(
            region="us-east-1",
            s3_bucket="bucket",
            log_group="/valkyrie/benchmarks-dev",
            log_retention_days=7,
        ),
        clients=provider,
    )


def test_list_task_log_attempts_pages_until_exact_streams_fill_the_page() -> None:
    client = Mock()
    client.describe_log_streams.side_effect = [
        {
            "logStreams": [
                {
                    "logStreamName": "task_deadbeef_not-an-attempt",
                    "creationTime": 1,
                }
            ],
            "nextToken": "describe-page-2",
        },
        {
            "logStreams": [
                {
                    "logStreamName": "task_deadbeef",
                    "creationTime": 2,
                    "firstEventTimestamp": 3,
                    "lastEventTimestamp": 4,
                    "lastIngestionTime": 5,
                },
                {
                    "logStreamName": "task_f00d",
                    "creationTime": 6,
                },
            ],
            "nextToken": "describe-page-3",
        },
    ]

    page = list_task_log_attempts(
        "run-id",
        "task",
        _runtime(client),
        limit=2,
        cursor=None,
    )

    assert [attempt.attempt_id for attempt in page.attempts] == ["deadbeef", "f00d"]
    assert page.attempts[0].creation_time_ms == 2
    assert page.attempts[0].first_event_time_ms == 3
    assert page.attempts[0].last_event_time_ms == 4
    assert page.attempts[0].last_ingestion_time_ms == 5
    assert page.attempts[1].first_event_time_ms is None
    assert page.next_cursor == "describe-page-3"
    assert client.describe_log_streams.call_args_list == [
        call(
            logGroupName="/valkyrie/benchmarks-dev/run-id",
            logStreamNamePrefix="task_",
            orderBy="LogStreamName",
            descending=True,
            limit=2,
        ),
        call(
            logGroupName="/valkyrie/benchmarks-dev/run-id",
            logStreamNamePrefix="task_",
            orderBy="LogStreamName",
            descending=True,
            limit=2,
            nextToken="describe-page-2",
        ),
    ]


def test_get_task_log_events_uses_attempt_derived_stream_and_direction_cursor() -> None:
    client = Mock()
    client.get_log_events.side_effect = [
        {
            "events": [{"timestamp": 10, "ingestionTime": 11, "message": "first"}],
            "nextForwardToken": "forward-2",
            "nextBackwardToken": "backward-1",
        },
        {
            "events": [{"timestamp": 20, "ingestionTime": 21, "message": "last"}],
            "nextForwardToken": "forward-3",
            "nextBackwardToken": "backward-2",
        },
    ]
    runtime = _runtime(client)

    forward = get_task_log_events(
        "run-id",
        "provider/model:fast",
        "deadbeef",
        runtime,
        direction="forward",
        limit=100,
        cursor="forward-1",
    )
    backward = get_task_log_events(
        "run-id",
        "provider/model:fast",
        "deadbeef",
        runtime,
        direction="backward",
        limit=50,
        cursor=None,
    )

    assert forward is not None
    assert backward is not None
    assert forward.events[0].message == "first"
    assert forward.older_cursor == "backward-1"
    assert forward.newer_cursor == "forward-2"
    assert backward.events[0].timestamp_ms == 20
    assert backward.older_cursor == "backward-2"
    assert backward.newer_cursor == "forward-3"
    assert client.get_log_events.call_args_list == [
        call(
            logGroupName="/valkyrie/benchmarks-dev/run-id",
            logStreamName="provider/model%3Afast_deadbeef",
            limit=100,
            startFromHead=True,
            nextToken="forward-1",
        ),
        call(
            logGroupName="/valkyrie/benchmarks-dev/run-id",
            logStreamName="provider/model%3Afast_deadbeef",
            limit=50,
            startFromHead=False,
        ),
    ]


def test_task_log_attempt_id_matches_worker_stream_suffix() -> None:
    started_at = datetime(2026, 7, 22, 12, 30, 15, 123456, tzinfo=ZoneInfo("UTC"))

    assert task_log_attempt_id(started_at) == f"{int(started_at.timestamp() * 1_000_000):x}"
    assert task_log_attempt_id(started_at.replace(tzinfo=None)) == task_log_attempt_id(started_at)


def test_list_task_log_attempts_requests_newest_page_when_stream_count_exceeds_limit() -> None:
    client = Mock()
    client.describe_log_streams.return_value = {
        "logStreams": [
            {"logStreamName": f"task_{attempt:x}", "creationTime": attempt} for attempt in range(30, 10, -1)
        ],
        "nextToken": "older-attempts",
    }

    page = list_task_log_attempts("run-id", "task", _runtime(client), limit=20, cursor=None)

    assert [attempt.attempt_id for attempt in page.attempts] == [f"{attempt:x}" for attempt in range(30, 10, -1)]
    assert page.next_cursor == "older-attempts"
    client.describe_log_streams.assert_called_once_with(
        logGroupName="/valkyrie/benchmarks-dev/run-id",
        logStreamNamePrefix="task_",
        orderBy="LogStreamName",
        descending=True,
        limit=20,
    )


def test_get_task_log_events_returns_absent_for_not_yet_created_stream() -> None:
    client = Mock()
    client.get_log_events.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "GetLogEvents",
    )

    page = get_task_log_events(
        "run-id",
        "task",
        "deadbeef",
        _runtime(client),
        direction="forward",
        limit=100,
        cursor=None,
    )

    assert page is None


def test_list_task_log_attempts_is_empty_before_log_group_exists() -> None:
    client = Mock()
    client.describe_log_streams.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "DescribeLogStreams",
    )

    page = list_task_log_attempts("run-id", "task", _runtime(client), limit=20, cursor=None)

    assert page.attempts == []
    assert page.next_cursor is None
