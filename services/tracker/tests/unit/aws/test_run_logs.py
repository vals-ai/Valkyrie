from unittest.mock import Mock, call

from botocore.exceptions import ClientError
import pytest

from tracker.aws.cloudwatch_logs import get_run_log_events
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


def test_run_log_cursor_pages_forward_and_tails_without_replaying() -> None:
    client = Mock()
    client.filter_log_events.side_effect = [
        {
            "events": [
                {
                    "eventId": "event-1",
                    "logStreamName": "provider/model%3Afast_deadbeef",
                    "timestamp": 10,
                    "ingestionTime": 11,
                    "message": "first",
                },
                {
                    "eventId": "ignored-event",
                    "logStreamName": "invalid_stream",
                    "timestamp": 20,
                    "ingestionTime": 21,
                    "message": "ignored",
                },
            ],
            "nextToken": "aws-page-2",
        },
        {
            "events": [
                {
                    "eventId": "event-2",
                    "logStreamName": "task_with_underscores_f00d",
                    "timestamp": 30,
                    "ingestionTime": 31,
                    "message": "second",
                }
            ]
        },
        {"events": []},
    ]
    runtime = _runtime(client)

    first = get_run_log_events("run-id", runtime, limit=100, cursor=None)
    second = get_run_log_events("run-id", runtime, limit=100, cursor=first.next_cursor)
    tail = get_run_log_events("run-id", runtime, limit=100, cursor=second.next_cursor)

    assert [event.task_id for event in first.events] == ["provider/model:fast"]
    assert first.events[0].event_id == "event-1"
    assert first.events[0].attempt_id == "deadbeef"
    assert first.at_tail is False
    assert [event.task_id for event in second.events] == ["task_with_underscores"]
    assert second.events[0].attempt_id == "f00d"
    assert second.at_tail is True
    assert tail.events == []
    assert tail.next_cursor == second.next_cursor
    assert tail.at_tail is True
    assert client.filter_log_events.call_args_list == [
        call(logGroupName="/valkyrie/benchmarks-dev/run-id", limit=100, startTime=0),
        call(logGroupName="/valkyrie/benchmarks-dev/run-id", limit=100, nextToken="aws-page-2"),
        call(logGroupName="/valkyrie/benchmarks-dev/run-id", limit=100, startTime=31),
    ]


def test_run_logs_preserve_identical_lines_by_event_id() -> None:
    client = Mock()
    client.filter_log_events.return_value = {
        "events": [
            {
                "eventId": event_id,
                "logStreamName": "task_deadbeef",
                "timestamp": 10,
                "ingestionTime": 11,
                "message": "same line",
            }
            for event_id in ("event-1", "event-2")
        ]
    }

    page = get_run_log_events("run-id", _runtime(client), limit=100, cursor=None)

    assert [event.event_id for event in page.events] == ["event-1", "event-2"]
    assert [event.message for event in page.events] == ["same line", "same line"]


def test_run_log_stream_parser_rejects_noncanonical_or_non_attempt_streams() -> None:
    client = Mock()
    client.filter_log_events.return_value = {
        "events": [
            {
                "eventId": "event-1",
                "logStreamName": "task%2fsub_deadbeef",
                "timestamp": 10,
                "ingestionTime": 11,
                "message": "lowercase escape",
            },
            {
                "eventId": "event-2",
                "logStreamName": "task_not-hex",
                "timestamp": 20,
                "ingestionTime": 21,
                "message": "invalid attempt",
            },
            {
                "eventId": "event-3",
                "logStreamName": "_deadbeef",
                "timestamp": 30,
                "ingestionTime": 31,
                "message": "missing task",
            },
        ]
    }

    page = get_run_log_events("run-id", _runtime(client), limit=100, cursor=None)

    assert page.events == []


def test_run_log_cursor_rejects_forged_values() -> None:
    client = Mock()

    with pytest.raises(ValueError, match="Invalid run log cursor"):
        get_run_log_events("run-id", _runtime(client), limit=100, cursor="not-a-cursor")

    client.filter_log_events.assert_not_called()


def test_run_log_page_is_empty_before_log_group_exists() -> None:
    client = Mock()
    client.filter_log_events.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "FilterLogEvents",
    )

    page = get_run_log_events("run-id", _runtime(client), limit=100, cursor=None)

    assert page.events == []
    assert page.next_cursor
    assert page.at_tail is True
