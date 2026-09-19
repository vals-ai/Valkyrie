"""Read immutable old logs through the same bounded provider used by routes."""

import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from time import monotonic
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from sqlalchemy import inspect

from tests.unit.aws.test_cloudwatch_log_provider import MockClients, MockLogsClient
from tests.unit.aws.test_log_history_archive import (
    DESTINATION_ACCOUNT,
    RUN_ID,
    FakeLogs,
    FakeS3,
    FakeSession,
    run_archive,
    scoped_input,
)
from tracker.aws import log_history_archive
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchLogProvider, task_log_stream_name
from tracker.aws.historical_logs import archive_authority_cache, historical_log_reader
from tracker.database.models import Benchmark
from tracker.runtime.log_history import ArchiveLimits
from tracker.runtime.logs import (
    LogEvent,
    LogPage,
    LogProviderError,
    RunLogReference,
    RunTaskLogReference,
    TaskLogReference,
)


class LiveLogs:
    def __init__(self) -> None:
        self.events = [LogEvent(datetime.fromtimestamp(0.002, UTC), "retry", event_id="live")]

    async def fetch(self, reference: Any, *, cursor: str | None = None, limit: int = 1000, **kwargs: Any) -> LogPage:
        position = int(cursor or 0)
        events = self.events[position : position + limit]
        following = position + len(events)
        return LogPage(events, str(following) if following < len(self.events) else None)

    async def stream_task(self, reference: Any, **kwargs: Any) -> AsyncIterator[LogEvent]:
        for event in self.events:
            yield event


def reader(
    tmp_path: Path, *, terminal: bool = True, logs: FakeLogs | None = None, limits: ArchiveLimits | None = None
) -> tuple[Any, FakeS3, Any]:
    storage = FakeS3()
    report = run_archive(log_history_archive, tmp_path, logs or FakeLogs(), storage, limits=limits or ArchiveLimits())
    try:
        module = import_module("tracker.aws.historical_logs")
    except ModuleNotFoundError:
        pytest.fail("historical reader is not implemented")
    provider = module.HistoricalLogProvider(
        report.reference,
        scoped_input(log_history_archive).location,
        FakeSession(DESTINATION_ACCOUNT, storage),
        LiveLogs(),
        terminal=terminal,
    )
    return provider, storage, report


async def test_old_duplicates_and_live_events_survive_page_boundaries(tmp_path: Path) -> None:
    provider, storage, _ = reader(tmp_path)
    reference = RunLogReference(RUN_ID)
    events: list[LogEvent] = []
    cursor = None
    for _ in range(10):
        page = await provider.fetch(reference, cursor=cursor, limit=1)
        events.extend(page.events)
        cursor = page.next_cursor
        if cursor is None:
            break
    assert cursor is None
    assert [event.event_id for event in events] == ["first", "second", "live"]
    assert [event.message for event in events[:2]] == ["private old message"] * 2
    assert all(
        request["ExpectedBucketOwner"] == DESTINATION_ACCOUNT for kind, request in storage.requests if kind == "get"
    )


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
async def test_declared_archive_failure_never_falls_back_to_live(tmp_path: Path, damage: str) -> None:
    provider, storage, _ = reader(tmp_path)
    if damage == "missing":
        storage.objects.clear()
    else:
        storage.corrupt = True
    with pytest.raises(LogProviderError, match="archive"):
        await provider.fetch(RunLogReference(RUN_ID))


async def test_cursor_cannot_change_run_query_or_task(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    page = await provider.fetch(RunLogReference(RUN_ID), limit=1)
    with pytest.raises(LogProviderError):
        await provider.fetch(RunLogReference(uuid4()), cursor=page.next_cursor)
    with pytest.raises(LogProviderError):
        await provider.fetch(RunLogReference(RUN_ID), query="changed", cursor=page.next_cursor)
    with pytest.raises(LogProviderError):
        await provider.fetch(TaskLogReference(RUN_ID, "old", datetime(2020, 1, 1, tzinfo=UTC)), cursor=page.next_cursor)


async def test_terminal_archive_only_follow_ends_and_task_scope_is_exact(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    provider.live.events = []
    reference = TaskLogReference(RUN_ID, "old", datetime(2020, 1, 1, tzinfo=UTC))
    events = [event async for event in provider.stream_task(reference)]
    assert events == []


async def test_literal_query_and_inclusive_millisecond_end(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    provider.live.events = []
    page = await provider.fetch(
        RunLogReference(RUN_ID),
        query="old message",
        start_time=datetime.fromtimestamp(0.001, UTC),
        end_time=datetime.fromtimestamp(0.001, UTC),
    )
    assert [event.event_id for event in page.events] == ["first", "second"]


def test_benchmark_stores_strict_nullable_archive_reference() -> None:
    assert "log_history" in inspect(Benchmark).columns
    assert inspect(Benchmark).columns.log_history.nullable


class StreamLogs(FakeLogs):
    def describe_log_streams(self, **request: Any) -> dict[str, Any]:
        return {
            "logStreams": [
                {"logStreamName": name}
                for name in sorted({"empty", *(event["logStreamName"] for event in self.events)})
            ]
        }


async def test_many_chunks_keep_bounded_empty_pages_and_resume(tmp_path: Path) -> None:
    logs = FakeLogs()
    logs.events = [{**logs.events[0], "eventId": str(index), "message": "x" * 120} for index in range(40)]
    provider, storage, report = reader(tmp_path, logs=logs, limits=ArchiveLimits(chunk_bytes=512))
    provider.live.events = []
    assert report.chunk_count > 16
    storage.requests.clear()
    first = await provider.fetch(RunLogReference(RUN_ID), query="absent")
    chunk_reads = [request for kind, request in storage.requests if kind == "get" and "/chunks/" in request["Key"]]
    assert len(chunk_reads) == 16
    assert first.events == [] and first.next_cursor
    cursor = first.next_cursor
    for _ in range(10):
        page = await provider.fetch(RunLogReference(RUN_ID), query="absent", cursor=cursor)
        assert page.events == []
        cursor = page.next_cursor
        if cursor is None:
            break
    assert cursor is None


async def test_interleaved_live_events_preserve_source_ties_and_multiplicity(tmp_path: Path) -> None:
    logs = FakeLogs()
    logs.events = [
        dict(logs.events[0], eventId="z"),
        dict(logs.events[0], eventId="a"),
        dict(logs.events[0], timestamp=3, eventId="last"),
    ]
    provider, _, _ = reader(tmp_path, logs=logs, limits=ArchiveLimits(chunk_bytes=512))
    provider.live.events.insert(
        0,
        LogEvent(
            datetime.fromtimestamp(0.001, UTC),
            "private old message",
            ingestion_time=datetime.fromtimestamp(0.002, UTC),
            event_id="live-tie",
        ),
    )
    page = await provider.fetch(RunLogReference(RUN_ID))
    assert [event.event_id for event in page.events] == ["z", "a", "live-tie", "live", "last"]


async def test_task_ambiguity_old_attempt_and_follow(tmp_path: Path) -> None:
    started = datetime(2026, 1, 1, tzinfo=UTC)
    canonical = task_log_stream_name("task:one", started)
    legacy = task_log_stream_name("task_one", started)
    logs = StreamLogs()
    logs.events = [
        dict(logs.events[0], logStreamName=canonical, eventId="canonical"),
        dict(logs.events[0], logStreamName=legacy, eventId="ambiguous"),
        dict(logs.events[0], logStreamName="old-attempt", eventId="old-attempt"),
    ]
    provider, _, _ = reader(tmp_path, logs=logs)
    selected = TaskLogReference(RUN_ID, "task:one", started, siblings=(RunTaskLogReference("task*one", started),))
    provider.live.events = []
    page = await provider.fetch(selected)
    assert [(event.event_id, event.task_id) for event in page.events] == [("canonical", "task:one")]
    run = RunLogReference(RUN_ID, (RunTaskLogReference("task:one", started), RunTaskLogReference("task*one", started)))
    aggregate = await provider.fetch(run)
    assert [(event.event_id, event.task_id) for event in aggregate.events] == [
        ("canonical", "task:one"),
        ("ambiguous", None),
        ("old-attempt", None),
    ]
    provider.live.events = [LogEvent(started, "retry", event_id="live")]
    followed = [event async for event in provider.stream_task(selected)]
    assert [event.event_id for event in followed] == ["canonical", "live"]


async def test_corrupt_chunk_fails_even_when_query_does_not_match(tmp_path: Path) -> None:
    provider, storage, _ = reader(tmp_path)
    key = next(key for key in storage.objects if "/chunks/" in key[0])
    storage.objects[key] = b"corrupt"
    with pytest.raises(LogProviderError, match="archive"):
        await provider.fetch(RunLogReference(RUN_ID), query="absent")


async def test_cursor_binds_immutable_manifest_and_time(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    first = await provider.fetch(RunLogReference(RUN_ID), limit=1)
    with pytest.raises(LogProviderError):
        await provider.fetch(
            RunLogReference(RUN_ID), cursor=first.next_cursor, start_time=datetime(2020, 1, 1, tzinfo=UTC)
        )
    provider.history = provider.history.model_copy(
        update={"manifest": provider.history.manifest.model_copy(update={"version_id": "new-version"})}
    )
    with pytest.raises(LogProviderError):
        await provider.fetch(RunLogReference(RUN_ID), cursor=first.next_cursor)


async def test_empty_live_pages_are_bounded_and_do_not_hide_later_logs(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    responses: list[dict[str, Any] | BaseException] = [{"events": [], "nextToken": str(index)} for index in range(17)]
    responses.append({"events": [{"timestamp": 2, "message": "later", "eventId": "later"}]})
    client = MockLogsClient(responses)
    provider.live = CloudWatchLogProvider(cast(AWSClientProvider, MockClients(client)), "logs")
    first = await provider.fetch(RunLogReference(RUN_ID))
    assert first.events == [] and first.next_cursor
    assert len(client.filter_requests) == 16
    second = await provider.fetch(RunLogReference(RUN_ID), cursor=first.next_cursor)
    assert [event.event_id for event in second.events] == ["first", "second", "later"]


@pytest.mark.parametrize("field,value", [("owner_id", "999"), ("versioning", "Suspended")])
async def test_reader_revalidates_bucket_identity(tmp_path: Path, field: str, value: str) -> None:
    provider, storage, _ = reader(tmp_path)
    setattr(storage, field, value)
    with pytest.raises(LogProviderError, match="archive"):
        await provider.fetch(RunLogReference(RUN_ID))


async def test_terminal_archive_follow_finishes_when_live_group_is_absent(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    missing = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "GetLogEvents")
    provider.live = CloudWatchLogProvider(cast(AWSClientProvider, MockClients(MockLogsClient([missing]))), "logs")
    reference = TaskLogReference(RUN_ID, "old", datetime(2020, 1, 1, tzinfo=UTC))
    assert [event async for event in provider.stream_task(reference)] == []


@pytest.mark.parametrize(
    "fault", ["archive_offset", "archive_end", "archive_chunk", "live_offset", "oversized", "invalid", "limit"]
)
async def test_invalid_cursor_positions_fail_without_returning_partial_history(tmp_path: Path, fault: str) -> None:
    provider, storage, _ = reader(tmp_path)
    reference = RunLogReference(RUN_ID)
    page = await provider.fetch(reference, limit=1)
    assert page.next_cursor is not None
    position = json.loads(base64.urlsafe_b64decode(page.next_cursor))
    if fault == "archive_offset":
        position["offset"] = 999
    elif fault == "archive_end":
        position.update(chunk=1, offset=1)
    elif fault == "archive_chunk":
        position.update(chunk=999, offset=0)
    elif fault == "live_offset":
        position["live_offset"] = 999
    cursor = base64.urlsafe_b64encode(json.dumps(position).encode()).decode()
    if fault == "oversized":
        cursor = "a" * 32769
    elif fault == "invalid":
        cursor = "invalid!"
    before = len(storage.requests)

    with pytest.raises(LogProviderError, match="cursor|page limit"):
        await provider.fetch(reference, cursor=cursor, limit=0 if fault == "limit" else 1000)

    if fault in {"oversized", "invalid", "limit"}:
        assert len(storage.requests) == before


async def test_live_pagination_cycle_does_not_return_archive_as_complete(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path)
    live = LiveLogs()
    live.fetch = AsyncMock(return_value=LogPage([], "repeat"))
    provider.live = live

    with pytest.raises(LogProviderError, match="did not advance"):
        await provider.fetch(RunLogReference(RUN_ID))

    assert live.fetch.await_count == 2


async def test_follow_maps_chunk_failure_without_exposing_provider_details(tmp_path: Path) -> None:
    provider, storage, _ = reader(tmp_path)
    key = next(key for key in storage.objects if "/chunks/" in key[0])
    storage.objects.pop(key)
    reference = TaskLogReference(RUN_ID, "old", datetime(2020, 1, 1, tzinfo=UTC))

    with pytest.raises(LogProviderError, match="Historical archive verification failed"):
        _ = [event async for event in provider.stream_task(reference)]


class ShiftingLiveLogs:
    """One live page whose events move between two reads of the same token."""

    def __init__(self, pages: list[list[LogEvent]]) -> None:
        self.pages = pages
        self.reads = 0

    async def fetch(self, reference: Any, *, cursor: str | None = None, limit: int = 1000, **kwargs: Any) -> LogPage:
        events = self.pages[min(self.reads, len(self.pages) - 1)]
        self.reads += 1
        return LogPage(list(events))

    async def stream_task(self, reference: Any, **kwargs: Any) -> AsyncIterator[LogEvent]:
        for event in self.pages[0]:
            yield event


def retry_event(milliseconds: int, name: str) -> LogEvent:
    return LogEvent(datetime.fromtimestamp(milliseconds / 1000, UTC), f"retry {name}", event_id=f"live-{name}")


@pytest.mark.parametrize(
    "shift",
    [
        [retry_event(2, "late"), retry_event(2, "a"), retry_event(3, "b"), retry_event(4, "c"), retry_event(5, "d")],
        [retry_event(4, "c"), retry_event(5, "d")],
    ],
    ids=["repeat", "drop"],
)
async def test_shifted_live_page_refuses_to_resume_instead_of_dropping_or_repeating(
    tmp_path: Path, shift: list[LogEvent]
) -> None:
    provider, _, _ = reader(tmp_path, terminal=False)
    original = [retry_event(2, "a"), retry_event(3, "b"), retry_event(4, "c"), retry_event(5, "d")]
    provider.live = ShiftingLiveLogs([original, shift])
    reference = RunLogReference(RUN_ID)
    first = await provider.fetch(reference, limit=4)
    assert [event.event_id for event in first.events] == ["first", "second", "live-a", "live-b"]
    assert first.next_cursor is not None

    with pytest.raises(LogProviderError, match="Live log page changed"):
        await provider.fetch(reference, cursor=first.next_cursor)


async def test_unshifted_live_page_still_resumes_after_the_last_emitted_event(tmp_path: Path) -> None:
    provider, _, _ = reader(tmp_path, terminal=False)
    original = [retry_event(2, "a"), retry_event(3, "b"), retry_event(4, "c")]
    provider.live = ShiftingLiveLogs([original, [*original, retry_event(6, "e")]])
    reference = RunLogReference(RUN_ID)
    first = await provider.fetch(reference, limit=3)
    second = await provider.fetch(reference, cursor=first.next_cursor)
    assert [event.event_id for event in first.events] == ["first", "second", "live-a"]
    assert [event.event_id for event in second.events] == ["live-b", "live-c", "live-e"]


class NamedSession(FakeSession):
    def __init__(self, account: str, client: Any, role: str) -> None:
        super().__init__(account, client)
        self.role = role

    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": self.account, "Arn": f"arn:aws:iam::{self.account}:role/{self.role}"}


def saved_runtime(session: Any) -> Any:
    runtime = Mock()
    runtime.clients.boto3_session.return_value = session
    runtime.resources = scoped_input(log_history_archive).destination.original_resources
    runtime.expected_bucket_owner = DESTINATION_ACCOUNT
    return runtime


def test_reused_authority_is_bound_to_one_verified_principal(tmp_path: Path) -> None:
    _, storage, report = reader(tmp_path)
    org_id = scoped_input(log_history_archive).location.org_id
    arguments = (RUN_ID, org_id, report.reference, LiveLogs())
    first = historical_log_reader(
        saved_runtime(NamedSession(DESTINATION_ACCOUNT, storage, "one")), *arguments, terminal=True
    )
    storage.versioning = "Suspended"
    storage.requests.clear()
    again = historical_log_reader(
        saved_runtime(NamedSession(DESTINATION_ACCOUNT, storage, "one")), *arguments, terminal=True
    )
    assert again.location == first.location
    assert not storage.requests

    with pytest.raises(LogProviderError, match="authority verification"):
        historical_log_reader(
            saved_runtime(NamedSession(DESTINATION_ACCOUNT, storage, "two")), *arguments, terminal=True
        )


def test_expired_authority_is_verified_again(tmp_path: Path) -> None:
    _, storage, report = reader(tmp_path)
    org_id = scoped_input(log_history_archive).location.org_id
    arguments = (RUN_ID, org_id, report.reference, LiveLogs())
    historical_log_reader(saved_runtime(NamedSession(DESTINATION_ACCOUNT, storage, "one")), *arguments, terminal=True)
    for key, (_, location, store) in list(archive_authority_cache.entries.items()):
        archive_authority_cache.entries[key] = (monotonic() - 1, location, store)
    storage.versioning = "Suspended"

    with pytest.raises(LogProviderError, match="authority verification"):
        historical_log_reader(
            saved_runtime(NamedSession(DESTINATION_ACCOUNT, storage, "one")), *arguments, terminal=True
        )


@pytest.mark.parametrize("fault", ["account", "organization"])
def test_saved_runtime_authority_is_verified_before_reading_events(tmp_path: Path, fault: str) -> None:
    provider, storage, report = reader(tmp_path)
    runtime = Mock()
    runtime.clients.boto3_session.return_value = provider.session
    runtime.resources = scoped_input(log_history_archive).destination.original_resources
    runtime.expected_bucket_owner = "999999999999" if fault == "account" else DESTINATION_ACCOUNT
    organization = uuid4() if fault == "organization" else scoped_input(log_history_archive).location.org_id
    storage.requests.clear()

    with pytest.raises(LogProviderError, match="authority verification"):
        historical_log_reader(runtime, RUN_ID, organization, report.reference, LiveLogs(), terminal=True)

    assert not any(method == "get" for method, _ in storage.requests)
