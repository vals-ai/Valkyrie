"""Local logs survive reopening and retain earlier task attempts.

Run: pytest tests/unit/local/test_logs.py
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest

from tracker.local.logs import FilesystemLogs
from tracker.runtime.logs import LogProviderError, RunLogReference, TaskLogReference, task_log_stream_name


async def test_attempts_pagination_filters_and_reopening(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path)
    run_id = uuid4()
    started = datetime.now(UTC)
    await logs.create_benchmark(str(run_id), retention_days=30)
    for index in range(3):
        stream = task_log_stream_name("group/task:one", started + timedelta(seconds=index))
        logs.write(f"{run_id}:{stream}", f"attempt {index}")
    logs.write(f"{run_id}:{task_log_stream_name('other', started)}", "another task")

    reopened = FilesystemLogs(tmp_path)
    reference = TaskLogReference(run_id, "group/task:one", started + timedelta(seconds=2))
    page = await reopened.fetch(reference, limit=2)
    assert [event.message for event in page.events] == ["attempt 0", "attempt 1"]
    assert page.next_cursor is not None
    last = await reopened.fetch(reference, cursor=page.next_cursor, limit=2)
    assert [event.message for event in last.events] == ["attempt 2"]
    assert last.next_cursor is None
    assert {event.task_id for event in page.events} == {"group/task:one"}
    assert len((await reopened.fetch(reference, query="attempt 1")).events) == 1
    assert not (await reopened.fetch(reference, end_time=started - timedelta(seconds=1))).events
    assert len((await reopened.fetch(RunLogReference(run_id))).events) == 4
    assert reopened.benchmark_location(str(run_id)) == str(tmp_path / str(run_id) / "logs.jsonl")


async def test_concurrent_writes_and_follow(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path)
    run_id = uuid4()
    started = datetime.now(UTC)
    await logs.create_benchmark(str(run_id), retention_days=30)
    stream_key = f"{run_id}:{task_log_stream_name('task', started)}"
    await asyncio.gather(*(asyncio.to_thread(logs.write, stream_key, str(index)) for index in range(20)))
    reference = TaskLogReference(run_id, "task", started)
    iterator = logs.stream_task(reference, poll_interval=0.01)
    received = [await anext(iterator) for _ in range(20)]
    assert {event.message for event in received} == {str(index) for index in range(20)}
    logs.write(stream_key, "new output")
    assert (await asyncio.wait_for(anext(iterator), timeout=2)).message == "new output"
    await iterator.aclose()


async def test_byte_cursors_skip_filtered_logs_and_wait_for_complete_records(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path)
    run_id = uuid4()
    started = datetime.now(UTC)
    await logs.create_benchmark(str(run_id), retention_days=0)
    stream_key = f"{run_id}:{task_log_stream_name('task', started)}"
    logs.write(stream_key, "match café\nsecond line")
    logs.write(stream_key, "filtered out")
    logs.write(stream_key, "match again")
    path = Path(logs.benchmark_location(str(run_id)))
    records = path.read_bytes().splitlines(keepends=True)
    path.write_bytes(b"".join(records[:-1]) + records[-1][:-1])

    reference = TaskLogReference(run_id, "task", started)
    page = await logs.fetch(reference, query="match", limit=1)
    assert [event.message for event in page.events] == ["match café\nsecond line"]
    assert page.events[0].event_id == str(len(records[0]))
    assert page.next_cursor is None
    assert not (await logs.fetch(reference, query="match", cursor=page.events[0].event_id)).events

    with path.open("ab") as output:
        output.write(b"\n")
    page = await logs.fetch(reference, query="match", limit=1)
    assert page.next_cursor == str(len(records[0]))
    final = await logs.fetch(reference, query="match", cursor=page.next_cursor)
    assert [event.message for event in final.events] == ["match again"]
    assert len((await logs.fetch(reference, start_time=started)).events) == 3


@pytest.mark.parametrize("cursor", ["-1", "not-a-byte-offset"])
async def test_invalid_cursors_are_reported_as_log_errors(tmp_path: Path, cursor: str) -> None:
    logs = FilesystemLogs(tmp_path)

    with pytest.raises(LogProviderError, match="Invalid local log cursor"):
        await logs.fetch(RunLogReference(uuid4()), cursor=cursor)


async def test_missing_and_corrupt_log_files(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path)
    run_id = uuid4()
    reference = RunLogReference(run_id)

    assert not (await logs.fetch(reference)).events

    await logs.create_benchmark(str(run_id), retention_days=0)
    Path(logs.benchmark_location(str(run_id))).write_bytes(b"not a JSON record\n")

    with pytest.raises(LogProviderError, match="Failed to read local task logs"):
        await logs.fetch(reference)


async def test_follow_filters_tasks_text_and_time_before_finishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logs = FilesystemLogs(tmp_path)
    run_id = uuid4()
    started = datetime(2000, 1, 1, tzinfo=UTC)
    clock = Mock(wraps=datetime)
    clock.now.return_value = started + timedelta(seconds=10)
    monkeypatch.setattr("tracker.local.logs.datetime", clock)
    await logs.create_benchmark(str(run_id), retention_days=0)
    records = [
        ("task", 1, "wanted but early"),
        ("other", 2, "wanted but different task"),
        ("task", 3, "wanted result"),
        ("task", 4, "different text"),
        ("task", 5, "wanted but late"),
    ]
    path = Path(logs.benchmark_location(str(run_id)))
    path.write_text(
        "".join(
            json.dumps(
                {
                    "stream": task_log_stream_name(task, started),
                    "timestamp": (started + timedelta(seconds=seconds)).timestamp(),
                    "message": message,
                }
            )
            + "\n"
            for task, seconds, message in records
        ),
        encoding="utf-8",
    )

    events = [
        event
        async for event in logs.stream_task(
            TaskLogReference(run_id, "task", started),
            query="wanted",
            start_time=started + timedelta(seconds=2),
            end_time=started + timedelta(seconds=4),
        )
    ]

    assert [event.message for event in events] == ["wanted result"]
