"""Local logs survive reopening and retain earlier task attempts."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from tracker.local.logs import FilesystemLogs
from tracker.runtime.logs import LogProviderError, RunLogReference, TaskLogReference, task_log_stream_name


async def test_attempts_pagination_filters_and_reopening(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path, Path("/host/logs"))
    run_id = uuid4()
    started = datetime.now(UTC)
    logs.create_benchmark(str(run_id), retention_days=30)
    for index in range(3):
        stream = task_log_stream_name("group/task:one", started + timedelta(seconds=index))
        logs.write(f"{run_id}:{stream}", f"attempt {index}")
    logs.write(f"{run_id}:{task_log_stream_name('other', started)}", "another task")

    reopened = FilesystemLogs(tmp_path, Path("/host/logs"))
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
    assert reopened.benchmark_location(str(run_id)) == f"/host/logs/{run_id}/logs.sqlite3"


async def test_concurrent_writes_and_follow(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path, tmp_path)
    run_id = uuid4()
    started = datetime.now(UTC)
    logs.create_benchmark(str(run_id), retention_days=30)
    stream_key = f"{run_id}:{task_log_stream_name('task', started)}"
    await asyncio.gather(*(asyncio.to_thread(logs.write, stream_key, str(index)) for index in range(20)))
    reference = TaskLogReference(run_id, "task", started)
    iterator = logs.stream_task(reference, poll_interval=0.01)
    received = [await anext(iterator) for _ in range(20)]
    assert {event.message for event in received} == {str(index) for index in range(20)}
    logs.write(stream_key, "new output")
    assert (await asyncio.wait_for(anext(iterator), timeout=2)).message == "new output"
    await iterator.aclose()


async def test_missing_logs_and_invalid_cursor(tmp_path: Path) -> None:
    logs = FilesystemLogs(tmp_path, tmp_path)
    reference = RunLogReference(uuid4())
    assert not (await logs.fetch(reference)).events
    with pytest.raises(LogProviderError, match="cursor"):
        await logs.fetch(reference, cursor="-1")
