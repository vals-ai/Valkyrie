from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import NoReturn
from unittest.mock import MagicMock

import pytest
from benchmark_service import Resources, TargetedSnapshotSource

import tracker.scheduler.admission as admission
from tracker.scheduler.admission import SandboxQueueContext


class QueuePathReached(Exception):
    pass


async def test_targeted_snapshot_enters_normal_queue_path(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = MagicMock()
    context = SandboxQueueContext(
        provider=provider,
        pool_id="pool_test",
        engine=MagicMock(),
    )

    @asynccontextmanager
    async def unavailable_lock() -> AsyncIterator[bool]:
        yield False

    async def stop_at_poll(_seconds: float) -> NoReturn:
        raise QueuePathReached

    monkeypatch.setattr(admission, "queue_pool_lock", lambda *_args: unavailable_lock())
    monkeypatch.setattr(admission.asyncio, "sleep", stop_at_poll)

    async with AsyncExitStack() as stack:
        with pytest.raises(QueuePathReached):
            await admission.enter_queued_sandbox(
                stack=stack,
                context=context,
                task_row_id=MagicMock(),
                expected_started_at=MagicMock(),
                authority=MagicMock(),
                source=TargetedSnapshotSource(snapshot="snapshot", target="different-pool"),
                resources=Resources(vcpu=1, memory=1, disk=1),
                create=MagicMock(),
            )

    provider.check_admission.assert_not_called()
