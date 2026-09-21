"""A provider mutation is verified immediately before it, not only on entry.

Every method here scans or reads first and mutates afterwards, so a lock lost
during that read must stop the mutation. The lock is a session-level advisory
lock on a dedicated connection, so its only loss is the backend disappearing;
each test models that by changing the state the scripted connection reports
while the provider is still reading.
"""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

import tracker.run_purge.providers as purge_providers
from tests.unit.test_purge_locking import FakeLockConnection
from tests.unit.test_transfer_archive import archive_boundary
from tests.unit.test_transfer_log_safety import quiet_run
from tracker.lifecycle import LifecycleConflict
from tracker.run_purge.locking import OperationLock

_BACKEND_PID = 4242
_HELD = (_BACKEND_PID, 1)
_LOST = (_BACKEND_PID + 1, 1)


def held_lock() -> tuple[FakeLockConnection, OperationLock]:
    connection = FakeLockConnection(live=_HELD)
    return connection, OperationLock(connection, _BACKEND_PID, (11,))


def bulky_events(count: int) -> list[dict[str, Any]]:
    """Large enough that the writer flushes a chunk before the scan ends."""
    return [
        {
            "timestamp": 1,
            "ingestionTime": 2,
            "message": "private message " + "p" * 400_000,
            "eventId": f"event-{ordinal}",
            "logStreamName": "old",
        }
        for ordinal in range(count)
    ]


@pytest.mark.asyncio
async def test_a_lock_lost_during_the_cleanup_scan_stops_the_log_group_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = quiet_run(tmp_path, monkeypatch, publish=False)
    archive, decision = await state.boundary.archive(
        state.request, state.run, dispatches=state.dispatches, acquired_at=state.acquired_at
    )
    connection, lock = held_lock()
    describe = state.logs.describe_log_groups

    def losing_describe(**request: Any) -> dict[str, Any]:
        connection.live = _LOST
        return describe(**request)

    monkeypatch.setattr(state.logs, "describe_log_groups", losing_describe)

    with pytest.raises(LifecycleConflict, match="advisory lock is no longer held"):
        await state.boundary.cleanup_logs(
            state.request,
            state.run,
            archive,
            dispatches=state.dispatches,
            acquired_at=state.acquired_at,
            log_completeness_sha256=decision,
            verify=lock.verify,
        )

    assert state.deleted == []
    assert not state.logs.absent


@pytest.mark.asyncio
async def test_a_lock_lost_after_the_first_chunk_stops_every_later_archive_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = quiet_run(tmp_path, monkeypatch, events=bulky_events(3), publish=False)
    connection, lock = held_lock()
    put_object = state.storage.put_object

    def losing_put(**request: Any) -> dict[str, Any]:
        connection.live = _LOST
        return put_object(**request)

    monkeypatch.setattr(state.storage, "put_object", losing_put)

    with pytest.raises(LifecycleConflict, match="advisory lock is no longer held"):
        await state.boundary.archive(
            state.request, state.run, dispatches=state.dispatches, acquired_at=state.acquired_at, verify=lock.verify
        )

    written = [key for key, _version in state.storage.objects]

    assert len(written) == 1
    assert written[0].endswith("chunks/00000000.json")


@pytest.mark.asyncio
async def test_a_lock_lost_during_the_sandbox_inventory_stops_the_sandbox_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, boundary, _, _ = archive_boundary(tmp_path)
    run = request.plan.runs[0]
    connection, lock = held_lock()
    sandbox = Mock(id="sandbox", labels={"Id": str(run.source.run_id)})
    provider = Mock()
    provider.delete_sandbox = AsyncMock()
    provider.close = AsyncMock()

    async def losing_inventory(_query: Any) -> Any:
        connection.live = _LOST
        if not provider.delete_sandbox.await_count:
            yield sandbox

    provider.list_sandboxes = losing_inventory
    configuration = Mock()
    configuration.create_provider.return_value = provider

    def lookup(*_arguments: Any) -> Any:
        return configuration

    monkeypatch.setattr(purge_providers, "fetch_sandbox_provider_config", lookup)

    with pytest.raises(LifecycleConflict, match="advisory lock is no longer held"):
        await boundary.drain(
            request,
            run,
            {"sandbox_provider": "daytona", "sandbox_provider_secret_name": "exact-source-secret"},
            cleanup=True,
            verify=lock.verify,
        )

    provider.delete_sandbox.assert_not_awaited()
    provider.close.assert_awaited_once()
