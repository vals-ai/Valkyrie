"""The purge advisory lock must fail loudly when its backend stops holding it."""

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import OperationalError
from sqlmodel import Session

import tracker.run_purge as run_purge
from tests.unit.test_lifecycle_abandon import make_checkpoint, seeded_hold, store_checkpoint
from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict
from tracker.run_purge import PurgeOperator
from tracker.run_purge.contracts import PurgeCheckpoint, PurgeRun
from tracker.run_purge.locking import OperationLock, _release_locks

_BACKEND_PID = 4242
_DROPPED = OperationalError("SELECT pg_backend_pid()", {}, Exception("server closed the connection"))


@contextmanager
def scripted_lock(lock: OperationLock) -> Generator[OperationLock]:
    yield lock


class FakeResult:
    def __init__(self, state: tuple[int, int]) -> None:
        self.state = state

    def one(self) -> tuple[int, int]:
        return self.state


class FakeLockConnection:
    """Answers each statement with the next scripted state or failure, and records the call.

    A caller that cannot predict how many verifications a provider makes passes
    `live` instead, and changes it to model the backend disappearing mid-operation.
    """

    def __init__(self, *states: tuple[int, int] | Exception, live: tuple[int, int] | None = None) -> None:
        self.states = list(states)
        self.live = live
        self.calls: list[Any] = []
        self.invalidated = False

    def execute(self, statement: Any, parameters: Any = None, /) -> Any:
        self.calls.append(parameters)
        if not self.states and self.live is not None:
            return FakeResult(self.live)

        state = self.states.pop(0)
        if isinstance(state, Exception):
            raise state

        return FakeResult(state)

    def invalidate(self) -> None:
        self.invalidated = True


class FakeSession:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.rolled_back = False

    def rollback(self) -> None:
        self.rolled_back = True
        if self.error is not None:
            raise self.error


def test_lock_verification_accepts_an_unchanged_backend_holding_every_key() -> None:
    connection = FakeLockConnection((_BACKEND_PID, 2), (_BACKEND_PID, 2))
    lock = OperationLock(connection, _BACKEND_PID, (11, 22))

    lock.verify()
    lock.verify()

    assert connection.states == []


@pytest.mark.parametrize(
    "state",
    [(_BACKEND_PID, 1), (_BACKEND_PID + 1, 2), _DROPPED],
    ids=["lock_released", "reconnected_backend", "connection_dropped"],
)
def test_lock_verification_refuses_a_lost_lock(state: tuple[int, int] | Exception) -> None:
    lock = OperationLock(FakeLockConnection(state), _BACKEND_PID, (11, 22))

    with pytest.raises(LifecycleConflict):
        lock.verify()


def test_release_unlocks_every_key_in_reverse_order() -> None:
    connection = FakeLockConnection((0, 0), (0, 0))
    session = FakeSession()

    _release_locks(session, connection, (11, 22))

    assert session.rolled_back
    assert connection.calls == [{"key": 22}, {"key": 11}]
    assert not connection.invalidated


def test_release_unlocks_although_the_rollback_fails() -> None:
    connection = FakeLockConnection((0, 0), (0, 0))
    session = FakeSession(RuntimeError("rollback failed"))

    with pytest.raises(RuntimeError):
        _release_locks(session, connection, (11, 22))

    assert connection.calls == [{"key": 22}, {"key": 11}]
    assert not connection.invalidated


def test_release_invalidates_a_connection_that_cannot_unlock() -> None:
    connection = FakeLockConnection(_DROPPED)
    session = FakeSession()

    _release_locks(session, connection, (11,))

    assert connection.invalidated


def prepared_checkpoint(run_id: UUID, digest: str) -> PurgeCheckpoint:
    return make_checkpoint("prepared", run_id=run_id, rows=False, digest=digest)


def operator_at_held(
    session: Session, monkeypatch: pytest.MonkeyPatch, boundary: Any = None
) -> tuple[PurgeOperator, PurgeRun, PurgeCheckpoint]:
    _, _, run, plan = seeded_hold(session)
    store_checkpoint(session, run.id, make_checkpoint("held", run_id=run.id, rows=False, digest=plan.digest()))
    monkeypatch.setattr(run_purge, "verify_database_target", lambda *_arguments: None)
    operator = PurgeOperator(session, plan, boundary if boundary is not None else MagicMock(), host_contract=None)

    return operator, plan.runs[0], prepared_checkpoint(run.id, plan.digest())


def test_a_checkpoint_commits_while_the_lock_is_held(
    database_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    operator, purge_run, prepared = operator_at_held(database_session, monkeypatch)
    lock = OperationLock(FakeLockConnection((_BACKEND_PID, 2)), _BACKEND_PID, (11, 22))

    operator._commit_checkpoint(purge_run, prepared, lock)

    record = database_session.get(RunLifecycle, purge_run.scope.run_id)
    assert record is not None and record.phase == "prepared"


def test_a_checkpoint_does_not_commit_after_the_lock_is_lost(
    database_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    operator, purge_run, prepared = operator_at_held(database_session, monkeypatch)
    lock = OperationLock(FakeLockConnection((_BACKEND_PID + 1, 2)), _BACKEND_PID, (11, 22))

    with pytest.raises(LifecycleConflict):
        operator._commit_checkpoint(purge_run, prepared, lock)

    record = database_session.get(RunLifecycle, purge_run.scope.run_id)
    assert record is not None and record.phase == "held"


@pytest.mark.parametrize("held", [True, False], ids=["lock_held", "lock_lost"])
def test_a_removed_run_checkpoint_commits_only_while_the_lock_is_held(
    database_session: Session, monkeypatch: pytest.MonkeyPatch, held: bool
) -> None:
    operator, purge_run, prepared = operator_at_held(database_session, monkeypatch)
    record = database_session.get(RunLifecycle, purge_run.scope.run_id)
    assert record is not None
    database_session.delete(database_session.get(Benchmark, purge_run.scope.run_id))
    database_session.flush()
    lock = OperationLock(FakeLockConnection((_BACKEND_PID if held else _BACKEND_PID + 1, 2)), _BACKEND_PID, (11, 22))

    if held:
        operator._commit_checkpoint(purge_run, prepared, lock, record=record)
    else:
        with pytest.raises(LifecycleConflict):
            operator._commit_checkpoint(purge_run, prepared, lock, record=record)
        database_session.rollback()

    stored = database_session.get(RunLifecycle, purge_run.scope.run_id)
    assert stored is not None and stored.phase == ("prepared" if held else "held")


@pytest.mark.parametrize("held", [True, False], ids=["lock_held", "lock_lost"])
def test_abandonment_commits_only_while_the_lock_is_held(
    database_session: Session, monkeypatch: pytest.MonkeyPatch, held: bool
) -> None:
    _, _, run, plan = seeded_hold(database_session)
    lock = OperationLock(FakeLockConnection((_BACKEND_PID if held else _BACKEND_PID + 1, 2)), _BACKEND_PID, (11, 22))
    monkeypatch.setattr(run_purge, "verify_database_target", lambda *_arguments: None)
    monkeypatch.setattr(run_purge, "exclusive_operation", lambda *_arguments: scripted_lock(lock))

    if held:
        assert run_purge.abandon_runs(database_session, plan, (run.id,)) == (run.id,)
    else:
        with pytest.raises(LifecycleConflict):
            run_purge.abandon_runs(database_session, plan, (run.id,))
        database_session.rollback()

    record = database_session.get(RunLifecycle, run.id)
    assert record is not None
    assert (record.released_at is not None) is held


@pytest.mark.parametrize("held", [True, False], ids=["lock_held", "lock_lost"])
def test_an_inspection_is_attested_only_while_the_lock_is_held(
    database_session: Session, monkeypatch: pytest.MonkeyPatch, held: bool
) -> None:
    operator, purge_run, _ = operator_at_held(database_session, monkeypatch, AsyncMock())
    lock = OperationLock(FakeLockConnection((_BACKEND_PID if held else _BACKEND_PID + 1, 2)), _BACKEND_PID, (11, 22))
    monkeypatch.setattr(run_purge, "exclusive_operation", lambda *_arguments: scripted_lock(lock))

    if held:
        inspection = asyncio.run(operator.inspect(request_nonce=uuid4()))
        assert tuple(item.scope for item in inspection.runs) == (purge_run.scope,)
    else:
        with pytest.raises(LifecycleConflict):
            asyncio.run(operator.inspect(request_nonce=uuid4()))
