"""The purge advisory lock must fail loudly when its backend stops holding it."""

from typing import Any

import pytest
from sqlalchemy.exc import OperationalError

from tracker.lifecycle import LifecycleConflict
from tracker.run_purge.locking import OperationLock, _release_locks

_BACKEND_PID = 4242
_DROPPED = OperationalError("SELECT pg_backend_pid()", {}, Exception("server closed the connection"))


class FakeResult:
    def __init__(self, state: tuple[int, int]) -> None:
        self.state = state

    def one(self) -> tuple[int, int]:
        return self.state


class FakeLockConnection:
    """Answers each statement with the next scripted state or failure, and records the call."""

    def __init__(self, *states: tuple[int, int] | Exception) -> None:
        self.states = list(states)
        self.calls: list[Any] = []
        self.invalidated = False

    def execute(self, statement: Any, parameters: Any = None, /) -> Any:
        self.calls.append(parameters)
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
