"""The purge advisory lock must fail loudly when its backend stops holding it."""

from typing import Any

import pytest
from sqlalchemy.exc import OperationalError

from tracker.lifecycle import LifecycleConflict
from tracker.run_purge.locking import OperationLock

_BACKEND_PID = 4242


class FakeResult:
    def __init__(self, state: tuple[int, int]) -> None:
        self.state = state

    def one(self) -> tuple[int, int]:
        return self.state


class FakeLockConnection:
    """Answers each lock-state read with the next scripted state or failure."""

    def __init__(self, *states: tuple[int, int] | Exception) -> None:
        self.states = list(states)

    def execute(self, statement: Any, parameters: Any = None, /) -> Any:
        state = self.states.pop(0)
        if isinstance(state, Exception):
            raise state

        return FakeResult(state)


def test_lock_verification_accepts_an_unchanged_backend_holding_every_key() -> None:
    connection = FakeLockConnection((_BACKEND_PID, 2), (_BACKEND_PID, 2))
    lock = OperationLock(connection, _BACKEND_PID, (11, 22))

    lock.verify()
    lock.verify()

    assert connection.states == []


@pytest.mark.parametrize(
    "state",
    [
        (_BACKEND_PID, 1),
        (_BACKEND_PID + 1, 2),
        OperationalError("SELECT pg_backend_pid()", {}, Exception("server closed the connection")),
    ],
    ids=["lock_released", "reconnected_backend", "connection_dropped"],
)
def test_lock_verification_refuses_a_lost_lock(state: tuple[int, int] | Exception) -> None:
    lock = OperationLock(FakeLockConnection(state), _BACKEND_PID, (11, 22))

    with pytest.raises(LifecycleConflict):
        lock.verify()
