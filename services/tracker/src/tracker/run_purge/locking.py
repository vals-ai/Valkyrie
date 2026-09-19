"""Keep one purge caller per run across provider calls and phase commits."""

import hashlib
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from tracker.lifecycle import LifecycleConflict, OperationIdentity

_LOCK_STATE = (
    "SELECT pg_backend_pid(), (SELECT count(*) FROM pg_locks "
    "WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND objsubid = 1 AND granted)"
)


class LockConnection(Protocol):
    def execute(self, statement: Any, parameters: Any = None, /) -> Any: ...


def advisory_key(run_id: UUID) -> int:
    return int.from_bytes(hashlib.sha256(b"tracker-purge:" + run_id.bytes).digest()[:8], signed=True)


def _lock_state(connection: LockConnection) -> tuple[int, int]:
    try:
        backend_pid, held = connection.execute(text(_LOCK_STATE)).one()
    except SQLAlchemyError as error:
        raise LifecycleConflict("Purge advisory lock connection is unusable") from error

    return int(backend_pid), int(held)


class OperationLock:
    """Session-level advisory locks on one dedicated autocommit connection.

    PostgreSQL releases session-level locks when their backend disappears, and a
    pooled connection can silently reconnect, so every mutating step re-reads the
    backend identity and the granted advisory locks instead of trusting acquisition.
    """

    def __init__(self, connection: LockConnection, backend_pid: int, keys: tuple[int, ...]) -> None:
        self._connection = connection
        self._backend_pid = backend_pid
        self._keys = keys

    def verify(self) -> None:
        backend_pid, held = _lock_state(self._connection)

        if backend_pid != self._backend_pid or held != len(self._keys):
            raise LifecycleConflict("Purge advisory lock is no longer held")


@contextmanager
def exclusive_operation(session: Session, identity: OperationIdentity) -> Generator[OperationLock]:
    verify_database_target(session, identity)
    binding = session.get_bind()
    engine = binding.engine if isinstance(binding, Connection) else binding
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        keys: list[int] = []
        try:
            for run_id in identity.run_ids:
                key = advisory_key(run_id)
                locked = connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar_one()
                if not locked:
                    raise LifecycleConflict("A purge operation is already active for this run")
                keys.append(key)
            backend_pid, _ = _lock_state(connection)
            lock = OperationLock(connection, backend_pid, tuple(keys))
            lock.verify()

            yield lock
        finally:
            session.rollback()
            try:
                for key in reversed(keys):
                    connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
            except SQLAlchemyError:
                connection.invalidate()


def database_target(session: Session) -> str:
    binding = session.get_bind()
    engine = binding.engine if isinstance(binding, Connection) else binding
    url = engine.url
    database = session.connection().execute(text("SELECT current_database()")).scalar_one()
    if database != url.database:
        raise LifecycleConflict("Connected database target differs from configured database")
    host = url.query.get("host", url.host or "localhost")
    port = url.port or url.query.get("port", "5432")
    return f"postgresql:{host}:{port}/{database}"


def verify_database_target(session: Session, identity: OperationIdentity) -> None:
    if database_target(session) != identity.database_target:
        raise LifecycleConflict("Actual database target does not match immutable plan")
