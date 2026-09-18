"""Keep one purge caller per run across provider calls and phase commits."""

import hashlib
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Connection, text
from sqlmodel import Session

from tracker.lifecycle import LifecycleConflict, OperationIdentity


@contextmanager
def exclusive_operation(session: Session, identity: OperationIdentity) -> Generator[None]:
    verify_database_target(session, identity)
    binding = session.get_bind()
    engine = binding.engine if isinstance(binding, Connection) else binding
    with engine.connect() as connection:
        keys: list[int] = []
        try:
            for run_id in identity.run_ids:
                key = int.from_bytes(hashlib.sha256(b"tracker-purge:" + run_id.bytes).digest()[:8], signed=True)
                locked = connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar_one()
                if not locked:
                    raise LifecycleConflict("A purge operation is already active for this run")
                keys.append(key)
            yield
        finally:
            session.rollback()
            for key in reversed(keys):
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})


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
