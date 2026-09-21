"""Engines whose sessions resolve every zoneless timestamp in one fixed zone."""

from typing import Any

from sqlalchemy import Engine, event
from sqlmodel import create_engine as _create_engine

from executor_protocol import DATABASE_SESSION_TIME_ZONE


def _pin_session_time_zone(connection: Any, _record: Any) -> None:
    previous = connection.autocommit
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SET TIME ZONE '{DATABASE_SESSION_TIME_ZONE}'")
    finally:
        connection.autocommit = previous


def create_engine(url: str, **options: Any) -> Engine:
    """Pin the session zone on every pooled connection, ahead of PGTZ and server defaults."""
    engine = _create_engine(url, **options)
    if engine.dialect.name == "postgresql":
        event.listen(engine, "connect", _pin_session_time_zone)

    return engine
