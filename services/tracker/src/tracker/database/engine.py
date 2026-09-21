"""Engines whose sessions resolve every zoneless timestamp in one fixed zone.

This module is the only place in the service that may name a raw engine
constructor; `tests/unit/test_database_engine.py` enforces that.
"""

from collections.abc import Mapping
from typing import Any

from sqlalchemy import Engine, event
from sqlalchemy import engine_from_config as _engine_from_config
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


def pin_session_time_zone(engine: Engine) -> Engine:
    """Pin the session zone on every pooled connection, ahead of PGTZ and server defaults."""
    if engine.dialect.name == "postgresql":
        event.listen(engine, "connect", _pin_session_time_zone)

    return engine


def create_engine(url: str, **options: Any) -> Engine:
    return pin_session_time_zone(_create_engine(url, **options))


def create_engine_from_config(configuration: Mapping[str, Any], **options: Any) -> Engine:
    return pin_session_time_zone(_engine_from_config(dict(configuration), prefix="sqlalchemy.", **options))
