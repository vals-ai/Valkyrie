import os
from collections.abc import Generator
from sqlite3 import Connection
from typing import Any

from sqlalchemy import event
from sqlmodel import Session, SQLModel, create_engine

from tracker.database.models import Benchmark, EvaluationResult, Task

_exposed_models: list[type[SQLModel]] = [Benchmark, EvaluationResult, Task]

_DATABASE_LOCATION = os.getenv("TEST_DATABASE_LOCATION", "src/tracker/database/tracker.db")
engine = create_engine(f"sqlite:///{_DATABASE_LOCATION}")


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection: Connection):
    """Enable WAL mode to allow for concurrent commits to the database."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


def get_session() -> Generator[Session, Any, None]:
    with Session(engine) as session:
        yield session


if __name__ == "__main__":
    SQLModel.metadata.create_all(bind=engine)
