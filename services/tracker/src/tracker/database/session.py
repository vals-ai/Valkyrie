from collections.abc import Generator
from typing import Any

from sqlmodel import Session, SQLModel, create_engine, select

from tracker.config import DATABASE_URL
from tracker.database.models import Benchmark, EvaluationResult, Org, Task

_exposed_models: list[type[SQLModel]] = [Benchmark, EvaluationResult, Org, Task]

engine = create_engine(
    DATABASE_URL,
    pool_size=50,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
)


def get_session() -> Generator[Session, Any, None]:
    with Session(engine, expire_on_commit=False) as session:
        yield session


def check_database_connection() -> bool:
    try:
        with Session(engine) as session:
            session.exec(select(1))
        return True
    except Exception:
        return False


if __name__ == "__main__":
    SQLModel.metadata.create_all(bind=engine)
