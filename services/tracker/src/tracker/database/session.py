import os
from collections.abc import Generator
from typing import Any

from sqlmodel import Session, SQLModel, create_engine
from tracker.database.models import Benchmark, EvaluationResult, Task

_exposed_models: list[type[SQLModel]] = [Benchmark, EvaluationResult, Task]

_DATABASE_LOCATION = os.getenv("TEST_DATABASE_LOCATION", "src/tracker/database/tracker.db")
engine = create_engine(f"sqlite:///{_DATABASE_LOCATION}")


def get_session() -> Generator[Session, Any, None]:
    with Session(engine) as session:
        yield session


if __name__ == "__main__":
    SQLModel.metadata.create_all(bind=engine)
