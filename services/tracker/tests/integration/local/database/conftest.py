"""Postgres fixtures for local database integration tests."""

from collections.abc import Generator

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from tracker.database.models import ExecutorAdmission


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Run a disposable Postgres instance for schema and health tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def postgres_engine(postgres_container: PostgresContainer) -> Generator[Engine, None, None]:
    """Create the tracker schema and always dispose its engine."""
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(ExecutorAdmission())
        session.commit()

    try:
        yield engine
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def postgres_session(postgres_engine: Engine) -> Generator[Session, None, None]:
    """Create a session for disposable Postgres."""
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session
