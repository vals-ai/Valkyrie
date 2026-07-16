"""Postgres fixtures for local database integration tests."""

from collections.abc import Generator
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, Any, None]:
    """Run a disposable Postgres instance for schema and health tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def postgres_engine(postgres_container: PostgresContainer):
    """Create the tracker schema in disposable Postgres."""
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture
def postgres_session(postgres_engine: Any) -> Generator[Session, Any, None]:
    """Create a session for disposable Postgres."""
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session
