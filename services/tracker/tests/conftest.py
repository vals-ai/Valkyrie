from collections.abc import Generator
from sqlite3 import Connection, Cursor
from typing import Any, cast
from uuid import uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.pool import ConnectionPoolEntry
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from tracker.database.models import *  # noqa: F403
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, PlatformContext

_ = load_dotenv()


@pytest.fixture(scope="function")
def database_session() -> Generator[Session, Any, None]:
    """Create an in-memory database and mock the session engine."""
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(test_engine, "connect")
    def set_sqlite_pragma(dbapi_connection: Connection, _connection_record: ConnectionPoolEntry) -> None:  # type: ignore
        """Enable WAL mode to allow for concurrent commits to the database."""
        cursor: Cursor = cast(Cursor, dbapi_connection.cursor())  # type: ignore
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    SQLModel.metadata.create_all(test_engine)

    with Session(test_engine, expire_on_commit=False) as session:
        yield session

    SQLModel.metadata.drop_all(test_engine)


@pytest.fixture
def contract() -> AgentContractRequest:
    return AgentContractRequest(
        name="dummy",
        install_cmd="echo installing dependencies...",
        run_cmd="echo running agent...",
    )


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest) -> Benchmark:
    return Benchmark(
        name="swebench",
        arguments=BenchmarkArguments(contract=contract, concurrency=5, task_ids=None, slice_str=None),
    )


@pytest.fixture
def platform_context() -> PlatformContext:
    return PlatformContext(
        project_id=uuid4(),
        launch_source="valkyrie_hosted",
        project_slug="benchmarking",
        org="vals",
        run_by="hung@vals.ai",
    )
