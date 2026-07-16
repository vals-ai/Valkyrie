from collections.abc import Generator
from sqlite3 import Connection, Cursor
from typing import cast
from uuid import UUID

import pytest
from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.pool import ConnectionPoolEntry
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, DEFAULT_ORG_NAME, Org

# Match the default organization seeded by the database fixture.
TEST_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")

_ = load_dotenv()


@pytest.fixture(scope="function")
def database_session() -> Generator[Session, None, None]:
    """Create an in-memory database and mock the session engine."""
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(test_engine, "connect")
    def set_sqlite_pragma(dbapi_connection: Connection, _connection_record: ConnectionPoolEntry) -> None:  # type: ignore
        """Enable WAL mode to allow for concurrent commits to the database."""
        cursor: Cursor = cast(Cursor, dbapi_connection.cursor())  # type: ignore
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    SQLModel.metadata.create_all(test_engine)

    try:
        with Session(test_engine, expire_on_commit=False) as session:
            # Seed the default organization so foreign keys resolve.
            session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
            session.commit()
            yield session
    finally:
        SQLModel.metadata.drop_all(test_engine)
        test_engine.dispose()


@pytest.fixture
def contract() -> AgentContractRequest:
    """Provide the smallest valid agent contract for tracker tests."""
    return AgentContractRequest(
        name="dummy",
        install_cmd="echo installing dependencies...",
        run_cmd="echo running agent...",
    )


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest) -> Benchmark:
    """Provide a pending benchmark with the shared test organization and contract."""
    return Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        arguments=BenchmarkArguments(contract=contract, concurrency=5, task_ids=None, slice_str=None),
    )
