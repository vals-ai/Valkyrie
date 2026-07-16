from collections.abc import Generator
from sqlite3 import Connection, Cursor
from typing import cast

import pytest
from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.pool import ConnectionPoolEntry
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.database.models import AgentContractRequest, Benchmark, DEFAULT_ORG_NAME, Org
from tracker.types import AWSCredentials

_ = load_dotenv()


@pytest.fixture
def aws_credentials() -> AWSCredentials:
    """Provide deterministic AWS credentials for non-live tests."""
    return AWSCredentials(
        aws_access_key_id="test-aws-access-key-id",
        aws_secret_access_key="test-aws-secret-access-key",
        aws_default_region="us-east-1",
    )


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
    """Provide a benchmark with the shared test organization and contract."""
    return make_benchmark(contract=contract, concurrency=5)
