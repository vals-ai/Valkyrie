from collections.abc import Callable, Generator
from datetime import UTC, datetime
from sqlite3 import Connection, Cursor
from typing import cast
from uuid import UUID, uuid4

import pytest
from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.pool import ConnectionPoolEntry
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    DEFAULT_ORG_NAME,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
)
from tracker.executor.execution_authority import ExecutionAuthority
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
            # Mirror migration-owned singleton rows and the default organization.
            session.add(ExecutorAdmission())
            session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
            session.commit()
            yield session
    finally:
        SQLModel.metadata.drop_all(test_engine)
        test_engine.dispose()


@pytest.fixture
def executor_authority_kwargs(
    database_session: Session,
) -> Callable[..., dict[str, object]]:
    """Create one live host claim for direct process_benchmark tests."""

    def create(
        benchmark: Benchmark,
        dispatch_id: UUID | None = None,
        *,
        session: Session | None = None,
    ) -> dict[str, object]:
        authority_session = session or database_session
        if benchmark.current_execution_release_id is None:
            release_id = "authority-test-release"
            release = authority_session.get(ExecutorRelease, release_id)
            if release is None:
                release = ExecutorRelease(
                    id=release_id,
                    artifact_uri="s3://artifacts/authority-test-release.pex",
                    artifact_digest="a" * 64,
                    protocol_version="1",
                    readiness_verified=True,
                )
                authority_session.add(release)
                authority_session.flush()
            benchmark.executor_release_id = release.id
            benchmark.current_execution_release_id = release.id
            benchmark.executor_artifact_uri = release.artifact_uri
            benchmark.executor_artifact_digest = release.artifact_digest
            benchmark.executor_protocol_version = release.protocol_version
        assert benchmark.current_execution_release_id is not None
        assert benchmark.executor_artifact_uri is not None
        assert benchmark.executor_artifact_digest is not None
        assert benchmark.executor_protocol_version is not None
        dispatch = authority_session.get(ExecutorDispatch, dispatch_id) if dispatch_id is not None else None
        if dispatch is None:
            dispatch = ExecutorDispatch(
                id=dispatch_id or uuid4(),
                benchmark_id=benchmark.id,
                kind=ExecutorDispatchKind.START,
                executor_release_id=benchmark.current_execution_release_id,
                executor_artifact_uri=benchmark.executor_artifact_uri,
                executor_artifact_digest=benchmark.executor_artifact_digest,
                executor_protocol_version=benchmark.executor_protocol_version,
            )
        dispatch.status = ExecutorDispatchStatus.RUNNING
        dispatch.started_at = datetime.now(UTC)
        authority_session.add(dispatch)
        authority_session.commit()
        return {"executor_dispatch_id": str(dispatch.id)}

    return create


@pytest.fixture
def executor_authority(
    executor_authority_kwargs: Callable[..., dict[str, object]],
) -> Callable[..., ExecutionAuthority]:
    def create(benchmark: Benchmark, **kwargs: object) -> ExecutionAuthority:
        authority_kwargs = executor_authority_kwargs(benchmark, **kwargs)
        return ExecutionAuthority(
            benchmark_id=benchmark.id,
            dispatch_id=UUID(str(authority_kwargs["executor_dispatch_id"])),
        )

    return create


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
