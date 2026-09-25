"""Postgres fixtures for local database integration tests."""

from collections.abc import Generator
from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from tracker.database.models import ExecutorAdmission


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Run a disposable Postgres instance for schema and health tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@contextmanager
def local_postgres_url(binary_directory: str) -> Generator[URL, None, None]:
    """Use an isolated Unix-socket server when Docker is unavailable locally."""
    binaries = Path(binary_directory)
    with tempfile.TemporaryDirectory(prefix="valk-pg-", dir="/tmp") as temporary_directory:
        directory = Path(temporary_directory)
        data_directory = directory / "data"
        subprocess.run(
            [str(binaries / "initdb"), "-D", str(data_directory), "-U", "valk_test", "-A", "trust", "--no-locale"],
            check=True,
            capture_output=True,
            timeout=30,
        )
        control = [str(binaries / "pg_ctl"), "-D", str(data_directory), "-w", "-t", "20"]
        subprocess.run(
            [
                *control,
                "-l",
                str(directory / "server.log"),
                "-o",
                f"-k {directory} -c listen_addresses='' -c timezone=UTC",
                "start",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        try:
            yield URL.create(
                "postgresql+psycopg2",
                username="valk_test",
                password="local-test-only",
                host="localhost",
                port=5432,
                database="postgres",
                query={"host": str(directory)},
            )
        finally:
            subprocess.run([*control, "stop", "-m", "immediate"], check=True, capture_output=True, timeout=30)


@pytest.fixture(scope="session")
def postgres_url(request: pytest.FixtureRequest) -> Generator[str | URL, None, None]:
    """Default to Docker; TEST_POSTGRES_BIN selects disposable local binaries."""
    binary_directory = os.environ.get("TEST_POSTGRES_BIN")
    if binary_directory:
        with local_postgres_url(binary_directory) as url:
            yield url
        return

    container: PostgresContainer = request.getfixturevalue("postgres_container")
    yield container.get_connection_url()


@pytest.fixture
def postgres_engine(postgres_url: str | URL) -> Generator[Engine, None, None]:
    """Create the tracker schema and always dispose its engine."""
    engine = create_engine(postgres_url)
    compatibility_run = bool(os.environ.get("TEST_EXECUTOR_COMPAT_PEX"))
    if compatibility_run:
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=Path(__file__).resolve().parents[4],
            env={**os.environ, "DATABASE_URL": engine.url.render_as_string(hide_password=False)},
            check=True,
            capture_output=True,
            timeout=120,
        )
    else:
        SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        if session.get(ExecutorAdmission, 1) is None:
            session.add(ExecutorAdmission())
            session.commit()

    try:
        yield engine
    finally:
        SQLModel.metadata.drop_all(engine)
        if compatibility_run:
            with engine.begin() as connection:
                connection.execute(text("DROP TABLE alembic_version"))
        engine.dispose()


@pytest.fixture
def postgres_session(postgres_engine: Engine) -> Generator[Session, None, None]:
    """Create a session for disposable Postgres."""
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session
