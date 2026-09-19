"""Two operators of one operation cannot hold the same run at the same time."""

from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.engine import make_url
from sqlmodel import Session

from tracker.lifecycle import LifecycleConflict, OperationIdentity
from tracker.run_purge.locking import exclusive_operation


class FakeServer:
    """Session-scoped advisory locks keyed by the value the caller locks on."""

    def __init__(self, database: str = "tracker") -> None:
        self.database = database
        self.locked: set[object] = set()


class FakeResult:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value

    def one(self) -> object:
        return self.value


class FakeConnection:
    def __init__(self, server: FakeServer) -> None:
        self.server = server
        self.held: set[object] = set()
        self.backend_pid = 4242

    def execution_options(self, **_options: object) -> "FakeConnection":
        return self

    def invalidate(self) -> None:
        self.server.locked -= self.held
        self.held.clear()

    def execute(self, statement: object, parameters: dict[str, Any] | None = None) -> FakeResult:
        text = str(statement)
        key = None if parameters is None else parameters.get("key")
        if "current_database" in text:
            return FakeResult(self.server.database)

        if "pg_backend_pid" in text:
            return FakeResult((self.backend_pid, len(self.held)))

        if "pg_try_advisory_lock" in text:
            if key in self.server.locked:
                return FakeResult(False)
            self.server.locked.add(key)
            self.held.add(key)
            return FakeResult(True)

        if "pg_advisory_unlock" in text:
            self.server.locked.discard(key)
            self.held.discard(key)
            return FakeResult(True)

        return FakeResult(None)

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_arguments: object) -> None:
        self.server.locked -= self.held
        self.held.clear()


class FakeEngine:
    def __init__(self, server: FakeServer) -> None:
        self.server = server
        self.url = make_url(f"postgresql://operator@localhost:5432/{server.database}")

    def connect(self) -> FakeConnection:
        return FakeConnection(self.server)


class FakeSession:
    def __init__(self, server: FakeServer) -> None:
        self.engine = FakeEngine(server)
        self.rollbacks = 0

    def get_bind(self) -> FakeEngine:
        return self.engine

    def connection(self) -> FakeConnection:
        return FakeConnection(self.engine.server)

    def rollback(self) -> None:
        self.rollbacks += 1


def operator(server: FakeServer) -> Session:
    return cast(Session, FakeSession(server))


def operation(server: FakeServer, run_ids: tuple[UUID, ...]) -> OperationIdentity:
    return OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=uuid4(),
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-east-1",
        environment="dev",
        database_target=f"postgresql:localhost:5432/{server.database}",
        run_ids=tuple(sorted(run_ids, key=str)),
    )


def test_a_second_operator_of_the_same_operation_is_refused_until_the_first_finishes() -> None:
    server = FakeServer()
    identity = operation(server, (uuid4(),))

    with exclusive_operation(operator(server), identity):
        with pytest.raises(LifecycleConflict):
            with exclusive_operation(operator(server), identity):
                raise AssertionError("A second operator entered the same operation")

    with exclusive_operation(operator(server), identity):
        pass

    assert not server.locked


def test_a_refused_operator_leaves_no_partially_acquired_run_locked() -> None:
    server = FakeServer()
    first_run, second_run = sorted([uuid4(), uuid4()], key=str)

    with exclusive_operation(operator(server), operation(server, (second_run,))):
        with pytest.raises(LifecycleConflict):
            with exclusive_operation(operator(server), operation(server, (first_run, second_run))):
                raise AssertionError("An overlapping operator entered the operation")

        with exclusive_operation(operator(server), operation(server, (first_run,))):
            pass


def test_an_operation_for_another_database_never_reaches_the_run_locks() -> None:
    server = FakeServer()
    identity = operation(server, (uuid4(),)).model_copy(update={"database_target": "postgresql:localhost:5432/other"})

    with pytest.raises(LifecycleConflict):
        with exclusive_operation(operator(server), identity):
            raise AssertionError("A mismatched database target reached the run locks")

    assert not server.locked
