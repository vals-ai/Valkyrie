"""Two operators of one operation cannot hold the same run at the same time."""

from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.engine import make_url
from sqlmodel import Session
from unittest.mock import MagicMock

from tests.factories import make_benchmark
from tests.unit.test_purge_locking import FakeLockConnection
from tests.utils import TEST_ORG_ID
from tracker.aws.runtime import AWSResources
from tracker.database.models import RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, acquire_hold
from tracker.lifecycle_completion import RelocationCheckpoint
from tracker.run_purge.locking import OperationLock, exclusive_operation
from tracker.run_relocation import RelocationOperator

_BACKEND_PID = 4242
_RESOURCES = AWSResources(region="us-east-1", s3_bucket="legacy-shared-storage", log_group="runs", log_retention_days=7)


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


def relocation_hold(session: Session) -> tuple[RunLifecycle, RelocationCheckpoint]:
    run = make_benchmark(org_id=TEST_ORG_ID)
    run.arguments = run.arguments.model_copy(update={"properties": _RESOURCES})
    session.add(run)
    session.commit()
    identity = operation(FakeServer(), (run.id,)).model_copy(update={"org_id": TEST_ORG_ID})
    record = acquire_hold(
        session,
        identity=identity,
        scope=RunScope(run_id=run.id, original_resources=_RESOURCES),
        purpose="relocation",
    )
    checkpoint = RelocationCheckpoint(
        identity_sha256="a" * 64,
        scope_sha256="b" * 64,
        child_plan_sha256="c" * 64,
        execution_arguments_sha256="d" * 64,
        execution_policy="history_only",
        destination_resources=AWSResources("us-east-1", "vs-dev-owner-42", "runs", 7),
        dispatch_ids=(),
    )
    record.checkpoint_json = checkpoint.model_dump_json()
    session.add(record)
    session.commit()

    return record, checkpoint


@pytest.mark.parametrize("held", [True, False], ids=["lock_held", "lock_lost"])
def test_a_relocation_checkpoint_commits_only_while_the_lock_is_held(database_session: Session, held: bool) -> None:
    record, checkpoint = relocation_hold(database_session)
    connection = FakeLockConnection((_BACKEND_PID if held else _BACKEND_PID + 1, 1))
    lock = OperationLock(connection, _BACKEND_PID, (11,))
    operator = RelocationOperator(database_session, MagicMock())

    if held:
        operator._commit_checkpoint(record, checkpoint.model_copy(update={"phase": "prepared"}), lock)
    else:
        with pytest.raises(LifecycleConflict):
            operator._commit_checkpoint(record, checkpoint.model_copy(update={"phase": "prepared"}), lock)
        database_session.rollback()

    stored = database_session.get(RunLifecycle, record.run_id)
    assert stored is not None and stored.phase == ("prepared" if held else "held")


def test_an_operation_for_another_database_never_reaches_the_run_locks() -> None:
    server = FakeServer()
    identity = operation(server, (uuid4(),)).model_copy(update={"database_target": "postgresql:localhost:5432/other"})

    with pytest.raises(LifecycleConflict):
        with exclusive_operation(operator(server), identity):
            raise AssertionError("A mismatched database target reached the run locks")

    assert not server.locked
