"""Run with `uv run pytest tests/unit/executor/test_release_entrypoint.py`."""

import builtins
import json
import os
import subprocess
import sys
from typing import cast

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError
from pytest import MonkeyPatch
from sqlmodel import Session

from tracker.executor import release_entrypoint
from tracker.database import session as tracker_session
from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus


def test_release_entrypoint_loads_database_config_after_sealed_environment() -> None:
    environment = os.environ.copy()
    for name in ("DATABASE_URL", "DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"):
        environment.pop(name, None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os

import tracker.executor.release_entrypoint

os.environ.update(
    DB_USERNAME="sealed-user",
    DB_PASSWORD="sealed-password",
    DB_HOST="database.internal",
    DB_PORT="5433",
    DB_NAME="sealed-database",
)
from tracker.database.session import engine

assert engine.url.username == "sealed-user"
assert engine.url.password == "sealed-password"
assert engine.url.host == "database.internal"
assert engine.url.port == 5433
assert engine.url.database == "sealed-database"
""",
        ],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_release_engine_does_not_retain_idle_connections(tmp_path: os.PathLike[str]) -> None:
    environment = {**os.environ, "DATABASE_URL": f"sqlite:///{tmp_path}/release.sqlite3"}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from sqlalchemy import event
from sqlmodel import Session, select
from tracker.database.session import engine

connections = []
event.listen(engine, "connect", lambda connection, _record: connections.append(connection))
for _ in range(2):
    with Session(engine) as session:
        assert session.exec(select(1)).one() == 1
assert len(connections) == 2, "the engine retained an idle database connection"
""",
        ],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr


class FakeSecretsManager:
    def __init__(self) -> None:
        self.secret_ids: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> dict[str, object]:
        self.secret_ids.append(SecretId)
        return {
            "SecretString": json.dumps(
                {
                    "username": "tracker",
                    "password": "secret",
                    "host": "ignored.invalid",
                    "port": 9999,
                }
            )
        }


IMAGE_REPOSITORY = "123456789012.dkr.ecr.us-east-1.amazonaws.com/valkyrie-executor"
REGISTERED_TASK_DEFINITION = "arn:aws:ecs:us-east-1:123456789012:task-definition/ValkyrieExecutorRunner:8"


class FakeEcrClient:
    def __init__(self, *, published: bool = True) -> None:
        self.published = published
        self.describe_calls: list[dict[str, object]] = []

    def describe_images(self, **kwargs: object) -> dict[str, object]:
        self.describe_calls.append(kwargs)
        if not self.published:
            raise ClientError({"Error": {"Code": "ImageNotFoundException"}}, "DescribeImages")
        return {"imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]}


class FakeEcsRegistry:
    def __init__(self) -> None:
        self.registered: list[dict[str, object]] = []

    def describe_task_definition(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"taskDefinition": "ValkyrieExecutorRunner"}
        return {
            "taskDefinition": {
                "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/ValkyrieExecutorRunner:7",
                "family": "ValkyrieExecutorRunner",
                "revision": 7,
                "status": "ACTIVE",
                "cpu": "1024",
                "memory": "4096",
                "runtimePlatform": {"cpuArchitecture": "ARM64"},
                "taskRoleArn": "arn:aws:iam::123456789012:role/runner-task",
                "executionRoleArn": "arn:aws:iam::123456789012:role/runner-execution",
                "requiresAttributes": [{"name": "ecs.capability.execution-role-ecr-pull"}],
                "compatibilities": ["FARGATE"],
                "registeredAt": "2026-01-01T00:00:00Z",
                "containerDefinitions": [
                    {"name": "ExecutorRunnerContainer", "image": "tracker:deployed", "stopTimeout": 120}
                ],
            }
        }

    def register_task_definition(self, **kwargs: object) -> dict[str, object]:
        self.registered.append(kwargs)
        return {"taskDefinition": {"taskDefinitionArn": REGISTERED_TASK_DEFINITION}}


class FakeEcsClient:
    def __init__(self) -> None:
        self.service_updates: list[dict[str, object]] = []
        self.stopped_tasks: list[str] = []
        self.already_stopping_tasks: list[str] = []
        self.list_calls: list[dict[str, object]] = []
        self.waited_for: list[dict[str, object]] = []
        self.pending_polls = 0
        self.tracker_polls = 0
        self.describe_polls = 0

    def get_waiter(self, name: str) -> "FakeEcsClient":
        assert name == "services_stable"
        return self

    def wait(self, **kwargs: object) -> None:
        self.waited_for.append(kwargs)

    def list_tasks(self, **kwargs: object) -> dict[str, object]:
        self.list_calls.append(kwargs)
        assert kwargs["family"] == "ValkyrieExecutorRunner"
        assert kwargs["cluster"] == "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie"
        if kwargs["desiredStatus"] == "PENDING":
            self.pending_polls += 1
            return {"taskArns": ["pending-runner"] if self.pending_polls == 1 else []}
        if kwargs["desiredStatus"] == "STOPPED":
            return {"taskArns": [*self.stopped_tasks, *self.already_stopping_tasks]}
        if self.stopped_tasks or self.already_stopping_tasks:
            return {"taskArns": []}
        if kwargs.get("nextToken") == "page-2":
            return {"taskArns": ["task-2"]}
        return {"taskArns": ["task-1"], "nextToken": "page-2"}

    def describe_services(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["services"] == ["Tracker"]
        self.tracker_polls += 1
        return {"services": [{"runningCount": 1 if self.tracker_polls == 1 else 0}]}

    def describe_tasks(self, **kwargs: object) -> dict[str, object]:
        self.describe_polls += 1
        return {
            "tasks": [
                {"taskArn": arn, "lastStatus": "RUNNING" if self.describe_polls == 1 else "STOPPED"}
                for arn in cast(list[str], kwargs["tasks"])
            ]
        }

    def stop_task(self, **kwargs: object) -> dict[str, object]:
        self.stopped_tasks.append(str(kwargs["task"]))
        return {}

    def update_service(self, **kwargs: object) -> dict[str, object]:
        self.service_updates.append(kwargs)
        return {}


def _release_arguments(monkeypatch: MonkeyPatch, *, artifact_digest: str = "a" * 64) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tracker.executor.release_entrypoint",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:tracker",
            "database.internal",
            "5432",
            "tracker",
            IMAGE_REPOSITORY,
            "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "Tracker",
            "2",
            "--runner-task-family",
            "ValkyrieExecutorRunner",
            "activate",
            "git-abc123-def456",
            f"{IMAGE_REPOSITORY}@sha256:{artifact_digest}",
            "1",
        ],
    )


def test_release_entrypoint_uses_sealed_configuration_and_persists_active_release(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "caller-credential")
    monkeypatch.setenv("AWS_ENDPOINT_URL_SECRETSMANAGER", "https://caller.invalid")
    monkeypatch.setenv("HTTPS_PROXY", "https://caller.invalid")
    monkeypatch.setenv("DB_HOST", "caller.invalid")
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())
    ecr = FakeEcrClient()
    ecs = FakeEcsRegistry()
    monkeypatch.setattr(release_entrypoint, "create_ecr_client", lambda: ecr)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    real_import = builtins.__import__

    def assert_sealed_environment_before_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "tracker.database.session":
            assert os.environ["DB_HOST"] == "database.internal"
            assert os.environ["DB_USERNAME"] == "tracker"
            assert os.environ["DB_PASSWORD"] == "secret"
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", assert_sealed_environment_before_import)

    release_entrypoint.main()

    database_session.expire_all()
    stored_release = database_session.get(ExecutorRelease, "git-abc123-def456")
    admission = database_session.get(ExecutorAdmission, 1)
    assert client.secret_ids == ["arn:aws:secretsmanager:us-east-1:123456789012:secret:tracker"]
    assert "AWS_ACCESS_KEY_ID" not in os.environ
    assert "AWS_ENDPOINT_URL_SECRETSMANAGER" not in os.environ
    assert "HTTPS_PROXY" not in os.environ
    assert stored_release is not None
    assert stored_release.status == ExecutorReleaseStatus.ACTIVE
    assert stored_release.task_definition_arn == REGISTERED_TASK_DEFINITION
    assert admission is not None
    assert admission.release_id == stored_release.id
    assert ecr.describe_calls == [
        {"repositoryName": "valkyrie-executor", "imageIds": [{"imageDigest": "sha256:" + "a" * 64}]}
    ]
    assert ecs.registered == [
        {
            "family": "ValkyrieExecutorRunner",
            "cpu": "1024",
            "memory": "4096",
            "runtimePlatform": {"cpuArchitecture": "ARM64"},
            "taskRoleArn": "arn:aws:iam::123456789012:role/runner-task",
            "executionRoleArn": "arn:aws:iam::123456789012:role/runner-execution",
            "containerDefinitions": [
                {
                    "name": "ExecutorRunnerContainer",
                    "image": f"{IMAGE_REPOSITORY}@sha256:{'a' * 64}",
                    "environment": [{"name": "SENTRY_RELEASE", "value": "git-abc123-def456"}],
                    "stopTimeout": 120,
                }
            ],
        }
    ]


def test_release_entrypoint_rerun_reuses_the_registered_task_definition(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: FakeSecretsManager())
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())
    ecs = FakeEcsRegistry()
    monkeypatch.setattr(release_entrypoint, "create_ecr_client", lambda: FakeEcrClient())
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)

    release_entrypoint.main()
    release_entrypoint.main()

    database_session.expire_all()
    stored_release = database_session.get(ExecutorRelease, "git-abc123-def456")
    assert stored_release is not None
    assert stored_release.task_definition_arn == REGISTERED_TASK_DEFINITION
    assert len(ecs.registered) == 1


def test_maintenance_begin_drains_paginated_runner_tasks_before_finish(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    target_sha = "b" * 40
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", target_sha]
    secrets = FakeSecretsManager()
    ecs = FakeEcsClient()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: secrets)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    release_entrypoint.main()

    database_session.expire_all()
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha == target_sha
    assert ecs.service_updates == [
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "service": "Tracker",
            "desiredCount": 0,
        }
    ]
    assert ecs.stopped_tasks == ["task-1", "task-2", "pending-runner"]
    assert ecs.list_calls[1]["nextToken"] == "page-2"
    assert ecs.pending_polls == 2
    assert ecs.tracker_polls == 2
    assert ecs.describe_polls == 2
    assert ecs.waited_for == []

    sys.argv = sys.argv[:11] + ["maintenance-finish", target_sha]
    release_entrypoint.main()

    database_session.expire_all()
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha is None
    assert ecs.service_updates[-1] == {
        "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
        "service": "Tracker",
        "desiredCount": 2,
    }
    assert len(ecs.service_updates) == 2
    assert ecs.waited_for == [
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "services": ["Tracker"],
        }
    ]


def test_maintenance_begin_fails_loudly_if_runners_do_not_drain(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", "b" * 40]
    ecs = FakeEcsClient()
    monkeypatch.setattr(ecs, "list_tasks", lambda **_kwargs: {"taskArns": ["still-running"]})
    monkeypatch.setattr(release_entrypoint, "MAINTENANCE_DRAIN_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    with pytest.raises(TimeoutError, match="Maintenance drain timed out"):
        release_entrypoint.main()
    database_session.expire_all()
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha == "b" * 40


def test_maintenance_begin_waits_for_previously_stopping_runner(
    monkeypatch: MonkeyPatch, database_session: Session
) -> None:
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", "b" * 40]
    ecs = FakeEcsClient()
    ecs.already_stopping_tasks.append("previously-stopping-runner")
    ecs.pending_polls = 1
    ecs.tracker_polls = 1
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(release_entrypoint.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    release_entrypoint.main()

    assert ecs.stopped_tasks == []
    assert ecs.describe_polls == 2
    assert sum(call["desiredStatus"] == "STOPPED" for call in ecs.list_calls) == 2


def test_maintenance_begin_times_out_for_previously_stopping_runner(
    monkeypatch: MonkeyPatch, database_session: Session
) -> None:
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", "b" * 40]
    ecs = FakeEcsClient()
    ecs.already_stopping_tasks.append("previously-stopping-runner")
    ecs.pending_polls = 1
    ecs.tracker_polls = 1
    monkeypatch.setattr(release_entrypoint, "MAINTENANCE_DRAIN_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(
        ecs,
        "describe_tasks",
        lambda **kwargs: {
            "tasks": [{"taskArn": arn, "lastStatus": "RUNNING"} for arn in cast(list[str], kwargs["tasks"])]
        },
    )
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    with pytest.raises(TimeoutError, match="Maintenance drain timed out"):
        release_entrypoint.main()
    assert ecs.stopped_tasks == []


def test_maintenance_begin_treats_expired_historical_stopped_task_as_gone(
    monkeypatch: MonkeyPatch, database_session: Session
) -> None:
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", "b" * 40]
    ecs = FakeEcsClient()
    ecs.already_stopping_tasks.append("expired-runner")
    ecs.pending_polls = 1
    ecs.tracker_polls = 1
    list_tasks = ecs.list_tasks
    describe_tasks = ecs.describe_tasks
    expired = False

    def list_after_expiry(**kwargs: object) -> dict[str, object]:
        response = list_tasks(**kwargs)
        if kwargs["desiredStatus"] == "STOPPED" and expired:
            return {"taskArns": [arn for arn in cast(list[str], response["taskArns"]) if arn != "expired-runner"]}
        return response

    def describe_after_expiry(**kwargs: object) -> dict[str, object]:
        nonlocal expired
        expired = True
        return describe_tasks(
            **{**kwargs, "tasks": [arn for arn in cast(list[str], kwargs["tasks"]) if arn != "expired-runner"]}
        )

    monkeypatch.setattr(ecs, "list_tasks", list_after_expiry)
    monkeypatch.setattr(ecs, "describe_tasks", describe_after_expiry)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(release_entrypoint.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    release_entrypoint.main()

    assert ecs.stopped_tasks == []
    assert sum(call["desiredStatus"] == "STOPPED" for call in ecs.list_calls) == 2


def test_maintenance_begin_waits_for_listed_runner_missing_from_describe(
    monkeypatch: MonkeyPatch, database_session: Session
) -> None:
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", "b" * 40]
    ecs = FakeEcsClient()
    ecs.already_stopping_tasks.append("previously-stopping-runner")
    ecs.pending_polls = 1
    ecs.tracker_polls = 1
    statuses = [None, "RUNNING", "STOPPED"]

    def describe_tasks(**kwargs: object) -> dict[str, object]:
        status = statuses.pop(0)
        tasks = cast(list[str], kwargs["tasks"])
        return {"tasks": [] if status is None else [{"taskArn": arn, "lastStatus": status} for arn in tasks]}

    monkeypatch.setattr(ecs, "describe_tasks", describe_tasks)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(release_entrypoint.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    release_entrypoint.main()

    assert statuses == []
    assert ecs.stopped_tasks == []


def test_maintenance_begin_times_out_when_a_task_it_stopped_cannot_be_described(
    monkeypatch: MonkeyPatch, database_session: Session
) -> None:
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", "b" * 40]
    ecs = FakeEcsClient()
    monkeypatch.setattr(ecs, "describe_tasks", lambda **_kwargs: {"tasks": []})
    monkeypatch.setattr(release_entrypoint, "MAINTENANCE_DRAIN_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    with pytest.raises(TimeoutError, match="Maintenance drain timed out"):
        release_entrypoint.main()
    assert ecs.stopped_tasks


def test_release_entrypoint_unpublished_image_registers_nothing_and_commits_no_release(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())
    ecs = FakeEcsRegistry()
    monkeypatch.setattr(release_entrypoint, "create_ecr_client", lambda: FakeEcrClient(published=False))
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)

    with pytest.raises(ClientError, match="ImageNotFoundException"):
        release_entrypoint.main()

    database_session.expire_all()
    assert ecs.registered == []
    assert database_session.get(ExecutorRelease, "git-abc123-def456") is None


def test_release_entrypoint_rejects_invalid_release_id_before_reading_secret(monkeypatch: MonkeyPatch) -> None:
    _release_arguments(monkeypatch)
    sys.argv[12] = "invalid release"
    monkeypatch.setattr(
        release_entrypoint,
        "create_secrets_manager_client",
        lambda: pytest.fail("invalid release input must not read Secrets Manager"),
    )

    with pytest.raises(ValidationError, match="release_id"):
        release_entrypoint.main()


@pytest.mark.parametrize(
    ("argument_index", "invalid_value", "error"),
    [
        (13, f"{IMAGE_REPOSITORY}:git-abc123", "configured ECR repository"),
        (13, f"{IMAGE_REPOSITORY}-other@sha256:{'a' * 64}", "configured ECR repository"),
        (14, "999", "Unsupported executor protocol"),
    ],
)
def test_release_entrypoint_rejects_invalid_artifact_identity_before_reading_secret(
    monkeypatch: MonkeyPatch,
    argument_index: int,
    invalid_value: str,
    error: str,
) -> None:
    _release_arguments(monkeypatch)
    sys.argv[argument_index] = invalid_value
    monkeypatch.setattr(
        release_entrypoint,
        "create_secrets_manager_client",
        lambda: pytest.fail("invalid artifact identity must not read Secrets Manager"),
    )

    with pytest.raises(ValueError, match=error):
        release_entrypoint.main()


def test_release_entrypoint_rejects_arbitrary_extra_command_arguments(monkeypatch: MonkeyPatch) -> None:
    _release_arguments(monkeypatch)
    sys.argv.append("python -c 'malicious'")
    monkeypatch.setattr(
        release_entrypoint,
        "create_secrets_manager_client",
        lambda: pytest.fail("invalid command shape must not read Secrets Manager"),
    )

    with pytest.raises(SystemExit, match="sealed release task"):
        release_entrypoint.main()
