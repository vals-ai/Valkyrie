"""Run with `uv run pytest tests/unit/executor/test_release_entrypoint.py`."""

import builtins
import hashlib
import json
import os
import sys
from io import BytesIO

import boto3
import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch
from sqlmodel import Session

from tracker.executor import release_entrypoint
from tracker.database import session as tracker_session
from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus


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


class FakeS3Client:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "releases"
        assert Key == "releases/git-abc123-def456/executor.pex"
        return {"Body": BytesIO(self.content)}


class FakeEcsClient:
    def __init__(self) -> None:
        self.service_updates: list[dict[str, object]] = []
        self.protection_updates: list[dict[str, object]] = []
        self.stopped_tasks: list[str] = []

    def get_waiter(self, name: str) -> "FakeEcsClient":
        assert name == "services_stable"
        return self

    def wait(self, **_kwargs: object) -> None:
        return

    def list_tasks(self, **_kwargs: object) -> dict[str, object]:
        return {"taskArns": ["task-1", "task-2"]}

    def update_task_protection(self, **kwargs: object) -> dict[str, object]:
        self.protection_updates.append(kwargs)
        return {}

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
            "releases",
            "releases",
            "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "ExecutorHost",
            "Tracker",
            "1",
            "2",
            "activate",
            "git-abc123-def456",
            "s3://releases/releases/git-abc123-def456/executor.pex",
            artifact_digest,
            "1",
        ],
    )


def test_release_entrypoint_uses_sealed_configuration_and_persists_active_release(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    content = b"sealed executor artifact"
    artifact_digest = hashlib.sha256(content).hexdigest()
    _release_arguments(monkeypatch, artifact_digest=artifact_digest)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "caller-credential")
    monkeypatch.setenv("AWS_ENDPOINT_URL_SECRETSMANAGER", "https://caller.invalid")
    monkeypatch.setenv("HTTPS_PROXY", "https://caller.invalid")
    monkeypatch.setenv("DB_HOST", "caller.invalid")
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    def create_s3_client(service_name: str) -> FakeS3Client:
        assert service_name == "s3"
        return FakeS3Client(content)

    monkeypatch.setattr(boto3, "client", create_s3_client)
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
    assert os.environ["EXECUTOR_RELEASE_BUCKET"] == "releases"
    assert stored_release is not None
    assert stored_release.status == ExecutorReleaseStatus.ACTIVE
    assert stored_release.readiness_verified
    assert admission is not None
    assert admission.release_id == stored_release.id


def test_maintenance_begin_fences_admission_and_stops_executor_hosts(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    target_sha = "b" * 40
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:12] + ["maintenance-begin", target_sha]
    secrets = FakeSecretsManager()
    ecs = FakeEcsClient()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: secrets)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    release_entrypoint.main()

    database_session.expire_all()
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha == target_sha
    assert ecs.service_updates == [
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "service": "ExecutorHost",
            "desiredCount": 0,
        },
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "service": "Tracker",
            "desiredCount": 0,
        },
    ]
    assert ecs.protection_updates[0]["tasks"] == ["task-1", "task-2"]
    assert ecs.stopped_tasks == ["task-1", "task-2"]

    sys.argv = sys.argv[:12] + ["maintenance-finish", target_sha]
    release_entrypoint.main()

    database_session.expire_all()
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha is None
    assert ecs.service_updates[-2:] == [
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "service": "ExecutorHost",
            "desiredCount": 1,
        },
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "service": "Tracker",
            "desiredCount": 2,
        },
    ]


def test_release_entrypoint_digest_failure_does_not_commit_release(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch, artifact_digest=hashlib.sha256(b"expected").hexdigest())
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())

    def create_s3_client(service_name: str) -> FakeS3Client:
        assert service_name == "s3"
        return FakeS3Client(b"different")

    monkeypatch.setattr(boto3, "client", create_s3_client)

    with pytest.raises(SystemExit, match="Executor release activation failed:.*digest mismatch"):
        release_entrypoint.main()

    database_session.expire_all()
    assert database_session.get(ExecutorRelease, "git-abc123-def456") is None


def test_release_entrypoint_rejects_invalid_release_id_before_reading_secret(monkeypatch: MonkeyPatch) -> None:
    _release_arguments(monkeypatch)
    sys.argv[13] = "invalid release"
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
        (14, "s3://other/releases/git-abc123-def456/executor.pex", "configured S3 bucket"),
        (16, "2", "Unsupported executor protocol"),
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
