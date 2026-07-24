import json
import os
import sys
from collections.abc import Sequence

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from tracker import release_cli, release_entrypoint


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


def _release_arguments(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tracker.release_entrypoint",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:tracker",
            "database.internal",
            "5432",
            "tracker",
            "releases",
            "releases",
            "git-abc123-def456",
            "s3://releases/releases/git-abc123-def456/executor.pex",
            "a" * 64,
            "1",
        ],
    )


def test_release_entrypoint_treats_task_command_as_validated_inputs_and_sanitizes_environment(
    monkeypatch: MonkeyPatch,
) -> None:
    _release_arguments(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "caller-credential")
    monkeypatch.setenv("AWS_ENDPOINT_URL_SECRETSMANAGER", "https://caller.invalid")
    monkeypatch.setenv("HTTPS_PROXY", "https://caller.invalid")
    monkeypatch.setenv("DB_HOST", "caller.invalid")
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    observed: list[str] = []

    def capture(argv: Sequence[str] | None = None) -> None:
        observed.extend(argv or [])

    monkeypatch.setattr(release_cli, "main", capture)

    release_entrypoint.main()

    assert client.secret_ids == ["arn:aws:secretsmanager:us-east-1:123456789012:secret:tracker"]
    assert "AWS_ACCESS_KEY_ID" not in os.environ
    assert "AWS_ENDPOINT_URL_SECRETSMANAGER" not in os.environ
    assert "HTTPS_PROXY" not in os.environ
    assert os.environ["DB_HOST"] == "database.internal"
    assert os.environ["EXECUTOR_RELEASE_BUCKET"] == "releases"
    assert observed == [
        "activate",
        "git-abc123-def456",
        "s3://releases/releases/git-abc123-def456/executor.pex",
        "a" * 64,
        "--protocol-version",
        "1",
    ]


def test_release_entrypoint_rejects_invalid_release_id_before_reading_secret(monkeypatch: MonkeyPatch) -> None:
    _release_arguments(monkeypatch)
    sys.argv[7] = "invalid release"
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
        (8, "s3://other/releases/git-abc123-def456/executor.pex", "configured S3 bucket"),
        (10, "2", "Unsupported executor protocol"),
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

    with pytest.raises(SystemExit, match="sealed configuration"):
        release_entrypoint.main()
