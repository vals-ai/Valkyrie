"""Sealed ECS entrypoint for activating one executor release."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from typing import Protocol, cast

import boto3
from pydantic import BaseModel, ConfigDict, Field

from executor_protocol import (
    SUPPORTED_PROTOCOL_VERSION,
    validate_executor_artifact_uri,
    validate_executor_digest,
)


class ReleaseInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    release_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    artifact_uri: str
    artifact_digest: str
    protocol_version: str = SUPPORTED_PROTOCOL_VERSION


class ReleaseTaskConfig(BaseModel):
    db_secret_arn: str
    db_host: str
    db_port: int
    db_name: str
    release_bucket: str
    release_prefix: str


class SecretsManagerClient(Protocol):
    def get_secret_value(self, *, SecretId: str) -> Mapping[str, object]: ...


def create_secrets_manager_client() -> SecretsManagerClient:
    return cast(
        SecretsManagerClient,
        boto3.client("secretsmanager"),  # pyright: ignore[reportUnknownMemberType]
    )


class DatabaseSecret(BaseModel):
    username: str
    password: str


_CALLER_CONTROLLED_ENV = (
    "ALL_PROXY",
    "AWS_ACCESS_KEY_ID",
    "AWS_CONFIG_FILE",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_DEFAULT_PROFILE",
    "AWS_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "DATABASE_URL",
    "DB_HOST",
    "DB_NAME",
    "DB_PASSWORD",
    "DB_PORT",
    "DB_USERNAME",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "all_proxy",
    "https_proxy",
    "http_proxy",
)


def _activate_sealed_release(task: ReleaseTaskConfig, release: ReleaseInput) -> None:
    from sqlmodel import Session

    from tracker.database.models import ExecutorRelease
    from tracker.database.session import engine
    from tracker.release_control import ReleaseControlError, activate_release

    try:
        with Session(engine) as session:
            activate_release(
                session,
                ExecutorRelease(
                    id=release.release_id,
                    artifact_uri=release.artifact_uri,
                    artifact_digest=release.artifact_digest,
                    protocol_version=release.protocol_version,
                ),
                expected_bucket=task.release_bucket,
                expected_prefix=task.release_prefix,
            )
            session.commit()
    except ReleaseControlError as error:
        raise SystemExit(f"Executor release activation failed: {error}") from error


def main() -> None:
    arguments = sys.argv[1:]
    if len(arguments) != 10:
        raise SystemExit("Release task requires its sealed configuration and four release inputs")
    task = ReleaseTaskConfig(
        db_secret_arn=arguments[0],
        db_host=arguments[1],
        db_port=int(arguments[2]),
        db_name=arguments[3],
        release_bucket=arguments[4],
        release_prefix=arguments[5],
    )
    release = ReleaseInput(
        release_id=arguments[6],
        artifact_uri=arguments[7],
        artifact_digest=validate_executor_digest(arguments[8]),
        protocol_version=arguments[9],
    )
    if release.protocol_version != SUPPORTED_PROTOCOL_VERSION:
        raise ValueError(f"Unsupported executor protocol version: {release.protocol_version}")
    validate_executor_artifact_uri(release.artifact_uri, task.release_bucket, task.release_prefix)

    for name in tuple(os.environ):
        if name in _CALLER_CONTROLLED_ENV or name.startswith("AWS_ENDPOINT_URL"):
            os.environ.pop(name, None)

    client = create_secrets_manager_client()
    response = client.get_secret_value(SecretId=task.db_secret_arn)
    secret = DatabaseSecret.model_validate_json(str(response["SecretString"]))
    os.environ.update(
        DB_USERNAME=secret.username,
        DB_PASSWORD=secret.password,
        DB_HOST=task.db_host,
        DB_PORT=str(task.db_port),
        DB_NAME=task.db_name,
        EXECUTOR_RELEASE_BUCKET=task.release_bucket,
        EXECUTOR_RELEASE_PREFIX=task.release_prefix,
    )

    _activate_sealed_release(task, release)


if __name__ == "__main__":
    main()
