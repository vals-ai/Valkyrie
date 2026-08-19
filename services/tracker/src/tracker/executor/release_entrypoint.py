"""Sealed ECS entrypoint for executor release and deployment control."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from typing import Protocol, cast

import boto3
from pydantic import BaseModel, ConfigDict, Field

from executor_protocol import (
    SUPPORTED_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    validate_executor_artifact_uri,
    validate_executor_digest,
)


class ReleaseInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    release_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    artifact_uri: str
    artifact_digest: str
    protocol_version: str = SUPPORTED_PROTOCOL_VERSION


class MaintenanceInput(BaseModel):
    target_sha: str = Field(pattern=r"^[0-9a-f]{40,64}$")


class ReleaseTaskConfig(BaseModel):
    db_secret_arn: str
    db_host: str
    db_port: int
    db_name: str
    release_bucket: str
    release_prefix: str
    cluster_arn: str
    executor_service_name: str
    tracker_service_name: str
    executor_desired_count: int
    tracker_desired_count: int


class SecretsManagerClient(Protocol):
    def get_secret_value(self, *, SecretId: str) -> Mapping[str, object]: ...


class ServicesStableWaiter(Protocol):
    def wait(self, **kwargs: object) -> None: ...


class EcsClient(Protocol):
    def get_waiter(self, name: str) -> ServicesStableWaiter: ...

    def list_tasks(self, **kwargs: object) -> Mapping[str, object]: ...

    def update_task_protection(self, **kwargs: object) -> Mapping[str, object]: ...

    def stop_task(self, **kwargs: object) -> Mapping[str, object]: ...

    def update_service(self, **kwargs: object) -> Mapping[str, object]: ...


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


def create_secrets_manager_client() -> SecretsManagerClient:
    return cast(
        SecretsManagerClient,
        boto3.client("secretsmanager"),  # pyright: ignore[reportUnknownMemberType]
    )


def create_ecs_client() -> EcsClient:
    return cast(EcsClient, boto3.client("ecs"))  # pyright: ignore[reportUnknownMemberType]


def _configure_database(task: ReleaseTaskConfig) -> None:
    for name in tuple(os.environ):
        if name in _CALLER_CONTROLLED_ENV or name.startswith("AWS_ENDPOINT_URL"):
            os.environ.pop(name, None)

    response = create_secrets_manager_client().get_secret_value(SecretId=task.db_secret_arn)
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


def _activate_sealed_release(task: ReleaseTaskConfig, release: ReleaseInput) -> None:
    from sqlmodel import Session

    from tracker.database.models import ExecutorRelease
    from tracker.database.session import engine
    from tracker.executor.release_control import ReleaseControlError, activate_release

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


def _executor_tasks(client: EcsClient, task: ReleaseTaskConfig) -> list[str]:
    task_arns: list[str] = []
    next_token: str | None = None
    while True:
        arguments: dict[str, object] = {
            "cluster": task.cluster_arn,
            "serviceName": task.executor_service_name,
            "desiredStatus": "RUNNING",
        }
        if next_token is not None:
            arguments["nextToken"] = next_token
        response = client.list_tasks(**arguments)
        task_arns.extend(str(value) for value in cast(Sequence[object], response.get("taskArns", [])))
        raw_next_token = response.get("nextToken")
        if raw_next_token is None:
            return task_arns
        next_token = str(raw_next_token)


def _force_stop_executor_hosts(client: EcsClient, task: ReleaseTaskConfig) -> int:
    task_arns = _executor_tasks(client, task)
    for start in range(0, len(task_arns), 10):
        client.update_task_protection(
            cluster=task.cluster_arn,
            tasks=task_arns[start : start + 10],
            protectionEnabled=False,
        )
    for task_arn in task_arns:
        client.stop_task(
            cluster=task.cluster_arn,
            task=task_arn,
            reason="Deployment maintenance",
        )
    return len(task_arns)


def _begin_maintenance(task: ReleaseTaskConfig, maintenance: MaintenanceInput) -> dict[str, int]:
    from sqlmodel import Session

    from tracker.database.session import engine
    from tracker.executor.maintenance_control import MaintenanceOwnershipError, begin_maintenance

    try:
        with Session(engine) as session:
            summary = begin_maintenance(session, target_sha=maintenance.target_sha)
            session.commit()
    except MaintenanceOwnershipError as error:
        raise SystemExit(f"Maintenance begin failed: {error}") from error

    client = create_ecs_client()
    client.update_service(cluster=task.cluster_arn, service=task.executor_service_name, desiredCount=0)
    client.update_service(cluster=task.cluster_arn, service=task.tracker_service_name, desiredCount=0)
    client.get_waiter("services_stable").wait(
        cluster=task.cluster_arn,
        services=[task.tracker_service_name],
    )
    stopped_hosts = _force_stop_executor_hosts(client, task)
    return {
        "benchmarks": summary.benchmarks,
        "tasks": summary.tasks,
        "dispatches": summary.dispatches,
        "executor_hosts": stopped_hosts,
    }


def _finish_maintenance(task: ReleaseTaskConfig, maintenance: MaintenanceInput) -> None:
    from sqlmodel import Session

    from tracker.database.session import engine
    from tracker.executor.maintenance_control import MaintenanceOwnershipError, finish_maintenance

    client = create_ecs_client()
    client.update_service(
        cluster=task.cluster_arn,
        service=task.executor_service_name,
        desiredCount=task.executor_desired_count,
    )
    client.update_service(
        cluster=task.cluster_arn,
        service=task.tracker_service_name,
        desiredCount=task.tracker_desired_count,
    )
    client.get_waiter("services_stable").wait(
        cluster=task.cluster_arn,
        services=[task.executor_service_name, task.tracker_service_name],
    )

    try:
        with Session(engine) as session:
            finish_maintenance(session, target_sha=maintenance.target_sha)
            session.commit()
    except MaintenanceOwnershipError as error:
        raise SystemExit(f"Maintenance finish failed: {error}") from error


def main() -> None:
    arguments = sys.argv[1:]
    if len(arguments) < 12:
        raise SystemExit("Release task requires its sealed configuration and an operation")
    task = ReleaseTaskConfig(
        db_secret_arn=arguments[0],
        db_host=arguments[1],
        db_port=int(arguments[2]),
        db_name=arguments[3],
        release_bucket=arguments[4],
        release_prefix=arguments[5],
        cluster_arn=arguments[6],
        executor_service_name=arguments[7],
        tracker_service_name=arguments[8],
        executor_desired_count=int(arguments[9]),
        tracker_desired_count=int(arguments[10]),
    )
    operation = arguments[11]

    if operation == "activate" and len(arguments) == 16:
        release = ReleaseInput(
            release_id=arguments[12],
            artifact_uri=arguments[13],
            artifact_digest=validate_executor_digest(arguments[14]),
            protocol_version=arguments[15],
        )
        if release.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
            raise ValueError(f"Unsupported executor protocol version: {release.protocol_version}")
        validate_executor_artifact_uri(release.artifact_uri, task.release_bucket, task.release_prefix)
        _configure_database(task)
        _activate_sealed_release(task, release)
        return

    if operation not in ("maintenance-begin", "maintenance-finish") or len(arguments) != 13:
        raise SystemExit(f"Invalid sealed release task operation: {operation}")
    maintenance = MaintenanceInput(target_sha=arguments[12])
    _configure_database(task)
    if operation == "maintenance-begin":
        print(json.dumps(_begin_maintenance(task, maintenance), sort_keys=True))
    else:
        _finish_maintenance(task, maintenance)
        print(json.dumps({"status": "open"}, sort_keys=True))


if __name__ == "__main__":
    main()
