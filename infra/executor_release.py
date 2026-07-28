"""Publish one executor artifact and invoke the sealed release-control task."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import boto3
from botocore.exceptions import ClientError

from constants import executor_release_launch_parameter


class S3Client(Protocol):
    def put_object(self, **kwargs: object) -> Mapping[str, object]: ...


class SsmClient(Protocol):
    def get_parameter(self, *, Name: str) -> Mapping[str, object]: ...


class TasksStoppedWaiter(Protocol):
    def wait(self, **kwargs: object) -> None: ...


class EcsClient(Protocol):
    def run_task(self, **kwargs: object) -> Mapping[str, object]: ...

    def get_waiter(self, name: str) -> TasksStoppedWaiter: ...

    def describe_tasks(self, **kwargs: object) -> Mapping[str, object]: ...


class StsClient(Protocol):
    def get_caller_identity(self) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class ArtifactManifest:
    release_id: str
    source_revision: str
    artifact_digest: str
    artifact_size_bytes: int
    key: str
    protocol_version: str

    @classmethod
    def load(cls, path: Path) -> ArtifactManifest:
        decoded: object = json.loads(path.read_text())
        if not isinstance(decoded, dict):
            raise ValueError("Executor artifact manifest must be an object")
        value = cast(Mapping[str, object], decoded)
        return cls(
            release_id=_string(value, "release_id"),
            source_revision=_string(value, "source_revision"),
            artifact_digest=_string(value, "artifact_digest"),
            artifact_size_bytes=_integer(value, "artifact_size_bytes"),
            key=_string(value, "key"),
            protocol_version=_string(value, "protocol_version"),
        )


@dataclass(frozen=True)
class LaunchConfig:
    bucket: str
    cluster: str
    container: str
    prefix: str
    security_group: str
    subnets: list[str]
    task_definition: str

    @classmethod
    def from_parameter(cls, response: Mapping[str, object]) -> LaunchConfig:
        parameter_value = response.get("Parameter")
        if not isinstance(parameter_value, dict):
            raise ValueError("Release launch parameter is missing")
        parameter = cast(Mapping[str, object], parameter_value)
        raw_value = parameter.get("Value")
        if not isinstance(raw_value, str):
            raise ValueError("Release launch parameter has no string value")
        decoded: object = json.loads(raw_value)
        if not isinstance(decoded, dict):
            raise ValueError("Release launch parameter must contain an object")
        value = cast(Mapping[str, object], decoded)
        subnets_value = value.get("subnets")
        if not isinstance(subnets_value, list):
            raise ValueError("Release launch parameter must contain public subnets")
        subnets = cast(list[object], subnets_value)
        if not subnets or not all(isinstance(subnet, str) for subnet in subnets):
            raise ValueError("Release launch parameter must contain public subnets")
        return cls(
            bucket=_string(value, "bucket"),
            cluster=_string(value, "cluster"),
            container=_string(value, "container"),
            prefix=_string(value, "prefix"),
            security_group=_string(value, "security_group"),
            subnets=cast(list[str], subnets),
            task_definition=_string(value, "task_definition"),
        )


def create_sts_client() -> StsClient:
    return cast(StsClient, boto3.client("sts"))  # pyright: ignore[reportUnknownMemberType]


def create_ssm_client() -> SsmClient:
    return cast(SsmClient, boto3.client("ssm"))  # pyright: ignore[reportUnknownMemberType]


def create_s3_client() -> S3Client:
    return cast(S3Client, boto3.client("s3"))  # pyright: ignore[reportUnknownMemberType]


def create_ecs_client() -> EcsClient:
    return cast(EcsClient, boto3.client("ecs"))  # pyright: ignore[reportUnknownMemberType]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_artifact(manifest: ArtifactManifest, artifact: Path) -> None:
    if artifact.stat().st_size != manifest.artifact_size_bytes:
        raise ValueError("Executor artifact size does not match its manifest")
    if sha256(artifact) != manifest.artifact_digest:
        raise ValueError("Executor artifact digest does not match its manifest")
    expected_key = f"releases/{manifest.release_id}/executor.pex"
    if manifest.key != expected_key:
        raise ValueError("Executor artifact key does not match its release identity")


def publish_artifact(
    client: S3Client,
    config: LaunchConfig,
    manifest: ArtifactManifest,
    artifact: Path,
) -> None:
    if not manifest.key.startswith(f"{config.prefix}/"):
        raise ValueError("Executor artifact key is outside the configured release prefix")
    checksum = base64.b64encode(bytes.fromhex(manifest.artifact_digest)).decode()
    try:
        with artifact.open("rb") as body:
            client.put_object(
                Bucket=config.bucket,
                Key=manifest.key,
                Body=body,
                IfNoneMatch="*",
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum,
                Metadata={
                    "artifact-digest": manifest.artifact_digest,
                    "protocol-version": manifest.protocol_version,
                    "release-id": manifest.release_id,
                    "source-revision": manifest.source_revision,
                },
            )
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"412", "PreconditionFailed"}:
            raise


def run_control_task(
    client: EcsClient,
    config: LaunchConfig,
    command: list[str],
) -> str:
    response = client.run_task(
        cluster=config.cluster,
        taskDefinition=config.task_definition,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "assignPublicIp": "ENABLED",
                "securityGroups": [config.security_group],
                "subnets": config.subnets,
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": config.container,
                    "command": command,
                }
            ]
        },
    )
    failures = response.get("failures")
    tasks_value = response.get("tasks")
    if failures or not isinstance(tasks_value, list):
        raise RuntimeError(f"ECS rejected executor release task: {failures!r}")
    tasks = cast(list[object], tasks_value)
    if len(tasks) != 1:
        raise RuntimeError(f"ECS rejected executor release task: {failures!r}")
    task_value = tasks[0]
    if not isinstance(task_value, dict):
        raise RuntimeError("ECS returned no executor release task ARN")
    task = cast(Mapping[str, object], task_value)
    task_arn_value = task.get("taskArn")
    if not isinstance(task_arn_value, str):
        raise RuntimeError("ECS returned no executor release task ARN")
    task_arn = task_arn_value
    client.get_waiter("tasks_stopped").wait(cluster=config.cluster, tasks=[task_arn])
    description = client.describe_tasks(cluster=config.cluster, tasks=[task_arn])
    stopped_tasks_value = description.get("tasks")
    if not isinstance(stopped_tasks_value, list):
        raise RuntimeError("ECS returned no stopped executor release task")
    stopped_tasks = cast(list[object], stopped_tasks_value)
    if len(stopped_tasks) != 1:
        raise RuntimeError("ECS returned no stopped executor release task")
    stopped_task_value = stopped_tasks[0]
    if not isinstance(stopped_task_value, dict):
        raise RuntimeError("ECS returned no stopped executor release task")
    stopped_task = cast(Mapping[str, object], stopped_task_value)
    containers_value = stopped_task.get("containers")
    if not isinstance(containers_value, list):
        raise RuntimeError("Executor release task returned no container status")
    containers = cast(list[object], containers_value)
    release_container = next(
        (
            cast(Mapping[str, object], container)
            for container in containers
            if isinstance(container, dict) and cast(Mapping[str, object], container).get("name") == config.container
        ),
        None,
    )
    if release_container is None or release_container.get("exitCode") != 0:
        raise RuntimeError(f"Executor release task failed: {release_container!r}")
    return task_arn


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--maintenance-operation", choices=("begin", "finish"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--region", required=True)
    parser.add_argument("--stage", choices=("dev", "prod", "release-test"), required=True)
    parser.add_argument("--target-sha")
    args = parser.parse_args(argv)

    if os.environ.get("AWS_REGION") != args.region:
        parser.error("AWS_REGION does not match the requested release region")

    sts = create_sts_client()
    identity = sts.get_caller_identity()
    if identity.get("Account") != args.account_id:
        parser.error("AWS credentials do not match the requested release account")

    ssm = create_ssm_client()
    config = LaunchConfig.from_parameter(ssm.get_parameter(Name=executor_release_launch_parameter(args.stage)))
    ecs = create_ecs_client()

    if args.maintenance_operation is not None:
        if args.target_sha is None or args.artifact is not None or args.manifest is not None:
            parser.error("Maintenance requires --target-sha and does not accept release artifacts")
        task_arn = run_control_task(
            ecs,
            config,
            [f"maintenance-{args.maintenance_operation}", args.target_sha],
        )
        print(json.dumps({"maintenance": args.maintenance_operation, "task_arn": task_arn}, sort_keys=True))
        return

    if args.artifact is None or args.manifest is None or args.target_sha is not None:
        parser.error("Release activation requires --artifact and --manifest")
    manifest = ArtifactManifest.load(args.manifest)
    validate_artifact(manifest, args.artifact)
    s3 = create_s3_client()
    publish_artifact(s3, config, manifest, args.artifact)
    task_arn = run_control_task(
        ecs,
        config,
        [
            "activate",
            manifest.release_id,
            f"s3://{config.bucket}/{manifest.key}",
            manifest.artifact_digest,
            manifest.protocol_version,
        ],
    )
    print(json.dumps({"release_id": manifest.release_id, "task_arn": task_arn}, sort_keys=True))


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} is required")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if not isinstance(item, int):
        raise ValueError(f"{key} must be an integer")
    return item


if __name__ == "__main__":
    main()
