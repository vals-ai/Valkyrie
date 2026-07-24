import hashlib
import json
import os
import tempfile
import unittest
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast
from unittest import mock

import executor_release
from botocore.exceptions import ClientError
from executor_release import (
    ArtifactManifest,
    LaunchConfig,
    publish_artifact,
    run_activation,
    validate_artifact,
)


class CollisionS3:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")


class RecordingSts:
    def get_caller_identity(self) -> dict[str, object]:
        return {"Account": "123456789012"}


class RecordingSsm:
    def get_parameter(self, *, Name: str) -> dict[str, object]:
        if Name != "/valkyrie/dev/executor-release/launch-config":
            raise AssertionError(f"unexpected launch parameter: {Name}")
        return {"Parameter": {"Value": json.dumps(asdict(launch_config()))}}


class RecordingWaiter:
    def __init__(self, failures_remaining: int = 0) -> None:
        self.calls: list[dict[str, object]] = []
        self.failures_remaining = failures_remaining

    def wait(self, **kwargs: object) -> None:
        self.calls.append(kwargs)
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("waiter failed")


class RecordingEcs:
    def __init__(
        self,
        exit_code: int = 0,
        *,
        failures: list[dict[str, str]] | None = None,
        waiter_failures: int = 0,
    ) -> None:
        self.exit_code = exit_code
        self.failures = failures or []
        self.run_calls: list[dict[str, object]] = []
        self.waiter = RecordingWaiter(waiter_failures)
        self.waiter_names: list[str] = []

    def run_task(self, **kwargs: object) -> dict[str, object]:
        self.run_calls.append(kwargs)
        task_arn = f"arn:aws:ecs:us-east-1:123:task/cluster/release-task-{len(self.run_calls)}"
        return {
            "tasks": [] if self.failures else [{"taskArn": task_arn}],
            "failures": self.failures,
        }

    def get_waiter(self, name: str) -> RecordingWaiter:
        self.waiter_names.append(name)
        return self.waiter

    def describe_tasks(self, **_kwargs: object) -> dict[str, object]:
        return {"tasks": [{"containers": [{"name": "ExecutorRelease", "exitCode": self.exit_code}]}]}


def manifest(content: bytes) -> ArtifactManifest:
    digest = hashlib.sha256(content).hexdigest()
    release_id = f"git-abcdef123456-{digest[:16]}"
    return ArtifactManifest(
        release_id=release_id,
        source_revision="abcdef1234567890",
        artifact_digest=digest,
        artifact_size_bytes=len(content),
        key=f"releases/{release_id}/executor.pex",
        protocol_version="1",
    )


def launch_config() -> LaunchConfig:
    return LaunchConfig(
        bucket="release-bucket",
        cluster="cluster-arn",
        container="ExecutorRelease",
        prefix="releases",
        security_group="sg-123",
        subnets=["subnet-a", "subnet-b"],
        task_definition="task-definition-arn",
    )


class ExecutorReleaseTest(unittest.TestCase):
    def test_manifest_and_artifact_must_match(self) -> None:
        content = b"immutable executor"
        release = manifest(content)
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "executor.pex"
            artifact.write_bytes(content)
            validate_artifact(release, artifact)

            artifact.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "size|digest"):
                validate_artifact(release, artifact)

    def test_conditional_upload_treats_collision_as_nonfatal(self) -> None:
        content = b"immutable executor"
        release = manifest(content)
        client = CollisionS3()
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "executor.pex"
            artifact.write_bytes(content)
            publish_artifact(client, launch_config(), release, artifact)

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["IfNoneMatch"], "*")

    def test_upload_collision_still_reaches_sealed_activation(self) -> None:
        content = b"immutable executor"
        release = manifest(content)
        s3_client = CollisionS3()
        ecs_client = RecordingEcs()
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "executor.pex"
            artifact.write_bytes(content)
            manifest_path = Path(directory) / "manifest.json"
            manifest_path.write_text(json.dumps(asdict(release)))
            with (
                mock.patch.dict(os.environ, {"AWS_REGION": "us-east-1"}, clear=True),
                mock.patch.object(executor_release, "create_sts_client", return_value=RecordingSts()),
                mock.patch.object(executor_release, "create_ssm_client", return_value=RecordingSsm()),
                mock.patch.object(executor_release, "create_s3_client", return_value=s3_client),
                mock.patch.object(executor_release, "create_ecs_client", return_value=ecs_client),
            ):
                executor_release.main(
                    [
                        "--account-id",
                        "123456789012",
                        "--artifact",
                        str(artifact),
                        "--manifest",
                        str(manifest_path),
                        "--region",
                        "us-east-1",
                        "--stage",
                        "dev",
                    ]
                )

        self.assertEqual(s3_client.calls[0]["IfNoneMatch"], "*")
        self.assertEqual(len(ecs_client.run_calls), 1)

    def test_activation_uses_one_task_with_release_values_as_data_arguments(self) -> None:
        release = manifest(b"immutable executor")
        client = RecordingEcs()

        task_arn = run_activation(client, launch_config(), release)

        self.assertEqual(task_arn, "arn:aws:ecs:us-east-1:123:task/cluster/release-task-1")
        self.assertEqual(len(client.run_calls), 1)
        call = client.run_calls[0]
        self.assertEqual(call["taskDefinition"], "task-definition-arn")
        overrides = cast(Mapping[str, Any], call["overrides"])
        container_overrides = cast(list[Mapping[str, Any]], overrides["containerOverrides"])
        self.assertEqual(
            container_overrides[0]["command"],
            [
                release.release_id,
                f"s3://release-bucket/{release.key}",
                release.artifact_digest,
                release.protocol_version,
            ],
        )
        self.assertNotIn("environment", container_overrides[0])
        self.assertEqual(client.waiter.calls, [{"cluster": "cluster-arn", "tasks": [task_arn]}])

    def test_activation_reports_ecs_start_failure(self) -> None:
        client = RecordingEcs(failures=[{"reason": "RESOURCE:CPU"}])

        with self.assertRaisesRegex(RuntimeError, "ECS rejected"):
            run_activation(client, launch_config(), manifest(b"immutable executor"))

    def test_activation_can_retry_after_waiter_failure(self) -> None:
        client = RecordingEcs(waiter_failures=1)
        release = manifest(b"immutable executor")

        with self.assertRaisesRegex(RuntimeError, "waiter failed"):
            run_activation(client, launch_config(), release)
        task_arn = run_activation(client, launch_config(), release)

        self.assertEqual(task_arn, "arn:aws:ecs:us-east-1:123:task/cluster/release-task-2")
        self.assertEqual(len(client.run_calls), 2)
        self.assertEqual(
            client.waiter.calls,
            [
                {
                    "cluster": "cluster-arn",
                    "tasks": ["arn:aws:ecs:us-east-1:123:task/cluster/release-task-1"],
                },
                {
                    "cluster": "cluster-arn",
                    "tasks": ["arn:aws:ecs:us-east-1:123:task/cluster/release-task-2"],
                },
            ],
        )

    def test_activation_fails_on_nonzero_release_container(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "activation failed"):
            run_activation(RecordingEcs(exit_code=2), launch_config(), manifest(b"immutable executor"))


if __name__ == "__main__":
    unittest.main()
