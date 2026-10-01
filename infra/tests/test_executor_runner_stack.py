"""Contract assertions for the per-dispatch ECS runner boundary."""

import json
import os
import unittest
from typing import Any
from unittest import mock

from constants import EXECUTOR_RELEASE_BUCKET_NAME
from stage import BENCH, DEV, PROD, RELEASE_TEST, Stage
from test_monitoring_stack import (
    TEST_BENCH_ENV,
    TEST_DEV_ENV,
    TEST_PROD_ENV,
    TEST_AWS_ACCOUNT,
    TEST_RELEASE_TEST_ENV,
    shared_template,
    service_templates,
)


class ExecutorRunnerStackTest(unittest.TestCase):
    def test_runner_replaces_host_but_retains_redis_exports_at_every_stage(self) -> None:
        for stage_name, environment in (
            (BENCH, TEST_BENCH_ENV),
            (DEV, TEST_DEV_ENV),
            (PROD, TEST_PROD_ENV),
            (RELEASE_TEST, TEST_RELEASE_TEST_ENV),
        ):
            with self.subTest(stage=stage_name), mock.patch.dict(os.environ, environment, clear=True):
                shared = shared_template(stage_name)
                tracker, executor, monitoring = service_templates(stage_name)

            self.assertEqual(len(shared.find_resources("AWS::ElastiCache::CacheCluster")), 1)
            self.assertEqual(len(shared.find_resources("AWS::ElastiCache::SubnetGroup")), 1)
            redis_exports = [
                output["Export"]["Name"]
                for output in shared.to_json().get("Outputs", {}).values()
                if "RedisCluster" in json.dumps(output) or "RedisSG" in json.dumps(output)
            ]
            self.assertEqual(len(redis_exports), 4)
            self.assertEqual(len(set(map(json.dumps, redis_exports))), 4)
            if stage_name == RELEASE_TEST:
                repositories = shared.find_resources("AWS::ECR::Repository")
                self.assertEqual(len(repositories), 2)
                self.assertTrue(
                    any(
                        repository["Properties"]["RepositoryName"] == "valkyrie/release-test/executor-host"
                        for repository in repositories.values()
                    )
                )
                legacy_exports = [
                    output
                    for output in shared.to_json().get("Outputs", {}).values()
                    if "ReleaseTestExecutorHostRepository" in json.dumps(output)
                ]
                self.assertEqual(len(legacy_exports), 2)
            self.assertFalse(executor.find_resources("AWS::ECS::Service"))
            self.assertFalse(executor.find_resources("AWS::ApplicationAutoScaling::ScalableTarget"))
            control_definitions = executor.find_resources("AWS::ECS::TaskDefinition")
            self.assertEqual(len(control_definitions), 1)
            control_argv = next(iter(control_definitions.values()))["Properties"]["ContainerDefinitions"][0][
                "EntryPoint"
            ]
            self.assertEqual(control_argv[-2:], ["--runner-task-family", Stage(stage_name).phys("ExecutorRunner")])
            self.assertNotIn("ExecutorHost", json.dumps(executor.to_json()))
            self.assertNotIn("Redis", json.dumps(monitoring.to_json()))

            runner = next(
                resource["Properties"]
                for resource in tracker.find_resources("AWS::ECS::TaskDefinition").values()
                if resource["Properties"].get("Family") == Stage(stage_name).phys("ExecutorRunner")
            )
            self.assertEqual(runner["Cpu"], "1024")
            self.assertEqual(runner["Memory"], "4096")
            self.assertEqual(runner["RuntimePlatform"]["CpuArchitecture"], "ARM64")
            container = runner["ContainerDefinitions"][0]
            self.assertEqual(container["Name"], "ExecutorRunnerContainer")
            self.assertEqual(container["StopTimeout"], 120)
            environment_values = {item["Name"]: item["Value"] for item in container["Environment"]}
            for name in (
                "DB_HOST",
                "DB_PORT",
                "DB_NAME",
                "EXECUTOR_RELEASE_BUCKET",
                "EXECUTOR_RELEASE_PREFIX",
                "EXECUTOR_CACHE_DIR",
                "EXECUTOR_PAYLOAD_KMS_KEY_ID",
            ):
                self.assertIn(name, environment_values)
            self.assertEqual(environment_values["EXECUTOR_LAUNCHER"], "ecs")
            self.assertEqual(
                {item["Name"] for item in container["Secrets"]},
                {"DB_USERNAME", "DB_PASSWORD"} | ({"SENTRY_DSN"} if stage_name != RELEASE_TEST else set()),
            )

    def test_launch_permissions_and_network_are_scoped(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEV_ENV, clear=True):
            tracker, _, _ = service_templates(DEV)
        resources = tracker.to_json()["Resources"]
        roles = {
            value["Properties"].get("RoleName"): logical_id
            for logical_id, value in resources.items()
            if value["Type"] == "AWS::IAM::Role"
        }
        tracker_role = roles["ValkyrieTrackerTaskRole-dev"]
        runner_role = roles["ValkyrieExecutorRunnerTaskRole-dev"]
        execution_role = roles["ValkyrieExecutorRunnerExecution-dev"]
        policies = [
            statement
            for value in resources.values()
            if value["Type"] == "AWS::IAM::Policy" and value["Properties"]["Roles"] == [{"Ref": tracker_role}]
            for statement in value["Properties"]["PolicyDocument"]["Statement"]
        ]

        def by_action(action: str) -> list[dict[str, Any]]:
            return [p for p in policies if action in (p["Action"] if isinstance(p["Action"], list) else [p["Action"]])]

        launch = by_action("ecs:RunTask")
        self.assertEqual(len(launch), 1)
        self.assertIn("ExecutorRunner-dev:*", json.dumps(launch[0]["Resource"]))
        self.assertIn("ecs:cluster", launch[0]["Condition"]["ArnEquals"])
        for action in ("ecs:DescribeTasks",):
            policy = by_action(action)
            self.assertEqual(len(policy), 1)
            self.assertIn(":task/", json.dumps(policy[0]["Resource"]))
            self.assertIn("/*", json.dumps(policy[0]["Resource"]))
            self.assertIn("ecs:cluster", policy[0]["Condition"]["ArnEquals"])
        passing = by_action("iam:PassRole")
        self.assertEqual(len(passing), 1)
        self.assertEqual(passing[0]["Condition"], {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}})
        self.assertIn(runner_role, json.dumps(passing[0]["Resource"]))
        self.assertIn(execution_role, json.dumps(passing[0]["Resource"]))
        generated = by_action("kms:GenerateDataKey")
        self.assertEqual(len(generated), 1)
        self.assertNotEqual(generated[0]["Resource"], "*")
        self.assertTrue(
            any(
                value["Type"] == "AWS::KMS::Key"
                and value["Properties"]["EnableKeyRotation"]
                and value["DeletionPolicy"] == "Retain"
                for value in resources.values()
            )
        )
        runner_policies = [
            statement
            for value in resources.values()
            if value["Type"] == "AWS::IAM::Policy" and value["Properties"]["Roles"] == [{"Ref": runner_role}]
            for statement in value["Properties"]["PolicyDocument"]["Statement"]
        ]
        payload_key_id = next(logical_id for logical_id, value in resources.items() if value["Type"] == "AWS::KMS::Key")
        release_bucket_name = f"{Stage(DEV).phys(EXECUTOR_RELEASE_BUCKET_NAME)}-{TEST_AWS_ACCOUNT}"
        payload_decrypt = [
            policy
            for policy in runner_policies
            if "kms:Decrypt" in (policy["Action"] if isinstance(policy["Action"], list) else [policy["Action"]])
            and policy["Resource"] == {"Fn::GetAtt": [payload_key_id, "Arn"]}
        ]
        runner_decrypts = [
            policy
            for policy in runner_policies
            if "kms:Decrypt" in (policy["Action"] if isinstance(policy["Action"], list) else [policy["Action"]])
        ]
        self.assertEqual(runner_decrypts, payload_decrypt)
        self.assertEqual(len(payload_decrypt), 1)
        self.assertEqual(payload_decrypt[0]["Action"], "kms:Decrypt")
        release_access = [
            policy
            for policy in runner_policies
            if "s3:GetObject" in (policy["Action"] if isinstance(policy["Action"], list) else [policy["Action"]])
            and release_bucket_name in json.dumps(policy["Resource"])
        ]
        self.assertEqual(len(release_access), 1)
        self.assertNotIn("*", [policy["Resource"] for policy in runner_policies if policy["Effect"] == "Allow"])
        self.assertEqual(release_access[0]["Action"], "s3:GetObject")
        self.assertEqual(
            release_access[0]["Resource"],
            {
                "Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, f":s3:::{release_bucket_name}/releases/*"]],
            },
        )
        # Benchmark services admit only the Tracker service security group, so runners must launch in it.
        tracker_container = next(
            container
            for value in resources.values()
            if value["Type"] == "AWS::ECS::TaskDefinition"
            for container in value["Properties"]["ContainerDefinitions"]
            if container["Name"] == "TrackerContainer"
        )
        runner_security_group = next(
            variable["Value"]
            for variable in tracker_container["Environment"]
            if variable["Name"] == "EXECUTOR_RUNNER_SECURITY_GROUP"
        )
        tracker_service = next(value for value in resources.values() if value["Type"] == "AWS::ECS::Service")
        service_groups = tracker_service["Properties"]["NetworkConfiguration"]["AwsvpcConfiguration"]["SecurityGroups"]
        self.assertEqual([runner_security_group], service_groups)


if __name__ == "__main__":
    unittest.main()
