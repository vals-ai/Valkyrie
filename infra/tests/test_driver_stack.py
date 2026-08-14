"""Synthesis tests for the release-test Package R driver boundary."""

import json
import os
import unittest
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import assertions, aws_ec2, aws_ecr, aws_ecs, aws_s3, aws_secretsmanager
from driver_stack import DriverStack
from stage import DEV, RELEASE_TEST, Stage

TEST_ENV = cdk.Environment(account="123456789012", region="us-east-1")
DRIVER_ENV = {
    "RELEASE_TEST_DRIVER_SECRET_ARN": (
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:example/release-test/driver-ABC123"
    ),
    "RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN": (
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:example/sandbox-provider-DEF456"
    ),
    "RELEASE_TEST_OPERATOR_PRINCIPAL_ARN": "arn:aws:iam::123456789012:role/ReleaseTestAdmin",
}


@contextmanager
def driver_template() -> Iterator[assertions.Template]:
    with mock.patch.dict(os.environ, DRIVER_ENV, clear=False):
        app = cdk.App()
        dependencies = cdk.Stack(app, "DriverDependencies", env=TEST_ENV)
        vpc = aws_ec2.Vpc(
            dependencies,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[aws_ec2.SubnetConfiguration(name="Public", subnet_type=aws_ec2.SubnetType.PUBLIC)],
        )
        cluster = aws_ecs.Cluster(dependencies, "Cluster", vpc=vpc)
        bucket = aws_s3.Bucket(dependencies, "Bucket")
        tracker_repository = aws_ecr.Repository(dependencies, "TrackerRepository")
        db_credentials = aws_secretsmanager.Secret(dependencies, "DbCredentials")
        redis_security_group = aws_ec2.SecurityGroup(dependencies, "RedisSecurityGroup", vpc=vpc)
        stage = Stage(RELEASE_TEST)
        stack = DriverStack(
            app,
            stage.stack_id("DriverStack"),
            stage=stage,
            vpc=vpc,
            cluster=cluster,
            bucket=bucket,
            tracker_repository=cast(aws_ecr.IRepository, tracker_repository),
            image_tag="package-r-test",
            db_host="tracker-db.internal",
            db_port="5432",
            db_credentials=cast(aws_secretsmanager.ISecret, db_credentials),
            redis_url="redis://redis.internal:6379",
            redis_security_group=redis_security_group,
            env=TEST_ENV,
        )
        yield assertions.Template.from_stack(stack)


def policy_statements(template: assertions.Template) -> list[Mapping[str, Any]]:
    statements: list[Mapping[str, Any]] = []
    for resource in template.find_resources("AWS::IAM::Policy").values():
        properties = cast(Mapping[str, Any], resource["Properties"])
        document = cast(Mapping[str, Any], properties["PolicyDocument"])
        statements.extend(cast(list[Mapping[str, Any]], document["Statement"]))
    return statements


class DriverStackTest(unittest.TestCase):
    def test_driver_stack_is_release_test_only(self) -> None:
        with mock.patch.dict(os.environ, DRIVER_ENV, clear=False):
            app = cdk.App()
            dependencies = cdk.Stack(app, "DevDependencies", env=TEST_ENV)
            vpc = aws_ec2.Vpc(dependencies, "Vpc", max_azs=2, nat_gateways=0)
            cluster = aws_ecs.Cluster(dependencies, "Cluster", vpc=vpc)
            bucket = aws_s3.Bucket(dependencies, "Bucket")
            tracker_repository = aws_ecr.Repository(dependencies, "TrackerRepository")
            db_credentials = aws_secretsmanager.Secret(dependencies, "DbCredentials")
            redis_security_group = aws_ec2.SecurityGroup(dependencies, "RedisSecurityGroup", vpc=vpc)

            with self.assertRaisesRegex(ValueError, "release-test"):
                DriverStack(
                    app,
                    "DriverStack",
                    stage=Stage(DEV),
                    vpc=vpc,
                    cluster=cluster,
                    bucket=bucket,
                    tracker_repository=cast(aws_ecr.IRepository, tracker_repository),
                    image_tag="package-r-test",
                    db_host="tracker-db.internal",
                    db_port="5432",
                    db_credentials=cast(aws_secretsmanager.ISecret, db_credentials),
                    redis_url="redis://redis.internal:6379",
                    redis_security_group=redis_security_group,
                    env=TEST_ENV,
                )

    def test_driver_is_one_task_definition_with_source_sg_redis_ingress(self) -> None:
        with driver_template() as template:
            template.resource_count_is("AWS::ECS::TaskDefinition", 1)
            template.resource_count_is("AWS::ECS::Service", 0)
            template.resource_count_is("AWS::EC2::SecurityGroup", 1)
            driver_security_group_id = next(
                logical_id
                for logical_id, resource in template.find_resources("AWS::EC2::SecurityGroup").items()
                if resource["Properties"]["GroupDescription"]
                == "No-ingress security group for the release-test Package R driver"
            )
            redis_ingress = [
                resource["Properties"]
                for resource in template.find_resources("AWS::EC2::SecurityGroupIngress").values()
                if resource["Properties"].get("FromPort") == 6379 or resource["Properties"].get("ToPort") == 6379
            ]
            self.assertEqual(len(redis_ingress), 1)
            ingress = redis_ingress[0]
            self.assertEqual(
                {key: ingress[key] for key in ("Description", "FromPort", "IpProtocol", "ToPort")},
                {
                    "Description": "Allow release-test Driver to connect to Redis",
                    "FromPort": 6379,
                    "IpProtocol": "tcp",
                    "ToPort": 6379,
                },
            )
            self.assertNotIn("CidrIp", ingress)
            self.assertIn("RedisSecurityGroup", ingress["GroupId"]["Fn::ImportValue"])
            self.assertEqual(
                ingress["SourceSecurityGroupId"],
                {"Fn::GetAtt": [driver_security_group_id, "GroupId"]},
            )
            template.has_resource_properties(
                "AWS::ECS::TaskDefinition",
                {
                    "Family": "PackageRDriver-release-test",
                    "NetworkMode": "awsvpc",
                    "RuntimePlatform": {
                        "CpuArchitecture": "ARM64",
                        "OperatingSystemFamily": "LINUX",
                    },
                    "ContainerDefinitions": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Command": [
                                        "/bin/sh",
                                        "-c",
                                        "echo 'A reviewed ECS command override is required for this release-test driver task.' >&2; exit 64",
                                    ],
                                    "Environment": assertions.Match.array_with(
                                        [
                                            {"Name": "DB_HOST", "Value": "tracker-db.internal"},
                                            assertions.Match.object_like({"Name": "TRACKER_BASE_URL"}),
                                        ]
                                    ),
                                    "Secrets": assertions.Match.array_with(
                                        [
                                            assertions.Match.object_like({"Name": "DB_USERNAME"}),
                                            assertions.Match.object_like({"Name": "DB_PASSWORD"}),
                                            assertions.Match.object_like({"Name": "TRACKER_API_KEY"}),
                                            assertions.Match.object_like({"Name": "BENCHMARK_AUTHORIZATION"}),
                                        ]
                                    ),
                                }
                            )
                        ]
                    ),
                },
            )
            task_definition = next(iter(template.find_resources("AWS::ECS::TaskDefinition").values()))
            rendered_secrets = json.dumps(task_definition["Properties"]["ContainerDefinitions"][0]["Secrets"])
            self.assertIn("driver-ABC123:tracker_api_key::", rendered_secrets)
            self.assertIn("driver-ABC123:benchmark_authorization::", rendered_secrets)

    def test_driver_publishes_stage_qualified_launch_contract(self) -> None:
        with driver_template() as template:
            parameters = template.find_resources("AWS::SSM::Parameter")
            names = {resource["Properties"]["Name"] for resource in parameters.values()}
            self.assertEqual(
                names,
                {
                    "/valkyrie/release-test/driver/task-definition-arn",
                    "/valkyrie/release-test/driver/security-group-id",
                    "/valkyrie/release-test/driver/log-group-name",
                    "/valkyrie/release-test/driver/operator-role-arn",
                },
            )

    def test_driver_task_can_read_only_the_configured_sandbox_provider_secret(self) -> None:
        with driver_template() as template:
            statements = policy_statements(template)
            secret_read = next(
                statement
                for statement in statements
                if "secretsmanager:GetSecretValue" in statement["Action"]
                and "sandbox-provider-DEF456" in json.dumps(statement["Resource"])
            )
            self.assertEqual(
                secret_read["Resource"],
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:example/sandbox-provider-DEF456",
            )

    def test_driver_task_can_read_only_the_campaign_artifacts_and_exact_agent_object(self) -> None:
        with driver_template() as template:
            statements = policy_statements(template)
            rendered = json.dumps(statements)
            self.assertIn("releases/package-r/*", rendered)
            self.assertIn("agents/coexistence_sleep_agent.zip", rendered)
            self.assertNotIn('"agents/*"', rendered)
            self.assertNotIn('"s3:List*"', rendered)
            self.assertNotIn('"s3:GetBucket*"', rendered)

    def test_driver_task_has_bounded_campaign_output_and_log_permissions(self) -> None:
        with driver_template() as template:
            statements = policy_statements(template)
            rendered = json.dumps(statements)
            self.assertIn("/benchmarks/*", rendered)
            self.assertIn("log-group:benchmarks/*", rendered)
            self.assertIn("logs:CreateLogGroup", rendered)
            self.assertIn("logs:PutRetentionPolicy", rendered)
            self.assertIn("logs:CreateLogStream", rendered)
            self.assertIn("logs:PutLogEvents", rendered)
            self.assertNotIn("s3:DeleteObject", rendered)
            benchmark_objects = next(
                statement
                for statement in statements
                if "/benchmarks/*" in json.dumps(statement["Resource"]) and "s3:PutObject" in statement["Action"]
            )
            self.assertEqual(
                set(benchmark_objects["Action"]),
                {"s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"},
            )
            list_bucket = [statement for statement in statements if statement["Action"] == "s3:ListBucket"]
            self.assertTrue(all("Condition" in statement for statement in list_bucket))
            campaign_list = [
                statement
                for statement in list_bucket
                if statement["Condition"] == {"StringLike": {"s3:prefix": ["benchmarks/*"]}}
            ]
            self.assertEqual(len(campaign_list), 1)
            self.assertNotEqual(campaign_list[0]["Resource"], "*")

    def test_operator_can_run_only_the_driver_task_and_pass_its_roles(self) -> None:
        with driver_template() as template:
            statements = policy_statements(template)
            run_task = next(statement for statement in statements if statement["Action"] == "ecs:RunTask")
            pass_roles = next(statement for statement in statements if statement["Action"] == "iam:PassRole")
            read_contract = next(
                statement for statement in statements if "ssm:GetParametersByPath" in statement["Action"]
            )
            self.assertNotEqual(run_task["Resource"], "*")
            self.assertNotEqual(pass_roles["Resource"], "*")
            self.assertNotEqual(read_contract["Resource"], "*")
            self.assertEqual(
                pass_roles["Condition"], {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}
            )


if __name__ == "__main__":
    unittest.main()
