"""Tests for the development account infrastructure boundary."""

import json
import os
import unittest
from collections.abc import Mapping
from typing import cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import assertions

from constants import (
    DEV_SHARED_ARTIFACT_BUCKET_PARAMETER,
    DEV_SHARED_AVAILABILITY_ZONES_PARAMETER,
    DEV_SHARED_CLUSTER_NAME_PARAMETER,
    DEV_SHARED_NAMESPACE_ARN_PARAMETER,
    DEV_SHARED_NAMESPACE_ID_PARAMETER,
    DEV_SHARED_NAMESPACE_NAME_PARAMETER,
    DEV_SHARED_PUBLIC_SUBNET_IDS_PARAMETER,
    DEV_SHARED_VPC_ID_PARAMETER,
    DEV_TRACKER_ALB_DNS_PARAMETER,
    DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER,
    DEV_TRACKER_SECURITY_GROUP_PARAMETER,
    executor_release_launch_parameter,
)
from shared import SharedStack
from executor_stack import ExecutorStack
from stage import DEV, PROD, RELEASE_TEST, Stage
from tracker_stack import TrackerStack

TEST_ACCOUNT = "123456789012"
TEST_REGION = "us-east-1"
TEST_ENV = cdk.Environment(account=TEST_ACCOUNT, region=TEST_REGION)
TEST_CONTEXT = {
    f"availability-zones:account={TEST_ACCOUNT}:region={TEST_REGION}": [
        f"{TEST_REGION}a",
        f"{TEST_REGION}b",
    ]
}
PROD_CONTEXT = {
    **TEST_CONTEXT,
    f"hosted-zone:account={TEST_ACCOUNT}:domainName=vals.ai:region={TEST_REGION}": {
        "Id": "/hostedzone/Z0000000000000000000",
        "Name": "vals.ai.",
    },
}

DEV_SHARED_CONTRACT_PARAMETERS = {
    DEV_SHARED_VPC_ID_PARAMETER,
    DEV_SHARED_AVAILABILITY_ZONES_PARAMETER,
    DEV_SHARED_PUBLIC_SUBNET_IDS_PARAMETER,
    DEV_SHARED_CLUSTER_NAME_PARAMETER,
    DEV_SHARED_NAMESPACE_NAME_PARAMETER,
    DEV_SHARED_NAMESPACE_ID_PARAMETER,
    DEV_SHARED_NAMESPACE_ARN_PARAMETER,
    DEV_SHARED_ARTIFACT_BUCKET_PARAMETER,
}
DEV_TRACKER_CONTRACT_PARAMETERS = {
    DEV_TRACKER_SECURITY_GROUP_PARAMETER,
    DEV_TRACKER_ALB_DNS_PARAMETER,
}
EXECUTOR_CONTRACT_PARAMETERS = {executor_release_launch_parameter(DEV)}


def published_parameter_names(template: assertions.Template) -> set[str]:
    resources = template.find_resources("AWS::SSM::Parameter")
    return {
        cast(dict[str, object], cast(dict[str, object], resource)["Properties"])["Name"]
        for resource in resources.values()
        if isinstance(resource, dict)
    }  # pyright: ignore[reportReturnType]


def dev_shared_stack() -> tuple[cdk.App, SharedStack]:
    app = cdk.App(context=TEST_CONTEXT)
    stage = Stage(DEV)
    shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
    return app, shared


def dev_service_templates() -> tuple[assertions.Template, assertions.Template]:
    app, shared = dev_shared_stack()
    stage = Stage(DEV)
    tracker = TrackerStack(
        app,
        stage.stack_id("TrackerStack"),
        stage=stage,
        vpc=shared.vpc,
        cluster=shared.cluster,
        namespace=shared.namespace,
        hosted_zone=shared.hosted_zone,
        bucket_name=shared.bucket_name,
        redis_url=shared.redis_url,
        env=TEST_ENV,
    )
    executor = ExecutorStack(
        app,
        stage.stack_id("WorkerStack"),
        stage=stage,
        vpc=shared.vpc,
        cluster=shared.cluster,
        namespace=shared.namespace,
        redis_url=shared.redis_url,
        bucket_name=shared.bucket_name,
        database=tracker.database,
        db_credentials=tracker.db_credentials,
        tracker_service=tracker.tracker_fargate_service,
        tracker_image=tracker.tracker_image,
        env=TEST_ENV,
    )
    return assertions.Template.from_stack(tracker), assertions.Template.from_stack(executor)


def dev_tracker_template() -> assertions.Template:
    tracker_template, _ = dev_service_templates()
    return tracker_template


def dev_executor_template() -> assertions.Template:
    _, executor_template = dev_service_templates()
    return executor_template


def ssm_parameter_id(template: Mapping[str, object], parameter_name: str) -> str:
    parameters = template.get("Parameters")
    if not isinstance(parameters, dict):
        raise AssertionError("template has no parameters")
    matches = [
        parameter_id
        for parameter_id, definition in cast(dict[str, object], parameters).items()
        if isinstance(definition, dict) and cast(dict[str, object], definition).get("Default") == parameter_name
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one CloudFormation parameter for {parameter_name}, got {matches}")
    return matches[0]


class DevAccountInfrastructureTest(unittest.TestCase):
    def test_dev_buckets_are_owned_and_hardened_by_their_domains(self) -> None:
        _, shared = dev_shared_stack()
        shared_template = assertions.Template.from_stack(shared)
        with mock.patch.dict(os.environ, {"DESCOPE_PROJECT_ID": "dev-project"}, clear=True):
            executor_template = dev_executor_template()

        shared_buckets = shared_template.find_resources("AWS::S3::Bucket")
        self.assertEqual(len(shared_buckets), 1)
        bucket = next(iter(shared_buckets.values()))
        self.assertEqual(bucket["Properties"]["BucketName"], "agentic-harness-dev")

        executor_buckets = executor_template.find_resources("AWS::S3::Bucket")
        self.assertEqual(len(executor_buckets), 1)
        release_bucket = next(iter(executor_buckets.values()))
        self.assertEqual(
            release_bucket["Properties"]["BucketName"],
            f"valkyrie-executor-releases-dev-{TEST_ACCOUNT}",
        )

        for retained_bucket in (bucket, release_bucket):
            self.assertEqual(retained_bucket["DeletionPolicy"], "Retain")
            self.assertEqual(retained_bucket["UpdateReplacePolicy"], "Retain")
            self.assertEqual(
                retained_bucket["Properties"]["PublicAccessBlockConfiguration"],
                {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
            )
            self.assertEqual(retained_bucket["Properties"]["VersioningConfiguration"], {"Status": "Enabled"})
            self.assertEqual(
                retained_bucket["Properties"]["OwnershipControls"],
                {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
            )
            self.assertEqual(
                retained_bucket["Properties"]["BucketEncryption"],
                {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
            )

        conditional_write_statements = [
            statement
            for policy in executor_template.find_resources("AWS::S3::BucketPolicy").values()
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
            if statement.get("Sid") == "RequireConditionalExecutorReleaseWrites"
        ]
        self.assertEqual(len(conditional_write_statements), 1)
        conditional_write = conditional_write_statements[0]
        self.assertEqual(conditional_write["Action"], "s3:PutObject")
        self.assertEqual(conditional_write["Effect"], "Deny")
        self.assertEqual(conditional_write["Principal"], {"AWS": "*"})
        self.assertEqual(conditional_write["Condition"], {"Null": {"s3:if-none-match": "true"}})
        self.assertIn("releases/*", json.dumps(conditional_write["Resource"]))

    def test_release_test_bucket_remains_account_qualified(self) -> None:
        app = cdk.App(context=TEST_CONTEXT)
        stage = Stage(RELEASE_TEST)
        shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
        shared_template = assertions.Template.from_stack(shared)

        buckets = shared_template.find_resources("AWS::S3::Bucket")
        self.assertEqual(len(buckets), 1)
        bucket = next(iter(buckets.values()))
        self.assertEqual(bucket["Properties"]["BucketName"], f"agentic-harness-release-test-{TEST_ACCOUNT}")

    def test_dev_tracker_owns_certificate_in_account_local_hosted_zone(self) -> None:
        dev_auth = {"AUTH_REQUIRED": "false", "DESCOPE_PROJECT_ID": "dev-project"}
        with mock.patch.dict(os.environ, dev_auth, clear=True):
            tracker_template = dev_tracker_template()

        template = cast(Mapping[str, object], tracker_template.to_json())
        hosted_zone_parameter = ssm_parameter_id(template, DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER)
        rendered = json.dumps(template)
        iam_policies = tracker_template.find_resources("AWS::IAM::Policy")
        rendered_policies = json.dumps(iam_policies)
        self.assertNotIn("s3:DeleteObject", rendered_policies)
        self.assertIn("devEvalInfraDescopeManagementKey", rendered)
        self.assertNotIn("/vals/dev/descope/project-id", rendered)
        self.assertNotIn("valkyrie/sentry-dsn", rendered)
        self.assertNotIn("/valkyrie/dev/dns/tracker/certificate-arn", rendered)
        certificates = tracker_template.find_resources("AWS::CertificateManager::Certificate")
        self.assertEqual(len(certificates), 1)
        certificate_id, certificate = next(iter(certificates.items()))
        self.assertEqual(certificate["Properties"]["DomainName"], "benchmark-tracker-dev.vals.ai")
        self.assertEqual(
            certificate["Properties"]["DomainValidationOptions"],
            [
                {
                    "DomainName": "benchmark-tracker-dev.vals.ai",
                    "HostedZoneId": {"Ref": hosted_zone_parameter},
                }
            ],
        )
        tracker_template.has_resource_properties(
            "AWS::ElasticLoadBalancingV2::Listener",
            {"Certificates": [{"CertificateArn": {"Ref": certificate_id}}]},
        )
        tracker_template.has_resource_properties(
            "AWS::Route53::RecordSet",
            {"HostedZoneId": {"Ref": hosted_zone_parameter}},
        )
        tracker_template.has_resource_properties(
            "AWS::RDS::DBInstance",
            {"PubliclyAccessible": False},
        )
        tracker_template.has_resource_properties(
            "AWS::ECS::TaskDefinition",
            {
                "ContainerDefinitions": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Environment": assertions.Match.array_with(
                                    [
                                        {"Name": "AUTH_REQUIRED", "Value": "true"},
                                        {"Name": "DESCOPE_PROJECT_ID", "Value": "dev-project"},
                                    ]
                                ),
                                "Secrets": assertions.Match.array_with(
                                    [assertions.Match.object_like({"Name": "DESCOPE_MANAGEMENT_KEY"})]
                                ),
                            }
                        )
                    ]
                )
            },
        )

    def test_dev_release_control_is_one_sealed_task_with_environment_bound_role(self) -> None:
        with mock.patch.dict(os.environ, {"DESCOPE_PROJECT_ID": "dev-project"}, clear=True):
            template = dev_executor_template()

        template.has_resource_properties(
            "AWS::ECS::TaskDefinition",
            {
                "Family": "ValkyrieExecutorRelease-dev",
                "ContainerDefinitions": [
                    assertions.Match.object_like(
                        {
                            "Name": "ExecutorRelease",
                            "EntryPoint": assertions.Match.array_with(
                                ["/app/.venv/bin/python", "-m", "tracker.executor.release_entrypoint"]
                            ),
                        }
                    )
                ],
            },
        )
        template.has_resource_properties(
            "AWS::SSM::Parameter",
            {"Name": executor_release_launch_parameter(DEV), "Type": "String"},
        )
        roles = template.find_resources("AWS::IAM::Role")
        release_role_id, release_role = next(
            (logical_id, role)
            for logical_id, role in roles.items()
            if role["Properties"].get("RoleName") == "ValkyrieExecutorRelease-dev"
        )
        trust = json.dumps(release_role["Properties"]["AssumeRolePolicyDocument"])
        self.assertIn("token.actions.githubusercontent.com:aud", trust)
        self.assertIn("sts.amazonaws.com", trust)
        self.assertIn("repo:vals-ai/Valkyrie:environment:dev", trust)

        release_policy = next(
            policy
            for policy in template.find_resources("AWS::IAM::Policy").values()
            if {"Ref": release_role_id} in policy["Properties"]["Roles"]
        )
        statements = cast(list[Mapping[str, object]], release_policy["Properties"]["PolicyDocument"]["Statement"])

        def statement_for(action: str) -> Mapping[str, object]:
            return next(
                statement
                for statement in statements
                if action
                in (
                    [statement["Action"]]
                    if isinstance(statement["Action"], str)
                    else cast(list[object], statement["Action"])
                )
            )

        s3_statement = statement_for("s3:PutObject")
        self.assertEqual(s3_statement["Action"], "s3:PutObject")
        self.assertIn("releases/*", json.dumps(s3_statement["Resource"]))
        self.assertNotEqual(s3_statement["Resource"], "*")

        run_statement = statement_for("ecs:RunTask")
        self.assertNotEqual(run_statement["Resource"], "*")
        self.assertIn("ecs:cluster", json.dumps(run_statement["Condition"]))

        describe_statement = statement_for("ecs:DescribeTasks")
        self.assertEqual(describe_statement["Resource"], "*")
        self.assertIn("ecs:cluster", json.dumps(describe_statement["Condition"]))

        pass_role_statement = statement_for("iam:PassRole")
        pass_role_resources = cast(list[object], pass_role_statement["Resource"])
        self.assertEqual(len(pass_role_resources), 2)
        self.assertNotIn("*", pass_role_resources)
        self.assertIn("ecs-tasks.amazonaws.com", json.dumps(pass_role_statement["Condition"]))

        ssm_statement = statement_for("ssm:GetParameter")
        self.assertNotEqual(ssm_statement["Resource"], "*")
        self.assertIn("ExecutorReleaseLaunchConfig", json.dumps(ssm_statement["Resource"]))

        policies = json.dumps(template.find_resources("AWS::IAM::Policy"))
        for forbidden in ("s3:GetObject", "s3:DeleteObject", "s3:ListBucket", "ecs:ExecuteCommand", "ecs:StopTask"):
            self.assertNotIn(forbidden, json.dumps(release_policy))
        self.assertNotIn("s3:DeleteObject", policies)

    def test_dev_tracker_requires_descope_project(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "Development deployments require DESCOPE_PROJECT_ID"):
                dev_tracker_template()

    def test_dev_stacks_publish_the_shared_resource_contract(self) -> None:
        _, shared = dev_shared_stack()
        shared_template = assertions.Template.from_stack(shared)
        self.assertEqual(published_parameter_names(shared_template), DEV_SHARED_CONTRACT_PARAMETERS)

        with mock.patch.dict(os.environ, {"DESCOPE_PROJECT_ID": "dev-project"}, clear=True):
            tracker_template, executor_template = dev_service_templates()
        self.assertEqual(published_parameter_names(tracker_template), DEV_TRACKER_CONTRACT_PARAMETERS)
        self.assertEqual(published_parameter_names(executor_template), EXECUTOR_CONTRACT_PARAMETERS)

    def test_prod_shared_stack_publishes_no_contract_parameters(self) -> None:
        app = cdk.App(context=PROD_CONTEXT)
        stage = Stage(PROD)
        shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
        template = assertions.Template.from_stack(shared)
        self.assertFalse(template.find_resources("AWS::SSM::Parameter"))


if __name__ == "__main__":
    unittest.main()
