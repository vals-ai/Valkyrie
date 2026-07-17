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
    DEV_TRACKER_CERTIFICATE_ARN_PARAMETER,
    DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER,
    DEV_TRACKER_SECURITY_GROUP_PARAMETER,
)
from shared import SharedStack
from stage import DEV, PROD, Stage
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


def dev_tracker_template() -> assertions.Template:
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
        bucket=shared.bucket,
        redis_url=shared.redis_url,
        env=TEST_ENV,
    )
    return assertions.Template.from_stack(tracker)


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
    def test_dev_bucket_is_account_qualified_and_hardened(self) -> None:
        _, shared = dev_shared_stack()
        shared_template = assertions.Template.from_stack(shared)

        buckets = shared_template.find_resources("AWS::S3::Bucket")
        self.assertEqual(len(buckets), 1)
        bucket = next(iter(buckets.values()))
        self.assertEqual(bucket["DeletionPolicy"], "Retain")
        self.assertEqual(bucket["UpdateReplacePolicy"], "Retain")
        self.assertEqual(bucket["Properties"]["BucketName"], f"agentic-harness-dev-{TEST_ACCOUNT}")
        self.assertEqual(
            bucket["Properties"]["PublicAccessBlockConfiguration"],
            {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        )
        self.assertEqual(bucket["Properties"]["VersioningConfiguration"], {"Status": "Enabled"})
        self.assertEqual(
            bucket["Properties"]["OwnershipControls"],
            {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        self.assertEqual(
            bucket["Properties"]["BucketEncryption"],
            {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
        )
        shared_template.has_resource_properties(
            "AWS::S3::BucketPolicy",
            {
                "PolicyDocument": {
                    "Statement": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                                    "Effect": "Deny",
                                }
                            )
                        ]
                    )
                }
            },
        )

    def test_dev_tracker_imports_account_local_dns_and_auth(self) -> None:
        dev_auth = {"AUTH_REQUIRED": "false", "DESCOPE_PROJECT_ID": "dev-project"}
        with mock.patch.dict(os.environ, dev_auth, clear=True):
            tracker_template = dev_tracker_template()

        template = cast(Mapping[str, object], tracker_template.to_json())
        hosted_zone_parameter = ssm_parameter_id(template, DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER)
        certificate_parameter = ssm_parameter_id(template, DEV_TRACKER_CERTIFICATE_ARN_PARAMETER)
        rendered = json.dumps(template)
        self.assertIn("devEvalInfraDescopeManagementKey", rendered)
        self.assertNotIn("/vals/dev/descope/project-id", rendered)
        self.assertNotIn("valkyrie/sentry-dsn", rendered)
        self.assertFalse(tracker_template.find_resources("AWS::CertificateManager::Certificate"))
        tracker_template.has_resource_properties(
            "AWS::ElasticLoadBalancingV2::Listener",
            {"Certificates": [{"CertificateArn": {"Ref": certificate_parameter}}]},
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

    def test_dev_tracker_requires_descope_project(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "Development deployments require DESCOPE_PROJECT_ID"):
                dev_tracker_template()

    def test_dev_stacks_publish_the_shared_resource_contract(self) -> None:
        _, shared = dev_shared_stack()
        shared_template = assertions.Template.from_stack(shared)
        self.assertEqual(published_parameter_names(shared_template), DEV_SHARED_CONTRACT_PARAMETERS)

        with mock.patch.dict(os.environ, {"DESCOPE_PROJECT_ID": "dev-project"}, clear=True):
            tracker_template = dev_tracker_template()
        self.assertEqual(published_parameter_names(tracker_template), DEV_TRACKER_CONTRACT_PARAMETERS)

    def test_prod_shared_stack_publishes_no_contract_parameters(self) -> None:
        app = cdk.App(context=PROD_CONTEXT)
        stage = Stage(PROD)
        shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
        template = assertions.Template.from_stack(shared)
        self.assertFalse(template.find_resources("AWS::SSM::Parameter"))


if __name__ == "__main__":
    unittest.main()
