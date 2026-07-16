"""Development-account infrastructure contract tests."""

from __future__ import annotations

import os
import unittest
from typing import cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import assertions

from shared import SharedStack
from stage import DEV, PROD, Stage
from tracker_stack import TrackerStack

TEST_ACCOUNT = "123456789012"
TEST_REGION = "us-east-1"
TEST_ENV = cdk.Environment(account=TEST_ACCOUNT, region=TEST_REGION)
AVAILABILITY_ZONE_CONTEXT = {
    f"availability-zones:account={TEST_ACCOUNT}:region={TEST_REGION}": [
        f"{TEST_REGION}a",
        f"{TEST_REGION}b",
    ]
}
PRODUCTION_CONTEXT = {
    **AVAILABILITY_ZONE_CONTEXT,
    f"hosted-zone:account={TEST_ACCOUNT}:domainName=vals.ai:region={TEST_REGION}": {
        "Id": "/hostedzone/ZTESTVALKYRIE",
        "Name": "vals.ai.",
    },
}


def _shared_template(stage_name: str) -> assertions.Template:
    context = PRODUCTION_CONTEXT if stage_name == PROD else AVAILABILITY_ZONE_CONTEXT
    app = cdk.App(context=context)
    stage = Stage(stage_name)
    stack = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
    return assertions.Template.from_stack(stack)


def _dev_tracker_template() -> assertions.Template:
    app = cdk.App(context=AVAILABILITY_ZONE_CONTEXT)
    stage = Stage(DEV)
    shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
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


def _parameter_defaults(template: assertions.Template) -> set[str]:
    document = cast(dict[str, object], template.to_json())
    parameters = document.get("Parameters")
    if not isinstance(parameters, dict):
        return set()
    typed_parameters = cast(dict[str, dict[str, object]], parameters)
    defaults: set[str] = set()
    for parameter in typed_parameters.values():
        default = parameter.get("Default")
        if isinstance(default, str):
            defaults.add(default)
    return defaults


class DevSharedInfrastructureTest(unittest.TestCase):
    def test_dev_bucket_is_account_qualified_and_hardened(self) -> None:
        template = _shared_template(DEV)

        template.has_resource(
            "AWS::S3::Bucket",
            {
                "DeletionPolicy": "Retain",
                "Properties": {
                    "BucketEncryption": {
                        "ServerSideEncryptionConfiguration": [
                            {
                                "ServerSideEncryptionByDefault": {
                                    "SSEAlgorithm": "AES256",
                                }
                            }
                        ]
                    },
                    "BucketName": f"agentic-harness-dev-{TEST_ACCOUNT}",
                    "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "BlockPublicPolicy": True,
                        "IgnorePublicAcls": True,
                        "RestrictPublicBuckets": True,
                    },
                    "VersioningConfiguration": {"Status": "Enabled"},
                },
                "UpdateReplacePolicy": "Retain",
            },
        )
        template.has_resource_properties(
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

    def test_dev_shared_resource_contract_is_complete(self) -> None:
        template = _shared_template(DEV)
        parameters = template.find_resources("AWS::SSM::Parameter")

        self.assertEqual(
            {resource["Properties"]["Name"] for resource in parameters.values()},
            {
                "/valkyrie/dev/shared/vpc-id",
                "/valkyrie/dev/shared/availability-zones",
                "/valkyrie/dev/shared/public-subnet-ids",
                "/valkyrie/dev/shared/cluster-name",
                "/valkyrie/dev/shared/cloud-map-namespace-name",
                "/valkyrie/dev/shared/cloud-map-namespace-id",
                "/valkyrie/dev/shared/cloud-map-namespace-arn",
                "/valkyrie/dev/shared/artifact-bucket-name",
            },
        )
        template.resource_count_is("AWS::EC2::NatGateway", 0)

    def test_production_bucket_and_ssm_behavior_is_unchanged(self) -> None:
        template = _shared_template(PROD)

        template.has_resource_properties("AWS::S3::Bucket", {"BucketName": "agentic-harness"})
        template.resource_count_is("AWS::SSM::Parameter", 0)


class DevTrackerInfrastructureTest(unittest.TestCase):
    def test_dev_imports_child_zone_and_certificate_from_ssm(self) -> None:
        template = _dev_tracker_template()

        self.assertTrue(
            {
                "/valkyrie/dev/dns/tracker/hosted-zone-id",
                "/valkyrie/dev/dns/tracker/certificate-arn",
            }.issubset(_parameter_defaults(template))
        )
        template.resource_count_is("AWS::CertificateManager::Certificate", 0)
        listeners = template.find_resources("AWS::ElasticLoadBalancingV2::Listener")
        self.assertTrue(any("Certificates" in listener["Properties"] for listener in listeners.values()))

    def test_dev_authentication_does_not_read_deployer_configuration(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "AUTH_REQUIRED": "false",
                "DESCOPE_PROJECT_ID": "deployer-project",
            },
            clear=True,
        ):
            template = _dev_tracker_template()

        self.assertIn("/vals/dev/descope/project-id", _parameter_defaults(template))
        template.has_resource_properties(
            "AWS::ECS::TaskDefinition",
            {
                "ContainerDefinitions": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Environment": assertions.Match.array_with(
                                    [
                                        {"Name": "AUTH_REQUIRED", "Value": "true"},
                                        assertions.Match.object_like({"Name": "DESCOPE_PROJECT_ID"}),
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

    def test_dev_database_is_not_public(self) -> None:
        template = _dev_tracker_template()
        template.has_resource_properties(
            "AWS::RDS::DBInstance",
            {"PubliclyAccessible": False},
        )

    def test_dev_tracker_resource_contract_is_complete(self) -> None:
        template = _dev_tracker_template()
        parameters = template.find_resources("AWS::SSM::Parameter")

        self.assertEqual(
            {resource["Properties"]["Name"] for resource in parameters.values()},
            {
                "/valkyrie/dev/tracker/security-group-id",
                "/valkyrie/dev/tracker/alb-dns-name",
            },
        )


if __name__ == "__main__":
    unittest.main()
