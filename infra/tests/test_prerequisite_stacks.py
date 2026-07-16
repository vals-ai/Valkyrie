import unittest

import aws_cdk as cdk
from aws_cdk import assertions

from deployment_access_stack import DeploymentAccessStack
from dns_stack import DnsStack

TEST_ACCOUNT = "123456789012"
TEST_REGION = "us-east-1"
TEST_ENV = cdk.Environment(account=TEST_ACCOUNT, region=TEST_REGION)


def _deployment_access_template() -> assertions.Template:
    app = cdk.App()
    stack = DeploymentAccessStack(app, "DeploymentAccessStack", env=TEST_ENV)
    return assertions.Template.from_stack(stack)


def _dns_template() -> assertions.Template:
    app = cdk.App()
    stack = DnsStack(app, "DnsStack", env=TEST_ENV)
    return assertions.Template.from_stack(stack)


class DeploymentAccessStackTest(unittest.TestCase):
    def test_github_identity_is_bound_to_the_dev_environment_and_branch(self) -> None:
        template = _deployment_access_template()
        provider_logical_id = next(iter(template.find_resources("AWS::IAM::OIDCProvider")))

        template.has_resource_properties(
            "AWS::IAM::OIDCProvider",
            {
                "Url": "https://token.actions.githubusercontent.com",
                "ClientIdList": ["sts.amazonaws.com"],
            },
        )
        roles = template.find_resources("AWS::IAM::Role")
        self.assertEqual(len(roles), 1)
        self.assertEqual(
            next(iter(roles.values()))["Properties"]["AssumeRolePolicyDocument"],
            {
                "Statement": [
                    {
                        "Action": "sts:AssumeRoleWithWebIdentity",
                        "Condition": {
                            "StringEquals": {
                                "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                                "token.actions.githubusercontent.com:sub": ("repo:vals-ai/Valkyrie:environment:dev"),
                                "token.actions.githubusercontent.com:repository_id": "1084629789",
                                "token.actions.githubusercontent.com:repository_owner_id": "129814943",
                                "token.actions.githubusercontent.com:ref": "refs/heads/dev",
                                "token.actions.githubusercontent.com:environment": "dev",
                            }
                        },
                        "Effect": "Allow",
                        "Principal": {"Federated": {"Fn::GetAtt": [provider_logical_id, "Arn"]}},
                    }
                ],
                "Version": "2012-10-17",
            },
        )

    def test_deployment_role_can_only_assume_default_cdk_bootstrap_roles(self) -> None:
        template = _deployment_access_template()
        expected_role_arns = [
            f"arn:aws:iam::{TEST_ACCOUNT}:role/cdk-hnb659fds-{role_type}-role-{TEST_ACCOUNT}-{TEST_REGION}"
            for role_type in ("lookup", "deploy", "file-publishing", "image-publishing")
        ]

        roles = template.find_resources("AWS::IAM::Role")
        self.assertEqual(len(roles), 1)
        role_properties = next(iter(roles.values()))["Properties"]
        self.assertEqual(role_properties["RoleName"], "ValkyrieDevGitHubDeploymentRole")
        self.assertEqual(
            role_properties["Policies"],
            [
                {
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Action": "sts:AssumeRole",
                                "Effect": "Allow",
                                "Resource": expected_role_arns,
                            }
                        ],
                        "Version": "2012-10-17",
                    },
                    "PolicyName": "AssumeCdkBootstrapRoles",
                }
            ],
        )
        self.assertNotIn("ManagedPolicyArns", role_properties)

    def test_access_resource_arns_are_published_to_ssm(self) -> None:
        template = _deployment_access_template()
        provider_logical_id = next(iter(template.find_resources("AWS::IAM::OIDCProvider")))
        role_logical_id = next(iter(template.find_resources("AWS::IAM::Role")))

        parameters = template.find_resources("AWS::SSM::Parameter")
        parameter_values = {
            resource["Properties"]["Name"]: resource["Properties"]["Value"] for resource in parameters.values()
        }
        self.assertEqual(
            parameter_values,
            {
                "/vals/dev/github/oidc-provider-arn": {
                    "Fn::GetAtt": [provider_logical_id, "Arn"],
                },
                "/vals/dev/github/valkyrie-role-arn": {
                    "Fn::GetAtt": [role_logical_id, "Arn"],
                },
            },
        )


class DnsStackTest(unittest.TestCase):
    def test_tracker_child_zone_is_retained(self) -> None:
        template = _dns_template()

        template.has_resource(
            "AWS::Route53::HostedZone",
            {
                "DeletionPolicy": "Retain",
                "Properties": {"Name": "benchmark-tracker-dev.vals.ai."},
                "UpdateReplacePolicy": "Retain",
            },
        )

    def test_tracker_zone_id_is_published_to_ssm(self) -> None:
        template = _dns_template()
        hosted_zone_logical_id = next(iter(template.find_resources("AWS::Route53::HostedZone")))

        template.has_resource_properties(
            "AWS::SSM::Parameter",
            {
                "Name": "/valkyrie/dev/dns/tracker/hosted-zone-id",
                "Type": "String",
                "Value": {"Ref": hosted_zone_logical_id},
            },
        )


if __name__ == "__main__":
    unittest.main()
