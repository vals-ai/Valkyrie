"""Tests for the environment-bound coordinated agent publisher roles."""

from __future__ import annotations

import json
import unittest
from collections.abc import Mapping
from typing import cast

import aws_cdk as cdk
from aws_cdk import assertions

from constants import (
    AGENT_REGISTRY_REPOSITORY,
    AGENT_REGISTRY_REPOSITORY_ID,
    COORDINATED_AGENT_ALIAS_KEY,
    COORDINATED_AGENT_PUBLISH_WORKFLOW_REF,
    VALS_AI_ORGANIZATION_ID,
    coordinated_agent_publisher_role_name,
    coordinated_agent_release_environment,
)
from shared import SharedStack
from stage import DEV, PROD, RELEASE_TEST, Stage

TEST_ACCOUNT = "123456789012"
TEST_REGION = "us-east-1"
TEST_ENV = cdk.Environment(account=TEST_ACCOUNT, region=TEST_REGION)
TEST_CONTEXT = {
    f"availability-zones:account={TEST_ACCOUNT}:region={TEST_REGION}": [
        f"{TEST_REGION}a",
        f"{TEST_REGION}b",
    ],
    f"hosted-zone:account={TEST_ACCOUNT}:domainName=vals.ai:region={TEST_REGION}": {
        "Id": "/hostedzone/Z0000000000000000000",
        "Name": "vals.ai.",
    },
}


def shared_template(stage_name: str) -> assertions.Template:
    app = cdk.App(context=TEST_CONTEXT)
    stage = Stage(stage_name)
    stack = SharedStack(
        app,
        stage.stack_id("SharedStack"),
        stage=stage,
        env=TEST_ENV,
    )
    return assertions.Template.from_stack(stack)


class CoordinatedAgentPublisherRoleTest(unittest.TestCase):
    def test_prod_and_dev_roles_are_exactly_bound_and_least_privileged(self) -> None:
        for stage_name in (PROD, DEV):
            with self.subTest(stage=stage_name):
                template = shared_template(stage_name)
                roles = template.find_resources("AWS::IAM::Role")
                role_id, role = next(
                    (logical_id, resource)
                    for logical_id, resource in roles.items()
                    if resource["Properties"].get("RoleName") == coordinated_agent_publisher_role_name(stage_name)
                )

                environment = coordinated_agent_release_environment(stage_name)
                expected_conditions = {
                    "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                    "token.actions.githubusercontent.com:environment": environment,
                    "token.actions.githubusercontent.com:job_workflow_ref": (COORDINATED_AGENT_PUBLISH_WORKFLOW_REF),
                    "token.actions.githubusercontent.com:ref": "refs/heads/main",
                    "token.actions.githubusercontent.com:repository": AGENT_REGISTRY_REPOSITORY,
                    "token.actions.githubusercontent.com:repository_id": (AGENT_REGISTRY_REPOSITORY_ID),
                    "token.actions.githubusercontent.com:repository_owner_id": (VALS_AI_ORGANIZATION_ID),
                    "token.actions.githubusercontent.com:sub": (
                        f"repo:{AGENT_REGISTRY_REPOSITORY}:environment:{environment}"
                    ),
                }
                statements = role["Properties"]["AssumeRolePolicyDocument"]["Statement"]
                self.assertEqual(len(statements), 1)
                self.assertEqual(
                    statements[0]["Condition"],
                    {"StringEquals": expected_conditions},
                )

                policies = template.find_resources("AWS::IAM::Policy")
                policy = next(
                    resource for resource in policies.values() if {"Ref": role_id} in resource["Properties"]["Roles"]
                )
                policy_statements = policy["Properties"]["PolicyDocument"]["Statement"]
                self.assertEqual(len(policy_statements), 1)
                statement = policy_statements[0]
                actions = cast(str | list[str], statement["Action"])
                self.assertEqual(
                    set(actions if isinstance(actions, list) else [actions]),
                    {"s3:AbortMultipartUpload", "s3:PutObject"},
                )
                resource = json.dumps(statement["Resource"])
                self.assertIn(f"/{COORDINATED_AGENT_ALIAS_KEY}", resource)
                self.assertNotIn("*", resource)

                outputs = cast(
                    Mapping[str, Mapping[str, object]],
                    template.to_json()["Outputs"],
                )
                self.assertEqual(
                    outputs["CoordinatedAgentPublisherRoleArn"]["Value"],
                    {"Fn::GetAtt": [role_id, "Arn"]},
                )

    def test_release_test_has_no_coordinated_publisher_role(self) -> None:
        template = shared_template(RELEASE_TEST)
        rendered = json.dumps(template.to_json())

        self.assertNotIn("CoordinatedAgentPublisherRole", rendered)
        self.assertNotIn("CoordinatedAgentPublisherRoleArn", rendered)
        self.assertNotIn(COORDINATED_AGENT_ALIAS_KEY, rendered)

    def test_alias_deny_is_deliberately_deferred(self) -> None:
        for stage_name in (PROD, DEV):
            with self.subTest(stage=stage_name):
                rendered = json.dumps(shared_template(stage_name).to_json())
                self.assertNotIn("DenyUncoordinatedAgentAliasWrites", rendered)
                self.assertNotIn("aws:PrincipalArn", rendered)


if __name__ == "__main__":
    unittest.main()
