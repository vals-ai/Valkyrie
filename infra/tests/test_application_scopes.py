"""Tests for scope-aware CDK application construction."""

import unittest

import aws_cdk as cdk

from app import build_stacks
from stage import DEV, PROD, Stage

TEST_ENV = cdk.Environment(account="123456789012", region="us-east-1")


class ApplicationScopeTest(unittest.TestCase):
    def test_prerequisite_scopes_construct_only_the_requested_stack(self) -> None:
        for scope, expected_stack_name in (
            ("deployment-access", "Valk-Dev-DeploymentAccessStack"),
            ("dns-zone", "Valk-Dev-DnsZoneStack"),
        ):
            with self.subTest(scope=scope):
                stacks = build_stacks(cdk.App(), Stage(DEV), scope, TEST_ENV)
                self.assertEqual([stack.stack_name for stack in stacks], [expected_stack_name])

    def test_production_rejects_dev_prerequisite_scopes(self) -> None:
        for scope in ("deployment-access", "dns-zone"):
            with self.subTest(scope=scope), self.assertRaisesRegex(ValueError, "only available for the dev stage"):
                build_stacks(cdk.App(), Stage(PROD), scope, TEST_ENV)

    def test_unknown_scope_is_rejected_before_stack_construction(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown deployment scope"):
            build_stacks(cdk.App(), Stage(DEV), "everything", TEST_ENV)


if __name__ == "__main__":
    unittest.main()
