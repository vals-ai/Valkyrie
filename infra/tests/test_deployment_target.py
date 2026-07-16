"""Tests for deployment target validation."""

from __future__ import annotations

import unittest

from deployment_target import (
    DEPLOYMENT_REGION,
    PRODUCTION_ACCOUNT_ID,
    DeploymentTarget,
    DeploymentTargetError,
    target_from_environment,
    validate_caller_identity,
)

DEV_ACCOUNT_ID = "123456789012"


def environment_for(stage: str) -> dict[str, str]:
    account_id = DEV_ACCOUNT_ID if stage == "dev" else PRODUCTION_ACCOUNT_ID
    return {
        "STAGE": stage,
        "AWS_REGION": DEPLOYMENT_REGION,
        "DEV_ACCOUNT_ID": DEV_ACCOUNT_ID,
        "CDK_DEFAULT_ACCOUNT": account_id,
        "CDK_DEFAULT_REGION": DEPLOYMENT_REGION,
    }


class DeploymentTargetTest(unittest.TestCase):
    def test_validates_explicit_dev_and_prod_targets(self) -> None:
        self.assertEqual(
            target_from_environment(environment_for("dev")),
            DeploymentTarget(stage="dev", account_id=DEV_ACCOUNT_ID, region=DEPLOYMENT_REGION),
        )
        self.assertEqual(
            target_from_environment(environment_for("prod")),
            DeploymentTarget(stage="prod", account_id=PRODUCTION_ACCOUNT_ID, region=DEPLOYMENT_REGION),
        )

    def test_requires_target_selection(self) -> None:
        required_variables = (
            ("STAGE", "STAGE is required."),
            ("AWS_REGION", "AWS_REGION is required."),
            ("DEV_ACCOUNT_ID", "DEV_ACCOUNT_ID is required."),
            ("CDK_DEFAULT_ACCOUNT", "CDK_DEFAULT_ACCOUNT is required."),
            ("CDK_DEFAULT_REGION", "CDK_DEFAULT_REGION is required."),
        )
        for variable, message in required_variables:
            with self.subTest(variable=variable):
                environment = environment_for("dev")
                del environment[variable]
                with self.assertRaisesRegex(DeploymentTargetError, message):
                    target_from_environment(environment)

    def test_rejects_non_deployment_region(self) -> None:
        environment = environment_for("dev")
        environment["AWS_REGION"] = "us-west-2"

        with self.assertRaisesRegex(DeploymentTargetError, "AWS_REGION must be us-east-1"):
            target_from_environment(environment)

    def test_rejects_production_account_as_dev(self) -> None:
        environment = environment_for("dev")
        environment["DEV_ACCOUNT_ID"] = PRODUCTION_ACCOUNT_ID
        environment["CDK_DEFAULT_ACCOUNT"] = PRODUCTION_ACCOUNT_ID

        with self.assertRaisesRegex(DeploymentTargetError, "must not be the production"):
            target_from_environment(environment)

    def test_rejects_cdk_environment_mismatch(self) -> None:
        mismatch_cases = (
            ("CDK_DEFAULT_ACCOUNT", "999999999999", "CDK_DEFAULT_ACCOUNT must be"),
            ("CDK_DEFAULT_REGION", "us-west-2", "CDK_DEFAULT_REGION must match"),
        )
        for variable, value, message in mismatch_cases:
            with self.subTest(variable=variable):
                environment = environment_for("dev")
                environment[variable] = value
                with self.assertRaisesRegex(DeploymentTargetError, message):
                    target_from_environment(environment)

    def test_rejects_sts_account_mismatch(self) -> None:
        target = target_from_environment(environment_for("dev"))

        with self.assertRaisesRegex(DeploymentTargetError, "credentials belong to account"):
            validate_caller_identity(target, {"Account": "999999999999"})


if __name__ == "__main__":
    unittest.main()
