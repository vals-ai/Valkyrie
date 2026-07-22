"""Tests for deployment target validation."""

import unittest

from deployment_target import (
    DEPLOYMENT_REGION,
    DeploymentTarget,
    DeploymentTargetError,
    target_from_environment,
    validate_caller_identity,
)

DEV_ACCOUNT_ID = "123456789012"
PRODUCTION_ACCOUNT_ID = "210987654321"


def environment_for(stage: str) -> dict[str, str]:
    account_id = DEV_ACCOUNT_ID if stage == "dev" else PRODUCTION_ACCOUNT_ID
    return {
        "STAGE": stage,
        "AWS_REGION": DEPLOYMENT_REGION,
        "DEV_ACCOUNT_ID": DEV_ACCOUNT_ID,
        "PRODUCTION_ACCOUNT_ID": PRODUCTION_ACCOUNT_ID,
        "CDK_DEFAULT_ACCOUNT": account_id,
        "CDK_DEFAULT_REGION": DEPLOYMENT_REGION,
    }


class DeploymentTargetTest(unittest.TestCase):
    def test_accepts_exact_dev_and_prod_targets(self) -> None:
        for stage, account_id in (("dev", DEV_ACCOUNT_ID), ("prod", PRODUCTION_ACCOUNT_ID)):
            with self.subTest(stage=stage):
                self.assertEqual(
                    target_from_environment(environment_for(stage)),
                    DeploymentTarget(stage=stage, account_id=account_id, region=DEPLOYMENT_REGION),
                )

    def test_rejects_target_mismatches(self) -> None:
        cases = (
            ("AWS_REGION", "us-west-2", "AWS_REGION must be us-east-1"),
            ("DEV_ACCOUNT_ID", PRODUCTION_ACCOUNT_ID, "must not be the production"),
            ("CDK_DEFAULT_ACCOUNT", "999999999999", "CDK_DEFAULT_ACCOUNT must be"),
            ("CDK_DEFAULT_REGION", "us-west-2", "CDK_DEFAULT_REGION must match"),
        )
        for variable, value, message in cases:
            with self.subTest(variable=variable):
                environment = environment_for("dev")
                environment[variable] = value
                if variable == "DEV_ACCOUNT_ID":
                    environment["CDK_DEFAULT_ACCOUNT"] = value
                with self.assertRaisesRegex(DeploymentTargetError, message):
                    target_from_environment(environment)

    def test_rejects_sts_account_mismatch(self) -> None:
        target = target_from_environment(environment_for("dev"))

        with self.assertRaisesRegex(DeploymentTargetError, "credentials belong to account"):
            validate_caller_identity(target, {"Account": "999999999999"})


if __name__ == "__main__":
    unittest.main()
