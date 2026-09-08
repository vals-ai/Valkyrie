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
BENCH_ACCOUNT_ID = "210987654321"
PRODUCTION_ACCOUNT_ID = "321098765432"


def environment_for(stage: str) -> dict[str, str]:
    account_id = (
        DEV_ACCOUNT_ID
        if stage in ("dev", "release-test")
        else PRODUCTION_ACCOUNT_ID
        if stage == "prod"
        else BENCH_ACCOUNT_ID
    )
    return {
        "STAGE": stage,
        "AWS_REGION": DEPLOYMENT_REGION,
        "DEV_ACCOUNT_ID": DEV_ACCOUNT_ID,
        "BENCH_ACCOUNT_ID": BENCH_ACCOUNT_ID,
        "PRODUCTION_ACCOUNT_ID": PRODUCTION_ACCOUNT_ID,
        "CDK_DEFAULT_ACCOUNT": account_id,
        "CDK_DEFAULT_REGION": DEPLOYMENT_REGION,
    }


class DeploymentTargetTest(unittest.TestCase):
    def test_accepts_exact_deployment_targets(self) -> None:
        for stage, account_id in (
            ("dev", DEV_ACCOUNT_ID),
            ("bench", BENCH_ACCOUNT_ID),
            ("prod", PRODUCTION_ACCOUNT_ID),
        ):
            with self.subTest(stage=stage):
                self.assertEqual(
                    target_from_environment(environment_for(stage)),
                    DeploymentTarget(stage=stage, account_id=account_id, region=DEPLOYMENT_REGION),
                )

    def test_accepts_release_test_in_dev_account(self) -> None:
        self.assertEqual(
            target_from_environment(environment_for("release-test")),
            DeploymentTarget(stage="release-test", account_id=DEV_ACCOUNT_ID, region=DEPLOYMENT_REGION),
        )

    def test_accepts_release_test_in_bench_account(self) -> None:
        environment = environment_for("release-test")
        environment["DEV_ACCOUNT_ID"] = BENCH_ACCOUNT_ID
        environment["CDK_DEFAULT_ACCOUNT"] = BENCH_ACCOUNT_ID
        self.assertEqual(
            target_from_environment(environment),
            DeploymentTarget(stage="release-test", account_id=BENCH_ACCOUNT_ID, region=DEPLOYMENT_REGION),
        )

    def test_accepts_release_test_in_production_account(self) -> None:
        environment = environment_for("release-test")
        environment["DEV_ACCOUNT_ID"] = PRODUCTION_ACCOUNT_ID
        environment["CDK_DEFAULT_ACCOUNT"] = PRODUCTION_ACCOUNT_ID
        self.assertEqual(
            target_from_environment(environment),
            DeploymentTarget(stage="release-test", account_id=PRODUCTION_ACCOUNT_ID, region=DEPLOYMENT_REGION),
        )

    def test_dev_and_release_test_do_not_require_production_accounts(self) -> None:
        for stage, unrelated_variables in (
            ("dev", ("BENCH_ACCOUNT_ID", "PRODUCTION_ACCOUNT_ID")),
            ("release-test", ("BENCH_ACCOUNT_ID", "PRODUCTION_ACCOUNT_ID")),
        ):
            with self.subTest(stage=stage):
                environment = environment_for(stage)
                for variable in unrelated_variables:
                    del environment[variable]
                target_from_environment(environment)

    def test_bench_and_prod_require_each_other_account(self) -> None:
        for stage, required_variable in (
            ("bench", "PRODUCTION_ACCOUNT_ID"),
            ("prod", "BENCH_ACCOUNT_ID"),
        ):
            with self.subTest(stage=stage):
                environment = environment_for(stage)
                del environment[required_variable]
                with self.assertRaisesRegex(DeploymentTargetError, f"{required_variable} is required"):
                    target_from_environment(environment)

    def test_rejects_selected_account_that_matches_supplied_account(self) -> None:
        for stage, selected_variable, other_variable in (
            ("dev", "DEV_ACCOUNT_ID", "BENCH_ACCOUNT_ID"),
            ("bench", "BENCH_ACCOUNT_ID", "PRODUCTION_ACCOUNT_ID"),
            ("prod", "PRODUCTION_ACCOUNT_ID", "BENCH_ACCOUNT_ID"),
        ):
            with self.subTest(stage=stage):
                environment = environment_for(stage)
                environment[selected_variable] = environment[other_variable]
                environment["CDK_DEFAULT_ACCOUNT"] = environment[other_variable]
                with self.assertRaisesRegex(DeploymentTargetError, "must not match"):
                    target_from_environment(environment)

    def test_rejects_target_mismatches(self) -> None:
        cases = (
            ("AWS_REGION", "us-west-2", "AWS_REGION must be us-east-1"),
            ("CDK_DEFAULT_ACCOUNT", "999999999999", "CDK_DEFAULT_ACCOUNT must be"),
            ("CDK_DEFAULT_REGION", "us-west-2", "CDK_DEFAULT_REGION must match"),
        )
        for variable, value, message in cases:
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
