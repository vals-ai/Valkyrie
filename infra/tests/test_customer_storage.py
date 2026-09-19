"""Offline production backup and storage authority checks."""

import json
import os
import re
import unittest
from typing import Any, cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import assertions

from shared import SharedStack
from stage import Stage

ACCOUNT = "123456789012"
REGION = "us-east-1"
ORG = "00000000-0000-0000-0000-000000000001"
LEGACY_BUCKET = "legacy-valsmith-shared"
INPUTS = {
    "VALSMITH_CUSTOMER_STORAGE_ENABLED": "true",
    "PRODUCTION_ACCOUNT_ID": ACCOUNT,
    "VALSMITH_STORAGE_ORG_ID": ORG,
    "VALSMITH_STORAGE_OIDC_PROVIDER_ARN": f"arn:aws:iam::{ACCOUNT}:oidc-provider/oidc.vercel.com/vals-ai",
    "VALSMITH_STORAGE_OIDC_AUDIENCE": "https://vercel.com/vals-ai",
    "VALSMITH_STORAGE_OIDC_SUBJECT": "owner:vals-ai:project:valsmith:environment:production",
    "VALSMITH_DATASET_VIEW_LAMBDA_NAME": "valsmith-dataset-view-prod",
    "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN": f"arn:aws:iam::{ACCOUNT}:role/StorageOperator",
    "VALSMITH_LEGACY_STORAGE_BUCKET": LEGACY_BUCKET,
    "VALSMITH_LEGACY_STORAGE_ACCOUNT_ID": "210987654321",
}


def synth(stage: str = "prod", environment: dict[str, str] | None = None) -> assertions.Template:
    context: dict[str, Any] = {
        f"availability-zones:account={ACCOUNT}:region={REGION}": [f"{REGION}a", f"{REGION}b"],
        f"hosted-zone:account={ACCOUNT}:domainName=vals.ai:region={REGION}": {
            "Id": "/hostedzone/Z0000000000000000000",
            "Name": "vals.ai.",
        },
    }
    with mock.patch.dict(os.environ, INPUTS if environment is None else environment, clear=True):
        app = cdk.App(context=context)
        stack = SharedStack(app, "Shared", stage=Stage(stage), env=cdk.Environment(account=ACCOUNT, region=REGION))
        return assertions.Template.from_stack(stack)


def role(template: assertions.Template, name: str) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    matches = [
        (key, value)
        for key, value in template.find_resources("AWS::IAM::Role").items()
        if value["Properties"].get("RoleName") == name
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one role {name}, got {len(matches)}")
    identifier, resource = matches[0]
    statements = [
        statement
        for policy in template.find_resources("AWS::IAM::Policy").values()
        if {"Ref": identifier} in policy["Properties"].get("Roles", [])
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]
    ]
    return identifier, resource["Properties"], statements


def actions(statement: dict[str, Any]) -> set[str]:
    value = statement["Action"]
    return {value} if isinstance(value, str) else set(value)


def rule(template: assertions.Template, plan_name: str) -> dict[str, Any]:
    plan = next(
        item
        for item in template.find_resources("AWS::Backup::BackupPlan").values()
        if item["Properties"]["BackupPlan"]["BackupPlanName"] == plan_name
    )
    return cast(dict[str, Any], plan["Properties"]["BackupPlan"]["BackupPlanRule"][0])


def legacy_scopes(resource: object) -> set[str]:
    """Return every key scope a resource grants inside the compatibility bucket."""
    parts = re.findall(rf'"[^"]*{LEGACY_BUCKET}([^"]*)"', json.dumps(resource))
    return set(parts)


class CustomerStorageTest(unittest.TestCase):
    def test_provisioner_can_head_and_check_empty_owner_buckets(self) -> None:
        _, _, statements = role(synth(), "ValSmithStorage-prod")
        unrestricted_owner_lists = [
            item
            for item in statements
            if item["Effect"] == "Allow"
            and "s3:ListBucket" in actions(item)
            and ":s3:::vs-prod-*" in json.dumps(item["Resource"])
            and "s3:prefix" not in json.dumps(item.get("Condition", {}))
        ]
        self.assertEqual(len(unrestricted_owner_lists), 1)
        self.assertTrue(
            {"s3:ListBucketVersions", "s3:ListBucketMultipartUploads"} <= actions(unrestricted_owner_lists[0])
        )
        self.assertEqual(unrestricted_owner_lists[0]["Condition"]["StringEquals"]["s3:ResourceAccount"], ACCOUNT)

    def test_lifecycle_has_scoped_destination_runtime_and_inspection(self) -> None:
        _, _, statements = role(synth(), "ValSmithLifecycle-prod")
        allowed = [item for item in statements if item["Effect"] == "Allow"]
        required = {
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:PutRetentionPolicy",
            "logs:Unmask",
        }
        for action in required:
            matches = [item for item in allowed if action in actions(item)]
            self.assertEqual(len(matches), 1, action)
            scope = json.dumps(matches[0]["Resource"])
            self.assertIn("/valkyrie/benchmarks-prod/????????-????-????-????-????????????", scope)
            self.assertIn(ACCOUNT, scope)
            self.assertIn(REGION, scope)
        ownership = [item for item in allowed if "s3:GetBucketOwnershipControls" in actions(item)]
        self.assertEqual(len(ownership), 1)
        self.assertEqual(ownership[0]["Condition"]["StringEquals"]["s3:ResourceAccount"], ACCOUNT)

    def test_backup_can_use_only_the_retained_vault_key(self) -> None:
        template = synth()
        key_identifier, key = next(iter(template.find_resources("AWS::KMS::Key").items()))
        self.assertEqual(key["DeletionPolicy"], "Retain")
        _, _, statements = role(template, "ValSmithBackup-prod")
        key_statements = [item for item in statements if any(action.startswith("kms:") for action in actions(item))]
        key_actions = set[str]().union(*(actions(item) for item in key_statements))
        self.assertTrue({"kms:GenerateDataKey", "kms:Decrypt", "kms:CreateGrant"} <= key_actions)
        for statement in key_statements:
            self.assertEqual(statement["Resource"], {"Fn::GetAtt": [key_identifier, "Arn"]})
        grant = next(item for item in key_statements if "kms:CreateGrant" in actions(item))
        self.assertEqual(grant["Condition"]["Bool"], {"kms:GrantIsForAWSResource": "true"})

    def test_operator_trust_rejects_application_and_runtime_roles(self) -> None:
        for name in (
            "ValSmithStorage-prod",
            "ValSmithDatasetView-prod",
            "ValSmithBackup-prod",
            "ValSmithLifecycle-prod",
            "ValkyrieTrackerTaskRole-prod",
            "ValkyrieExecutorTaskRole-prod",
        ):
            with self.subTest(role=name), self.assertRaisesRegex(ValueError, "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN"):
                synth(
                    environment={
                        **INPUTS,
                        "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN": f"arn:aws:iam::{ACCOUNT}:role/{name}",
                    }
                )

    def test_access_point_cleanup_is_account_and_region_bound_without_name_assumptions(self) -> None:
        _, _, statements = role(synth(), "ValSmithLifecycle-prod")
        for action, service in (("backup:DeleteBackupAccessPoint", "backup"), ("s3:DeleteAccessPoint", "s3")):
            statement = next(item for item in statements if action in actions(item))
            self.assertIn(f":{service}:{REGION}:{ACCOUNT}:accesspoint/*", json.dumps(statement["Resource"]))

    def test_data_roots_and_legacy_reads_cannot_widen_to_shared_writes(self) -> None:
        template = synth()
        for name, expected_roots in (
            (
                "ValSmithStorage-prod",
                {"valsmith-manifests", "valsmith-datasets", "valsmith-dataset-views", "valsmith-repositories"},
            ),
            ("ValSmithDatasetView-prod", {"valsmith-dataset-views"}),
        ):
            _, _, statements = role(template, name)
            write = next(item for item in statements if item["Effect"] == "Allow" and "s3:PutObject" in actions(item))
            resource = write["Resource"]
            resources = cast(list[object], resource) if isinstance(resource, list) else [resource]
            self.assertEqual(len(resources), len(expected_roots))
            for root in expected_roots:
                self.assertIn(f":s3:::vs-prod-*/benchmarks/{root}/*", json.dumps(resources))
            legacy = [
                item
                for item in statements
                if item["Effect"] == "Allow" and "legacy-valsmith-shared" in json.dumps(item["Resource"])
            ]
            self.assertTrue(legacy)
            for statement in legacy:
                self.assertTrue(
                    actions(statement)
                    <= {
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:ListBucket",
                        "s3:ListBucketVersions",
                        "s3:GetBucketLocation",
                    }
                )
                self.assertEqual(statement["Condition"]["StringEquals"]["s3:ResourceAccount"], "210987654321")
            self.assertNotIn("s3:DeleteAccessPoint", set[str]().union(*(actions(item) for item in statements)))

        _, _, app_statements = role(template, "ValSmithStorage-prod")
        invocation = next(item for item in app_statements if "lambda:InvokeFunction" in actions(item))
        self.assertIn(
            f":lambda:{REGION}:{ACCOUNT}:function:valsmith-dataset-view-prod", json.dumps(invocation["Resource"])
        )
        self.assertNotIn("*", json.dumps(invocation["Resource"]))

    def test_backup_and_log_permissions_use_service_specific_arn_formats(self) -> None:
        template = synth()
        for name, action, expected in (
            ("ValSmithBackup-prod", "backup:TagResource", f":backup:{REGION}:{ACCOUNT}:recovery-point:*"),
            (
                "ValSmithDatasetView-prod",
                "logs:PutLogEvents",
                f":logs:{REGION}:{ACCOUNT}:log-group:/aws/lambda/valsmith-dataset-view-prod:*",
            ),
            (
                "ValSmithLifecycle-prod",
                "logs:DeleteLogGroup",
                f":logs:{REGION}:{ACCOUNT}:log-group:/valkyrie/benchmarks-prod/????????-????-????-????-????????????:*",
            ),
        ):
            _, _, statements = role(template, name)
            statement = next(item for item in statements if action in actions(item))
            self.assertIn(expected, json.dumps(statement["Resource"]))

    def test_bucket_policies_cannot_regrant_owner_deletion_to_application_or_lambda(self) -> None:
        template = synth()
        for name in ("ValSmithStorage-prod", "ValSmithDatasetView-prod"):
            _, _, statements = role(template, name)
            denied = set[str]().union(
                *(actions(item) for item in statements if item["Effect"] == "Deny" and "Condition" not in item)
            )
            self.assertTrue({"s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket"} <= denied)

    def test_enabled_production_creates_periodic_retained_unlocked_backup(self) -> None:
        template = synth()
        template.resource_count_is("AWS::Backup::BackupVault", 1)
        vault = next(iter(template.find_resources("AWS::Backup::BackupVault").values()))
        self.assertEqual(vault["DeletionPolicy"], "Retain")
        self.assertEqual(vault["UpdateReplacePolicy"], "Retain")
        self.assertNotIn("LockConfiguration", vault["Properties"])
        self.assertIn("EncryptionKeyArn", vault["Properties"])
        template.resource_count_is("AWS::Backup::BackupPlan", 2)
        for plan_name in ("valsmith-customer-storage-prod", "valsmith-system-prod"):
            periodic = rule(template, plan_name)
            self.assertEqual(periodic["ScheduleExpression"], "cron(0 3 * * ? *)")
            self.assertEqual(periodic["Lifecycle"], {"DeleteAfterDays": 30})
            self.assertFalse(periodic.get("EnableContinuousBackup", False))
            self.assertIn("BackupVaultName", json.dumps(periodic["TargetBackupVault"]))
        template.resource_count_is("AWS::S3::Bucket", 1)
        bucket = next(iter(template.find_resources("AWS::S3::Bucket").values()))
        self.assertEqual(bucket["DeletionPolicy"], "Retain")
        self.assertNotIn("NoncurrentVersionExpiration", json.dumps(bucket))

    def test_plan_tags_recovery_points_with_the_keys_the_vault_policy_requires(self) -> None:
        template = synth()
        expected = {"valsmith:backup": "true", "valsmith:environment": "prod"}
        self.assertEqual(rule(template, "valsmith-customer-storage-prod")["RecoveryPointTags"], expected)
        self.assertNotIn("RecoveryPointTags", rule(template, "valsmith-system-prod"))
        vault = next(iter(template.find_resources("AWS::Backup::BackupVault").values()))
        deletion = next(
            item
            for item in vault["Properties"]["AccessPolicy"]["Statement"]
            if "backup:DeleteRecoveryPoint" in actions(item)
        )
        self.assertEqual(
            deletion["Condition"]["StringEquals"],
            {f"aws:ResourceTag/{key}": value for key, value in expected.items()},
        )

    def test_legacy_read_scope_stays_pinned_to_the_reviewed_roots(self) -> None:
        template = synth()
        expected = {
            "/benchmarks/valsmith-manifests/*",
            "/benchmarks/valsmith-datasets/*",
            "/benchmarks/valsmith-dataset-views/*",
            "/benchmarks/valsmith-repositories/*",
            "/benchmarks/????????-????-????-????-????????????/*",
        }
        for name in ("ValSmithStorage-prod", "ValSmithDatasetView-prod", "ValSmithLifecycle-prod"):
            _, _, statements = role(template, name)
            legacy = [item for item in statements if LEGACY_BUCKET in json.dumps(item["Resource"])]
            objects = next(item for item in legacy if "s3:GetObject" in actions(item))
            self.assertEqual(legacy_scopes(objects["Resource"]), expected)
            listing = next(item for item in legacy if "s3:ListBucket" in actions(item))
            self.assertEqual(legacy_scopes(listing["Resource"]), {""})
            self.assertEqual(
                set(listing["Condition"]["StringLike"]["s3:prefix"]), {scope.lstrip("/") for scope in expected}
            )

    def test_defaults_disable_customer_storage_in_every_stage(self) -> None:
        for stage in ("bench", "dev", "release-test", "prod"):
            with self.subTest(stage=stage):
                template = synth(stage, {})
                template.resource_count_is("AWS::Backup::BackupVault", 0)
                self.assertNotIn("ValSmith", json.dumps(template.find_resources("AWS::IAM::Role")))

    def test_explicit_enable_fails_closed_outside_prod(self) -> None:
        for stage in ("bench", "dev", "release-test"):
            with self.subTest(stage=stage), self.assertRaisesRegex(ValueError, "prod"):
                synth(stage)

    def test_missing_or_invalid_inputs_fail_before_synthesis(self) -> None:
        for name in INPUTS:
            if name == "VALSMITH_CUSTOMER_STORAGE_ENABLED":
                continue
            environment = dict(INPUTS)
            del environment[name]
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, name):
                synth(environment=environment)
        invalid = {
            "VALSMITH_CUSTOMER_STORAGE_ENABLED": "yes",
            "PRODUCTION_ACCOUNT_ID": "210987654321",
            "VALSMITH_STORAGE_ORG_ID": "not-a-uuid",
            "VALSMITH_STORAGE_OIDC_PROVIDER_ARN": "arn:aws:iam::210987654321:oidc-provider/oidc.vercel.com/vals-ai",
            "VALSMITH_STORAGE_OIDC_AUDIENCE": "*",
            "VALSMITH_STORAGE_OIDC_SUBJECT": "owner:vals-ai:project:valsmith:environment:preview",
            "VALSMITH_DATASET_VIEW_LAMBDA_NAME": "*",
            "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN": f"arn:aws:iam::{ACCOUNT}:root",
            "VALSMITH_LEGACY_STORAGE_BUCKET": "*",
            "VALSMITH_LEGACY_STORAGE_ACCOUNT_ID": "bad",
        }
        for name, value in invalid.items():
            with self.subTest(invalid=name), self.assertRaisesRegex(ValueError, name):
                synth(environment={**INPUTS, name: value})

    def test_owner_selection_requires_both_tags_and_shared_selection_is_separate(self) -> None:
        selections = [
            item["Properties"]["BackupSelection"]
            for item in synth().find_resources("AWS::Backup::BackupSelection").values()
        ]
        self.assertEqual(len(selections), 2)
        owner = next(item for item in selections if item["SelectionName"] == "valsmith-owners-prod")
        self.assertEqual(
            owner["Conditions"],
            {
                "StringEquals": [
                    {"ConditionKey": "aws:ResourceTag/valsmith:backup", "ConditionValue": "true"},
                    {"ConditionKey": "aws:ResourceTag/valsmith:environment", "ConditionValue": "prod"},
                ]
            },
        )
        self.assertNotIn("ListOfTags", owner)
        self.assertIn(":s3:::vs-prod-*", json.dumps(owner["Resources"]))
        system = next(item for item in selections if item["SelectionName"] == "valsmith-system-prod")
        self.assertNotIn("Conditions", system)
        self.assertNotIn("vs-prod-*", json.dumps(system["Resources"]))

    def test_operator_arn_rejects_invalid_role_names_and_paths(self) -> None:
        resources = (
            "role//",
            "role/operators/",
            f"role/{'a' * 65}",
            f"role/{'a' * 511}/Operator",
            "role/operators with spaces/Operator",
            "role/operators\x7f/Operator",
            "role/operators/Operator!",
            "role/operators*/Operator",
            "role/operators?/Operator",
        )
        for resource in resources:
            with (
                self.subTest(resource=resource),
                self.assertRaisesRegex(ValueError, "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN"),
            ):
                synth(
                    environment={**INPUTS, "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN": f"arn:aws:iam::{ACCOUNT}:{resource}"}
                )

    def test_operator_arn_accepts_valid_role_name_and_path_boundaries(self) -> None:
        for resource in (f"role/{'a' * 64}", "role/teams!/@finance/Operator_+=,.@-", f"role/{'a' * 510}/Operator"):
            operator = f"arn:aws:iam::{ACCOUNT}:{resource}"
            with self.subTest(resource=resource):
                template = synth(environment={**INPUTS, "VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN": operator})
                _, properties, _ = role(template, "ValSmithLifecycle-prod")
                self.assertEqual(properties["AssumeRolePolicyDocument"]["Statement"][0]["Principal"], {"AWS": operator})

    def test_trust_is_exact_and_app_lambda_have_no_lifecycle_permissions(self) -> None:
        template = synth()
        for name, principal, action in (
            (
                "ValSmithStorage-prod",
                {"Federated": INPUTS["VALSMITH_STORAGE_OIDC_PROVIDER_ARN"]},
                "sts:AssumeRoleWithWebIdentity",
            ),
            ("ValSmithDatasetView-prod", {"Service": "lambda.amazonaws.com"}, "sts:AssumeRole"),
            ("ValSmithLifecycle-prod", {"AWS": INPUTS["VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN"]}, "sts:AssumeRole"),
        ):
            _, properties, statements = role(template, name)
            trust = properties["AssumeRolePolicyDocument"]["Statement"]
            self.assertEqual(len(trust), 1)
            self.assertEqual(trust[0]["Principal"], principal)
            self.assertEqual(trust[0]["Action"], action)
            if name == "ValSmithStorage-prod":
                self.assertEqual(
                    trust[0]["Condition"],
                    {
                        "StringEquals": {
                            "oidc.vercel.com/vals-ai:aud": "https://vercel.com/vals-ai",
                            "oidc.vercel.com/vals-ai:sub": "owner:vals-ai:project:valsmith:environment:production",
                        }
                    },
                )
            if name != "ValSmithLifecycle-prod":
                granted: set[str] = set[str]().union(
                    *(actions(item) for item in statements if item["Effect"] == "Allow")
                )
                self.assertFalse(any(item.startswith(("backup:", "rds:", "iam:")) for item in granted))
                self.assertFalse({"s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket"} & granted)

    def test_owner_data_grants_are_account_bound_and_foreign_access_is_denied(self) -> None:
        template = synth()
        for name in (
            "ValSmithStorage-prod",
            "ValSmithDatasetView-prod",
            "ValSmithBackup-prod",
            "ValSmithLifecycle-prod",
        ):
            _, _, statements = role(template, name)
            owner_allows = [
                item
                for item in statements
                if item["Effect"] == "Allow"
                and ":s3:::vs-prod-*" in json.dumps(item["Resource"])
                and "s3:CreateBucket" not in actions(item)
            ]
            self.assertTrue(owner_allows)
            for statement in owner_allows:
                self.assertEqual(statement["Condition"]["StringEquals"]["s3:ResourceAccount"], ACCOUNT)
            denies = [
                item
                for item in statements
                if item["Effect"] == "Deny"
                and "vs-prod-*" in json.dumps(item["Resource"])
                and "StringNotEquals" in item.get("Condition", {})
            ]
            self.assertTrue(denies)
            denied_actions: set[str] = set[str]().union(*(actions(item) for item in denies))
            self.assertIn("s3:GetObject", denied_actions)
            self.assertIn("s3:ListBucket", denied_actions)
            for statement in denies:
                self.assertEqual(statement["Condition"]["StringNotEquals"]["s3:ResourceAccount"], ACCOUNT)

    def test_backup_role_keeps_current_required_grants_without_global_s3_data_access(self) -> None:
        _, _, statements = role(synth(), "ValSmithBackup-prod")
        granted: set[str] = set[str]().union(*(actions(item) for item in statements if item["Effect"] == "Allow"))
        self.assertTrue(
            {
                "s3:ListTagsForResource",
                "s3:PutInventoryConfiguration",
                "s3:PutBucketNotification",
                "s3:GetObjectVersion",
                "backup:TagResource",
                "events:PutRule",
                "cloudwatch:GetMetricData",
            }
            <= granted
        )
        for statement in statements:
            if any(item.startswith("s3:") and item != "s3:ListAllMyBuckets" for item in actions(statement)):
                self.assertNotEqual(statement["Resource"], "*")

    def test_lifecycle_passrole_vault_and_access_point_boundaries(self) -> None:
        template = synth()
        backup_identifier, _, _ = role(template, "ValSmithBackup-prod")
        lifecycle_identifier, _, statements = role(template, "ValSmithLifecycle-prod")
        pass_role = next(item for item in statements if "iam:PassRole" in actions(item))
        self.assertEqual(pass_role["Resource"], {"Fn::GetAtt": [backup_identifier, "Arn"]})
        self.assertEqual(pass_role["Condition"], {"StringEquals": {"iam:PassedToService": "backup.amazonaws.com"}})
        self.assertFalse(any("backup:DeleteRecoveryPoint" in actions(item) for item in statements))
        vault = next(iter(template.find_resources("AWS::Backup::BackupVault").values()))
        deletion = next(
            item
            for item in vault["Properties"]["AccessPolicy"]["Statement"]
            if "backup:DeleteRecoveryPoint" in actions(item)
        )
        self.assertEqual(deletion["Effect"], "Allow")
        self.assertEqual(deletion["Principal"], {"AWS": {"Fn::GetAtt": [lifecycle_identifier, "Arn"]}})
        granted: set[str] = set[str]().union(*(actions(item) for item in statements if item["Effect"] == "Allow"))
        self.assertTrue(
            {
                "backup:ListBackupAccessPointsByRecoveryPoint",
                "backup:DeleteBackupAccessPoint",
                "backup:DescribeBackupAccessPoint",
                "s3:GetAccessPoint",
                "s3:DeleteAccessPoint",
            }
            <= granted
        )
        self.assertNotIn("backup:DeleteBackupVault", granted)
        self.assertNotIn("s3:DeleteBucketPolicy", granted)

    def test_outputs_publish_the_integration_contract(self) -> None:
        outputs = synth().to_json()["Outputs"]
        for name in (
            "CustomerStorageVaultName",
            "CustomerStorageVaultArn",
            "CustomerStorageBackupRoleArn",
            "CustomerStorageApplicationRoleArn",
            "CustomerStorageLambdaRoleArn",
            "CustomerStorageLifecycleRoleArn",
            "CustomerStorageAllowedOrgEnvironments",
        ):
            self.assertIn(name, outputs)
        self.assertEqual(json.loads(outputs["CustomerStorageAllowedOrgEnvironments"]["Value"]), {ORG: ["prod"]})


if __name__ == "__main__":
    unittest.main()
