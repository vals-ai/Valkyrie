import json
import os
import unittest
from dataclasses import replace
from typing import cast
from unittest import mock
from uuid import UUID

import aws_cdk as cdk  # pyright: ignore[reportMissingImports]
from aws_cdk import assertions, aws_s3  # pyright: ignore[reportMissingImports]

from runtime_iam import create_executor_task_role, create_tracker_task_role
from stage import BENCH, DEV, RELEASE_TEST, Stage
from stage_config import BENCH_CONFIG, DEV_CONFIG, ManagedAWSRuntimeConfig
from test_monitoring_stack import (
    TEST_AWS_ACCOUNT,
    TEST_AWS_REGION,
    TEST_BENCH_ENV,
    TEST_DEV_ENV,
    TEST_MANAGED_ORG_ID,
    TEST_RELEASE_TEST_ENV,
    TEST_TRACKER_SECRET_NAME_PREFIX,
    JsonObject,
    service_templates,
)


def _named_role(template: assertions.Template, role_name: str) -> tuple[str, JsonObject]:
    resources = cast(dict[str, JsonObject], template.find_resources("AWS::IAM::Role"))
    matches = [
        (logical_id, resource)
        for logical_id, resource in resources.items()
        if cast(JsonObject, resource.get("Properties", {})).get("RoleName") == role_name
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one role named {role_name!r}, found {len(matches)}")
    return matches[0]


def _role_policy_statements(template: assertions.Template, role_logical_id: str) -> list[JsonObject]:
    statements: list[JsonObject] = []
    policies = cast(dict[str, JsonObject], template.find_resources("AWS::IAM::Policy"))
    for policy in policies.values():
        properties = cast(JsonObject, policy.get("Properties", {}))
        if {"Ref": role_logical_id} not in cast(list[JsonObject], properties.get("Roles", [])):
            continue
        policy_document = cast(JsonObject, properties["PolicyDocument"])
        policy_statements = policy_document["Statement"]
        if isinstance(policy_statements, list):
            statements.extend(cast(list[JsonObject], policy_statements))
        else:
            statements.append(cast(JsonObject, policy_statements))
    return statements


def _statement_actions(statement: JsonObject) -> set[str]:
    actions = cast(str | list[str], statement["Action"])
    return set(actions) if isinstance(actions, list) else {actions}


def _lambda_function_resource(function_name: str) -> JsonObject:
    return {
        "Fn::Join": [
            "",
            [
                "arn:",
                {"Ref": "AWS::Partition"},
                f":lambda:{TEST_AWS_REGION}:{TEST_AWS_ACCOUNT}:function:{function_name}",
            ],
        ]
    }


def _owner_bucket_resource(environment: str, *, objects: bool = False) -> JsonObject:
    suffix = "/benchmarks/*" if objects else ""
    return {
        "Fn::Join": [
            "",
            ["arn:", {"Ref": "AWS::Partition"}, f":s3:::vs-{environment}-*{suffix}"],
        ]
    }


class RuntimeIamTest(unittest.TestCase):
    def test_tracker_archive_reads_are_limited_to_permitted_owner_buckets_and_run_history(self) -> None:
        for environments in (frozenset[str](), frozenset({"prod"}), frozenset({"dev", "prod"})):
            with self.subTest(environments=environments):
                app = cdk.App()
                stack = cdk.Stack(
                    app,
                    "ArchiveRuntimeIamStack",
                    env=cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION),
                )
                bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
                config = ManagedAWSRuntimeConfig(
                    benchmark_log_group_prefix="/valkyrie/benchmarks",
                    benchmark_log_retention_days=7,
                    deployment_role_org_ids=(TEST_MANAGED_ORG_ID,),
                    managed_storage_org_environments={UUID(TEST_MANAGED_ORG_ID): environments} if environments else {},
                )
                create_tracker_task_role(stack, Stage("prod"), bucket, config)
                create_executor_task_role(stack, Stage("prod"), bucket, config)
                template = assertions.Template.from_stack(stack)

                for role_name in ("ValkyrieTrackerTaskRole-prod", "ValkyrieExecutorTaskRole-prod"):
                    role_logical_id, _ = _named_role(template, role_name)
                    statements = _role_policy_statements(template, role_logical_id)
                    for action, suffix in (
                        ("s3:GetBucketOwnershipControls", ""),
                        ("s3:GetObjectVersion", "/benchmarks/????????-????-????-????-????????????/log-history/*"),
                    ):
                        grants = [
                            statement
                            for statement in statements
                            if statement["Effect"] == "Allow" and action in _statement_actions(statement)
                        ]
                        expected_count = int(bool(environments) and role_name == "ValkyrieTrackerTaskRole-prod")
                        self.assertEqual(len(grants), expected_count, (role_name, action))
                        for grant in grants:
                            self.assertEqual(
                                grant["Condition"], {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}}
                            )
                            resources = grant["Resource"]
                            actual_resources = (
                                cast(list[JsonObject], resources) if isinstance(resources, list) else [resources]
                            )
                            self.assertEqual(
                                actual_resources,
                                [
                                    {
                                        "Fn::Join": [
                                            "",
                                            ["arn:", {"Ref": "AWS::Partition"}, f":s3:::vs-{environment}-*{suffix}"],
                                        ]
                                    }
                                    for environment in sorted(environments)
                                ],
                            )

    def test_bench_passes_canonical_owner_storage_settings_to_both_services(self) -> None:
        configured_mapping = json.dumps({TEST_MANAGED_ORG_ID: ["prod", "dev"]})
        environment = {
            **TEST_BENCH_ENV,
            "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS": configured_mapping,
            "AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED": "true",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            tracker_template, executor_template, _ = service_templates(BENCH)

        expected_settings = {
            "AWS_DEPLOYMENT_ACCOUNT_ID": TEST_AWS_ACCOUNT,
            "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS": f'{{"{TEST_MANAGED_ORG_ID}":["dev","prod"]}}',
            "AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED": "true",
        }
        for template, role_name in (
            (tracker_template, "ValkyrieTrackerTaskRole"),
            (executor_template, "ValkyrieExecutorTaskRole"),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                task_definitions = cast(dict[str, JsonObject], template.find_resources("AWS::ECS::TaskDefinition"))
                role_task_definition = next(
                    task_definition
                    for task_definition in task_definitions.values()
                    if cast(JsonObject, task_definition["Properties"]).get("TaskRoleArn")
                    == {"Fn::GetAtt": [role_logical_id, "Arn"]}
                )
                containers = cast(list[JsonObject], role_task_definition["Properties"]["ContainerDefinitions"])
                actual_environment = {
                    cast(str, variable["Name"]): cast(str, variable["Value"])
                    for variable in cast(list[JsonObject], containers[0]["Environment"])
                }
                for name, value in expected_settings.items():
                    self.assertEqual(actual_environment[name], value)

    def test_owner_storage_patterns_follow_only_the_configured_environment_union(self) -> None:
        environment_cases: tuple[tuple[dict[UUID, frozenset[str]], set[str]], ...] = (
            ({}, set[str]()),
            ({UUID(TEST_MANAGED_ORG_ID): frozenset({"dev"})}, {"dev"}),
            ({UUID(TEST_MANAGED_ORG_ID): frozenset({"prod"})}, {"prod"}),
            ({UUID(TEST_MANAGED_ORG_ID): frozenset({"dev", "prod"})}, {"dev", "prod"}),
        )
        for configured_mapping, expected_environments in environment_cases:
            with self.subTest(environments=expected_environments):
                app = cdk.App()
                stack = cdk.Stack(
                    app,
                    "RuntimeIamStack",
                    env=cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION),
                )
                bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
                config = ManagedAWSRuntimeConfig(
                    benchmark_log_group_prefix="/valkyrie/benchmarks",
                    benchmark_log_retention_days=7,
                    deployment_role_org_ids=(TEST_MANAGED_ORG_ID,),
                    managed_storage_org_environments=configured_mapping,
                )
                create_tracker_task_role(stack, Stage(BENCH), bucket, config)
                create_executor_task_role(stack, Stage(BENCH), bucket, config)
                template = assertions.Template.from_stack(stack)

                for role_name in ("ValkyrieTrackerTaskRole", "ValkyrieExecutorTaskRole"):
                    role_logical_id, _ = _named_role(template, role_name)
                    statements_json = json.dumps(_role_policy_statements(template, role_logical_id))
                    for owner_environment in {"dev", "prod"}:
                        self.assertEqual(
                            f"vs-{owner_environment}-*" in statements_json,
                            owner_environment in expected_environments,
                        )
                    self.assertNotIn("vs-bench-*", statements_json)

    def test_owner_storage_policy_is_account_guarded_and_prefix_bounded(self) -> None:
        environment = {
            **TEST_BENCH_ENV,
            "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS": json.dumps({TEST_MANAGED_ORG_ID: ["dev", "prod"]}),
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            tracker_template, executor_template, _ = service_templates(BENCH)

        bucket_resources = [_owner_bucket_resource("dev"), _owner_bucket_resource("prod")]
        object_resources = [
            _owner_bucket_resource("dev", objects=True),
            _owner_bucket_resource("prod", objects=True),
        ]
        same_account_condition = {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}}
        foreign_account_condition = {"StringNotEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}}
        for template, role_name, role_actions in (
            (
                tracker_template,
                "ValkyrieTrackerTaskRole",
                {"s3:DeleteObject", "s3:DeleteObjectVersion"},
            ),
            (executor_template, "ValkyrieExecutorTaskRole", {"s3:AbortMultipartUpload"}),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                statements = _role_policy_statements(template, role_logical_id)
                owner_statements = [statement for statement in statements if "vs-" in json.dumps(statement["Resource"])]
                owner_allows = [statement for statement in owner_statements if statement.get("Effect") != "Deny"]
                owner_denies = [statement for statement in owner_statements if statement.get("Effect") == "Deny"]

                is_tracker = role_name == "ValkyrieTrackerTaskRole"
                self.assertEqual(len(owner_allows), 4 if is_tracker else 3)
                self.assertEqual(len(owner_denies), 1)
                for statement in owner_allows:
                    self.assertEqual(statement["Condition"], same_account_condition)

                bucket_allow = next(
                    statement
                    for statement in owner_allows
                    if _statement_actions(statement)
                    == {"s3:ListBucket", "s3:GetBucketTagging", "s3:GetBucketVersioning"}
                    | ({"s3:GetBucketOwnershipControls"} if is_tracker else set())
                )
                self.assertEqual(bucket_allow["Resource"], bucket_resources)
                self.assertNotIn("s3:prefix", json.dumps(bucket_allow.get("Condition", {})))

                object_allow = next(
                    statement
                    for statement in owner_allows
                    if _statement_actions(statement) == {"s3:GetObject", "s3:PutObject"}
                )
                self.assertEqual(object_allow["Resource"], object_resources)

                role_allow = next(
                    statement for statement in owner_allows if _statement_actions(statement) == role_actions
                )
                self.assertEqual(role_allow["Resource"], object_resources)

                deny = owner_denies[0]
                self.assertEqual(_statement_actions(deny), {"s3:*"})
                self.assertEqual(deny["Resource"], bucket_resources + object_resources)
                self.assertEqual(deny["Condition"], foreign_account_condition)

                owner_policy_json = json.dumps(owner_statements)
                self.assertNotIn("agents/*", owner_policy_json)
                self.assertNotIn("vs-bench-*", owner_policy_json)
                allow_actions = set[str]().union(
                    *(_statement_actions(statement) for statement in statements if statement.get("Effect") != "Deny")
                )
                self.assertTrue(
                    {"s3:CreateBucket", "s3:PutBucketTagging", "s3:PutBucketPolicy"}.isdisjoint(allow_actions)
                )

                if role_name.startswith("ValkyrieExecutor"):
                    release_statement = next(
                        statement
                        for statement in statements
                        if _statement_actions(statement) == {"s3:GetObject"}
                        and "/releases/*" in json.dumps(statement["Resource"])
                    )
                    release_resource = cast(JsonObject, release_statement["Resource"])
                    release_join = cast(list[object], release_resource["Fn::Join"])
                    release_parts = cast(list[object], release_join[1])
                    release_bucket_reference = cast(JsonObject, release_parts[0])
                    release_bucket_attribute = cast(list[str], release_bucket_reference["Fn::GetAtt"])
                    self.assertTrue(release_bucket_attribute[0].startswith("ExecutorReleaseBucket"))
                    self.assertEqual(release_bucket_attribute[1], "Arn")
                    self.assertEqual(release_parts[1], "/releases/*")
                    self.assertNotIn("Condition", release_statement)
                    self.assertNotIn("vs-", json.dumps(release_statement["Resource"]))

    def test_managed_runtime_rejects_invalid_authority_configuration(self) -> None:
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
        )
        lambda_arn = f"arn:aws:lambda:{TEST_AWS_REGION}:{TEST_AWS_ACCOUNT}:function:example"
        invalid_values: list[tuple[str, object]] = [
            ("benchmark_log_retention_days", 0),
            ("benchmark_log_retention_days", -1),
            ("deployment_role_org_ids", ("not-a-uuid",)),
            ("deployment_role_org_ids", ("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",)),
            ("tracker_secret_name_prefixes", ("",)),
            ("tracker_secret_name_prefixes", ("*",)),
            ("tracker_secret_name_prefixes", ("valkyrie/*",)),
            ("executor_secret_name_prefixes", ("",)),
            ("executor_secret_name_prefixes", ("*",)),
            ("executor_secret_name_prefixes", ("valkyrie/*",)),
            ("tracker_lambda_function_name_patterns", ("",)),
            ("tracker_lambda_function_name_patterns", ("*",)),
            ("tracker_lambda_function_name_patterns", ("?suffix",)),
            ("tracker_lambda_function_name_patterns", ("name:qualifier",)),
            ("tracker_lambda_function_name_patterns", (lambda_arn,)),
            ("executor_lambda_function_name_patterns", ("",)),
            ("executor_lambda_function_name_patterns", ("*",)),
            ("executor_lambda_function_name_patterns", ("?suffix",)),
            ("executor_lambda_function_name_patterns", (lambda_arn,)),
            ("kms_key_arns", ("*",)),
            ("kms_key_arns", (f"arn:aws:s3:{TEST_AWS_REGION}:{TEST_AWS_ACCOUNT}:bucket/example",)),
            ("kms_key_arns", (f"arn:aws:kms:*:{TEST_AWS_ACCOUNT}:key/example",)),
            ("kms_key_arns", (f"arn:aws:kms:{TEST_AWS_REGION}:*:key/example",)),
            ("kms_key_arns", (f"arn:aws:kms:{TEST_AWS_REGION}:{TEST_AWS_ACCOUNT}:alias/example",)),
        ]

        for field_name, value in invalid_values:
            with self.subTest(field=field_name, value=value):
                with self.assertRaisesRegex(ValueError, field_name):
                    replace(config, **{field_name: value})

        with self.assertRaisesRegex(ValueError, "executor_all_secret_access"):
            replace(
                config,
                executor_all_secret_access=True,
                executor_secret_name_prefixes=("valkyrie/executor/",),
            )

    def test_dev_managed_runtime_is_enabled_for_the_configured_org(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEV_ENV, clear=True):
            tracker_template, executor_template, _ = service_templates(DEV)

        expected_environment = assertions.Match.array_with(
            [
                {
                    "Name": "AWS_DEPLOYMENT_ROLE_ORG_IDS",
                    "Value": TEST_MANAGED_ORG_ID,
                },
                {"Name": "AWS_DEPLOYMENT_REGION", "Value": TEST_AWS_REGION},
                assertions.Match.object_like({"Name": "AWS_DEPLOYMENT_S3_BUCKET"}),
                {"Name": "AWS_DEPLOYMENT_LOG_GROUP", "Value": "/valkyrie/benchmarks-dev"},
                {"Name": "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "Value": "7"},
                {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "true"},
            ]
        )

        expected_actions = {
            "s3:ListBucket",
            "s3:GetObject",
            "s3:PutObject",
        }
        for template, role_name, output_name, service_actions in (
            (
                tracker_template,
                "ValkyrieTrackerTaskRole-dev",
                "TrackerTaskRoleArn",
                expected_actions
                | {
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion",
                    "secretsmanager:GetSecretValue",
                    "lambda:InvokeFunction",
                    "logs:GetLogEvents",
                    "logs:FilterLogEvents",
                },
            ),
            (
                executor_template,
                "ValkyrieExecutorTaskRole-dev",
                "ExecutorTaskRoleArn",
                expected_actions
                | {
                    "s3:AbortMultipartUpload",
                    "secretsmanager:GetSecretValue",
                    "logs:CreateLogGroup",
                    "logs:PutRetentionPolicy",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "ecs:UpdateTaskProtection",
                    "lambda:InvokeFunction",
                },
            ),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                task_definitions = cast(
                    dict[str, JsonObject],
                    template.find_resources("AWS::ECS::TaskDefinition"),
                )
                role_task_definitions = [
                    task_definition
                    for task_definition in task_definitions.values()
                    if cast(JsonObject, task_definition["Properties"]).get("TaskRoleArn")
                    == {"Fn::GetAtt": [role_logical_id, "Arn"]}
                ]
                self.assertEqual(len(role_task_definitions), 1)
                task_properties = cast(JsonObject, role_task_definitions[0]["Properties"])
                self.assertEqual(task_properties["TaskRoleArn"], {"Fn::GetAtt": [role_logical_id, "Arn"]})
                self.assertIn("ExecutionRoleArn", task_properties)
                self.assertNotEqual(task_properties["TaskRoleArn"], task_properties["ExecutionRoleArn"])
                self.assertEqual(
                    template.to_json()["Outputs"][output_name]["Value"],
                    {"Fn::GetAtt": [role_logical_id, "Arn"]},
                )
                template.has_resource_properties(
                    "AWS::ECS::TaskDefinition",
                    {
                        "ContainerDefinitions": assertions.Match.array_with(
                            [assertions.Match.object_like({"Environment": expected_environment})]
                        )
                    },
                )

                statements = _role_policy_statements(template, role_logical_id)
                actions = set[str]().union(*(_statement_actions(statement) for statement in statements))
                self.assertEqual(actions, service_actions)

                list_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"s3:ListBucket"}
                )
                self.assertEqual(
                    list_statement["Condition"],
                    {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}},
                )
                self.assertNotIn("s3:prefix", json.dumps(list_statement["Condition"]))
                get_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"s3:GetObject"}
                )
                self.assertEqual(
                    get_statement["Condition"],
                    {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}},
                )
                self.assertIn("agents/*", json.dumps(get_statement["Resource"]))
                self.assertIn("benchmarks/*", json.dumps(get_statement["Resource"]))
                put_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"s3:PutObject"}
                )
                self.assertEqual(
                    put_statement["Condition"],
                    {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}},
                )
                self.assertIn("benchmarks/*", json.dumps(put_statement["Resource"]))
                self.assertNotIn("agents/*", json.dumps(put_statement["Resource"]))

                delete_statements = [
                    statement
                    for statement in statements
                    if _statement_actions(statement) == {"s3:DeleteObject", "s3:DeleteObjectVersion"}
                ]
                if role_name.startswith("ValkyrieTracker"):
                    self.assertEqual(len(delete_statements), 1)
                    self.assertEqual(
                        delete_statements[0]["Condition"],
                        {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}},
                    )
                    self.assertIn("benchmarks/*", json.dumps(delete_statements[0]["Resource"]))
                    self.assertNotIn("agents/*", json.dumps(delete_statements[0]["Resource"]))
                else:
                    self.assertEqual(delete_statements, [])

                abort_statements = [
                    statement
                    for statement in statements
                    if _statement_actions(statement) == {"s3:AbortMultipartUpload"}
                ]
                if role_name.startswith("ValkyrieExecutor"):
                    self.assertEqual(len(abort_statements), 1)
                    self.assertEqual(
                        abort_statements[0]["Condition"],
                        {"StringEquals": {"s3:ResourceAccount": TEST_AWS_ACCOUNT}},
                    )
                    self.assertIn("benchmarks/*", json.dumps(abort_statements[0]["Resource"]))
                    self.assertNotIn("agents/*", json.dumps(abort_statements[0]["Resource"]))
                else:
                    self.assertEqual(abort_statements, [])

                for statement in statements:
                    resources = statement["Resource"]
                    if resources == "*" or (isinstance(resources, list) and "*" in resources):
                        self.assertEqual(_statement_actions(statement), {"ecs:UpdateTaskProtection"})

                secret_statement = next(
                    statement
                    for statement in statements
                    if _statement_actions(statement) == {"secretsmanager:GetSecretValue"}
                )
                if role_name.startswith("ValkyrieExecutor"):
                    secret_resources = json.dumps(secret_statement["Resource"])
                    self.assertIn("secretsmanager", secret_resources)
                    self.assertIn("secret:*", secret_resources)
                    self.assertNotIn(TEST_TRACKER_SECRET_NAME_PREFIX, secret_resources)
                    log_statement = next(
                        statement for statement in statements if "logs:CreateLogStream" in _statement_actions(statement)
                    )
                    self.assertEqual(
                        _statement_actions(log_statement),
                        {"logs:CreateLogGroup", "logs:PutRetentionPolicy", "logs:CreateLogStream", "logs:PutLogEvents"},
                    )
                    self.assertIn("/valkyrie/benchmarks-dev/*", json.dumps(log_statement["Resource"]))
                    self.assertNotIn(":log-stream:", json.dumps(log_statement["Resource"]))
                    lambda_statement = next(
                        statement
                        for statement in statements
                        if _statement_actions(statement) == {"lambda:InvokeFunction"}
                    )
                    self.assertEqual(
                        lambda_statement["Resource"],
                        [
                            _lambda_function_resource(pattern)
                            for pattern in DEV_CONFIG.managed_aws.executor_lambda_function_name_patterns
                        ],
                    )
                else:
                    secret_resources = json.dumps(secret_statement["Resource"])
                    self.assertIn("secretsmanager", secret_resources)
                    self.assertIn(f"secret:{TEST_TRACKER_SECRET_NAME_PREFIX}*", secret_resources)
                    log_statement = next(
                        statement
                        for statement in statements
                        if _statement_actions(statement) == {"logs:GetLogEvents", "logs:FilterLogEvents"}
                    )
                    self.assertIn("/valkyrie/benchmarks-dev/*", json.dumps(log_statement["Resource"]))
                    self.assertNotIn(":log-stream:", json.dumps(log_statement["Resource"]))
                    lambda_statement = next(
                        statement
                        for statement in statements
                        if _statement_actions(statement) == {"lambda:InvokeFunction"}
                    )
                    self.assertEqual(
                        lambda_statement["Resource"],
                        [
                            _lambda_function_resource(pattern)
                            for pattern in DEV_CONFIG.managed_aws.tracker_lambda_function_name_patterns
                        ],
                    )

    def test_bench_managed_runtime_uses_bench_inventory_and_task_roles(self) -> None:
        with mock.patch.dict(os.environ, TEST_BENCH_ENV, clear=True):
            tracker_template, executor_template, _ = service_templates(BENCH)

        expected_environment = assertions.Match.array_with(
            [
                {"Name": "AWS_DEPLOYMENT_ROLE_ORG_IDS", "Value": TEST_MANAGED_ORG_ID},
                {"Name": "AWS_DEPLOYMENT_REGION", "Value": TEST_AWS_REGION},
                assertions.Match.object_like({"Name": "AWS_DEPLOYMENT_S3_BUCKET"}),
                {"Name": "AWS_DEPLOYMENT_LOG_GROUP", "Value": "/valkyrie/benchmarks"},
                {"Name": "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "Value": "365"},
                {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "true"},
            ]
        )

        for template, role_name in (
            (tracker_template, "ValkyrieTrackerTaskRole"),
            (executor_template, "ValkyrieExecutorTaskRole"),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                template.has_resource_properties(
                    "AWS::ECS::TaskDefinition",
                    {
                        "TaskRoleArn": {"Fn::GetAtt": [role_logical_id, "Arn"]},
                        "ContainerDefinitions": assertions.Match.array_with(
                            [assertions.Match.object_like({"Environment": expected_environment})]
                        ),
                    },
                )

                secret_statement = next(
                    statement
                    for statement in _role_policy_statements(template, role_logical_id)
                    if _statement_actions(statement) == {"secretsmanager:GetSecretValue"}
                )
                secret_resources = json.dumps(secret_statement["Resource"])
                if role_name.startswith("ValkyrieExecutor"):
                    self.assertIn("secret:*", secret_resources)
                    self.assertNotIn(TEST_TRACKER_SECRET_NAME_PREFIX, secret_resources)
                    lambda_statements = [
                        statement
                        for statement in _role_policy_statements(template, role_logical_id)
                        if _statement_actions(statement) == {"lambda:InvokeFunction"}
                    ]
                    self.assertEqual(len(lambda_statements), 1)
                    self.assertEqual(
                        lambda_statements[0]["Resource"],
                        [
                            _lambda_function_resource(pattern)
                            for pattern in BENCH_CONFIG.managed_aws.executor_lambda_function_name_patterns
                        ],
                    )
                else:
                    self.assertIn(f"secret:{TEST_TRACKER_SECRET_NAME_PREFIX}*", secret_resources)
                    lambda_statements = [
                        statement
                        for statement in _role_policy_statements(template, role_logical_id)
                        if _statement_actions(statement) == {"lambda:InvokeFunction"}
                    ]
                    self.assertEqual(len(lambda_statements), 1)
                    self.assertEqual(
                        lambda_statements[0]["Resource"],
                        [
                            _lambda_function_resource(pattern)
                            for pattern in BENCH_CONFIG.managed_aws.tracker_lambda_function_name_patterns
                        ],
                    )

    def test_release_test_managed_runtime_remains_closed(self) -> None:
        with mock.patch.dict(os.environ, TEST_RELEASE_TEST_ENV, clear=True):
            tracker_template, executor_template, _ = service_templates(RELEASE_TEST)

        expected_environment = assertions.Match.array_with(
            [
                {"Name": "AWS_DEPLOYMENT_ROLE_ORG_IDS", "Value": ""},
                {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "false"},
            ]
        )
        for template, role_name in (
            (tracker_template, "ValkyrieTrackerTaskRole-release-test"),
            (executor_template, "ValkyrieExecutorTaskRole-release-test"),
        ):
            template.has_resource_properties(
                "AWS::ECS::TaskDefinition",
                {
                    "ContainerDefinitions": assertions.Match.array_with(
                        [assertions.Match.object_like({"Environment": expected_environment})]
                    )
                },
            )
            role_logical_id, _ = _named_role(template, role_name)
            actions = set[str]().union(
                *(_statement_actions(statement) for statement in _role_policy_statements(template, role_logical_id))
            )
            self.assertNotIn("secretsmanager:GetSecretValue", actions)

    def test_managed_runtime_optional_grants_are_limited_to_configured_resources(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(
            app,
            "RuntimeIamStack",
            env=cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION),
        )
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        kms_key_arn = f"arn:aws:kms:{TEST_AWS_REGION}:{TEST_AWS_ACCOUNT}:key/test-key"
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            tracker_secret_name_prefixes=("valkyrie/tracker/",),
            executor_secret_name_prefixes=("valkyrie/executor/",),
            tracker_lambda_function_name_patterns=("valkyrie-analyzer-*",),
            executor_lambda_function_name_patterns=("valkyrie-post-run-*",),
            kms_key_arns=(kms_key_arn,),
        )
        create_tracker_task_role(stack, Stage(DEV), bucket, config)
        create_executor_task_role(stack, Stage(DEV), bucket, config)
        template = assertions.Template.from_stack(stack)

        for role_name, secret_prefix, lambda_pattern in (
            ("ValkyrieTrackerTaskRole-dev", "valkyrie/tracker/", "valkyrie-analyzer-*"),
            ("ValkyrieExecutorTaskRole-dev", "valkyrie/executor/", "valkyrie-post-run-*"),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                statements = _role_policy_statements(template, role_logical_id)

                secret_statement = next(
                    statement
                    for statement in statements
                    if _statement_actions(statement) == {"secretsmanager:GetSecretValue"}
                )
                self.assertIn(f"secret:{secret_prefix}*", json.dumps(secret_statement["Resource"]))

                lambda_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"lambda:InvokeFunction"}
                )
                self.assertIn(f"function:{lambda_pattern}", json.dumps(lambda_statement["Resource"]))

                kms_statement = next(
                    statement for statement in statements if "kms:Decrypt" in _statement_actions(statement)
                )
                self.assertEqual(
                    _statement_actions(kms_statement),
                    {"kms:Decrypt", "kms:GenerateDataKey"},
                )
                self.assertEqual(kms_statement["Resource"], kms_key_arn)

                for statement in (secret_statement, lambda_statement, kms_statement):
                    self.assertNotEqual(statement["Resource"], "*")


if __name__ == "__main__":
    unittest.main()
