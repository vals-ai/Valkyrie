import json
import os
import unittest
from dataclasses import replace
from typing import cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import assertions, aws_s3

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


class RuntimeIamTest(unittest.TestCase):
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
                self.assertNotIn("Condition", list_statement)
                get_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"s3:GetObject"}
                )
                self.assertIn("agents/*", json.dumps(get_statement["Resource"]))
                self.assertIn("benchmarks/*", json.dumps(get_statement["Resource"]))
                put_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"s3:PutObject"}
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
                    lambda_statement = next(
                        statement
                        for statement in statements
                        if _statement_actions(statement) == {"lambda:InvokeFunction"}
                    )
                    self.assertEqual(lambda_statement["Resource"], _lambda_function_resource("analysis-*"))

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
                    self.assertEqual(lambda_statements[0]["Resource"], _lambda_function_resource("analysis-*"))

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
