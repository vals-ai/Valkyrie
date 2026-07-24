import json
import os
import unittest
from typing import cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import assertions, aws_s3

from runtime_iam import create_tracker_task_role, create_worker_task_role, managed_runtime_environment
from stage import DEV, Stage
from stage_config import ManagedAWSRuntimeConfig
from test_monitoring_stack import (
    TEST_AWS_ACCOUNT,
    TEST_AWS_REGION,
    TEST_DEV_ENV,
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


class RuntimeIamTest(unittest.TestCase):
    def test_managed_runtime_task_roles_are_scoped_and_closed_by_default(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEV_ENV, clear=True):
            tracker_template, worker_template = service_templates(DEV)

        expected_environment = assertions.Match.array_with(
            [
                {"Name": "AWS_MANAGED_TENANT_IDS", "Value": "vals.ai"},
                {"Name": "AWS_DEPLOYMENT_REGION", "Value": TEST_AWS_REGION},
                assertions.Match.object_like({"Name": "AWS_DEPLOYMENT_S3_BUCKET"}),
                {"Name": "AWS_DEPLOYMENT_LOG_GROUP", "Value": "/valkyrie/benchmarks-dev"},
                {"Name": "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "Value": "7"},
                {"Name": "AWS_DEPLOYMENT_SANDBOX_PROVIDER", "Value": "daytona"},
                {"Name": "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME", "Value": ""},
                {"Name": "AWS_MANAGED_AGENT_SECRET_NAMES", "Value": ""},
                {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "false"},
                {
                    "Name": "BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX",
                    "Value": "valkyrie/benchmark-service-access-key-dev/",
                },
            ]
        )

        expected_actions = {
            "s3:ListBucket",
            "s3:GetObject",
            "s3:AbortMultipartUpload",
            "s3:PutObject",
        }
        for template, role_name, output_name, service_actions in (
            (
                tracker_template,
                "ValkyrieTrackerTaskRole-dev",
                "TrackerTaskRoleArn",
                expected_actions
                | {
                    "logs:DescribeLogStreams",
                    "logs:FilterLogEvents",
                    "logs:GetLogEvents",
                    "secretsmanager:CreateSecret",
                    "secretsmanager:GetSecretValue",
                },
            ),
            (
                worker_template,
                "ValkyrieWorkerTaskRole-dev",
                "WorkerTaskRoleArn",
                expected_actions
                | {
                    "s3:DeleteObject",
                    "logs:CreateLogGroup",
                    "logs:PutRetentionPolicy",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "ecs:UpdateTaskProtection",
                    "secretsmanager:GetSecretValue",
                },
            ),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                task_definitions = cast(
                    dict[str, JsonObject],
                    template.find_resources("AWS::ECS::TaskDefinition"),
                )
                self.assertEqual(len(task_definitions), 1)
                task_properties = cast(JsonObject, next(iter(task_definitions.values()))["Properties"])
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
                    statement
                    for statement in statements
                    if _statement_actions(statement) == {"s3:AbortMultipartUpload", "s3:PutObject"}
                )
                self.assertIn("benchmarks/*", json.dumps(put_statement["Resource"]))
                self.assertNotIn("agents/*", json.dumps(put_statement["Resource"]))
                if role_name.startswith("ValkyrieWorker"):
                    delete_statement = next(
                        statement for statement in statements if _statement_actions(statement) == {"s3:DeleteObject"}
                    )
                    self.assertIn(
                        "benchmarks/*/*/.valkyrie/artifacts/*/generations/*",
                        json.dumps(delete_statement["Resource"]),
                    )

                managed_secret_statement = next(
                    statement
                    for statement in statements
                    if "valkyrie/benchmark-service-access-key-dev/" in json.dumps(statement["Resource"])
                )
                expected_secret_actions = {"secretsmanager:GetSecretValue"}
                if role_name.startswith("ValkyrieTracker"):
                    expected_secret_actions.add("secretsmanager:CreateSecret")
                self.assertEqual(_statement_actions(managed_secret_statement), expected_secret_actions)

                for statement in statements:
                    resources = statement["Resource"]
                    if resources == "*" or (isinstance(resources, list) and "*" in resources):
                        self.assertEqual(_statement_actions(statement), {"ecs:UpdateTaskProtection"})

                if role_name.startswith("ValkyrieWorker"):
                    log_statement = next(
                        statement for statement in statements if "logs:CreateLogStream" in _statement_actions(statement)
                    )
                    self.assertEqual(
                        _statement_actions(log_statement),
                        {"logs:CreateLogGroup", "logs:PutRetentionPolicy", "logs:CreateLogStream", "logs:PutLogEvents"},
                    )
                    self.assertIn("/valkyrie/benchmarks-dev/*", json.dumps(log_statement["Resource"]))
                    self.assertNotIn(":log-stream:", json.dumps(log_statement["Resource"]))
                else:
                    log_statement = next(
                        statement for statement in statements if "logs:GetLogEvents" in _statement_actions(statement)
                    )
                    self.assertEqual(
                        _statement_actions(log_statement),
                        {
                            "logs:DescribeLogStreams",
                            "logs:FilterLogEvents",
                            "logs:GetLogEvents",
                        },
                    )
                    self.assertIn("/valkyrie/benchmarks-dev/*", json.dumps(log_statement["Resource"]))
                    self.assertNotIn(":log-stream:", json.dumps(log_statement["Resource"]))

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
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="sandbox/provider",
            tracker_secret_name_prefixes=("valkyrie/tracker/",),
            worker_secret_name_prefixes=("valkyrie/worker/",),
            worker_secret_names=("agent/model-key",),
            tracker_lambda_function_name_patterns=("valkyrie-analyzer-*",),
            worker_lambda_function_name_patterns=("valkyrie-post-run-*",),
            kms_key_arns=(kms_key_arn,),
        )
        create_tracker_task_role(stack, Stage(DEV), bucket, config)
        create_worker_task_role(stack, Stage(DEV), bucket, config)
        template = assertions.Template.from_stack(stack)

        for role_name, secret_prefix, lambda_pattern in (
            ("ValkyrieTrackerTaskRole-dev", "valkyrie/tracker/", "valkyrie-analyzer-*"),
            ("ValkyrieWorkerTaskRole-dev", "valkyrie/worker/", "valkyrie-post-run-*"),
        ):
            with self.subTest(role=role_name):
                role_logical_id, _ = _named_role(template, role_name)
                statements = _role_policy_statements(template, role_logical_id)

                secret_statement = next(
                    statement for statement in statements if secret_prefix in json.dumps(statement["Resource"])
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

                provider_statement = next(
                    statement
                    for statement in statements
                    if "secret:sandbox/provider-??????" in json.dumps(statement["Resource"])
                )
                self.assertEqual(_statement_actions(provider_statement), {"secretsmanager:GetSecretValue"})
                if role_name.startswith("ValkyrieWorker"):
                    exact_secret_statement = next(
                        statement
                        for statement in statements
                        if "secret:agent/model-key-??????" in json.dumps(statement["Resource"])
                    )
                    self.assertEqual(
                        _statement_actions(exact_secret_statement),
                        {"secretsmanager:GetSecretValue"},
                    )

    def test_enabled_managed_runtime_requires_sandbox_configuration(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(app, "RuntimeEnvironmentStack")
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="",
        )

        with mock.patch.dict(
            os.environ,
            {
                "AWS_MANAGED_SUBMISSIONS_ENABLED": "true",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "sandbox provider and secret name"):
                managed_runtime_environment(stack, Stage(DEV), bucket, config)

    def test_stage_uses_vals_tenant_and_closed_runtime_defaults(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(app, "BlankEnvironmentStack")
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="",
        )

        with mock.patch.dict(
            os.environ,
            {
                "AWS_DEPLOYMENT_SANDBOX_PROVIDER": "",
                "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME": "",
                "AWS_MANAGED_AGENT_SECRET_NAMES": "",
                "AWS_MANAGED_TENANT_IDS": "",
                "AWS_MANAGED_SUBMISSIONS_ENABLED": "",
            },
            clear=True,
        ):
            environment = managed_runtime_environment(stack, Stage(DEV), bucket, config)

        self.assertEqual(environment["AWS_MANAGED_TENANT_IDS"], "vals.ai")
        self.assertEqual(environment["AWS_DEPLOYMENT_SANDBOX_PROVIDER"], "daytona")
        self.assertEqual(environment["AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME"], "")
        self.assertEqual(environment["AWS_MANAGED_AGENT_SECRET_NAMES"], "")
        self.assertEqual(environment["AWS_MANAGED_SUBMISSIONS_ENABLED"], "false")

    def test_managed_runtime_rejects_invalid_agent_secret_names(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(app, "InvalidAgentSecretStack")
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="",
        )

        with mock.patch.dict(os.environ, {"AWS_MANAGED_AGENT_SECRET_NAMES": "valid,*"}, clear=True):
            with self.assertRaisesRegex(ValueError, "Secret name must be a Secrets Manager name"):
                managed_runtime_environment(stack, Stage(DEV), bucket, config)

    def test_managed_runtime_rejects_invalid_tenant_ids(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(app, "InvalidOrgStack")
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="",
        )

        with mock.patch.dict(os.environ, {"AWS_MANAGED_TENANT_IDS": "vals.ai,bad tenant"}, clear=True):
            with self.assertRaisesRegex(ValueError, "unique, comma-separated tenant IDs"):
                managed_runtime_environment(stack, Stage(DEV), bucket, config)

    def test_task_role_rejects_wildcard_sandbox_secret_name(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(app, "InvalidSecretStack")
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="*",
        )

        with self.assertRaisesRegex(ValueError, "Secret name must be a Secrets Manager name"):
            create_tracker_task_role(stack, Stage(DEV), bucket, config)

    def test_managed_runtime_rejects_duplicate_tenant_ids(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(app, "EmptyOrgStack")
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        config = ManagedAWSRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
            sandbox_provider="daytona",
            sandbox_provider_secret_name="sandbox/provider",
        )

        with mock.patch.dict(
            os.environ,
            {
                "AWS_MANAGED_TENANT_IDS": "vals.ai,vals.ai",
                "AWS_MANAGED_SUBMISSIONS_ENABLED": "true",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "unique, comma-separated tenant IDs"):
                managed_runtime_environment(stack, Stage(DEV), bucket, config)


if __name__ == "__main__":
    unittest.main()
