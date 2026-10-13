import json
import os
import unittest
from dataclasses import replace
from typing import cast
from unittest import mock

import aws_cdk as cdk  # pyright: ignore[reportMissingImports]
from aws_cdk import assertions, aws_s3  # pyright: ignore[reportMissingImports]

from runtime_iam import (
    create_executor_task_role,
    create_tenant_access_role,
    create_tracker_task_role,
)
from stage import BENCH, DEV, PROD, RELEASE_TEST, Stage
from stage_config import BENCH_CONFIG, DEV_CONFIG, PROD_CONFIG, ManagedAWSRuntimeConfig
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


def _flatten(value: object) -> object:
    """Expand CloudFormation joins and the partition into the plain IAM JSON AWS evaluates."""
    if isinstance(value, dict):
        node = cast(dict[str, object], value)
        if "Fn::Join" in node:
            separator, parts = cast(tuple[str, list[object]], node["Fn::Join"])
            return separator.join(str(_flatten(part)) for part in parts)
        if node == {"Ref": "AWS::Partition"}:
            return "aws"
        return {key: _flatten(item) for key, item in node.items()}
    if isinstance(value, list):
        return [_flatten(item) for item in cast(list[object], value)]
    return value


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


def _task_role_container(template: assertions.Template, role_name: str) -> JsonObject:
    role_logical_id, _ = _named_role(template, role_name)
    task_definitions = cast(dict[str, JsonObject], template.find_resources("AWS::ECS::TaskDefinition"))
    role_task_definition = next(
        task_definition
        for task_definition in task_definitions.values()
        if cast(JsonObject, task_definition["Properties"]).get("TaskRoleArn")
        == {"Fn::GetAtt": [role_logical_id, "Arn"]}
    )
    containers = cast(list[JsonObject], role_task_definition["Properties"]["ContainerDefinitions"])
    return containers[0]


class RuntimeIamTest(unittest.TestCase):
    def test_only_tracker_creates_tenant_buckets_and_both_roles_use_them(self) -> None:
        """Tracker creates and configures valkyrie-<account>-<stage>-* buckets; executors only use their objects."""
        for stage_name in (DEV, BENCH, PROD, RELEASE_TEST):
            with self.subTest(stage=stage_name):
                stage = Stage(stage_name)
                stack = cdk.Stack(cdk.App(), "TenantBucketStack")
                bucket = aws_s3.Bucket.from_bucket_name(stack, "Bucket", f"agent-library-{stage_name}")
                create_tracker_task_role(stack, stage, bucket, DEV_CONFIG.managed_aws)
                create_executor_task_role(stack, stage, bucket, DEV_CONFIG.managed_aws)
                template = assertions.Template.from_stack(stack)
                buckets = stack.resolve(f"arn:{stack.partition}:s3:::valkyrie-{stack.account}-{stage_name}-*")
                same_account = {"StringEquals": {"s3:ResourceAccount": stack.resolve(stack.account)}}
                expected_actions = {
                    "ValkyrieTrackerTaskRole": {
                        "s3:CreateBucket",
                        "s3:ListBucket",
                        "s3:PutBucketVersioning",
                        "s3:PutLifecycleConfiguration",
                        "s3:TagResource",
                        "s3:GetBucketTagging",
                        "s3:PutBucketABAC",
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                        "s3:DeleteObject",
                        "s3:DeleteObjectVersion",
                    },
                    "ValkyrieExecutorRunnerTaskRole": {
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                    },
                }

                for role_name, actions in expected_actions.items():
                    role_logical_id, _ = _named_role(template, stage.phys(role_name))
                    tenant_statements = [
                        statement
                        for statement in _role_policy_statements(template, role_logical_id)
                        if f"-{stage_name}-*" in json.dumps(statement["Resource"])
                    ]
                    self.assertEqual(set[str]().union(*map(_statement_actions, tenant_statements)), actions)
                    for statement in tenant_statements:
                        if _statement_actions(statement) == {"s3:CreateBucket", "s3:TagResource"}:
                            self.assertEqual(statement["Resource"], buckets)
                            self.assertNotIn("Condition", statement)
                        else:
                            self.assertEqual(statement["Condition"], same_account)

    def test_tenant_access_roles_limit_bucket_objects_and_secrets_to_the_session_org(self) -> None:
        stage = Stage(DEV)
        stack = cdk.Stack(
            cdk.App(), "TenantAccessStack", env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        bucket = aws_s3.Bucket.from_bucket_name(stack, "Bucket", "agent-library-dev")
        managed_aws = replace(DEV_CONFIG.managed_aws, tracker_agent_library_read_only=True)
        tracker = create_tracker_task_role(stack, stage, bucket, managed_aws)
        executor = create_executor_task_role(stack, stage, bucket, managed_aws)
        create_tenant_access_role(stack, stage, bucket, managed_aws, service="Tracker", task_role=tracker)
        create_tenant_access_role(stack, stage, bucket, managed_aws, service="Executor", task_role=executor)
        template = assertions.Template.from_stack(stack)
        org_tag, scope_tag = "valkyrie:org-id", "valkyrie:secret-scope"
        session_org = f"${{aws:PrincipalTag/{org_tag}}}"
        secrets = "arn:aws:secretsmanager:us-east-1:123456789012:secret:"
        own_bucket = {"StringEquals": {f"aws:ResourceTag/{org_tag}": session_org, "s3:ResourceAccount": "123456789012"}}
        tagging = [
            "s3:TagResource",
            "s3:UntagResource",
            "s3:PutBucketTagging",
            "s3:DeleteBucketTagging",
            "s3:PutBucketABAC",
        ]

        def object_access(prefix: str, actions: list[str]) -> JsonObject:
            return {
                "Action": actions,
                "Condition": own_bucket,
                "Effect": "Allow",
                "Resource": f"arn:aws:s3:::*/{prefix}",
            }

        secret_reads: list[JsonObject] = [
            {
                "Action": "secretsmanager:GetSecretValue",
                "Condition": {"StringEquals": {f"secretsmanager:ResourceTag/{org_tag}": session_org}},
                "Effect": "Allow",
                "Resource": f"{secrets}valkyrie/tenants/{session_org}/*",
            },
            {
                "Action": "secretsmanager:GetSecretValue",
                "Condition": {
                    "StringEquals": {
                        f"secretsmanager:ResourceTag/{scope_tag}": "shared-internal",
                        f"aws:PrincipalTag/{scope_tag}": "shared-internal",
                    }
                },
                "Effect": "Allow",
                "Resource": f"{secrets}*",
            },
        ]
        list_buckets: JsonObject = {
            "Action": "s3:ListBucket",
            "Condition": own_bucket,
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::*",
        }
        expected: dict[str, tuple[str, list[JsonObject]]] = {
            "Tracker": (
                "ValkyrieTrackerTaskRole",
                [
                    list_buckets,
                    object_access(
                        "agents/*", ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"]
                    ),
                    object_access(
                        "benchmarks/*", ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]
                    ),
                    *secret_reads,
                    {
                        "Action": ["secretsmanager:CreateSecret", "secretsmanager:TagResource"],
                        "Condition": {
                            "StringEquals": {f"aws:RequestTag/{org_tag}": session_org},
                            "ForAllValues:StringEquals": {"aws:TagKeys": [org_tag]},
                        },
                        "Effect": "Allow",
                        "Resource": f"{secrets}valkyrie/tenants/{session_org}/*",
                    },
                    {
                        "Action": ["secretsmanager:PutSecretValue", "secretsmanager:DeleteSecret"],
                        "Condition": {"StringEquals": {f"secretsmanager:ResourceTag/{org_tag}": session_org}},
                        "Effect": "Allow",
                        "Resource": f"{secrets}valkyrie/tenants/{session_org}/*",
                    },
                    {"Action": "secretsmanager:ListSecrets", "Effect": "Allow", "Resource": "*"},
                    {
                        "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"],
                        "Effect": "Deny",
                        "Resource": "arn:aws:s3:::agent-library-dev/agents/*",
                    },
                    {"Action": ["secretsmanager:UntagResource", *tagging], "Effect": "Deny", "Resource": "*"},
                ],
            ),
            "Executor": (
                "ValkyrieExecutorRunnerTaskRole",
                [
                    list_buckets,
                    object_access("benchmarks/*", ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]),
                    *secret_reads,
                    {
                        "Action": ["secretsmanager:UntagResource", *tagging, "secretsmanager:TagResource"],
                        "Effect": "Deny",
                        "Resource": "*",
                    },
                ],
            ),
        }

        for service, (task_role_name, statements) in expected.items():
            with self.subTest(service=service):
                task_role_id = _named_role(template, stage.phys(task_role_name))[0]
                tenant_id, tenant = _named_role(template, stage.phys(f"Valkyrie{service}TenantAccessRole"))
                properties = cast(JsonObject, tenant["Properties"])
                self.assertEqual(properties["MaxSessionDuration"], 3600)
                self.assertEqual(
                    cast(JsonObject, properties["AssumeRolePolicyDocument"])["Statement"],
                    [
                        {
                            "Action": ["sts:AssumeRole", "sts:TagSession"],
                            "Condition": {
                                "StringLike": {f"aws:RequestTag/{org_tag}": "?*"},
                                "ForAllValues:StringEquals": {"aws:TagKeys": [org_tag, scope_tag]},
                            },
                            "Effect": "Allow",
                            "Principal": {"AWS": {"Fn::GetAtt": [task_role_id, "Arn"]}},
                        }
                    ],
                )
                self.assertEqual(_flatten(_role_policy_statements(template, tenant_id)), statements)
                self.assertIn(
                    {
                        "Action": ["sts:AssumeRole", "sts:TagSession"],
                        "Effect": "Allow",
                        "Resource": {"Fn::GetAtt": [tenant_id, "Arn"]},
                    },
                    _role_policy_statements(template, task_role_id),
                )

    def test_each_container_gets_its_own_tenant_role_only_with_authentication(self) -> None:
        for stage_name, environment, enabled in (
            (DEV, TEST_DEV_ENV, True),
            (BENCH, TEST_BENCH_ENV, False),
            (
                BENCH,
                {
                    **TEST_BENCH_ENV,
                    "AUTH_REQUIRED": "true",
                    "DESCOPE_PROJECT_ID": "project",
                    "DESCOPE_MANAGEMENT_KEY_SECRET_NAME": "descope-management-key",
                },
                True,
            ),
        ):
            with self.subTest(stage=stage_name, enabled=enabled):
                with mock.patch.dict(os.environ, environment, clear=True):
                    tracker_template, _, _ = service_templates(stage_name)
                role_names = {
                    cast(JsonObject, resource["Properties"]).get("RoleName")
                    for resource in cast(JsonObject, tracker_template.to_json()["Resources"]).values()
                    if cast(JsonObject, resource)["Type"] == "AWS::IAM::Role"
                }
                for task_role, service in (
                    ("ValkyrieTrackerTaskRole", "Tracker"),
                    ("ValkyrieExecutorRunnerTaskRole", "Executor"),
                ):
                    tenant_role = Stage(stage_name).phys(f"Valkyrie{service}TenantAccessRole")
                    task_role_id = _named_role(tracker_template, Stage(stage_name).phys(task_role))[0]
                    container = _task_role_container(tracker_template, Stage(stage_name).phys(task_role))
                    arn = {
                        cast(str, variable["Name"]): variable["Value"]
                        for variable in cast(list[JsonObject], container["Environment"])
                    }["AWS_TENANT_ACCESS_ROLE_ARN"]
                    assumes = [
                        statement["Resource"]
                        for statement in _role_policy_statements(tracker_template, task_role_id)
                        if "sts:AssumeRole" in _statement_actions(statement)
                    ]
                    self.assertEqual(tenant_role in role_names, enabled)
                    if enabled:
                        tenant_id = _named_role(tracker_template, tenant_role)[0]
                        self.assertEqual(arn, {"Fn::GetAtt": [tenant_id, "Arn"]})
                        self.assertEqual(assumes, [{"Fn::GetAtt": [tenant_id, "Arn"]}])
                    else:
                        self.assertEqual(arn, "")
                        self.assertEqual(assumes, [])

    def test_only_tracker_receives_the_tenant_bucket_prefix(self) -> None:
        with mock.patch.dict(os.environ, TEST_BENCH_ENV, clear=True):
            tracker_template, _, _ = service_templates(BENCH)

        for role_name, expected in (
            ("ValkyrieTrackerTaskRole", f"valkyrie-{TEST_AWS_ACCOUNT}-{BENCH}"),
            ("ValkyrieExecutorRunnerTaskRole", None),
        ):
            with self.subTest(role=role_name):
                container = _task_role_container(tracker_template, role_name)
                environment = {
                    cast(str, variable["Name"]): variable["Value"]
                    for variable in cast(list[JsonObject], container["Environment"])
                }
                self.assertEqual(environment.get("AWS_TENANT_BUCKET_PREFIX"), expected)

    def test_tracker_writes_agent_bundles(self) -> None:
        """Tracker can upload and remove agents in every stage; executors only read agents."""
        for stage_name in (DEV, BENCH, PROD, RELEASE_TEST):
            with self.subTest(stage=stage_name):
                stage = Stage(stage_name)
                stack = cdk.Stack(cdk.App(), "AgentUploadStack")
                bucket = aws_s3.Bucket.from_bucket_name(stack, "Bucket", f"agent-library-{stage_name}")
                create_tracker_task_role(stack, stage, bucket, DEV_CONFIG.managed_aws)
                create_executor_task_role(stack, stage, bucket, DEV_CONFIG.managed_aws)
                template = assertions.Template.from_stack(stack)

                for role_name in ("ValkyrieTrackerTaskRole", "ValkyrieExecutorRunnerTaskRole"):
                    role_logical_id, _ = _named_role(template, stage.phys(role_name))
                    agent_statements = [
                        statement
                        for statement in _role_policy_statements(template, role_logical_id)
                        if "agents/*" in json.dumps(statement["Resource"])
                    ]
                    actions = set[str]().union(*(_statement_actions(statement) for statement in agent_statements))
                    expected_actions = {"s3:GetObject"}
                    if role_name == "ValkyrieTrackerTaskRole":
                        expected_actions |= {"s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"}
                        upload_statement = next(
                            statement
                            for statement in agent_statements
                            if "s3:PutObject" in _statement_actions(statement)
                        )
                        self.assertEqual(
                            upload_statement["Resource"], stack.resolve(bucket.arn_for_objects("agents/*"))
                        )
                        self.assertEqual(
                            upload_statement["Condition"],
                            {"StringEquals": {"s3:ResourceAccount": stack.resolve(stack.account)}},
                        )
                    self.assertEqual(actions, expected_actions)

    def test_bench_tracker_cannot_change_agents(self) -> None:
        """Bench Tracker keeps the agent library read-only; prod and dev keep normal agent writes."""
        for stage_name, managed_aws in (
            (BENCH, BENCH_CONFIG.managed_aws),
            (PROD, PROD_CONFIG.managed_aws),
            (DEV, DEV_CONFIG.managed_aws),
        ):
            with self.subTest(stage=stage_name):
                stage = Stage(stage_name)
                stack = cdk.Stack(cdk.App(), "AgentDenyStack")
                bucket = aws_s3.Bucket.from_bucket_name(stack, "Bucket", f"agent-library-{stage_name}")
                create_tracker_task_role(stack, stage, bucket, managed_aws)
                template = assertions.Template.from_stack(stack)
                role_logical_id, _ = _named_role(template, stage.phys("ValkyrieTrackerTaskRole"))

                agent_denials = [
                    statement
                    for statement in _role_policy_statements(template, role_logical_id)
                    if statement.get("Effect") == "Deny" and "agents/" in json.dumps(statement["Resource"])
                ]

                if stage_name == BENCH:
                    self.assertEqual(len(agent_denials), 1)
                    self.assertEqual(
                        _statement_actions(agent_denials[0]),
                        {"s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"},
                    )
                    self.assertEqual(agent_denials[0]["Resource"], stack.resolve(bucket.arn_for_objects("agents/*")))
                else:
                    self.assertEqual(agent_denials, [])

    def test_organization_identifiers_stay_plaintext_container_settings(self) -> None:
        with mock.patch.dict(os.environ, TEST_BENCH_ENV, clear=True):
            tracker_template, _, _ = service_templates(BENCH)

        organization_settings = ("AWS_DEPLOYMENT_ROLE_ORG_IDS",)
        for template, role_name in (
            (tracker_template, "ValkyrieTrackerTaskRole"),
            (tracker_template, "ValkyrieExecutorRunnerTaskRole"),
        ):
            with self.subTest(role=role_name):
                container = _task_role_container(template, role_name)
                plaintext = {
                    cast(str, variable["Name"]) for variable in cast(list[JsonObject], container["Environment"])
                }
                referenced = {
                    cast(str, variable["Name"]) for variable in cast(list[JsonObject], container.get("Secrets", []))
                }

                self.assertTrue(referenced)
                for name in organization_settings:
                    self.assertIn(name, plaintext)
                    self.assertNotIn(name, referenced)

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
            tracker_template, _, _ = service_templates(DEV)

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
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion",
                    "secretsmanager:GetSecretValue",
                    "lambda:InvokeFunction",
                    "logs:GetLogEvents",
                    "logs:FilterLogEvents",
                },
            ),
            (
                tracker_template,
                "ValkyrieExecutorRunnerTaskRole-dev",
                "ExecutorRunnerTaskRoleArn",
                expected_actions
                | {
                    "s3:AbortMultipartUpload",
                    "secretsmanager:GetSecretValue",
                    "logs:CreateLogGroup",
                    "logs:PutRetentionPolicy",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
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
                if role_name.startswith("ValkyrieTracker"):
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
                self.assertTrue(service_actions <= actions)

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
                        self.fail(f"Unscoped task role action: {statement}")

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
            tracker_template, _, _ = service_templates(BENCH)

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
            (tracker_template, "ValkyrieExecutorRunnerTaskRole"),
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
            tracker_template, _, _ = service_templates(RELEASE_TEST)

        expected_environment = assertions.Match.array_with(
            [
                {"Name": "AWS_DEPLOYMENT_ROLE_ORG_IDS", "Value": ""},
                {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "false"},
            ]
        )
        for template, role_name in (
            (tracker_template, "ValkyrieTrackerTaskRole-release-test"),
            (tracker_template, "ValkyrieExecutorRunnerTaskRole-release-test"),
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
            ("ValkyrieExecutorRunnerTaskRole-dev", "valkyrie/executor/", "valkyrie-post-run-*"),
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
