import json
import os
import unittest
from typing import Any, cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import (
    assertions,
    aws_ec2,
    aws_ecs,
    aws_elasticache,
    aws_elasticloadbalancingv2 as aws_elb,
    aws_rds,
    aws_s3,
)

from constants import (
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV,
    SLACK_WORKSPACE_ID_ENV,
    TRACKER_LOG_GROUP_NAME,
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV,
    WORKER_LOG_GROUP_NAME,
    get_slack_notification_config,
)
from monitoring_stack import MonitoringStack
from runtime_iam import create_tracker_task_role, create_worker_task_role
from shared import SharedStack
from stage import DEV, DEV_STACK_PREFIX, PROD, Stage
from stage_config import ManagedAwsRuntimeConfig
from tracker_stack import TrackerStack
from worker_stack import WorkerStack

TEST_ALERTS_SLACK_ENV = {
    SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE",
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV: "CALERTSCHANNEL",
}
TEST_DEPLOYMENT_SLACK_ENV = {
    SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE",
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV: "CDEPLOYCHANNEL",
}
TEST_DEV_ENV = {"DESCOPE_PROJECT_ID": "dev-project"}
TEST_AWS_ACCOUNT = os.environ.get("CDK_DEFAULT_ACCOUNT", "123456789012")
TEST_AWS_REGION = os.environ.get("CDK_DEFAULT_REGION", "us-east-1")
SHARED_STACK_CONTEXT = {
    f"availability-zones:account={TEST_AWS_ACCOUNT}:region={TEST_AWS_REGION}": [
        f"{TEST_AWS_REGION}a",
        f"{TEST_AWS_REGION}b",
    ],
    f"hosted-zone:account={TEST_AWS_ACCOUNT}:domainName=vals.ai:region={TEST_AWS_REGION}": {
        "Id": "/hostedzone/ZTESTVALKYRIE",
        "Name": "vals.ai.",
    },
}
JsonObject = dict[str, Any]


def _has_resource_property(
    template: assertions.Template,
    resource_type: str,
    property_name: str,
    expected_value: object,
) -> bool:
    return any(
        resource.get("Properties", {}).get(property_name) == expected_value
        for resource in template.find_resources(resource_type).values()
    )


def _has_logical_id_prefix(template: assertions.Template, resource_type: str, prefix: str) -> bool:
    return any(logical_id.startswith(prefix) for logical_id in template.find_resources(resource_type))


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


def _monitoring_template(stage_name: str = PROD) -> assertions.Template:
    app = cdk.App()
    stage = Stage(stage_name)
    resources = cdk.Stack(
        app,
        "MonitoringTestResources",
        env=cdk.Environment(account="123456789012", region="us-west-2"),
    )

    vpc = aws_ec2.Vpc(resources, "Vpc", max_azs=2)
    cluster = aws_ecs.Cluster(resources, "Cluster", vpc=vpc, cluster_name=stage.phys("AgenticHarnessCluster"))

    tracker_task = aws_ecs.FargateTaskDefinition(resources, "TrackerTask")
    tracker_task.add_container("TrackerContainer", image=aws_ecs.ContainerImage.from_registry("busybox"))
    tracker_service = aws_ecs.FargateService(
        resources,
        "TrackerService",
        cluster=cluster,
        task_definition=tracker_task,
        service_name=stage.phys("Tracker"),
    )

    worker_task = aws_ecs.FargateTaskDefinition(resources, "WorkerTask")
    worker_task.add_container("WorkerContainer", image=aws_ecs.ContainerImage.from_registry("busybox"))
    worker_service = aws_ecs.FargateService(
        resources,
        "WorkerService",
        cluster=cluster,
        task_definition=worker_task,
        service_name=stage.phys("Worker"),
    )

    load_balancer = aws_elb.ApplicationLoadBalancer(resources, "LoadBalancer", vpc=vpc)
    target_group = aws_elb.ApplicationTargetGroup(resources, "TargetGroup", vpc=vpc, port=8000)
    load_balancer.add_listener("HttpListener", port=80, default_target_groups=[target_group])
    database = aws_rds.DatabaseInstance(
        resources,
        "Database",
        engine=aws_rds.DatabaseInstanceEngine.postgres(version=aws_rds.PostgresEngineVersion.VER_16),
        instance_type=aws_ec2.InstanceType("t4g.micro"),
        vpc=vpc,
        credentials=aws_rds.Credentials.from_generated_secret("tracker"),
        allocated_storage=20,
    )
    redis_cluster = aws_elasticache.CfnCacheCluster(
        resources,
        "RedisCluster",
        cache_node_type="cache.t4g.micro",
        engine="redis",
        num_cache_nodes=1,
    )
    monitoring = MonitoringStack(
        app,
        "MonitoringStack",
        stage=stage,
        cluster=cluster,
        tracker_service=tracker_service,
        worker_service=worker_service,
        load_balancer=load_balancer,
        target_group=target_group,
        database=database,
        redis_cluster=redis_cluster,
        env=cdk.Environment(account="123456789012", region="us-west-2"),
    )

    return assertions.Template.from_stack(monitoring)


def _shared_template(stage_name: str = PROD) -> assertions.Template:
    app = cdk.App(context=SHARED_STACK_CONTEXT)
    stage = Stage(stage_name)
    shared = SharedStack(
        app,
        stage.stack_id("SharedStack"),
        stage=stage,
        env=cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION),
    )

    return assertions.Template.from_stack(shared)


def _service_templates(stage_name: str) -> tuple[assertions.Template, assertions.Template]:
    app = cdk.App(context=SHARED_STACK_CONTEXT)
    stage = Stage(stage_name)
    env = cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION)
    shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=env)
    tracker = TrackerStack(
        app,
        stage.stack_id("TrackerStack"),
        stage=stage,
        vpc=shared.vpc,
        cluster=shared.cluster,
        namespace=shared.namespace,
        hosted_zone=shared.hosted_zone,
        bucket_name=shared.bucket_name,
        redis_url=shared.redis_url,
        env=env,
    )
    worker = WorkerStack(
        app,
        stage.stack_id("WorkerStack"),
        stage=stage,
        vpc=shared.vpc,
        cluster=shared.cluster,
        namespace=shared.namespace,
        redis_url=shared.redis_url,
        bucket_name=shared.bucket_name,
        database=tracker.database,
        db_credentials=tracker.db_credentials,
        tracker_service=tracker.tracker_fargate_service,
        env=env,
    )

    return assertions.Template.from_stack(tracker), assertions.Template.from_stack(worker)


class MonitoringStackTest(unittest.TestCase):
    def test_dev_stack_ids_are_valk_scoped(self) -> None:
        self.assertEqual(Stage(PROD).stack_id("TrackerStack"), "TrackerStack")
        self.assertEqual(Stage(DEV).stack_id("TrackerStack"), f"{DEV_STACK_PREFIX}TrackerStack")

    def test_alerts_topic_is_wired_to_slack(self) -> None:
        with mock.patch.dict(os.environ, TEST_ALERTS_SLACK_ENV, clear=True):
            template = _monitoring_template()

        template.has_resource_properties(
            "AWS::Chatbot::SlackChannelConfiguration",
            {
                "ConfigurationName": "valkyrie-alerts",
                "SlackChannelId": TEST_ALERTS_SLACK_ENV[VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV],
                "SlackWorkspaceId": TEST_ALERTS_SLACK_ENV[SLACK_WORKSPACE_ID_ENV],
                "SnsTopicArns": assertions.Match.array_with(
                    [{"Ref": assertions.Match.string_like_regexp("ValkyrieAlertsTopic")}]
                ),
            },
        )

    def test_deployment_notifications_are_wired_to_deployment_slack_channel(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEPLOYMENT_SLACK_ENV, clear=True):
            template = _shared_template()

        template.has_resource_properties(
            "AWS::Chatbot::SlackChannelConfiguration",
            {
                "ConfigurationName": "deployment-notifications",
                "SlackChannelId": TEST_DEPLOYMENT_SLACK_ENV[DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV],
                "SlackWorkspaceId": TEST_DEPLOYMENT_SLACK_ENV[SLACK_WORKSPACE_ID_ENV],
                "SnsTopicArns": assertions.Match.array_with(
                    [{"Ref": assertions.Match.string_like_regexp("StackNotificationTopic")}]
                ),
            },
        )

    def test_missing_slack_environment_values_skip_slack_wiring(self) -> None:
        for env in (
            {},
            {SLACK_WORKSPACE_ID_ENV: "", VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV: ""},
            {SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE"},
            TEST_DEPLOYMENT_SLACK_ENV,
        ):
            with self.subTest(env=env), mock.patch.dict(os.environ, env, clear=True):
                self.assertIsNone(get_slack_notification_config(VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV))
                template = _monitoring_template()

                self.assertFalse(
                    _has_resource_property(
                        template,
                        "AWS::Chatbot::SlackChannelConfiguration",
                        "ConfigurationName",
                        "valkyrie-alerts",
                    )
                )

    def test_missing_slack_environment_skips_deployment_notification_resources(self) -> None:
        for env in ({}, {SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE"}, TEST_ALERTS_SLACK_ENV):
            with self.subTest(env=env), mock.patch.dict(os.environ, env, clear=True):
                template = _shared_template()

                self.assertFalse(
                    _has_resource_property(
                        template,
                        "AWS::Chatbot::SlackChannelConfiguration",
                        "ConfigurationName",
                        "deployment-notifications",
                    )
                )
                self.assertFalse(_has_logical_id_prefix(template, "AWS::Events::Rule", "StackDeploy"))
                self.assertFalse(
                    _has_resource_property(
                        template,
                        "AWS::SNS::Topic",
                        "TopicName",
                        "agentic-harness-notifications",
                    )
                )

    def test_partial_slack_environment_values_raise_clear_error(self) -> None:
        with mock.patch.dict(
            os.environ,
            {VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV: "CALERTSCHANNEL"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Incomplete Slack notification environment configuration. "
                f"Set {SLACK_WORKSPACE_ID_ENV} when setting {VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV}. "
                f"Missing: {SLACK_WORKSPACE_ID_ENV}",
            ):
                get_slack_notification_config(VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV)

    def test_dev_stage_wires_stage_config_to_resources(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEV_ENV, clear=True):
            tracker_template, worker_template = _service_templates(DEV)
        with mock.patch.dict(os.environ, TEST_ALERTS_SLACK_ENV, clear=True):
            monitoring_template = _monitoring_template(DEV)

        tracker_template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {"LogGroupName": f"{TRACKER_LOG_GROUP_NAME}-dev", "RetentionInDays": 7},
        )
        tracker_template.has_resource_properties(
            "AWS::RDS::DBInstance",
            {"DBInstanceClass": "db.t4g.micro", "BackupRetentionPeriod": 1},
        )
        tracker_template.has_resource_properties(
            "AWS::ApplicationAutoScaling::ScalableTarget",
            {"MinCapacity": 1, "MaxCapacity": 1},
        )
        worker_template.has_resource_properties(
            "AWS::ECS::Service",
            {"DesiredCount": 1},
        )
        worker_template.has_resource_properties(
            "AWS::ApplicationAutoScaling::ScalableTarget",
            {"MinCapacity": 1, "MaxCapacity": 2},
        )
        worker_template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {"LogGroupName": f"{WORKER_LOG_GROUP_NAME}-dev", "RetentionInDays": 7},
        )
        monitoring_template.has_resource_properties(
            "AWS::CloudWatch::Alarm",
            {"AlarmName": "Valkyrie-DB-Connections-High-dev", "Threshold": 65},
        )

    def test_service_environment_labels_follow_stage(self) -> None:
        for stage_name, expected_environment, expected_namespace in (
            (PROD, "production", "local"),
            (DEV, "dev", "local-dev"),
        ):
            environment = TEST_DEV_ENV if stage_name == DEV else {}
            with self.subTest(stage=stage_name), mock.patch.dict(os.environ, environment, clear=True):
                tracker_template, worker_template = _service_templates(stage_name)

                expected_env = assertions.Match.array_with(
                    [
                        {"Name": "BROKER_ENVIRONMENT", "Value": expected_environment},
                        {"Name": "ENVIRONMENT", "Value": expected_environment},
                        {"Name": "BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE", "Value": expected_namespace},
                    ]
                )
                tracker_template.has_resource_properties(
                    "AWS::ECS::TaskDefinition",
                    {
                        "ContainerDefinitions": assertions.Match.array_with(
                            [assertions.Match.object_like({"Environment": expected_env})]
                        )
                    },
                )
                worker_template.has_resource_properties(
                    "AWS::ECS::TaskDefinition",
                    {
                        "ContainerDefinitions": assertions.Match.array_with(
                            [assertions.Match.object_like({"Environment": expected_env})]
                        )
                    },
                )

    def test_dev_sentry_secret_is_optional(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEV_ENV, clear=True):
            tracker_template, worker_template = _service_templates(DEV)

        self.assertNotIn("SENTRY_DSN", str(tracker_template.to_json()))
        self.assertNotIn("SENTRY_DSN", str(worker_template.to_json()))

        custom_sentry_secret_name = "custom/dev-sentry-dsn"
        sentry_environment = {
            **TEST_DEV_ENV,
            "SENTRY_DSN_SECRET_NAME": custom_sentry_secret_name,
        }
        with mock.patch.dict(os.environ, sentry_environment, clear=True):
            tracker_template, worker_template = _service_templates(DEV)

        for template in (tracker_template, worker_template):
            template.has_resource_properties(
                "AWS::ECS::TaskDefinition",
                {
                    "ContainerDefinitions": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Secrets": assertions.Match.array_with(
                                        [assertions.Match.object_like({"Name": "SENTRY_DSN"})]
                                    )
                                }
                            )
                        ]
                    )
                },
            )
            sentry_value_from = [
                secret["ValueFrom"]
                for task_definition in template.find_resources("AWS::ECS::TaskDefinition").values()
                for container in task_definition["Properties"]["ContainerDefinitions"]
                for secret in container.get("Secrets", [])
                if secret["Name"] == "SENTRY_DSN"
            ]
            self.assertEqual(len(sentry_value_from), 1)
            self.assertIn(custom_sentry_secret_name, str(sentry_value_from[0]))

    def test_managed_runtime_task_roles_are_scoped_and_closed_by_default(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            tracker_template, worker_template = _service_templates(DEV)

        expected_environment = assertions.Match.array_with(
            [
                {"Name": "AWS_DEPLOYMENT_ROLE_ORG_IDS", "Value": ""},
                {"Name": "AWS_DEPLOYMENT_REGION", "Value": TEST_AWS_REGION},
                assertions.Match.object_like({"Name": "AWS_DEPLOYMENT_S3_BUCKET"}),
                {"Name": "AWS_DEPLOYMENT_LOG_GROUP", "Value": "/valkyrie/benchmarks-dev"},
                {"Name": "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "Value": "7"},
                {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "false"},
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
                expected_actions,
            ),
            (
                worker_template,
                "ValkyrieWorkerTaskRole-dev",
                "WorkerTaskRoleArn",
                expected_actions
                | {
                    "logs:CreateLogGroup",
                    "logs:PutRetentionPolicy",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "ecs:UpdateTaskProtection",
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
                self.assertFalse(
                    actions
                    & {
                        "s3:DeleteObject",
                        "sts:AssumeRole",
                        "secretsmanager:GetSecretValue",
                        "lambda:InvokeFunction",
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                    }
                )

                list_statement = next(
                    statement for statement in statements if _statement_actions(statement) == {"s3:ListBucket"}
                )
                self.assertEqual(
                    list_statement["Condition"],
                    {"StringLike": {"s3:prefix": ["agents/*", "benchmarks/*"]}},
                )
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

                for statement in statements:
                    resources = statement["Resource"]
                    if resources == "*" or (isinstance(resources, list) and "*" in resources):
                        self.assertEqual(_statement_actions(statement), {"ecs:UpdateTaskProtection"})

                if role_name.startswith("ValkyrieWorker"):
                    log_statement = next(
                        statement for statement in statements if "logs:CreateLogGroup" in _statement_actions(statement)
                    )
                    self.assertIn("/valkyrie/benchmarks-dev/*", json.dumps(log_statement["Resource"]))
                    log_stream_statement = next(
                        statement for statement in statements if "logs:PutLogEvents" in _statement_actions(statement)
                    )
                    self.assertIn(":log-stream:*", json.dumps(log_stream_statement["Resource"]))

    def test_managed_runtime_optional_grants_are_limited_to_configured_resources(self) -> None:
        app = cdk.App()
        stack = cdk.Stack(
            app,
            "RuntimeIamStack",
            env=cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION),
        )
        bucket = aws_s3.Bucket.from_bucket_name(stack, "ManagedRuntimeBucket", "managed-runtime-bucket")
        kms_key_arn = f"arn:aws:kms:{TEST_AWS_REGION}:{TEST_AWS_ACCOUNT}:key/test-key"
        config = ManagedAwsRuntimeConfig(
            benchmark_log_group_prefix="/valkyrie/benchmarks",
            benchmark_log_retention_days=7,
            tracker_secret_name_prefixes=("valkyrie/tracker/",),
            worker_secret_name_prefixes=("valkyrie/worker/",),
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
