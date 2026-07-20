import os
import unittest
from unittest import mock
from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import (
    assertions,
    aws_ec2,
    aws_ecs,
    aws_elasticache,
    aws_elasticloadbalancingv2 as aws_elb,
    aws_rds,
)

from constants import (
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV,
    SANDBOX_CLEANUP_DLQ_NAME,
    SANDBOX_CLEANUP_FUNCTION_NAME,
    SANDBOX_CLEANUP_LOG_GROUP_NAME,
    SANDBOX_CLEANUP_SCHEDULE_NAME,
    SANDBOX_CLEANUP_SECRET_NAME,
    SLACK_WORKSPACE_ID_ENV,
    TRACKER_LOG_GROUP_NAME,
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV,
    WORKER_LOG_GROUP_NAME,
    get_slack_notification_config,
)
from monitoring_stack import MonitoringStack
from shared import SharedStack
from stage import DEV, DEV_STACK_PREFIX, PROD, Stage
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


def _cleanup_function(template: assertions.Template) -> dict[str, Any]:
    for resource in template.find_resources("AWS::Lambda::Function").values():
        properties = cast(dict[str, Any], resource.get("Properties", {}))
        if properties.get("FunctionName") == SANDBOX_CLEANUP_FUNCTION_NAME:
            return properties
    raise AssertionError("Sandbox cleanup Lambda function not found")


def _resource_with_logical_id_prefix(
    template: assertions.Template,
    resource_type: str,
    prefix: str,
) -> tuple[str, dict[str, Any]]:
    for logical_id, resource in template.find_resources(resource_type).items():
        if logical_id.startswith(prefix):
            return logical_id, cast(dict[str, Any], resource)
    raise AssertionError(f"{resource_type} resource with prefix {prefix!r} not found")


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
        bucket=shared.bucket,
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
        bucket=shared.bucket,
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

    def test_dev_does_not_create_sandbox_cleanup_resources(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                **TEST_DEV_ENV,
                "SANDBOX_CLEANUP_ENABLED": "true",
                "SANDBOX_CLEANUP_DRY_RUN": "false",
            },
            clear=True,
        ):
            _, worker_template = _service_templates(DEV)

        worker_template.resource_count_is("AWS::Scheduler::Schedule", 0)
        worker_template.resource_count_is("AWS::Lambda::Function", 0)
        worker_template.resource_count_is("AWS::Lambda::EventInvokeConfig", 0)
        self.assertFalse(
            _has_resource_property(
                worker_template,
                "AWS::Logs::LogGroup",
                "LogGroupName",
                f"{SANDBOX_CLEANUP_LOG_GROUP_NAME}-dev",
            )
        )
        self.assertFalse(
            _has_resource_property(
                worker_template,
                "AWS::SQS::Queue",
                "QueueName",
                f"{SANDBOX_CLEANUP_DLQ_NAME}-dev",
            )
        )

    def test_prod_sandbox_cleanup_schedule_is_safe_by_default(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            _, worker_template = _service_templates(PROD)

        worker_template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {
                "Name": SANDBOX_CLEANUP_SCHEDULE_NAME,
                "ScheduleExpression": "rate(1 hour)",
                "State": "DISABLED",
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "Target": assertions.Match.object_like(
                    {
                        "DeadLetterConfig": assertions.Match.any_value(),
                        "RetryPolicy": {
                            "MaximumEventAgeInSeconds": 1800,
                            "MaximumRetryAttempts": 1,
                        },
                    }
                ),
            },
        )

        worker_template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Architectures": ["arm64"],
                "Description": "Delete sandboxes older than 48 hours unless they opt out",
                "FunctionName": SANDBOX_CLEANUP_FUNCTION_NAME,
                "MemorySize": 512,
                "PackageType": "Image",
                "ReservedConcurrentExecutions": 1,
                "Timeout": 14 * 60,
            },
        )
        worker_template.has_resource_properties(
            "AWS::Lambda::EventInvokeConfig",
            {
                "DestinationConfig": {
                    "OnFailure": {"Destination": assertions.Match.any_value()},
                },
                "MaximumEventAgeInSeconds": 1800,
                "MaximumRetryAttempts": 0,
                "Qualifier": "$LATEST",
            },
        )
        worker_template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {"LogGroupName": SANDBOX_CLEANUP_LOG_GROUP_NAME, "RetentionInDays": 365},
        )
        worker_template.has_resource_properties(
            "AWS::SQS::Queue",
            {
                "QueueName": SANDBOX_CLEANUP_DLQ_NAME,
                "MessageRetentionPeriod": 14 * 24 * 60 * 60,
                "SqsManagedSseEnabled": True,
            },
        )
        cleanup_function_role_id, cleanup_function_role = _resource_with_logical_id_prefix(
            worker_template,
            "AWS::IAM::Role",
            "SandboxCleanupFunctionServiceRole",
        )
        cleanup_function_role_properties = cast(dict[str, Any], cleanup_function_role["Properties"])
        self.assertEqual(
            cleanup_function_role_properties["AssumeRolePolicyDocument"]["Statement"][0]["Principal"],
            {"Service": "lambda.amazonaws.com"},
        )
        self.assertIn("AWSLambdaBasicExecutionRole", str(cleanup_function_role_properties["ManagedPolicyArns"]))

        _, cleanup_function_policy = _resource_with_logical_id_prefix(
            worker_template,
            "AWS::IAM::Policy",
            "SandboxCleanupFunctionServiceRoleDefaultPolicy",
        )
        cleanup_function_policy_properties = cast(dict[str, Any], cleanup_function_policy["Properties"])
        self.assertIn({"Ref": cleanup_function_role_id}, cleanup_function_policy_properties["Roles"])
        cleanup_function_statements = cast(
            list[dict[str, Any]],
            cleanup_function_policy_properties["PolicyDocument"]["Statement"],
        )
        secret_statement = next(
            statement
            for statement in cleanup_function_statements
            if "secretsmanager:GetSecretValue" in statement["Action"]
        )
        self.assertEqual(
            set(secret_statement["Action"]),
            {"secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"},
        )
        self.assertNotEqual(secret_statement["Resource"], "*")
        self.assertIn(SANDBOX_CLEANUP_SECRET_NAME, str(secret_statement["Resource"]))

        cleanup_function_actions: set[str] = set()
        for statement in cleanup_function_statements:
            self.assertNotEqual(statement["Resource"], "*")
            actions = statement["Action"]
            if isinstance(actions, str):
                cleanup_function_actions.add(actions)
            else:
                cleanup_function_actions.update(cast(list[str], actions))
        self.assertEqual(
            cleanup_function_actions,
            {
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "sqs:SendMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
            },
        )

        scheduler_role_id, scheduler_role = _resource_with_logical_id_prefix(
            worker_template,
            "AWS::IAM::Role",
            "SchedulerRoleForTarget",
        )
        scheduler_role_properties = cast(dict[str, Any], scheduler_role["Properties"])
        self.assertEqual(
            scheduler_role_properties["AssumeRolePolicyDocument"]["Statement"][0]["Principal"],
            {"Service": "scheduler.amazonaws.com"},
        )
        _, scheduler_policy = _resource_with_logical_id_prefix(
            worker_template,
            "AWS::IAM::Policy",
            "SchedulerRoleForTarget",
        )
        scheduler_policy_properties = cast(dict[str, Any], scheduler_policy["Properties"])
        self.assertIn({"Ref": scheduler_role_id}, scheduler_policy_properties["Roles"])
        scheduler_statements = cast(
            list[dict[str, Any]],
            scheduler_policy_properties["PolicyDocument"]["Statement"],
        )
        scheduler_actions: set[str] = set()
        for statement in scheduler_statements:
            self.assertNotEqual(statement["Resource"], "*")
            actions = statement["Action"]
            if isinstance(actions, str):
                scheduler_actions.add(actions)
            else:
                scheduler_actions.update(cast(list[str], actions))
        self.assertEqual(scheduler_actions, {"lambda:InvokeFunction", "sqs:SendMessage"})

        cleanup_function = _cleanup_function(worker_template)
        self.assertNotIn("VpcConfig", cleanup_function)
        environment = cast(dict[str, str], cast(dict[str, Any], cleanup_function["Environment"])["Variables"])
        self.assertEqual(
            environment,
            {
                "SANDBOX_CLEANUP_DRY_RUN": "true",
                "SANDBOX_CLEANUP_PROVIDER": "daytona",
                "SANDBOX_CLEANUP_SECRET_NAME": SANDBOX_CLEANUP_SECRET_NAME,
                "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
                "ENVIRONMENT": "production",
            },
        )

    def test_prod_sandbox_cleanup_rollout_configuration_is_configurable(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "SANDBOX_CLEANUP_ENABLED": "true",
                "SANDBOX_CLEANUP_DRY_RUN": "false",
                "SANDBOX_CLEANUP_PROVIDER": "custom-provider",
                "SANDBOX_CLEANUP_SECRET_NAME": "custom/cleanup-credentials",
            },
            clear=True,
        ):
            _, worker_template = _service_templates(PROD)

        worker_template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {"State": "ENABLED"},
        )
        cleanup_function = _cleanup_function(worker_template)
        environment = cast(dict[str, str], cast(dict[str, Any], cleanup_function["Environment"])["Variables"])
        self.assertEqual(environment["SANDBOX_CLEANUP_DRY_RUN"], "false")
        self.assertEqual(environment["SANDBOX_CLEANUP_PROVIDER"], "custom-provider")
        self.assertEqual(environment["SANDBOX_CLEANUP_SECRET_NAME"], "custom/cleanup-credentials")

    def test_unrecognized_sandbox_cleanup_rollout_flags_fail_closed(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "SANDBOX_CLEANUP_ENABLED": "sometimes",
                "SANDBOX_CLEANUP_DRY_RUN": "sometimes",
                "SANDBOX_CLEANUP_PROVIDER": "",
                "SANDBOX_CLEANUP_SECRET_NAME": "",
            },
            clear=True,
        ):
            _, worker_template = _service_templates(PROD)

        worker_template.has_resource_properties("AWS::Scheduler::Schedule", {"State": "DISABLED"})
        cleanup_function = _cleanup_function(worker_template)
        environment = cast(dict[str, str], cast(dict[str, Any], cleanup_function["Environment"])["Variables"])
        self.assertEqual(environment["SANDBOX_CLEANUP_DRY_RUN"], "true")
        self.assertEqual(environment["SANDBOX_CLEANUP_PROVIDER"], "daytona")
        self.assertEqual(environment["SANDBOX_CLEANUP_SECRET_NAME"], SANDBOX_CLEANUP_SECRET_NAME)

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


if __name__ == "__main__":
    unittest.main()
