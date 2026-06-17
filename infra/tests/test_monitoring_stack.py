import os
import unittest
from unittest import mock

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
    SLACK_WORKSPACE_ID_ENV,
    TRACKER_LOG_GROUP_NAME,
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV,
    WORKER_LOG_GROUP_NAME,
    get_slack_notification_config,
)
from monitoring_stack import MonitoringStack
from shared import SharedStack
from stage import DEV, PROD, Stage
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
        with mock.patch.dict(os.environ, {}, clear=True):
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
            with self.subTest(stage=stage_name), mock.patch.dict(os.environ, {}, clear=True):
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


if __name__ == "__main__":
    unittest.main()
