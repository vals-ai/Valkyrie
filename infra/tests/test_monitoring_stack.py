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
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV,
    get_slack_notification_config,
)
from monitoring_stack import MonitoringStack
from shared import SharedStack

TEST_ALERTS_SLACK_ENV = {
    SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE",
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV: "CALERTSCHANNEL",
}
TEST_DEPLOYMENT_SLACK_ENV = {
    SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE",
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV: "CDEPLOYCHANNEL",
}
TEST_ALL_SLACK_ENV = TEST_ALERTS_SLACK_ENV | {
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV: "CDEPLOYCHANNEL",
}
SHARED_STACK_CONTEXT = {
    "availability-zones:account=613431292675:region=us-east-1": ["us-east-1a", "us-east-1b"],
    "hosted-zone:account=613431292675:domainName=vals.ai:region=us-east-1": {
        "Id": "/hostedzone/Z047985721WA50ZRLCDNC",
        "Name": "vals.ai.",
    },
}


def _monitoring_template() -> assertions.Template:
    app = cdk.App()
    resources = cdk.Stack(
        app,
        "MonitoringTestResources",
        env=cdk.Environment(account="123456789012", region="us-west-2"),
    )

    vpc = aws_ec2.Vpc(resources, "Vpc", max_azs=2)
    cluster = aws_ecs.Cluster(resources, "Cluster", vpc=vpc, cluster_name="AgenticHarnessCluster")

    tracker_task = aws_ecs.FargateTaskDefinition(resources, "TrackerTask")
    tracker_task.add_container("TrackerContainer", image=aws_ecs.ContainerImage.from_registry("busybox"))
    tracker_service = aws_ecs.FargateService(
        resources,
        "TrackerService",
        cluster=cluster,
        task_definition=tracker_task,
        service_name="Tracker",
    )

    worker_task = aws_ecs.FargateTaskDefinition(resources, "WorkerTask")
    worker_task.add_container("WorkerContainer", image=aws_ecs.ContainerImage.from_registry("busybox"))
    worker_service = aws_ecs.FargateService(
        resources,
        "WorkerService",
        cluster=cluster,
        task_definition=worker_task,
        service_name="Worker",
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


def _shared_template() -> assertions.Template:
    app = cdk.App(context=SHARED_STACK_CONTEXT)
    shared = SharedStack(
        app,
        "SharedStack",
        env=cdk.Environment(account="613431292675", region="us-east-1"),
    )

    return assertions.Template.from_stack(shared)


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

    def test_runtime_exceptions_stay_out_of_cloudwatch_log_metric_filters(self) -> None:
        with mock.patch.dict(os.environ, TEST_ALERTS_SLACK_ENV, clear=True):
            template = _monitoring_template()

        template.resource_count_is("AWS::Logs::MetricFilter", 0)

    def test_slack_config_reads_environment(self) -> None:
        with mock.patch.dict(os.environ, TEST_ALL_SLACK_ENV, clear=True):
            self.assertEqual(
                get_slack_notification_config(VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV),
                ("TTESTWORKSPACE", "CALERTSCHANNEL"),
            )
            self.assertEqual(
                get_slack_notification_config(DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV),
                ("TTESTWORKSPACE", "CDEPLOYCHANNEL"),
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

                template.resource_count_is("AWS::Chatbot::SlackChannelConfiguration", 0)

    def test_missing_slack_environment_skips_deployment_notification_resources(self) -> None:
        for env in ({}, {SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE"}, TEST_ALERTS_SLACK_ENV):
            with self.subTest(env=env), mock.patch.dict(os.environ, env, clear=True):
                template = _shared_template()

                template.resource_count_is("AWS::Chatbot::SlackChannelConfiguration", 0)
                template.resource_count_is("AWS::Events::Rule", 0)
                template.resource_count_is("AWS::SNS::Topic", 0)

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


if __name__ == "__main__":
    unittest.main()
