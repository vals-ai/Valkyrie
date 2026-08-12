import json
import os
import unittest
from typing import cast
from unittest import mock

import aws_cdk as cdk
from aws_cdk import (
    assertions,
    aws_ec2,
    aws_ecr,
    aws_ecs,
    aws_elasticache,
    aws_elasticloadbalancingv2 as aws_elb,
    aws_rds,
)

from constants import (
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV,
    SANDBOX_CLEANUP_DLQ_NAME,
    SANDBOX_CLEANUP_FUNCTION_NAME,
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
from executor_stack import ExecutorStack
from stage import DEV, DEV_STACK_PREFIX, PROD, RELEASE_TEST, Stage
from tracker_stack import TrackerStack

TEST_ALERTS_SLACK_ENV = {
    SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE",
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV: "CALERTSCHANNEL",
}
TEST_DEPLOYMENT_SLACK_ENV = {
    SLACK_WORKSPACE_ID_ENV: "TTESTWORKSPACE",
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV: "CDEPLOYCHANNEL",
}
TEST_DESCOPE_MANAGEMENT_KEY_SECRET_NAME = "example-descope-management-key"
TEST_DEV_ENV = {
    "DESCOPE_PROJECT_ID": "dev-project",
    "DESCOPE_MANAGEMENT_KEY_SECRET_NAME": TEST_DESCOPE_MANAGEMENT_KEY_SECRET_NAME,
}
TEST_PROD_ENV = {"SENTRY_DSN_SECRET_NAME": "example/sentry-dsn"}
TEST_RELEASE_TEST_ENV = {
    "DESCOPE_PROJECT_ID": "release-test",
    "DESCOPE_MANAGEMENT_KEY_SECRET_NAME": TEST_DESCOPE_MANAGEMENT_KEY_SECRET_NAME,
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


def _service_templates(
    stage_name: str,
) -> tuple[assertions.Template, assertions.Template, assertions.Template]:
    app = cdk.App(context=SHARED_STACK_CONTEXT)
    stage = Stage(stage_name)
    env = cdk.Environment(account=TEST_AWS_ACCOUNT, region=TEST_AWS_REGION)
    shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=env)
    tracker_repository = cast(aws_ecr.IRepository, shared.tracker_repository) if stage.is_release_test else None
    executor_host_repository = (
        cast(aws_ecr.IRepository, shared.executor_host_repository) if stage.is_release_test else None
    )
    image_tag = "package-r-test" if stage.is_release_test else None
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
        tracker_repository=tracker_repository,
        image_tag=image_tag,
        env=env,
    )
    executor = ExecutorStack(
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
        tracker_image=tracker.tracker_image,
        executor_host_repository=executor_host_repository,
        image_tag=image_tag,
        env=env,
    )
    monitoring = MonitoringStack(
        app,
        stage.stack_id("MonitoringStack"),
        stage=stage,
        cluster=shared.cluster,
        tracker_service=tracker.tracker_fargate_service,
        load_balancer=tracker.service.load_balancer,
        target_group=tracker.service.target_group,
        database=tracker.database,
        redis_cluster=shared.redis_cluster,
        env=env,
    )
    monitoring.add_dependency(tracker)

    return (
        assertions.Template.from_stack(tracker),
        assertions.Template.from_stack(executor),
        assertions.Template.from_stack(monitoring),
    )


class MonitoringStackTest(unittest.TestCase):
    def test_dev_stack_ids_are_valk_scoped(self) -> None:
        self.assertEqual(Stage(PROD).stack_id("TrackerStack"), "TrackerStack")
        self.assertEqual(Stage(DEV).stack_id("TrackerStack"), f"{DEV_STACK_PREFIX}TrackerStack")

    def test_release_test_names_are_namespaced(self) -> None:
        stage = Stage(RELEASE_TEST)
        self.assertEqual(stage.stack_id("SharedStack"), "ValkReleaseTestSharedStack")
        self.assertEqual(stage.phys("AgenticHarnessCluster"), "AgenticHarnessCluster-release-test")
        self.assertEqual(stage.domain("benchmark-tracker.vals.ai"), "benchmark-tracker-release-test.vals.ai")

    def test_tracker_transport_follows_stage_contract(self) -> None:
        tracker_templates: dict[str, assertions.Template] = {}
        for stage_name, environment in (
            (PROD, TEST_PROD_ENV),
            (DEV, TEST_DEV_ENV),
            (RELEASE_TEST, TEST_RELEASE_TEST_ENV),
        ):
            with self.subTest(stage=stage_name), mock.patch.dict(os.environ, environment, clear=True):
                tracker_templates[stage_name] = _service_templates(stage_name)[0]

        for stage_name in (PROD, DEV):
            listeners = {
                (resource["Properties"]["Port"], resource["Properties"]["Protocol"]): resource
                for resource in tracker_templates[stage_name]
                .find_resources("AWS::ElasticLoadBalancingV2::Listener")
                .values()
            }
            self.assertEqual(set(listeners), {(80, "HTTP"), (443, "HTTPS")})
            self.assertTrue(listeners[(443, "HTTPS")]["Properties"]["Certificates"])
            redirect = listeners[(80, "HTTP")]["Properties"]["DefaultActions"][0]
            self.assertEqual(redirect["Type"], "redirect")
            self.assertEqual(
                redirect["RedirectConfig"],
                {"Port": "443", "Protocol": "HTTPS", "StatusCode": "HTTP_301"},
            )

        for stage_name in (PROD, DEV):
            self.assertEqual(
                len(tracker_templates[stage_name].find_resources("AWS::CertificateManager::Certificate")),
                1,
            )

        release_test_template = tracker_templates[RELEASE_TEST]
        self.assertFalse(release_test_template.find_resources("AWS::CertificateManager::Certificate"))
        release_test_template.has_resource_properties(
            "AWS::ElasticLoadBalancingV2::Listener",
            {"Port": 80, "Protocol": "HTTP"},
        )
        self.assertEqual(
            len(release_test_template.find_resources("AWS::ElasticLoadBalancingV2::Listener")),
            1,
        )

    def test_release_test_owns_immutable_service_image_repositories(self) -> None:
        release_template = _shared_template(RELEASE_TEST)
        repositories = release_template.find_resources("AWS::ECR::Repository")
        self.assertEqual(
            {resource["Properties"]["RepositoryName"] for resource in repositories.values()},
            {"valkyrie/release-test/tracker", "valkyrie/release-test/executor-host"},
        )
        self.assertTrue(
            all(resource["Properties"]["ImageTagMutability"] == "IMMUTABLE" for resource in repositories.values())
        )
        self.assertFalse(_shared_template(DEV).find_resources("AWS::ECR::Repository"))

    def test_release_roles_are_bound_to_stage_environments(self) -> None:
        for stage, role_name, expected_subject in (
            (DEV, "ValkyrieExecutorRelease-dev", "repo:vals-ai/Valkyrie:environment:dev"),
            (PROD, "ValkyrieExecutorRelease", "repo:vals-ai/Valkyrie:environment:prod"),
        ):
            with self.subTest(stage=stage):
                env = TEST_DEV_ENV if stage == DEV else TEST_PROD_ENV
                with mock.patch.dict(os.environ, env, clear=False):
                    _, executor_template, _ = _service_templates(stage)
                roles = executor_template.find_resources("AWS::IAM::Role")
                release_role = next(role for role in roles.values() if role["Properties"].get("RoleName") == role_name)
                trust = json.dumps(release_role["Properties"]["AssumeRolePolicyDocument"])
                self.assertIn(expected_subject, trust)
                self.assertNotIn("production-release", trust)
                self.assertNotIn("refs/heads/prod", trust)

        with mock.patch.dict(os.environ, TEST_PROD_ENV, clear=False):
            synthesized = json.dumps(_service_templates(PROD)[1].to_json())
        self.assertIn("tracker.executor.release_entrypoint", synthesized)
        self.assertIn("ecs:UpdateTaskProtection", synthesized)
        self.assertIn("ecs:StopTask", synthesized)
        self.assertIn("ecs:UpdateService", synthesized)

    def test_release_test_templates_use_external_benchmark_service_and_namespaced_outputs(self) -> None:
        with mock.patch.dict(
            os.environ,
            TEST_RELEASE_TEST_ENV,
            clear=False,
        ):
            tracker_template, executor_template, _ = _service_templates(RELEASE_TEST)

        parameter_names = [
            resource["Properties"]["Name"]
            for resource in executor_template.find_resources("AWS::SSM::Parameter").values()
        ]
        self.assertTrue(parameter_names)
        self.assertTrue(all("/valkyrie/release-test/" in name for name in parameter_names))
        self.assertIn("/valkyrie/release-test/executor-release/launch-config", parameter_names)
        executor_template.has_resource_properties(
            "AWS::ECS::TaskDefinition",
            {"Family": "ValkyrieExecutorRelease-release-test"},
        )
        roles = executor_template.find_resources("AWS::IAM::Role")
        self.assertFalse(
            any(role["Properties"].get("RoleName") == "ValkyrieExecutorRelease-release-test" for role in roles.values())
        )
        self.assertFalse(tracker_template.find_resources("AWS::Route53::RecordSet"))
        self.assertFalse(tracker_template.find_resources("AWS::Route53::RecordSetGroup"))
        load_balancers = tracker_template.find_resources("AWS::ElasticLoadBalancingV2::LoadBalancer")
        self.assertTrue(any(resource["Properties"]["Scheme"] == "internal" for resource in load_balancers.values()))
        self.assertTrue(
            any(
                environment["Name"] == "BENCHMARK_SERVICE_BASE_URL" and environment["Value"] == "benchmarks.vals.ai"
                for task_definition in tracker_template.find_resources("AWS::ECS::TaskDefinition").values()
                for container in task_definition["Properties"]["ContainerDefinitions"]
                for environment in container.get("Environment", [])
            )
        )
        self.assertTrue(
            any(
                resource["Properties"]["ServiceName"].endswith("-release-test")
                for resource in executor_template.find_resources("AWS::ECS::Service").values()
            )
        )

    def test_executor_stack_owns_the_host_and_release_control(self) -> None:
        with mock.patch.dict(os.environ, TEST_PROD_ENV, clear=False):
            _, executor_template, monitoring_template = _service_templates(PROD)
        services = executor_template.find_resources("AWS::ECS::Service")
        task_definitions = executor_template.find_resources("AWS::ECS::TaskDefinition")
        scalable_targets = executor_template.find_resources("AWS::ApplicationAutoScaling::ScalableTarget")

        self.assertEqual(len(services), 1)
        self.assertEqual(len(task_definitions), 2)
        self.assertEqual(len(scalable_targets), 1)
        executor_template.has_resource_properties(
            "AWS::ECS::Service",
            {"ServiceName": "ExecutorHost"},
        )
        executor_template.has_resource_properties(
            "AWS::ECS::TaskDefinition",
            {"Family": "ValkyrieExecutorRelease"},
        )

        synthesized = json.dumps(executor_template.to_json())
        self.assertIn('"STABLE_QUEUE_NAME", "Value": "valkyrie-stable"', synthesized)
        self.assertNotIn("taskiq", synthesized)
        self.assertNotIn("WorkerTaskDef", synthesized)
        self.assertNotIn("WorkerService", synthesized)
        self.assertNotIn("WorkerCpuScaling", synthesized)
        protection_policies = [
            policy
            for policy in executor_template.find_resources("AWS::IAM::Policy").values()
            if "ecs:UpdateTaskProtection" in json.dumps(policy)
        ]
        self.assertEqual(len(protection_policies), 2)

        worker_log_group = executor_template.to_json()["Resources"]["WorkerLogGroup31FDBE4A"]
        self.assertEqual(worker_log_group["DeletionPolicy"], "Retain")
        self.assertEqual(worker_log_group["UpdateReplacePolicy"], "Retain")
        self.assertNotIn("WorkerStack", json.dumps(monitoring_template.to_json()))

    def test_monitoring_has_no_legacy_worker_alarm_or_widgets(self) -> None:
        synthesized = json.dumps(_monitoring_template().to_json())

        self.assertNotIn("WorkerServiceDownAlarm", synthesized)
        self.assertNotIn("Valkyrie-Worker", synthesized)
        self.assertNotIn("Worker Running Tasks", synthesized)
        self.assertNotIn("Worker CPU / Memory", synthesized)

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
            tracker_template, worker_template, _ = _service_templates(DEV)
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
        worker_log_group = worker_template.to_json()["Resources"]["WorkerLogGroup31FDBE4A"]
        self.assertEqual(worker_log_group["DeletionPolicy"], "Retain")
        self.assertEqual(worker_log_group["UpdateReplacePolicy"], "Retain")
        worker_policies = worker_template.find_resources("AWS::IAM::Policy")
        executor_host_policies = [
            policy
            for logical_id, policy in worker_policies.items()
            if logical_id.startswith("ExecutorHostTaskDefTaskRoleDefaultPolicy")
        ]
        self.assertEqual(len(executor_host_policies), 1)
        executor_host_policy = json.dumps(executor_host_policies[0])
        self.assertIn("s3:GetObject", executor_host_policy)
        self.assertIn("releases/*", executor_host_policy)
        for forbidden_action in ("s3:GetObject*", "s3:GetBucket*", "s3:List*"):
            self.assertNotIn(forbidden_action, executor_host_policy)
        monitoring_template.has_resource_properties(
            "AWS::CloudWatch::Alarm",
            {"AlarmName": "Valkyrie-DB-Connections-High-dev", "Threshold": 65},
        )

    def test_service_environment_labels_follow_stage(self) -> None:
        for stage_name, expected_environment, expected_namespace in (
            (PROD, "production", "local"),
            (DEV, "dev", "local-dev"),
        ):
            environment = TEST_DEV_ENV if stage_name == DEV else TEST_PROD_ENV
            with self.subTest(stage=stage_name), mock.patch.dict(os.environ, environment, clear=True):
                tracker_template, worker_template, _ = _service_templates(stage_name)

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
            },
            clear=True,
        ):
            _, worker_template, _ = _service_templates(DEV)

        worker_template.resource_count_is("AWS::Scheduler::Schedule", 0)
        worker_template.resource_count_is("AWS::Lambda::Function", 0)
        worker_template.resource_count_is("AWS::SQS::Queue", 0)

    def test_prod_sandbox_cleanup_is_disabled_and_bounded_by_default(self) -> None:
        with mock.patch.dict(os.environ, TEST_PROD_ENV, clear=True):
            _, worker_template, _ = _service_templates(PROD)

        worker_template.resource_count_is("AWS::Scheduler::Schedule", 1)
        worker_template.resource_count_is("AWS::Lambda::Function", 1)
        worker_template.resource_count_is("AWS::Lambda::EventInvokeConfig", 1)
        worker_template.resource_count_is("AWS::SQS::Queue", 1)
        worker_template.has_resource_properties(
            "AWS::Scheduler::Schedule",
            {
                "Name": SANDBOX_CLEANUP_SCHEDULE_NAME,
                "ScheduleExpression": "rate(1 hour)",
                "State": "DISABLED",
                "Target": assertions.Match.object_like({"DeadLetterConfig": assertions.Match.any_value()}),
            },
        )
        worker_template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": SANDBOX_CLEANUP_FUNCTION_NAME,
                "ReservedConcurrentExecutions": 1,
                "Timeout": 14 * 60,
                "Environment": {
                    "Variables": {
                        "SANDBOX_CLEANUP_PROVIDER": "daytona",
                        "SANDBOX_CLEANUP_SECRET_NAME": SANDBOX_CLEANUP_SECRET_NAME,
                        "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
                        "ENVIRONMENT": "production",
                    }
                },
            },
        )
        worker_template.has_resource_properties(
            "AWS::Lambda::EventInvokeConfig",
            {
                "DestinationConfig": {
                    "OnFailure": {"Destination": assertions.Match.any_value()},
                },
            },
        )
        worker_template.has_resource_properties(
            "AWS::SQS::Queue",
            {
                "QueueName": SANDBOX_CLEANUP_DLQ_NAME,
                "SqsManagedSseEnabled": True,
            },
        )
        secret_statements = [
            statement
            for policy in worker_template.find_resources("AWS::IAM::Policy").values()
            for statement in policy["Properties"]["PolicyDocument"]["Statement"]
            if statement.get("Action") == ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
            and SANDBOX_CLEANUP_SECRET_NAME in str(statement.get("Resource"))
        ]
        self.assertEqual(len(secret_statements), 1)
        self.assertNotEqual(secret_statements[0]["Resource"], "*")

    def test_prod_sandbox_cleanup_schedule_requires_exact_true(self) -> None:
        for enabled, expected_state in (("true", "ENABLED"), ("TRUE", "DISABLED")):
            with (
                self.subTest(enabled=enabled),
                mock.patch.dict(
                    os.environ,
                    {
                        **TEST_PROD_ENV,
                        "SANDBOX_CLEANUP_ENABLED": enabled,
                        "SANDBOX_CLEANUP_PROVIDER": "daytona",
                        "SANDBOX_CLEANUP_SECRET_NAME": "custom/cleanup-credentials",
                    },
                    clear=True,
                ),
            ):
                _, worker_template, _ = _service_templates(PROD)

            worker_template.has_resource_properties(
                "AWS::Scheduler::Schedule",
                {"State": expected_state},
            )
            worker_template.has_resource_properties(
                "AWS::Lambda::Function",
                {
                    "Environment": {
                        "Variables": assertions.Match.object_like(
                            {
                                "SANDBOX_CLEANUP_PROVIDER": "daytona",
                                "SANDBOX_CLEANUP_SECRET_NAME": "custom/cleanup-credentials",
                                "ENVIRONMENT": "production",
                            }
                        )
                    }
                },
            )

    def test_dev_sentry_secret_is_optional(self) -> None:
        with mock.patch.dict(os.environ, TEST_DEV_ENV, clear=True):
            tracker_template, worker_template, _ = _service_templates(DEV)

        self.assertNotIn("SENTRY_DSN", str(tracker_template.to_json()))
        self.assertNotIn("SENTRY_DSN", str(worker_template.to_json()))

        custom_sentry_secret_name = "custom/dev-sentry-dsn"
        sentry_environment = {
            **TEST_DEV_ENV,
            "SENTRY_DSN_SECRET_NAME": custom_sentry_secret_name,
        }
        with mock.patch.dict(os.environ, sentry_environment, clear=True):
            tracker_template, worker_template, _ = _service_templates(DEV)

        tracker_template.has_resource_properties(
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
            for task_definition in tracker_template.find_resources("AWS::ECS::TaskDefinition").values()
            for container in task_definition["Properties"]["ContainerDefinitions"]
            for secret in container.get("Secrets", [])
            if secret["Name"] == "SENTRY_DSN"
        ]
        self.assertEqual(len(sentry_value_from), 1)
        self.assertIn(custom_sentry_secret_name, str(sentry_value_from[0]))
        self.assertNotIn("SENTRY_DSN", str(worker_template.to_json()))


if __name__ == "__main__":
    unittest.main()
