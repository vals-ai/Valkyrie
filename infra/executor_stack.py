"""Executor release infrastructure and maintenance control task."""

import os
from typing import Any, cast

import aws_cdk as cdk
from aws_cdk.aws_ecr_assets import Platform
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_ecs,
    aws_iam,
    aws_lambda,
    aws_lambda_destinations,
    aws_logs,
    aws_rds,
    aws_s3,
    aws_scheduler,
    aws_scheduler_targets,
    aws_secretsmanager,
    aws_sqs,
    aws_ssm,
)
from constants import (
    EXECUTOR_RELEASE_BUCKET_NAME,
    EXECUTOR_RELEASE_PREFIX,
    EXECUTOR_RELEASE_ROLE_NAME,
    POSTGRES_DB,
    POSTGRES_PORT,
    SANDBOX_CLEANUP_DLQ_NAME,
    SANDBOX_CLEANUP_FUNCTION_NAME,
    SANDBOX_CLEANUP_LOG_GROUP_NAME,
    SANDBOX_CLEANUP_SCHEDULE_NAME,
    SANDBOX_CLEANUP_SECRET_NAME,
    VPC_CIDR,
    executor_release_launch_parameter,
)
from constructs import Construct
from stage import Stage
from stage_config import StageConfig, config_for

_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class ExecutorStack(Stack):
    """Own the sealed executor release and maintenance-control boundary."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        stage: Stage,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        database_proxy: aws_rds.DatabaseProxy,
        db_credentials: aws_rds.DatabaseSecret,
        tracker_service: aws_ecs.FargateService,
        tracker_image: aws_ecs.ContainerImage,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)
        stage_config = config_for(stage)

        self.executor_release_bucket = aws_s3.Bucket(
            self,
            "ExecutorReleaseBucket",
            bucket_name=f"{stage.phys(EXECUTOR_RELEASE_BUCKET_NAME)}-{self.account}",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            object_ownership=aws_s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        self.executor_release_bucket.add_to_resource_policy(
            aws_iam.PolicyStatement(
                sid="RequireConditionalExecutorReleaseWrites",
                effect=aws_iam.Effect.DENY,
                principals=[cast(aws_iam.IPrincipal, aws_iam.AnyPrincipal())],
                actions=["s3:PutObject"],
                resources=[self.executor_release_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
                conditions={"Null": {"s3:if-none-match": "true"}},
            )
        )

        self._create_executor_release_control(
            stage=stage,
            stage_config=stage_config,
            vpc=vpc,
            cluster=cluster,
            tracker_image=tracker_image,
            tracker_service=tracker_service,
            database_proxy=database_proxy,
            db_secret=cast(aws_secretsmanager.ISecret, db_credentials),
        )

        if stage.is_production:
            self._add_sandbox_cleanup_schedule(
                stage=stage,
                log_retention=stage_config.service_log_retention,
            )

    def _add_sandbox_cleanup_schedule(
        self,
        *,
        stage: Stage,
        log_retention: aws_logs.RetentionDays,
    ) -> None:
        cleanup_enabled = os.environ.get("SANDBOX_CLEANUP_ENABLED") == "true"
        cleanup_provider = os.environ.get("SANDBOX_CLEANUP_PROVIDER") or "daytona"
        configured_cleanup_secret_name = os.environ.get("SANDBOX_CLEANUP_SECRET_NAME") or ""
        if cleanup_enabled and not configured_cleanup_secret_name:
            raise ValueError("Sandbox cleanup requires SANDBOX_CLEANUP_SECRET_NAME.")
        cleanup_secret_name = configured_cleanup_secret_name or SANDBOX_CLEANUP_SECRET_NAME

        cleanup_log_group = aws_logs.LogGroup(
            self,
            "SandboxCleanupLogGroup",
            log_group_name=stage.phys(SANDBOX_CLEANUP_LOG_GROUP_NAME),
            retention=log_retention,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        cleanup_credentials = aws_secretsmanager.Secret.from_secret_name_v2(
            self,
            "SandboxCleanupCredentials",
            cleanup_secret_name,
        )
        cleanup_dlq = aws_sqs.Queue(
            self,
            "SandboxCleanupDlq",
            queue_name=stage.phys(SANDBOX_CLEANUP_DLQ_NAME),
            encryption=aws_sqs.QueueEncryption.SQS_MANAGED,
            enforce_ssl=True,
            retention_period=Duration.days(14),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        cleanup_function = aws_lambda.DockerImageFunction(
            self,
            "SandboxCleanupFunction",
            code=aws_lambda.DockerImageCode.from_image_asset(
                "../services/tracker",
                file="Dockerfile.lambda",
                platform=Platform.LINUX_ARM64,
            ),
            architecture=aws_lambda.Architecture.ARM_64,
            function_name=stage.phys(SANDBOX_CLEANUP_FUNCTION_NAME),
            description="Delete sandboxes older than 48 hours unless they opt out",
            memory_size=512,
            timeout=Duration.minutes(14),
            reserved_concurrent_executions=1,
            environment={
                "SANDBOX_CLEANUP_PROVIDER": cleanup_provider,
                "SANDBOX_CLEANUP_SECRET_NAME": cleanup_secret_name,
                "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
                "ENVIRONMENT": "production",
            },
            log_group=cleanup_log_group,
            retry_attempts=0,
            max_event_age=Duration.minutes(30),
            on_failure=cast(
                aws_lambda.IDestination,
                aws_lambda_destinations.SqsDestination(cleanup_dlq),
            ),
        )
        cleanup_credentials.grant_read(cleanup_function)

        cleanup_target = aws_scheduler_targets.LambdaInvoke(
            cast(aws_lambda.IFunction, cleanup_function),
            dead_letter_queue=cleanup_dlq,
            retry_attempts=1,
            max_event_age=Duration.minutes(30),
        )
        self.sandbox_cleanup_schedule = aws_scheduler.Schedule(
            self,
            "SandboxCleanupSchedule",
            schedule=aws_scheduler.ScheduleExpression.rate(Duration.hours(1)),
            target=cast(aws_scheduler.IScheduleTarget, cleanup_target),
            enabled=cleanup_enabled,
            schedule_name=stage.phys(SANDBOX_CLEANUP_SCHEDULE_NAME),
            description="Delete sandboxes older than 48 hours unless they opt out",
        )

    def _create_executor_release_control(
        self,
        *,
        stage: Stage,
        stage_config: StageConfig,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        tracker_image: aws_ecs.ContainerImage,
        tracker_service: aws_ecs.FargateService,
        database_proxy: aws_rds.DatabaseProxy,
        db_secret: aws_secretsmanager.ISecret,
    ) -> None:
        control_security_group = aws_ec2.SecurityGroup(
            self,
            "ExecutorReleaseSecurityGroup",
            vpc=vpc,
            description="No-ingress security group for executor-release control tasks",
            allow_all_outbound=False,
        )
        control_security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.tcp(POSTGRES_PORT),
            "Tracker RDS proxy",
        )
        control_security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.udp(53),
            "VPC DNS UDP",
        )
        control_security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.tcp(53),
            "VPC DNS TCP",
        )
        control_security_group.add_egress_rule(
            aws_ec2.Peer.any_ipv4(),
            aws_ec2.Port.tcp(443),
            "AWS API endpoints",
        )

        task_role = aws_iam.Role(
            self,
            "ExecutorReleaseTaskRole",
            role_name=stage.phys("ValkyrieExecutorReleaseTask"),
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com")),
        )
        execution_role = aws_iam.Role(
            self,
            "ExecutorReleaseExecutionRole",
            role_name=stage.phys("ValkyrieExecutorReleaseExecution"),
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com")),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[self.executor_release_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
            )
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[db_secret.secret_arn],
            )
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:ListTasks"],
                resources=["*"],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        executor_task_arn = self.format_arn(
            service="ecs",
            resource="task",
            resource_name=f"{cluster.cluster_name}/*",
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:StopTask", "ecs:DescribeTasks"],
                resources=[executor_task_arn],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:DescribeServices", "ecs:UpdateService"],
                resources=[tracker_service.service_arn],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )

        task_definition = aws_ecs.FargateTaskDefinition(
            self,
            "ExecutorReleaseTaskDefinition",
            family=stage.phys("ValkyrieExecutorRelease"),
            cpu=512,
            memory_limit_mib=1024,
            runtime_platform=_ARM64_PLATFORM,
            execution_role=cast(aws_iam.IRole, execution_role),
            task_role=cast(aws_iam.IRole, task_role),
        )
        container_name = "ExecutorRelease"
        task_definition.add_container(
            container_name,
            image=tracker_image,
            entry_point=[
                "/app/.venv/bin/python",
                "-m",
                "tracker.executor.release_entrypoint",
                db_secret.secret_arn,
                database_proxy.endpoint,
                str(POSTGRES_PORT),
                POSTGRES_DB,
                self.executor_release_bucket.bucket_name,
                EXECUTOR_RELEASE_PREFIX,
                cluster.cluster_arn,
                tracker_service.service_name,
                str(stage_config.tracker.min_tasks),
                "--runner-task-family",
                stage.phys("ExecutorRunner"),
            ],
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix=container_name,
                log_group=aws_logs.LogGroup(
                    self,
                    "ExecutorReleaseLogGroup",
                    log_group_name=stage.phys("/valkyrie/executor-release"),
                    retention=stage_config.service_log_retention,
                    removal_policy=cdk.RemovalPolicy.RETAIN,
                ),
            ),
        )

        launch_parameter = aws_ssm.StringParameter(
            self,
            "ExecutorReleaseLaunchConfig",
            parameter_name=executor_release_launch_parameter(stage.name),
            string_value=self.to_json_string(
                {
                    "bucket": self.executor_release_bucket.bucket_name,
                    "cluster": cluster.cluster_arn,
                    "container": container_name,
                    "prefix": EXECUTOR_RELEASE_PREFIX,
                    "security_group": control_security_group.security_group_id,
                    "subnets": vpc.select_subnets(subnet_type=aws_ec2.SubnetType.PUBLIC).subnet_ids,
                    "task_definition": task_definition.task_definition_arn,
                }
            ),
        )

        if stage.is_release_test:
            return

        oidc_provider = aws_iam.OpenIdConnectProvider.from_open_id_connect_provider_arn(
            self,
            "GitHubOidcProvider",
            f"arn:{self.partition}:iam::{self.account}:oidc-provider/token.actions.githubusercontent.com",
        )
        release_role = aws_iam.Role(
            self,
            "ExecutorReleaseRole",
            role_name=stage.phys(EXECUTOR_RELEASE_ROLE_NAME),
            assumed_by=cast(
                aws_iam.IPrincipal,
                aws_iam.WebIdentityPrincipal(
                    oidc_provider.open_id_connect_provider_arn,
                    conditions={
                        "StringEquals": {
                            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                            "token.actions.githubusercontent.com:sub": (
                                f"repo:vals-ai/Valkyrie:environment:{stage.github_environment}"
                            ),
                        }
                    },
                ),
            ),
        )
        release_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[self.executor_release_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
            )
        )
        release_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=[task_definition.task_definition_arn],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        release_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:DescribeTasks"],
                resources=["*"],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        release_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[task_role.role_arn, execution_role.role_arn],
                conditions={"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
            )
        )
        release_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=[launch_parameter.parameter_arn],
            )
        )
