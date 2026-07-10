"""Worker service stack - Taskiq worker.

Deployed independently from the tracker so that long-running benchmark
tasks can finish while a new worker version is rolled out.
"""

import os
from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_ecs,
    aws_iam,
    aws_logs,
    aws_rds,
    aws_s3,
    aws_scheduler,
    aws_scheduler_targets,
    aws_secretsmanager,
    aws_servicediscovery,
    aws_sqs,
)
from aws_cdk.aws_ecr_assets import Platform
from constants import (
    DAYTONA_CLEANUP_DLQ_NAME,
    DAYTONA_CLEANUP_LOG_GROUP_NAME,
    DAYTONA_CLEANUP_SCHEDULE_NAME,
    DEFAULT_DAYTONA_CLEANUP_SECRET_NAME,
    POSTGRES_DB,
    WORKER_LOG_GROUP_NAME,
    WORKER_SCALING_CPU_PERCENT,
    WORKER_STOP_TIMEOUT_SECONDS,
)
from constructs import Construct
from stage import Stage
from stage_config import config_for

_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class WorkerStack(Stack):
    """Worker stack: Taskiq worker as a Fargate service.

    Connects to the shared ElastiCache Redis in SharedStack (used as the
    Taskiq message broker) and to the RDS database in TrackerStack.

    Deployment is configured with ``min_healthy_percent=100`` and
    ``max_healthy_percent=200`` so ECS starts new tasks before stopping old
    ones.  A Taskiq middleware enables ECS Task Protection while benchmarks
    are running, preventing ECS from killing tasks with active work.
    Protection is automatically released once all benchmarks on a task finish.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        stage: Stage,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        namespace: aws_servicediscovery.IPrivateDnsNamespace,
        redis_url: str,
        bucket: aws_s3.IBucket,
        database: aws_rds.DatabaseInstance,
        db_credentials: aws_rds.DatabaseSecret,
        tracker_service: aws_ecs.FargateService,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)
        stage_config = config_for(stage)

        # Reuse the tracker's security group so both services share the same
        # SG — benchmark services only need to whitelist one group.
        tracker_sg = tracker_service.connections.security_groups[0]

        worker_image = aws_ecs.ContainerImage.from_asset(
            "../services/tracker",
            file="Dockerfile",
            platform=Platform.LINUX_ARM64,
        )

        shared_env = {
            "BROKER_ENVIRONMENT": stage_config.runtime_environment,
            "AWS_S3_BUCKET": bucket.bucket_name,
            "ENVIRONMENT": stage_config.runtime_environment,
            "BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE": namespace.namespace_name,
            "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
        }

        db_env = {
            "DB_HOST": database.db_instance_endpoint_address,
            "DB_PORT": database.db_instance_endpoint_port,
            "DB_NAME": POSTGRES_DB,
        }

        db_credentials_secret = cast(aws_secretsmanager.ISecret, db_credentials)
        db_secrets = {
            "DB_USERNAME": aws_ecs.Secret.from_secrets_manager(db_credentials_secret, field="username"),
            "DB_PASSWORD": aws_ecs.Secret.from_secrets_manager(db_credentials_secret, field="password"),
        }

        sentry_secret = aws_secretsmanager.Secret.from_secret_name_v2(self, "SentryDsnSecret", "valkyrie/sentry-dsn")

        sentry_secrets = {
            "SENTRY_DSN": aws_ecs.Secret.from_secrets_manager(sentry_secret),
        }

        # ── Worker service ────────────────────────────────────────────────

        worker_task_def = aws_ecs.FargateTaskDefinition(
            self,
            "WorkerTaskDef",
            cpu=stage_config.worker.cpu,
            memory_limit_mib=stage_config.worker.memory_mib,
            runtime_platform=_ARM64_PLATFORM,
        )

        worker_task_def.add_container(
            "WorkerContainer",
            image=worker_image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="Worker",
                log_group=aws_logs.LogGroup(
                    self,
                    "WorkerLogGroup",
                    log_group_name=stage.phys(WORKER_LOG_GROUP_NAME),
                    retention=stage_config.service_log_retention,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
            environment={
                **shared_env,
                **db_env,
                "REDIS_URL": redis_url,
                "SENTRY_RELEASE": os.environ.get("SENTRY_RELEASE", ""),
            },
            secrets={
                **db_secrets,
                **sentry_secrets,
            },
            command=[
                "uv",
                "run",
                "--no-sync",
                "taskiq",
                "worker",
                "--no-configure-logging",
                "tracker.config:broker",
                "tracker.utils",
            ],
            stop_timeout=Duration.seconds(WORKER_STOP_TIMEOUT_SECONDS),
        )

        # Allow the worker to toggle ECS Task Protection while benchmarks run
        worker_task_def.task_role.add_to_principal_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:UpdateTaskProtection"],
                resources=["*"],
            )
        )

        self.worker_service = aws_ecs.FargateService(
            self,
            "WorkerService",
            cluster=cluster,
            task_definition=worker_task_def,
            desired_count=stage_config.worker.min_tasks,
            service_name=stage.phys("Worker"),
            security_groups=[tracker_sg],
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            min_healthy_percent=100,
            max_healthy_percent=200,
            assign_public_ip=True,
        )

        # Worker auto-scaling
        worker_scaling = self.worker_service.auto_scale_task_count(
            min_capacity=stage_config.worker.min_tasks,
            max_capacity=stage_config.worker.max_tasks,
        )
        worker_scaling.scale_on_cpu_utilization(
            "WorkerCpuScaling",
            target_utilization_percent=WORKER_SCALING_CPU_PERCENT,
        )

        if stage.is_prod:
            self._add_daytona_cleanup_schedule(
                stage=stage,
                vpc=vpc,
                cluster=cluster,
                image=worker_image,
                log_retention=stage_config.service_log_retention,
            )

    def _add_daytona_cleanup_schedule(
        self,
        *,
        stage: Stage,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        image: aws_ecs.ContainerImage,
        log_retention: aws_logs.RetentionDays,
    ) -> None:
        cleanup_enabled = os.environ.get("DAYTONA_CLEANUP_ENABLED") == "true"
        cleanup_dry_run = os.environ.get("DAYTONA_CLEANUP_DRY_RUN") != "false"
        cleanup_secret_name = os.environ.get("DAYTONA_CLEANUP_SECRET_NAME") or DEFAULT_DAYTONA_CLEANUP_SECRET_NAME

        cleanup_log_group = aws_logs.LogGroup(
            self,
            "DaytonaCleanupLogGroup",
            log_group_name=stage.phys(DAYTONA_CLEANUP_LOG_GROUP_NAME),
            retention=log_retention,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        cleanup_credentials = aws_secretsmanager.Secret.from_secret_name_v2(
            self,
            "DaytonaCleanupCredentials",
            cleanup_secret_name,
        )
        cleanup_task = aws_ecs.FargateTaskDefinition(
            self,
            "DaytonaCleanupTaskDef",
            cpu=256,
            memory_limit_mib=512,
            runtime_platform=_ARM64_PLATFORM,
        )
        cleanup_task.add_container(
            "DaytonaCleanupContainer",
            image=image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="DaytonaCleanup",
                log_group=cleanup_log_group,
            ),
            environment={
                "DAYTONA_CLEANUP_DRY_RUN": str(cleanup_dry_run).lower(),
                "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
            },
            secrets={
                "DAYTONA_API_KEY": aws_ecs.Secret.from_secrets_manager(
                    cleanup_credentials,
                    field="DAYTONA_API_KEY",
                ),
                "DAYTONA_API_URL": aws_ecs.Secret.from_secrets_manager(
                    cleanup_credentials,
                    field="DAYTONA_API_URL",
                ),
                "DAYTONA_TARGET": aws_ecs.Secret.from_secrets_manager(
                    cleanup_credentials,
                    field="DAYTONA_TARGET",
                ),
            },
            command=["uv", "run", "--no-sync", "python", "-m", "tracker.daytona_cleanup"],
        )

        cleanup_security_group = aws_ec2.SecurityGroup(
            self,
            "DaytonaCleanupSecurityGroup",
            vpc=vpc,
            description="Outbound-only network access for the Daytona cleanup task",
            allow_all_outbound=True,
        )
        cleanup_dlq = aws_sqs.Queue(
            self,
            "DaytonaCleanupDlq",
            queue_name=stage.phys(DAYTONA_CLEANUP_DLQ_NAME),
            encryption=aws_sqs.QueueEncryption.SQS_MANAGED,
            enforce_ssl=True,
            retention_period=Duration.days(14),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        cleanup_target = aws_scheduler_targets.EcsRunFargateTask(
            cluster,
            task_definition=cleanup_task,
            assign_public_ip=True,
            security_groups=[cleanup_security_group],
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PUBLIC),
            task_count=1,
            dead_letter_queue=cleanup_dlq,
            retry_attempts=1,
            max_event_age=Duration.minutes(30),
        )
        self.daytona_cleanup_schedule = aws_scheduler.Schedule(
            self,
            "DaytonaCleanupSchedule",
            schedule=aws_scheduler.ScheduleExpression.rate(Duration.hours(1)),
            target=cast(aws_scheduler.IScheduleTarget, cleanup_target),
            enabled=cleanup_enabled,
            schedule_name=stage.phys(DAYTONA_CLEANUP_SCHEDULE_NAME),
            description="Delete explicitly managed Daytona sandboxes older than 48 hours",
        )
