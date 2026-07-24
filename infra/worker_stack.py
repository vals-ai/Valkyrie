"""Stable ExecutorHost service in the historical WorkerStack."""

from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_ecr,
    aws_ecs,
    aws_iam,
    aws_logs,
    aws_rds,
    aws_s3,
    aws_secretsmanager,
    aws_servicediscovery,
)
from aws_cdk.aws_ecr_assets import Platform
from constants import (
    DOCKER_ASSET_EXCLUDES,
    EXECUTOR_HOST_LOG_GROUP_NAME,
    EXECUTOR_RELEASE_PREFIX,
    POSTGRES_DB,
    WORKER_LOG_GROUP_NAME,
    WORKER_SCALING_CPU_PERCENT,
    WORKER_STOP_TIMEOUT_SECONDS,
)
from constructs import Construct
from stage import Stage
from stage_config import benchmark_service_base_url, config_for

_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class WorkerStack(Stack):
    """Stable ExecutorHost resources under the existing WorkerStack identity.

    The host connects to Redis and PostgreSQL, consumes ``valkyrie-stable``,
    and launches pinned executor artifacts. ECS Task Protection prevents task
    termination while a benchmark is running.
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
        bucket_name: str,
        executor_release_bucket: aws_s3.IBucket,
        database: aws_rds.DatabaseInstance,
        db_credentials: aws_rds.DatabaseSecret,
        tracker_service: aws_ecs.FargateService,
        executor_host_repository: aws_ecr.IRepository | None = None,
        image_tag: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)
        stage_config = config_for(stage)

        # Reuse the tracker's security group so both services share the same
        # SG — benchmark services only need to whitelist one group.
        tracker_sg = tracker_service.connections.security_groups[0]

        if stage.is_release_test:
            if executor_host_repository is None or image_tag is None:
                raise ValueError(
                    "Release-test WorkerStack requires an executor-host repository and immutable image tag"
                )
            executor_host_image = aws_ecs.ContainerImage.from_ecr_repository(executor_host_repository, image_tag)
        else:
            executor_host_image = aws_ecs.ContainerImage.from_asset(
                "..",
                file="services/executor_host/Dockerfile",
                platform=Platform.LINUX_ARM64,
                exclude=list(DOCKER_ASSET_EXCLUDES),
            )

        benchmark_service_url = benchmark_service_base_url(stage)
        shared_env = {
            "BROKER_ENVIRONMENT": stage_config.runtime_environment,
            "AWS_S3_BUCKET": bucket_name,
            "ENVIRONMENT": stage_config.runtime_environment,
            "BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE": namespace.namespace_name,
            "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
            **({"BENCHMARK_SERVICE_BASE_URL": benchmark_service_url} if benchmark_service_url else {}),
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

        # Preserve historical Worker logs when the legacy service is removed.
        # Keeping this construct ID preserves the deployed CloudFormation resource.
        aws_logs.LogGroup(
            self,
            "WorkerLogGroup",
            log_group_name=stage.phys(WORKER_LOG_GROUP_NAME),
            retention=stage_config.service_log_retention,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # ── Stable executor host ─────────────────────────────────────────

        executor_task_def = aws_ecs.FargateTaskDefinition(
            self,
            "ExecutorHostTaskDef",
            cpu=stage_config.worker.cpu,
            memory_limit_mib=stage_config.worker.memory_mib,
            runtime_platform=_ARM64_PLATFORM,
        )
        executor_task_def.add_container(
            "ExecutorHostContainer",
            image=executor_host_image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="ExecutorHost",
                log_group=aws_logs.LogGroup(
                    self,
                    "ExecutorHostLogGroup",
                    log_group_name=stage.phys(EXECUTOR_HOST_LOG_GROUP_NAME),
                    retention=stage_config.service_log_retention,
                    removal_policy=cdk.RemovalPolicy.RETAIN,
                ),
            ),
            environment={
                **shared_env,
                **db_env,
                "REDIS_URL": redis_url,
                "STABLE_QUEUE_NAME": "valkyrie-stable",
                "EXECUTOR_RELEASE_BUCKET": executor_release_bucket.bucket_name,
                "EXECUTOR_RELEASE_PREFIX": EXECUTOR_RELEASE_PREFIX,
            },
            secrets=db_secrets,
            stop_timeout=Duration.seconds(WORKER_STOP_TIMEOUT_SECONDS),
        )
        cast(aws_iam.Role, executor_task_def.task_role).add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:UpdateTaskProtection"],
                resources=["*"],
            )
        )
        cast(aws_iam.Role, executor_task_def.task_role).add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[executor_release_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
            )
        )

        executor_host_service = aws_ecs.FargateService(
            self,
            "ExecutorHostService",
            cluster=cluster,
            task_definition=executor_task_def,
            desired_count=stage_config.worker.min_tasks,
            service_name=stage.phys("ExecutorHost"),
            security_groups=[tracker_sg],
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            min_healthy_percent=100,
            max_healthy_percent=200,
            assign_public_ip=True,
        )
        executor_scaling = executor_host_service.auto_scale_task_count(
            min_capacity=stage_config.worker.min_tasks,
            max_capacity=stage_config.worker.max_tasks,
        )
        executor_scaling.scale_on_cpu_utilization(
            "ExecutorHostCpuScaling",
            target_utilization_percent=WORKER_SCALING_CPU_PERCENT,
        )
