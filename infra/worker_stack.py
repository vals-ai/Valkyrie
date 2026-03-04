"""Worker service stack - Taskiq worker.

Deployed independently from the tracker so that long-running benchmark
tasks can finish while a new worker version is rolled out.
"""

from typing import Any

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
)
from aws_cdk.aws_ecr_assets import Platform
from constants import (
    NAMESPACE,
    POSTGRES_DB,
    WORKER_CPU,
    WORKER_MAX_TASKS,
    WORKER_MEMORY,
    WORKER_MIN_TASKS,
    WORKER_SCALING_CPU_PERCENT,
    WORKER_STOP_TIMEOUT_SECONDS,
)
from constructs import Construct

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
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        redis_url: str,
        bucket: aws_s3.IBucket,
        database: aws_rds.DatabaseInstance,
        db_credentials: aws_rds.DatabaseSecret,
        tracker_service: aws_ecs.FargateService,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)

        # Reuse the tracker's security group so both services share the same
        # SG — benchmark services only need to whitelist one group.
        tracker_sg = tracker_service.connections.security_groups[0]

        worker_image = aws_ecs.ContainerImage.from_asset(
            "../services/tracker",
            file="Dockerfile",
            platform=Platform.LINUX_ARM64,
        )

        shared_env = {
            "BROKER_ENVIRONMENT": "production",
            "BENCHMARK_SERVICE_NAMESPACE": NAMESPACE,
            "AWS_S3_BUCKET": bucket.bucket_name,
        }

        db_env = {
            "DB_HOST": database.db_instance_endpoint_address,
            "DB_PORT": database.db_instance_endpoint_port,
            "DB_NAME": POSTGRES_DB,
        }

        db_secrets = {
            "DB_USERNAME": aws_ecs.Secret.from_secrets_manager(db_credentials, field="username"),
            "DB_PASSWORD": aws_ecs.Secret.from_secrets_manager(db_credentials, field="password"),
        }

        # ── Worker service ────────────────────────────────────────────────

        worker_task_def = aws_ecs.FargateTaskDefinition(
            self,
            "WorkerTaskDef",
            cpu=WORKER_CPU,
            memory_limit_mib=WORKER_MEMORY,
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
                    retention=aws_logs.RetentionDays.ONE_WEEK,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
            environment={
                **shared_env,
                **db_env,
                "REDIS_URL": redis_url,
            },
            secrets=db_secrets,
            command=["uv", "run", "--no-sync", "taskiq", "worker", "tracker.config:broker", "tracker.utils"],
            stop_timeout=Duration.seconds(WORKER_STOP_TIMEOUT_SECONDS),
        )

        # Allow the worker to toggle ECS Task Protection while benchmarks run
        worker_task_def.task_role.add_to_policy(
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
            desired_count=WORKER_MIN_TASKS,
            service_name="Worker",
            security_groups=[tracker_sg],
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            min_healthy_percent=100,
            max_healthy_percent=200,
            assign_public_ip=True,
        )

        # Worker auto-scaling
        worker_scaling = self.worker_service.auto_scale_task_count(
            min_capacity=WORKER_MIN_TASKS,
            max_capacity=WORKER_MAX_TASKS,
        )
        worker_scaling.scale_on_cpu_utilization(
            "WorkerCpuScaling",
            target_utilization_percent=WORKER_SCALING_CPU_PERCENT,
        )
