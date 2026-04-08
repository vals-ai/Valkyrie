"""Tracker service stack - public-facing API with ALB and shared RDS database."""

from typing import Any

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_ecs,
    aws_ecs_patterns,
    aws_elasticloadbalancingv2,
    aws_logs,
    aws_rds,
    aws_route53,
    aws_s3,
    aws_servicediscovery,
)
from aws_cdk.aws_ecr_assets import Platform
from constants import (
    ALB_HEALTH_INTERVAL_SECONDS,
    ALB_IDLE_TIMEOUT_SECONDS,
    ALLOWED_IPS,
    CONTAINER_HEALTH_INTERVAL_SECONDS,
    CONTAINER_HEALTH_RETRIES,
    CONTAINER_HEALTH_START_PERIOD_SECONDS,
    CONTAINER_HEALTH_TIMEOUT_SECONDS,
    POSTGRES_DB,
    POSTGRES_PORT,
    POSTGRES_USER,
    RDS_ALLOCATED_STORAGE_GB,
    RDS_INSTANCE_CLASS,
    TRACKER_CPU,
    TRACKER_DOMAIN,
    TRACKER_MAX_TASKS,
    TRACKER_MEMORY,
    TRACKER_MIN_TASKS,
    TRACKER_PORT,
    TRACKER_SCALING_CPU_PERCENT,
    VPC_CIDR,
)
from constructs import Construct

_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class TrackerStack(Stack):
    """Tracker stack: public-facing API behind an ALB, plus the shared RDS
    database used by both the tracker and the worker.

    Exposes ``database``, ``db_credentials``, and ``tracker_fargate_service``
    so that :class:`WorkerStack` can wire up cross-stack network rules and
    environment variables.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        namespace: aws_servicediscovery.IPrivateDnsNamespace,
        hosted_zone: aws_route53.IHostedZone,
        bucket: aws_s3.IBucket,
        redis_url: str,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)

        # Docker image for the tracker API
        tracker_image = aws_ecs.ContainerImage.from_asset(
            "../services/tracker",
            file="Dockerfile",
            platform=Platform.LINUX_ARM64,
        )

        # Shared environment variables
        shared_env = {
            "BROKER_ENVIRONMENT": "production",
            "AWS_S3_BUCKET": bucket.bucket_name,
            "ENVIRONMENT": "production",
        }

        # ── RDS ──────────────────────────────────────────────────────────

        db_security_group = aws_ec2.SecurityGroup(
            self,
            "TrackerDbSecurityGroup",
            vpc=vpc,
            description="Security group for Tracker RDS instance",
            allow_all_outbound=False,
        )

        self.db_credentials = aws_rds.DatabaseSecret(
            self,
            "TrackerDbCredentials",
            username=POSTGRES_USER,
        )

        self.database = aws_rds.DatabaseInstance(
            self,
            "TrackerDatabase",
            engine=aws_rds.DatabaseInstanceEngine.postgres(
                version=aws_rds.PostgresEngineVersion.VER_16,
            ),
            instance_type=aws_ec2.InstanceType(RDS_INSTANCE_CLASS),
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PUBLIC),
            security_groups=[db_security_group],
            credentials=aws_rds.Credentials.from_secret(self.db_credentials),
            database_name=POSTGRES_DB,
            allocated_storage=RDS_ALLOCATED_STORAGE_GB,
            publicly_accessible=True,
            deletion_protection=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            backup_retention=Duration.days(7),
        )

        db_env = {
            "DB_HOST": self.database.db_instance_endpoint_address,
            "DB_PORT": self.database.db_instance_endpoint_port,
            "DB_NAME": POSTGRES_DB,
        }

        db_secrets = {
            "DB_USERNAME": aws_ecs.Secret.from_secrets_manager(self.db_credentials, field="username"),
            "DB_PASSWORD": aws_ecs.Secret.from_secrets_manager(self.db_credentials, field="password"),
        }

        # ── Tracker API service ──────────────────────────────────────────

        tracker_task_def = aws_ecs.FargateTaskDefinition(
            self,
            "TrackerTaskDef",
            cpu=TRACKER_CPU,
            memory_limit_mib=TRACKER_MEMORY,
            runtime_platform=_ARM64_PLATFORM,
        )

        tracker_task_def.add_container(
            "TrackerContainer",
            image=tracker_image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="Tracker",
                log_group=aws_logs.LogGroup(
                    self,
                    "TrackerLogGroup",
                    retention=aws_logs.RetentionDays.ONE_YEAR,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
            port_mappings=[aws_ecs.PortMapping(container_port=TRACKER_PORT)],
            environment={
                **shared_env,
                **db_env,
                "REDIS_URL": redis_url,
            },
            secrets=db_secrets,
            command=["uv", "run", "--no-sync", "python", "-m", "tracker.serve"],
            health_check=aws_ecs.HealthCheck(
                command=["CMD-SHELL", f"curl -f http://localhost:{TRACKER_PORT}/health || exit 1"],
                interval=Duration.seconds(CONTAINER_HEALTH_INTERVAL_SECONDS),
                retries=CONTAINER_HEALTH_RETRIES,
                start_period=Duration.seconds(CONTAINER_HEALTH_START_PERIOD_SECONDS),
                timeout=Duration.seconds(CONTAINER_HEALTH_TIMEOUT_SECONDS),
            ),
        )

        self.service = aws_ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "TrackerService",
            cluster=cluster,
            desired_count=TRACKER_MIN_TASKS,
            task_definition=tracker_task_def,
            service_name="Tracker",
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            domain_name=TRACKER_DOMAIN,
            domain_zone=hosted_zone,
            protocol=aws_elasticloadbalancingv2.ApplicationProtocol.HTTPS,
            redirect_http=True,
            open_listener=False,
            assign_public_ip=True,
            public_load_balancer=True,
        )

        # Expose the inner FargateService for cross-stack security group rules
        self.tracker_fargate_service = self.service.service

        # Cloud Map registration for internal access
        self.service.service.enable_cloud_map(
            name="tracker",
            cloud_map_namespace=namespace,
        )

        # ALB health check
        self.service.target_group.configure_health_check(
            path="/health",
            port=str(TRACKER_PORT),
            interval=Duration.seconds(ALB_HEALTH_INTERVAL_SECONDS),
        )

        # Request timeout
        self.service.load_balancer.set_attribute("idle_timeout.timeout_seconds", str(ALB_IDLE_TIMEOUT_SECONDS))

        # Allow HTTP -> HTTPS redirect
        self.service.load_balancer.connections.allow_from(
            aws_ec2.Peer.any_ipv4(),
            aws_ec2.Port.tcp(80),
            description="Allow HTTP from anywhere (redirects to HTTPS)",
        )

        # Allow HTTPS from whitelisted IPs only
        for ip, desc in ALLOWED_IPS:
            self.service.load_balancer.connections.allow_from(
                aws_ec2.Peer.ipv4(ip),
                aws_ec2.Port.tcp(443),
                description=f"Allow HTTPS from {desc}",
            )

        # Tracker auto-scaling
        tracker_scaling = self.service.service.auto_scale_task_count(
            min_capacity=TRACKER_MIN_TASKS,
            max_capacity=TRACKER_MAX_TASKS,
        )
        tracker_scaling.scale_on_cpu_utilization(
            "CpuScaling",
            target_utilization_percent=TRACKER_SCALING_CPU_PERCENT,
        )

        # ── Network access ───────────────────────────────────────────────

        # Allow VPC services (tracker + worker) to reach RDS.
        db_security_group.add_ingress_rule(
            peer=aws_ec2.Peer.ipv4(VPC_CIDR),
            connection=aws_ec2.Port.tcp(POSTGRES_PORT),
            description="Allow VPC services to connect to RDS",
        )
