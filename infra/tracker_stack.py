"""Tracker service stack - public-facing API gateway to all other services."""

from typing import Any

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_ecs,
    aws_ecs_patterns,
    aws_elasticloadbalancingv2,
    aws_iam,
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
    NAMESPACE,
    POSTGRES_DB,
    POSTGRES_PORT,
    POSTGRES_USER,
    RDS_ALLOCATED_STORAGE_GB,
    RDS_INSTANCE_CLASS,
    REDIS_HEALTH_INTERVAL_SECONDS,
    REDIS_HEALTH_START_PERIOD_SECONDS,
    REDIS_PORT,
    TRACKER_CPU,
    TRACKER_DOMAIN,
    TRACKER_MAX_TASKS,
    TRACKER_MEMORY,
    TRACKER_MIN_TASKS,
    TRACKER_PORT,
    TRACKER_SCALING_CPU_PERCENT,
)
from constructs import Construct


class TrackerStack(Stack):
    """Tracker service: public API that orchestrates benchmark runs."""

    _SERVICE_NAME: str = "Tracker"

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        namespace: aws_servicediscovery.IPrivateDnsNamespace,
        hosted_zone: aws_route53.IHostedZone,
        bucket: aws_s3.IBucket,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)

        # RDS PostgreSQL instance
        db_security_group = aws_ec2.SecurityGroup(
            self,
            f"{self._SERVICE_NAME}DbSecurityGroup",
            vpc=vpc,
            description="Security group for Tracker RDS instance",
            allow_all_outbound=False,
        )

        # RDS credentials stored in Secrets Manager
        db_credentials = aws_rds.DatabaseSecret(
            self,
            f"{self._SERVICE_NAME}DbCredentials",
            username=POSTGRES_USER,
        )

        self.database = aws_rds.DatabaseInstance(
            self,
            f"{self._SERVICE_NAME}Database",
            engine=aws_rds.DatabaseInstanceEngine.postgres(
                version=aws_rds.PostgresEngineVersion.VER_16,
            ),
            instance_type=aws_ec2.InstanceType(RDS_INSTANCE_CLASS),
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PUBLIC),
            security_groups=[db_security_group],
            credentials=aws_rds.Credentials.from_secret(db_credentials),
            database_name=POSTGRES_DB,
            allocated_storage=RDS_ALLOCATED_STORAGE_GB,
            publicly_accessible=True,  # Required for public subnet, access controlled by security group
            deletion_protection=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            backup_retention=Duration.days(7),
        )

        # fargate task
        task_def = aws_ecs.FargateTaskDefinition(
            self,
            f"{self._SERVICE_NAME}TaskDef",
            cpu=TRACKER_CPU,
            memory_limit_mib=TRACKER_MEMORY,
            runtime_platform=aws_ecs.RuntimePlatform(
                cpu_architecture=aws_ecs.CpuArchitecture.X86_64,
                operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
            ),
        )

        # redis sidecar container
        redis_container = task_def.add_container(
            f"{self._SERVICE_NAME}RedisContainer",
            container_name="redis",
            image=aws_ecs.ContainerImage.from_registry("redis:7-alpine"),
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix=f"{self._SERVICE_NAME}Redis",
                log_group=aws_logs.LogGroup(
                    self,
                    f"{self._SERVICE_NAME}RedisLogGroup",
                    retention=aws_logs.RetentionDays.ONE_WEEK,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
            health_check=aws_ecs.HealthCheck(
                command=["CMD", "redis-cli", "ping"],
                interval=Duration.seconds(REDIS_HEALTH_INTERVAL_SECONDS),
                retries=CONTAINER_HEALTH_RETRIES,
                start_period=Duration.seconds(REDIS_HEALTH_START_PERIOD_SECONDS),
                timeout=Duration.seconds(CONTAINER_HEALTH_TIMEOUT_SECONDS),
            ),
        )
        redis_container.add_port_mappings(aws_ecs.PortMapping(container_port=REDIS_PORT))

        tracker_container = task_def.add_container(
            f"{self._SERVICE_NAME}Container",
            image=aws_ecs.ContainerImage.from_asset(
                "../services/tracker",
                file="Dockerfile",
                platform=Platform.LINUX_AMD64,
            ),
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix=self._SERVICE_NAME,
                log_group=aws_logs.LogGroup(
                    self,
                    f"{self._SERVICE_NAME}LogGroup",
                    retention=aws_logs.RetentionDays.ONE_WEEK,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
            port_mappings=[aws_ecs.PortMapping(container_port=TRACKER_PORT)],
            environment={
                "REDIS_URL": f"redis://localhost:{REDIS_PORT}",
                "BROKER_ENVIRONMENT": "production",
                "BENCHMARK_SERVICE_URL": f"http://swebench.{NAMESPACE}:{8000}",
                "AWS_S3_BUCKET": bucket.bucket_name,
                "DB_HOST": self.database.db_instance_endpoint_address,
                "DB_PORT": self.database.db_instance_endpoint_port,
                "DB_NAME": POSTGRES_DB,
            },
            secrets={
                "DB_USERNAME": aws_ecs.Secret.from_secrets_manager(db_credentials, field="username"),
                "DB_PASSWORD": aws_ecs.Secret.from_secrets_manager(db_credentials, field="password"),
            },
            health_check=aws_ecs.HealthCheck(
                command=["CMD-SHELL", f"curl -f http://localhost:{TRACKER_PORT}/health || exit 1"],
                interval=Duration.seconds(CONTAINER_HEALTH_INTERVAL_SECONDS),
                retries=CONTAINER_HEALTH_RETRIES,
                start_period=Duration.seconds(CONTAINER_HEALTH_START_PERIOD_SECONDS),
                timeout=Duration.seconds(CONTAINER_HEALTH_TIMEOUT_SECONDS),
            ),
        )

        # sidecar dependencies
        tracker_container.add_container_dependencies(
            aws_ecs.ContainerDependency(
                container=redis_container,
                condition=aws_ecs.ContainerDependencyCondition.HEALTHY,
            ),
        )

        task_def.default_container = tracker_container

        # fargate service with public domain
        self.service = aws_ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            f"{self._SERVICE_NAME}Service",
            cluster=cluster,
            desired_count=TRACKER_MIN_TASKS,
            task_definition=task_def,
            service_name=self._SERVICE_NAME,
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            domain_name=TRACKER_DOMAIN,
            domain_zone=hosted_zone,
            protocol=aws_elasticloadbalancingv2.ApplicationProtocol.HTTPS,
            redirect_http=True,
            open_listener=False,  # security group not configured, manually add whitelisted IPs (no public access by default)
            assign_public_ip=True,
            public_load_balancer=True,
        )

        # register with service discovery for internal access
        self.service.service.enable_cloud_map(
            name="tracker",
            cloud_map_namespace=namespace,
        )

        # load balancer health check
        self.service.target_group.configure_health_check(
            path="/health",
            port=str(TRACKER_PORT),
            interval=Duration.seconds(ALB_HEALTH_INTERVAL_SECONDS),
        )

        # request timeout
        self.service.load_balancer.set_attribute("idle_timeout.timeout_seconds", str(ALB_IDLE_TIMEOUT_SECONDS))

        # allow HTTP to HTTPS redirect
        self.service.load_balancer.connections.allow_from(
            aws_ec2.Peer.any_ipv4(),
            aws_ec2.Port.tcp(80),
            description="Allow HTTP from anywhere (redirects to HTTPS)",
        )

        # allow HTTPS from whitelisted IPs only
        for ip, desc in ALLOWED_IPS:
            self.service.load_balancer.connections.allow_from(
                aws_ec2.Peer.ipv4(ip),
                aws_ec2.Port.tcp(443),
                description=f"Allow HTTPS from {desc}",
            )

        # auto-scaling
        scaling = self.service.service.auto_scale_task_count(
            min_capacity=TRACKER_MIN_TASKS,
            max_capacity=TRACKER_MAX_TASKS,
        )
        scaling.scale_on_cpu_utilization(
            "CpuScaling",
            target_utilization_percent=TRACKER_SCALING_CPU_PERCENT,
        )

        # Grant S3 read/write permissions to the task
        bucket.grant_read_write(task_def.task_role)

        # CloudWatch Logs permissions for log groups
        task_def.add_to_task_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:PutRetentionPolicy",
                    "logs:DeleteLogStream",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                ],
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:benchmarks/*",
                ],
            )
        )

        # Lambda invoke permissions
        task_def.add_to_task_role_policy(
            aws_iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[f"arn:aws:lambda:{self.region}:{self.account}:function:*"],
            )
        )

        # Secrets Manager read permissions (secret name is provided by the client at runtime)
        task_def.add_to_task_role_policy(
            aws_iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:*"],
            )
        )

        # Allow Fargate service to connect to RDS
        self.database.connections.allow_from(
            self.service.service,
            aws_ec2.Port.tcp(POSTGRES_PORT),
            description="Allow Tracker Fargate service to connect to RDS",
        )
