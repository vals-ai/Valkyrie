"""Tracker service stack - public-facing API with ALB and shared RDS database."""

import os
from pathlib import Path
from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_ecr,
    aws_ecs,
    aws_ecs_patterns,
    aws_elasticloadbalancingv2,
    aws_iam,
    aws_logs,
    aws_rds,
    aws_route53,
    aws_s3,
    aws_secretsmanager,
    aws_servicediscovery,
    aws_ssm,
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
    DOCKER_ASSET_EXCLUDES,
    POSTGRES_DB,
    POSTGRES_PORT,
    POSTGRES_USER,
    REDIS_PORT,
    TRACKER_DOMAIN,
    TRACKER_ALB_DNS_PARAMETER_PATH,
    TRACKER_HOSTED_ZONE_ID_PARAMETER_PATH,
    TRACKER_LOG_GROUP_NAME,
    TRACKER_PORT,
    TRACKER_SCALING_CPU_PERCENT,
    TRACKER_SECURITY_GROUP_PARAMETER_PATH,
    VPC_CIDR,
    stage_parameter_name,
)
from constructs import Construct
from runtime_iam import create_tracker_task_role, managed_runtime_environment
from stage import PROD, Stage
from stage_config import benchmark_service_base_url, config_for

_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class TrackerStack(Stack):
    """Tracker stack: public API behind an ALB and the shared RDS database.

    Exposes its image, database, credentials, and service so ExecutorStack can
    consume the shared runtime contracts without owning Tracker resources.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        stage: Stage,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        namespace: aws_servicediscovery.IPrivateDnsNamespace,
        hosted_zone: aws_route53.IHostedZone | None,
        bucket_name: str,
        redis_url: str,
        redis_security_group: aws_ec2.ISecurityGroup,
        tracker_repository: aws_ecr.IRepository | None = None,
        image_tag: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)
        stage_config = config_for(stage)
        bucket = aws_s3.Bucket.from_bucket_name(self, "ManagedRuntimeBucket", bucket_name)

        # Release-test writes images only to its stage-qualified repository;
        # other stages retain the established CDK asset path.
        if stage.is_release_test:
            if tracker_repository is None or image_tag is None:
                raise ValueError("Release-test Tracker requires a repository and immutable image tag")
            tracker_image = aws_ecs.ContainerImage.from_ecr_repository(tracker_repository, image_tag)
        else:
            tracker_image = aws_ecs.ContainerImage.from_asset(
                str(Path(__file__).resolve().parent.parent / "services" / "tracker"),
                file="Dockerfile",
                platform=Platform.LINUX_ARM64,
                exclude=list(DOCKER_ASSET_EXCLUDES),
                ignore_mode=cdk.IgnoreMode.DOCKER,
            )
        self.tracker_image = tracker_image

        # Shared environment variables
        benchmark_service_url = benchmark_service_base_url(stage)
        shared_env = {
            "BROKER_ENVIRONMENT": stage_config.runtime_environment,
            "AWS_S3_BUCKET": bucket_name,
            "ENVIRONMENT": stage_config.runtime_environment,
            "BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE": namespace.namespace_name,
            "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
            **({"BENCHMARK_SERVICE_BASE_URL": benchmark_service_url} if benchmark_service_url else {}),
            **managed_runtime_environment(self, stage, bucket, stage_config.managed_aws),
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
        db_credentials_secret = cast(aws_secretsmanager.ISecret, self.db_credentials)

        self.database = aws_rds.DatabaseInstance(
            self,
            "TrackerDatabase",
            engine=aws_rds.DatabaseInstanceEngine.postgres(
                version=aws_rds.PostgresEngineVersion.VER_16,
            ),
            instance_type=aws_ec2.InstanceType(stage_config.database.instance_class),
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PUBLIC),
            security_groups=[db_security_group],
            credentials=aws_rds.Credentials.from_secret(db_credentials_secret),
            database_name=POSTGRES_DB,
            allocated_storage=stage_config.database.allocated_storage_gb,
            publicly_accessible=stage.is_bench,
            storage_encrypted=True if stage.name == PROD else None,
            deletion_protection=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            backup_retention=Duration.days(stage_config.database.backup_retention_days),
        )

        db_env = {
            "DB_HOST": self.database.db_instance_endpoint_address,
            "DB_PORT": self.database.db_instance_endpoint_port,
            "DB_NAME": POSTGRES_DB,
        }

        db_secrets = {
            "DB_USERNAME": aws_ecs.Secret.from_secrets_manager(db_credentials_secret, field="username"),
            "DB_PASSWORD": aws_ecs.Secret.from_secrets_manager(db_credentials_secret, field="password"),
        }

        sentry_secret_name = os.environ.get("SENTRY_DSN_SECRET_NAME", "")
        if not stage.is_release_test and not sentry_secret_name:
            raise ValueError("Dev and production deployments require SENTRY_DSN_SECRET_NAME.")

        sentry_secrets: dict[str, aws_ecs.Secret] = {}
        if sentry_secret_name:
            sentry_secret = aws_secretsmanager.Secret.from_secret_name_v2(
                self,
                "SentryDsnSecret",
                sentry_secret_name,
            )
            sentry_secrets["SENTRY_DSN"] = aws_ecs.Secret.from_secrets_manager(sentry_secret)

        auth_required = os.environ.get("AUTH_REQUIRED", "false")
        benchmark_catalog_url = os.environ.get("BENCHMARK_CATALOG_URL", "")
        descope_project_id = os.environ.get("DESCOPE_PROJECT_ID", "")
        if not stage.is_bench:
            auth_required = "true"
            if not descope_project_id:
                raise ValueError(f"{stage.name} deployments require DESCOPE_PROJECT_ID.")

        descope_secrets: dict[str, aws_ecs.Secret] = {}
        if auth_required.lower() == "true":
            descope_management_key_secret_name = os.environ.get("DESCOPE_MANAGEMENT_KEY_SECRET_NAME", "")
            if not descope_management_key_secret_name:
                raise ValueError("Authenticated deployments require DESCOPE_MANAGEMENT_KEY_SECRET_NAME.")
            descope_management_key_secret = aws_secretsmanager.Secret.from_secret_name_v2(
                self,
                "DescopeManagementKeySecret",
                descope_management_key_secret_name,
            )
            descope_secrets["DESCOPE_MANAGEMENT_KEY"] = aws_ecs.Secret.from_secrets_manager(
                descope_management_key_secret,
            )

        # ── Tracker API service ──────────────────────────────────────────

        self.tracker_task_role = create_tracker_task_role(self, stage, bucket, stage_config.managed_aws)
        tracker_task_def = aws_ecs.FargateTaskDefinition(
            self,
            "TrackerTaskDef",
            cpu=stage_config.tracker.cpu,
            memory_limit_mib=stage_config.tracker.memory_mib,
            runtime_platform=_ARM64_PLATFORM,
            task_role=cast(aws_iam.IRole, self.tracker_task_role),
        )

        cdk.CfnOutput(self, "TrackerTaskRoleArn", value=self.tracker_task_role.role_arn)

        tracker_task_def.add_container(
            "TrackerContainer",
            image=tracker_image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="Tracker",
                log_group=aws_logs.LogGroup(
                    self,
                    "TrackerLogGroup",
                    log_group_name=stage.phys(TRACKER_LOG_GROUP_NAME),
                    retention=stage_config.service_log_retention,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
            port_mappings=[aws_ecs.PortMapping(container_port=TRACKER_PORT)],
            environment={
                **shared_env,
                **db_env,
                "REDIS_URL": redis_url,
                "AUTH_REQUIRED": auth_required,
                "BENCHMARK_CATALOG_URL": benchmark_catalog_url,
                "DESCOPE_PROJECT_ID": descope_project_id,
                "SENTRY_RELEASE": os.environ.get("SENTRY_RELEASE", ""),
            },
            secrets={
                **db_secrets,
                **sentry_secrets,
                **descope_secrets,
            },
            command=["uv", "run", "--no-sync", "python", "-m", "tracker.serve"],
            health_check=aws_ecs.HealthCheck(
                command=["CMD-SHELL", f"curl -f http://localhost:{TRACKER_PORT}/health || exit 1"],
                interval=Duration.seconds(CONTAINER_HEALTH_INTERVAL_SECONDS),
                retries=CONTAINER_HEALTH_RETRIES,
                start_period=Duration.seconds(CONTAINER_HEALTH_START_PERIOD_SECONDS),
                timeout=Duration.seconds(CONTAINER_HEALTH_TIMEOUT_SECONDS),
            ),
        )

        tracker_domain: str | None = None
        tracker_hosted_zone: aws_route53.IHostedZone | None = None
        if stage.is_bench:
            if hosted_zone is None:
                raise ValueError("Bench requires the vals.ai hosted zone")
            tracker_domain = stage.domain(TRACKER_DOMAIN)
            tracker_hosted_zone = hosted_zone
        elif not stage.is_release_test:
            tracker_domain = stage.domain(TRACKER_DOMAIN)
            tracker_hosted_zone = aws_route53.HostedZone.from_hosted_zone_attributes(
                self,
                "TrackerHostedZone",
                hosted_zone_id=aws_ssm.StringParameter.value_for_string_parameter(
                    self,
                    stage_parameter_name(stage.name, TRACKER_HOSTED_ZONE_ID_PARAMETER_PATH),
                ),
                zone_name=tracker_domain,
            )
        tls_enabled = not stage.is_release_test

        self.service = aws_ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "TrackerService",
            cluster=cluster,
            desired_count=stage_config.tracker.min_tasks,
            task_definition=tracker_task_def,
            service_name=stage.phys("Tracker"),
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            domain_name=tracker_domain,
            domain_zone=tracker_hosted_zone,
            protocol=(
                aws_elasticloadbalancingv2.ApplicationProtocol.HTTPS
                if tls_enabled
                else aws_elasticloadbalancingv2.ApplicationProtocol.HTTP
            ),
            redirect_http=tls_enabled,
            open_listener=False,
            assign_public_ip=True,
            public_load_balancer=not stage.is_release_test,
        )

        # Expose the inner FargateService for cross-stack security group rules.
        self.tracker_fargate_service = self.service.service

        # The stage-specific namespace isolates the stable tracker service name.
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

        if stage.is_release_test:
            self.service.load_balancer.connections.allow_from(
                aws_ec2.Peer.ipv4(VPC_CIDR),
                aws_ec2.Port.tcp(80),
                description="Allow release-test Tracker access from the VPC",
            )
        else:
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
            min_capacity=stage_config.tracker.min_tasks,
            max_capacity=stage_config.tracker.max_tasks,
        )
        tracker_scaling.scale_on_cpu_utilization(
            "CpuScaling",
            target_utilization_percent=TRACKER_SCALING_CPU_PERCENT,
        )

        # ── Network access ───────────────────────────────────────────────

        tracker_security_group = self.tracker_fargate_service.connections.security_groups[0]
        aws_ec2.CfnSecurityGroupIngress(
            self,
            "TrackerToRedisIngress",
            group_id=redis_security_group.security_group_id,
            source_security_group_id=tracker_security_group.security_group_id,
            ip_protocol="tcp",
            from_port=REDIS_PORT,
            to_port=REDIS_PORT,
            description="Allow Tracker and ExecutorHost to connect to Redis",
        )

        # Allow VPC services (Tracker and ExecutorHost) to reach RDS.
        db_security_group.add_ingress_rule(
            peer=aws_ec2.Peer.ipv4(VPC_CIDR),
            connection=aws_ec2.Port.tcp(POSTGRES_PORT),
            description="Allow VPC services to connect to RDS",
        )

        if not stage.is_bench:
            aws_ssm.StringParameter(
                self,
                "TrackerSecurityGroupParameter",
                parameter_name=stage_parameter_name(stage.name, TRACKER_SECURITY_GROUP_PARAMETER_PATH),
                string_value=tracker_security_group.security_group_id,
            )
            aws_ssm.StringParameter(
                self,
                "TrackerAlbDnsParameter",
                parameter_name=stage_parameter_name(stage.name, TRACKER_ALB_DNS_PARAMETER_PATH),
                string_value=self.service.load_balancer.load_balancer_dns_name,
            )
