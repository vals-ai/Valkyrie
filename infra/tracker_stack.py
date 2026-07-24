"""Tracker service stack - public-facing API with ALB and shared RDS database."""

import os
from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_certificatemanager,
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
    DEV_TRACKER_ALB_DNS_PARAMETER,
    DEV_TRACKER_CERTIFICATE_ARN_PARAMETER,
    DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER,
    DEV_TRACKER_SECURITY_GROUP_PARAMETER,
    DOCKER_ASSET_EXCLUDES,
    EXECUTOR_RELEASE_PREFIX,
    EXECUTOR_RELEASE_ROLE_NAME,
    POSTGRES_DB,
    POSTGRES_PORT,
    POSTGRES_USER,
    TRACKER_DOMAIN,
    TRACKER_LOG_GROUP_NAME,
    TRACKER_PORT,
    TRACKER_SCALING_CPU_PERCENT,
    VPC_CIDR,
    executor_release_launch_parameter,
    stage_parameter_name,
)
from constructs import Construct
from stage import Stage
from stage_config import StageConfig, benchmark_service_base_url, config_for

_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class TrackerStack(Stack):
    """Tracker stack: public API behind an ALB and the shared RDS database.

    Exposes ``database``, ``db_credentials``, and ``tracker_fargate_service``
    so the historical :class:`WorkerStack` can connect ExecutorHost to the
    shared network and database.
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
        executor_release_bucket: aws_s3.IBucket,
        redis_url: str,
        tracker_repository: aws_ecr.IRepository | None = None,
        image_tag: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)
        stage_config = config_for(stage)

        # Release-test writes images only to its stage-qualified repository;
        # other stages retain the established CDK asset path.
        if stage.is_release_test:
            if tracker_repository is None or image_tag is None:
                raise ValueError("Release-test Tracker requires a repository and immutable image tag")
            tracker_image = aws_ecs.ContainerImage.from_ecr_repository(tracker_repository, image_tag)
        else:
            tracker_image = aws_ecs.ContainerImage.from_asset(
                "..",
                file="services/tracker/Dockerfile",
                platform=Platform.LINUX_ARM64,
                exclude=list(DOCKER_ASSET_EXCLUDES),
            )

        # Shared environment variables
        benchmark_service_url = benchmark_service_base_url(stage)
        shared_env = {
            "BROKER_ENVIRONMENT": stage_config.runtime_environment,
            "AWS_S3_BUCKET": bucket_name,
            "ENVIRONMENT": stage_config.runtime_environment,
            "BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE": namespace.namespace_name,
            "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
            **({"BENCHMARK_SERVICE_BASE_URL": benchmark_service_url} if benchmark_service_url else {}),
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
            publicly_accessible=stage.is_prod,
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

        sentry_secret_name = "valkyrie/sentry-dsn" if stage.is_prod else os.environ.get("SENTRY_DSN_SECRET_NAME", "")
        sentry_secrets: dict[str, aws_ecs.Secret] = {}
        if sentry_secret_name:
            sentry_secret = aws_secretsmanager.Secret.from_secret_name_v2(
                self,
                "SentryDsnSecret",
                sentry_secret_name,
            )
            sentry_secrets["SENTRY_DSN"] = aws_ecs.Secret.from_secrets_manager(sentry_secret)

        auth_required = os.environ.get("AUTH_REQUIRED", "false")
        descope_project_id = os.environ.get("DESCOPE_PROJECT_ID", "")
        if not stage.is_prod:
            auth_required = "true"
            if not descope_project_id:
                raise ValueError("Development deployments require DESCOPE_PROJECT_ID.")

        descope_secrets: dict[str, aws_ecs.Secret] = {}
        if auth_required.lower() == "true":
            descope_management_key_secret = aws_secretsmanager.Secret.from_secret_name_v2(
                self,
                "DescopeManagementKeySecret",
                "devEvalInfraDescopeManagementKey",
            )
            descope_secrets["DESCOPE_MANAGEMENT_KEY"] = aws_ecs.Secret.from_secrets_manager(
                descope_management_key_secret,
            )

        # ── Tracker API service ──────────────────────────────────────────

        tracker_task_def = aws_ecs.FargateTaskDefinition(
            self,
            "TrackerTaskDef",
            cpu=stage_config.tracker.cpu,
            memory_limit_mib=stage_config.tracker.memory_mib,
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
                "BENCHMARK_CATALOG_URL": os.environ.get("BENCHMARK_CATALOG_URL", ""),
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
        certificate: aws_certificatemanager.ICertificate | None = None
        if stage.is_prod:
            if hosted_zone is None:
                raise ValueError("Production requires the vals.ai hosted zone")
            tracker_domain = TRACKER_DOMAIN
            tracker_hosted_zone = hosted_zone
        elif not stage.is_release_test:
            tracker_domain = stage.domain(TRACKER_DOMAIN)
            tracker_hosted_zone = aws_route53.HostedZone.from_hosted_zone_attributes(
                self,
                "DevTrackerHostedZone",
                hosted_zone_id=aws_ssm.StringParameter.value_for_string_parameter(
                    self,
                    DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER,
                ),
                zone_name=tracker_domain,
            )
            certificate = aws_certificatemanager.Certificate.from_certificate_arn(
                self,
                "DevTrackerCertificate",
                aws_ssm.StringParameter.value_for_string_parameter(
                    self,
                    DEV_TRACKER_CERTIFICATE_ARN_PARAMETER,
                ),
            )

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
            certificate=certificate,
            protocol=(
                aws_elasticloadbalancingv2.ApplicationProtocol.HTTPS
                if certificate is not None
                else aws_elasticloadbalancingv2.ApplicationProtocol.HTTP
            ),
            redirect_http=certificate is not None,
            open_listener=False,
            assign_public_ip=True,
            public_load_balancer=not stage.is_release_test,
        )

        # Expose the inner FargateService for cross-stack security group rules
        self.tracker_fargate_service = self.service.service
        self._create_executor_release_control(
            stage=stage,
            stage_config=stage_config,
            vpc=vpc,
            cluster=cluster,
            tracker_image=tracker_image,
            release_bucket=executor_release_bucket,
            db_secret=db_credentials_secret,
        )

        # Cloud Map registration for internal access.
        # Intentionally unstaged: dev gets its own namespace, and benchmark
        # services reach the tracker by the same internal name in either env.
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

        # Allow VPC services (tracker + worker) to reach RDS.
        db_security_group.add_ingress_rule(
            peer=aws_ec2.Peer.ipv4(VPC_CIDR),
            connection=aws_ec2.Port.tcp(POSTGRES_PORT),
            description="Allow VPC services to connect to RDS",
        )

        if not stage.is_prod:
            aws_ssm.StringParameter(
                self,
                "TrackerSecurityGroupParameter",
                parameter_name=stage_parameter_name(DEV_TRACKER_SECURITY_GROUP_PARAMETER, stage.name),
                string_value=self.service.service.connections.security_groups[0].security_group_id,
            )
            aws_ssm.StringParameter(
                self,
                "TrackerAlbDnsParameter",
                parameter_name=stage_parameter_name(DEV_TRACKER_ALB_DNS_PARAMETER, stage.name),
                string_value=self.service.load_balancer.load_balancer_dns_name,
            )

    def _create_executor_release_control(
        self,
        *,
        stage: Stage,
        stage_config: StageConfig,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        tracker_image: aws_ecs.ContainerImage,
        release_bucket: aws_s3.IBucket,
        db_secret: aws_secretsmanager.ISecret,
    ) -> None:
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
                resources=[release_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
            )
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[db_secret.secret_arn],
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
                "tracker.release_entrypoint",
                db_secret.secret_arn,
                self.database.db_instance_endpoint_address,
                self.database.db_instance_endpoint_port,
                POSTGRES_DB,
                release_bucket.bucket_name,
                EXECUTOR_RELEASE_PREFIX,
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

        tracker_security_group = self.tracker_fargate_service.connections.security_groups[0]
        launch_parameter = aws_ssm.StringParameter(
            self,
            "ExecutorReleaseLaunchConfig",
            parameter_name=executor_release_launch_parameter(stage.name),
            string_value=self.to_json_string(
                {
                    "bucket": release_bucket.bucket_name,
                    "cluster": cluster.cluster_arn,
                    "container": container_name,
                    "prefix": EXECUTOR_RELEASE_PREFIX,
                    "security_group": tracker_security_group.security_group_id,
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
        github_environment = "production-release" if stage.is_prod else "dev"
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
                                f"repo:vals-ai/Valkyrie:environment:{github_environment}"
                            ),
                        }
                    },
                ),
            ),
        )
        release_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[release_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
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
