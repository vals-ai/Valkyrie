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
    aws_kms,
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
    BENCHMARK_SERVICE_PORT,
    CONTAINER_HEALTH_INTERVAL_SECONDS,
    CONTAINER_HEALTH_RETRIES,
    CONTAINER_HEALTH_START_PERIOD_SECONDS,
    CONTAINER_HEALTH_TIMEOUT_SECONDS,
    DOCKER_ASSET_EXCLUDES,
    EXECUTOR_RELEASE_BUCKET_NAME,
    EXECUTOR_RELEASE_PREFIX,
    EXECUTOR_RUNNER_LOG_GROUP_NAME,
    RUNNER_STOP_TIMEOUT_SECONDS,
    POSTGRES_DB,
    POSTGRES_PORT,
    POSTGRES_USER,
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
from runtime_iam import create_executor_task_role, create_tracker_task_role, managed_runtime_environment
from stage import PROD, Stage
from stage_config import benchmark_service_base_url, config_for
from tracker_access_logs import create_tracker_access_logs

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
            "AWS_S3_BUCKET": bucket_name,
            "ENVIRONMENT": stage_config.runtime_environment,
            "SENTRY_ENVIRONMENT": stage_config.sentry_environment,
            "BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE": namespace.namespace_name,
            "DAYTONA_HAPPY_EYEBALLS_DELAY": "none",
            "SANDBOX_QUEUE_ENABLED": os.environ.get("SANDBOX_QUEUE_ENABLED") or "false",
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

        proxy_security_group = aws_ec2.SecurityGroup(
            self,
            "TrackerDbProxySecurityGroup",
            vpc=vpc,
            description="Security group for Tracker RDS proxy",
        )
        proxy_security_group.add_ingress_rule(
            peer=aws_ec2.Peer.ipv4(VPC_CIDR),
            connection=aws_ec2.Port.tcp(POSTGRES_PORT),
            description="Allow VPC services to connect to RDS proxy",
        )
        # Queued sandbox admission holds a session advisory lock while it waits on sandbox creation.
        # The proxy closes idle clients after 30 minutes by default, which would release that lock.
        self.database_proxy = self.database.add_proxy(
            "TrackerDatabaseProxy",
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PUBLIC),
            secrets=[db_credentials_secret],
            security_groups=[proxy_security_group],
            idle_client_timeout=Duration.hours(8),
        )

        # Retain old endpoint exports until consumer stacks have deployed the proxy endpoint.
        self.export_value(self.database.db_instance_endpoint_address)
        self.export_value(self.database.db_instance_endpoint_port)

        db_env = {
            "DB_HOST": self.database_proxy.endpoint,
            "DB_PORT": str(POSTGRES_PORT),
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

        # Runner revisions live with the Tracker image; active dispatches keep their revision.
        runner_family = stage.phys("ExecutorRunner")
        runner_bucket = aws_s3.Bucket.from_bucket_name(
            self, "ExecutorReleaseArtifacts", f"{stage.phys(EXECUTOR_RELEASE_BUCKET_NAME)}-{self.account}"
        )
        payload_key = aws_kms.Key(
            self, "ExecutorPayloadKey", enable_key_rotation=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        runner_security_group = aws_ec2.SecurityGroup(
            self, "ExecutorRunnerSG", vpc=vpc,
            description="No-ingress security group for one-dispatch executor tasks",
            allow_all_outbound=False,
        )
        runner_security_group.add_egress_rule(aws_ec2.Peer.ipv4(VPC_CIDR), aws_ec2.Port.tcp(POSTGRES_PORT), "Tracker RDS proxy")
        runner_security_group.add_egress_rule(aws_ec2.Peer.ipv4(VPC_CIDR), aws_ec2.Port.udp(53), "VPC DNS UDP")
        runner_security_group.add_egress_rule(aws_ec2.Peer.ipv4(VPC_CIDR), aws_ec2.Port.tcp(53), "VPC DNS TCP")
        runner_security_group.add_egress_rule(aws_ec2.Peer.any_ipv4(), aws_ec2.Port.tcp(443), "AWS API endpoints and release artifacts")

        runner_task_role = create_executor_task_role(self, stage, bucket, stage_config.managed_aws)
        runner_task_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[runner_bucket.arn_for_objects(f"{EXECUTOR_RELEASE_PREFIX}/*")],
        ))
        payload_key.grant_decrypt(runner_task_role)
        runner_execution_role = aws_iam.Role(
            self, "ExecutorRunnerExecutionRole",
            role_name=stage.phys("ValkyrieExecutorRunnerExecution"),
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com")),
            managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonECSTaskExecutionRolePolicy"
            )],
        )
        db_credentials_secret.grant_read(runner_execution_role)
        if sentry_secret_name:
            sentry_secret.grant_read(runner_execution_role)
        runner_task_def = aws_ecs.FargateTaskDefinition(
            self, "ExecutorRunnerTaskDef", family=runner_family,
            cpu=1024, memory_limit_mib=4096, runtime_platform=_ARM64_PLATFORM,
            task_role=cast(aws_iam.IRole, runner_task_role),
            execution_role=cast(aws_iam.IRole, runner_execution_role),
        )
        runner_task_def.add_container(
            "ExecutorRunnerContainer", image=tracker_image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="ExecutorRunner",
                log_group=aws_logs.LogGroup(
                    self, "ExecutorRunnerLogGroup",
                    log_group_name=stage.phys(EXECUTOR_RUNNER_LOG_GROUP_NAME),
                    retention=stage_config.service_log_retention,
                    removal_policy=cdk.RemovalPolicy.RETAIN,
                ),
            ),
            environment={
                **shared_env, **db_env,
                "EXECUTOR_RELEASE_BUCKET": runner_bucket.bucket_name,
                "EXECUTOR_RELEASE_PREFIX": EXECUTOR_RELEASE_PREFIX,
                "EXECUTOR_CACHE_DIR": "/tmp/executor-cache",
                "SENTRY_RELEASE": os.environ.get("SENTRY_RELEASE", ""),
                "EXECUTOR_LAUNCHER": "ecs",
                "EXECUTOR_PAYLOAD_KMS_KEY_ID": payload_key.key_id,
            },
            secrets={**db_secrets, **sentry_secrets},
            stop_timeout=Duration.seconds(RUNNER_STOP_TIMEOUT_SECONDS),
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

        runner_family_arn = self.format_arn(
            service="ecs", resource="task-definition", resource_name=f"{runner_family}:*"
        )
        self.tracker_task_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["ecs:RunTask"], resources=[runner_family_arn],
            conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
        ))
        self.tracker_task_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=[runner_task_role.role_arn, runner_execution_role.role_arn],
            conditions={"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
        ))
        self.tracker_task_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["kms:GenerateDataKey"], resources=[payload_key.key_arn],
        ))
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
                "EXECUTOR_LAUNCHER": "ecs",
                "EXECUTOR_RUNNER_CLUSTER": cluster.cluster_arn,
                "EXECUTOR_RUNNER_TASK_DEFINITION": runner_task_def.task_definition_arn,
                "EXECUTOR_RUNNER_SUBNETS": cdk.Fn.join(",", vpc.select_subnets(subnet_type=aws_ec2.SubnetType.PUBLIC).subnet_ids),
                "EXECUTOR_RUNNER_SECURITY_GROUP": runner_security_group.security_group_id,
                "EXECUTOR_RUNNER_CONTAINER": "ExecutorRunnerContainer",
                "EXECUTOR_PAYLOAD_KMS_KEY_ID": payload_key.key_id,
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

        tracker_security_group = self.service.service.connections.security_groups[0]
        cfn_tracker_security_group = cast(aws_ec2.CfnSecurityGroup, tracker_security_group.node.default_child)
        cfn_tracker_security_group.security_group_egress = [
            aws_ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="tcp",
                from_port=POSTGRES_PORT,
                to_port=POSTGRES_PORT,
                cidr_ip=VPC_CIDR,
                description="Tracker PostgreSQL",
            ),
            aws_ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="tcp",
                from_port=BENCHMARK_SERVICE_PORT,
                to_port=BENCHMARK_SERVICE_PORT,
                cidr_ip=VPC_CIDR,
                description="Benchmark service Cloud Map calls",
            ),
            aws_ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="udp",
                from_port=53,
                to_port=53,
                cidr_ip=VPC_CIDR,
                description="VPC DNS UDP",
            ),
            aws_ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="tcp",
                from_port=53,
                to_port=53,
                cidr_ip=VPC_CIDR,
                description="VPC DNS TCP",
            ),
            aws_ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="tcp",
                from_port=443,
                to_port=443,
                cidr_ip="0.0.0.0/0",
                description="AWS API endpoints",
            ),
        ]

        create_tracker_access_logs(
            self,
            stage=stage,
            load_balancer=self.service.load_balancer,
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
        # Retained only so WorkerStack can drop this import in this deploy.
        # Delete the export in the follow-up cleanup PR with the host-removal classifier rule.
        self.export_value(tracker_security_group.security_group_id)
        # Allow Tracker and one-dispatch runners in the VPC to reach RDS.
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
