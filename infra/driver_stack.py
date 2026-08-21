"""Release-test-only one-shot driver task and least-privilege launch boundary."""

from __future__ import annotations

import os
from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_ec2,
    aws_ecr,
    aws_ecs,
    aws_iam,
    aws_logs,
    aws_s3,
    aws_secretsmanager,
    aws_ssm,
)
from constants import (
    DRIVER_LOG_GROUP_PARAMETER_PATH,
    DRIVER_OPERATOR_ROLE_PARAMETER_PATH,
    DRIVER_SECURITY_GROUP_PARAMETER_PATH,
    DRIVER_TASK_DEFINITION_PARAMETER_PATH,
    DRIVER_LOG_GROUP_NAME,
    POSTGRES_DB,
    POSTGRES_PORT,
    REDIS_PORT,
    RELEASE_TEST_DRIVER_SECRET_ARN_ENV,
    RELEASE_TEST_OPERATOR_PRINCIPAL_ARN_ENV,
    RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN_ENV,
    TRACKER_ALB_DNS_PARAMETER_PATH,
    VPC_CIDR,
    stage_parameter_name,
)
from constructs import Construct
from stage import Stage
from stage_config import config_for

_ARTIFACT_PREFIX = "releases/package-r"
_CAMPAIGN_AGENT_KEY = "agents/coexistence_sleep_agent.zip"
_ARM64_PLATFORM = aws_ecs.RuntimePlatform(
    cpu_architecture=aws_ecs.CpuArchitecture.ARM64,
    operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
)


class DriverStack(Stack):
    """Static Package R driver contract; operators launch tasks explicitly."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        stage: Stage,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        bucket: aws_s3.IBucket,
        tracker_repository: aws_ecr.IRepository,
        image_tag: str,
        db_host: str,
        db_port: str,
        db_credentials: aws_secretsmanager.ISecret,
        redis_url: str,
        redis_security_group: aws_ec2.ISecurityGroup,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        if not stage.is_release_test:
            raise ValueError("Package R DriverStack is restricted to release-test")

        driver_secret_arn = os.environ.get(RELEASE_TEST_DRIVER_SECRET_ARN_ENV)
        if not driver_secret_arn:
            raise ValueError(f"Release-test synthesis requires {RELEASE_TEST_DRIVER_SECRET_ARN_ENV}")
        sandbox_provider_secret_arn = os.environ.get(RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN_ENV)
        if not sandbox_provider_secret_arn:
            raise ValueError(f"Release-test synthesis requires {RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN_ENV}")
        operator_principal_arn = os.environ.get(RELEASE_TEST_OPERATOR_PRINCIPAL_ARN_ENV)
        if not operator_principal_arn:
            raise ValueError(f"Release-test synthesis requires {RELEASE_TEST_OPERATOR_PRINCIPAL_ARN_ENV}")

        stage_config = config_for(stage)
        tracker_alb_dns = aws_ssm.StringParameter.value_for_string_parameter(
            self,
            stage_parameter_name(stage.name, TRACKER_ALB_DNS_PARAMETER_PATH),
        )
        driver_image = aws_ecs.ContainerImage.from_ecr_repository(tracker_repository, image_tag)
        driver_secret = aws_secretsmanager.Secret.from_secret_complete_arn(
            self,
            "DriverCredentials",
            driver_secret_arn,
        )
        sandbox_provider_secret = aws_secretsmanager.Secret.from_secret_complete_arn(
            self,
            "SandboxProviderCredentials",
            sandbox_provider_secret_arn,
        )

        self.security_group = aws_ec2.SecurityGroup(
            self,
            "DriverSecurityGroup",
            vpc=vpc,
            security_group_name=stage.phys("PackageRDriver"),
            description="No-ingress security group for the release-test Package R driver",
            allow_all_outbound=False,
        )
        aws_ec2.CfnSecurityGroupIngress(
            self,
            "DriverToRedisIngress",
            group_id=redis_security_group.security_group_id,
            source_security_group_id=self.security_group.security_group_id,
            ip_protocol="tcp",
            from_port=REDIS_PORT,
            to_port=REDIS_PORT,
            description="Allow release-test Driver to connect to Redis",
        )
        self.security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.tcp(80),
            "Internal Tracker ALB",
        )
        self.security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.tcp(POSTGRES_PORT),
            "Release-test PostgreSQL",
        )
        self.security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.tcp(REDIS_PORT),
            "Release-test Redis",
        )
        self.security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.udp(53),
            "VPC DNS UDP",
        )
        self.security_group.add_egress_rule(
            aws_ec2.Peer.ipv4(VPC_CIDR),
            aws_ec2.Port.tcp(53),
            "VPC DNS TCP",
        )
        self.security_group.add_egress_rule(
            aws_ec2.Peer.any_ipv4(),
            aws_ec2.Port.tcp(443),
            "Approved AWS and benchmark HTTPS endpoints",
        )

        execution_role = aws_iam.Role(
            self,
            "DriverExecutionRole",
            role_name=stage.phys("PackageRDriverExecution"),
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com")),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
        )
        task_role = aws_iam.Role(
            self,
            "DriverTaskRole",
            role_name=stage.phys("PackageRDriverTask"),
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com")),
        )

        self.task_definition = aws_ecs.FargateTaskDefinition(
            self,
            "DriverTaskDefinition",
            family=stage.phys("PackageRDriver"),
            cpu=stage_config.tracker.cpu,
            memory_limit_mib=stage_config.tracker.memory_mib,
            runtime_platform=_ARM64_PLATFORM,
            execution_role=cast(aws_iam.IRole, execution_role),
            task_role=cast(aws_iam.IRole, task_role),
        )
        self.log_group = aws_logs.LogGroup(
            self,
            "DriverLogGroup",
            log_group_name=stage.phys(DRIVER_LOG_GROUP_NAME),
            retention=stage_config.service_log_retention,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        self.task_definition.add_container(
            "DriverContainer",
            image=driver_image,
            logging=aws_ecs.LogDriver.aws_logs(
                stream_prefix="PackageRDriver",
                log_group=self.log_group,
            ),
            environment={
                "AWS_S3_BUCKET": bucket.bucket_name,
                "DB_HOST": db_host,
                "DB_PORT": db_port,
                "DB_NAME": POSTGRES_DB,
                "REDIS_URL": redis_url,
                "TRACKER_BASE_URL": f"http://{tracker_alb_dns}",
            },
            secrets={
                "DB_USERNAME": aws_ecs.Secret.from_secrets_manager(db_credentials, field="username"),
                "DB_PASSWORD": aws_ecs.Secret.from_secrets_manager(db_credentials, field="password"),
                "TRACKER_API_KEY": aws_ecs.Secret.from_secrets_manager(driver_secret, field="tracker_api_key"),
                "BENCHMARK_AUTHORIZATION": aws_ecs.Secret.from_secrets_manager(
                    driver_secret,
                    field="benchmark_authorization",
                ),
            },
            command=[
                "/bin/sh",
                "-c",
                "echo 'A reviewed ECS command override is required for this release-test driver task.' >&2; exit 64",
            ],
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[
                    bucket.arn_for_objects(f"{_ARTIFACT_PREFIX}/*"),
                    bucket.arn_for_objects(_CAMPAIGN_AGENT_KEY),
                ],
            )
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
                resources=[bucket.arn_for_objects("benchmarks/*")],
            )
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": ["benchmarks/*"]}},
            )
        )
        campaign_log_group_arn = self.format_arn(
            service="logs",
            resource="log-group",
            resource_name="benchmarks/*",
            arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
        )
        task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:PutRetentionPolicy",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[campaign_log_group_arn, f"{campaign_log_group_arn}:log-stream:*"],
            )
        )
        sandbox_provider_secret.grant_read(task_role)

        self.operator_role = aws_iam.Role(
            self,
            "DriverOperatorRole",
            role_name=stage.phys("PackageRDriverOperator"),
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ArnPrincipal(operator_principal_arn)),
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=[self.task_definition.task_definition_arn],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[task_role.role_arn, execution_role.role_arn],
                conditions={"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
            )
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ecs:DescribeTasks"],
                resources=["*"],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["logs:DescribeLogStreams", "logs:GetLogEvents", "logs:FilterLogEvents"],
                resources=[self.log_group.log_group_arn, f"{self.log_group.log_group_arn}:*"],
            )
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
                resources=[bucket.arn_for_objects(f"{_ARTIFACT_PREFIX}/*")],
            )
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": [f"{_ARTIFACT_PREFIX}/*"]}},
            )
        )
        self.operator_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"],
                resources=[
                    self.format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name="valkyrie/release-test/driver/*",
                    )
                ],
            )
        )

        self._publish_launch_contract(stage)

    def _publish_launch_contract(self, stage: Stage) -> None:
        parameters = (
            (
                "DriverTaskDefinitionParameter",
                DRIVER_TASK_DEFINITION_PARAMETER_PATH,
                self.task_definition.task_definition_arn,
            ),
            (
                "DriverSecurityGroupParameter",
                DRIVER_SECURITY_GROUP_PARAMETER_PATH,
                self.security_group.security_group_id,
            ),
            ("DriverLogGroupParameter", DRIVER_LOG_GROUP_PARAMETER_PATH, self.log_group.log_group_name),
            (
                "DriverOperatorRoleParameter",
                DRIVER_OPERATOR_ROLE_PARAMETER_PATH,
                self.operator_role.role_arn,
            ),
        )
        for construct_id, parameter, value in parameters:
            aws_ssm.StringParameter(
                self,
                construct_id,
                parameter_name=stage_parameter_name(stage.name, parameter),
                string_value=value,
            )
