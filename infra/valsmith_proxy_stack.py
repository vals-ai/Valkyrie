"""Retained proxy images and redundant unprivileged Fargate proxy services."""

from typing import TYPE_CHECKING, cast

from aws_cdk import CfnOutput, Duration, Environment, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_route53resolver as resolver
from constructs import Construct
from valsmith_network_config import ACCOUNT, REGION, validate_proxy_image
from valsmith_network_endpoints import PROXY_EXECUTION_ROLE_NAME, PROXY_LOG_GROUP

if TYPE_CHECKING:
    from valsmith_network_stack import ValSmithNetworkStack


class ValSmithProxyImagesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, env: Environment) -> None:
        if env.account != ACCOUNT or env.region != REGION:
            raise ValueError("Proxy images require the reviewed Production account and Region")

        super().__init__(scope, construct_id, env=env)
        repository = ecr.Repository(
            self,
            "ProxyRepository",
            repository_name="valsmith-outbound-proxy-prod",
            image_tag_mutability=ecr.TagMutability.IMMUTABLE,
            image_scan_on_push=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        CfnOutput(self, "ProxyRepositoryUri", value=repository.repository_uri)


def add_proxy_services(stack: "ValSmithNetworkStack", image_uri: str) -> None:
    validate_proxy_image(image_uri)
    trust = iam.ServicePrincipal(
        "ecs-tasks.amazonaws.com",
        conditions={
            "StringEquals": {"aws:SourceAccount": ACCOUNT},
            "ArnLike": {"aws:SourceArn": f"arn:aws:ecs:{REGION}:{ACCOUNT}:*"},
        },
    )
    task_role = iam.Role(stack, "ProxyTaskRole", assumed_by=cast(iam.IPrincipal, trust))
    execution_role = iam.Role(
        stack,
        "ProxyExecutionRole",
        role_name=PROXY_EXECUTION_ROLE_NAME,
        assumed_by=cast(iam.IPrincipal, trust),
    )
    repository = ecr.Repository.from_repository_name(stack, "ProxyImage", "valsmith-outbound-proxy-prod")
    log_group = logs.LogGroup(
        stack,
        "ProxyLogs",
        log_group_name=PROXY_LOG_GROUP,
        retention=logs.RetentionDays.ONE_WEEK,
        removal_policy=RemovalPolicy.RETAIN,
    )
    cluster = ecs.Cluster(stack, "ProxyCluster", vpc=stack.vpc)
    task = ecs.FargateTaskDefinition(
        stack,
        "ProxyTask",
        cpu=512,
        memory_limit_mib=1024,
        task_role=cast(iam.IRole, task_role),
        execution_role=cast(iam.IRole, execution_role),
        runtime_platform=ecs.RuntimePlatform(
            cpu_architecture=ecs.CpuArchitecture.ARM64, operating_system_family=ecs.OperatingSystemFamily.LINUX
        ),
        volumes=[ecs.Volume(name="proxy-tmp")],
    )
    linux = ecs.LinuxParameters(stack, "ProxyLinux")
    linux.drop_capabilities(ecs.Capability.ALL)
    container = task.add_container(
        "proxy",
        image=ecs.ContainerImage.from_ecr_repository(repository, tag=image_uri.split("@")[1]),
        user="13",
        readonly_root_filesystem=True,
        linux_parameters=linux,
        logging=ecs.LogDrivers.aws_logs(log_group=log_group, stream_prefix="proxy"),
        health_check=ecs.HealthCheck(
            command=["CMD", "python3", "/opt/proxy/healthcheck.py"],
            interval=Duration.seconds(30),
            timeout=Duration.seconds(5),
            retries=3,
            start_period=Duration.seconds(15),
        ),
    )
    container.add_port_mappings(ecs.PortMapping(container_port=3128), ecs.PortMapping(container_port=3129))
    container.add_mount_points(ecs.MountPoint(container_path="/tmp", source_volume="proxy-tmp", read_only=False))
    services = [
        ecs.FargateService(
            stack,
            f"ProxyService{index}",
            cluster=cluster,
            task_definition=task,
            desired_count=1,
            vpc_subnets=ec2.SubnetSelection(subnets=[subnet]),
            security_groups=[stack.proxy_group],
            assign_public_ip=True,
            enable_execute_command=False,
            min_healthy_percent=100,
            max_healthy_percent=200,
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
        )
        for index, subnet in enumerate(stack.proxy_subnets)
    ]
    for port in (3128, 3129):
        target_group = elbv2.NetworkTargetGroup(
            stack,
            f"ProxyTargets{port}",
            vpc=stack.vpc,
            port=port,
            protocol=elbv2.Protocol.TCP,
            target_type=elbv2.TargetType.IP,
            preserve_client_ip=False,
            deregistration_delay=Duration.seconds(30),
            health_check=elbv2.HealthCheck(protocol=elbv2.Protocol.TCP, port="traffic-port"),
        )
        listener = stack.load_balancer.add_listener(
            f"ProxyListener{port}", port=port, default_target_groups=[target_group]
        )
        for service in services:
            target_group.add_target(service.load_balancer_target(container_name="proxy", container_port=port))
            service.node.add_dependency(listener)

    for resource in stack.node.find_all():
        if isinstance(
            resource,
            (
                ec2.CfnVPCEndpoint,
                resolver.CfnFirewallRuleGroupAssociation,
                resolver.CfnResolverQueryLoggingConfigAssociation,
            ),
        ):
            for service in services:
                service.node.add_dependency(resource)

    CfnOutput(stack, "ProxyLogGroupName", value=log_group.log_group_name)
