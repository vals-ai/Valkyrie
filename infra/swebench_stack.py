"""SWE-bench service stack - private benchmark execution engine."""

from typing import Any

import aws_cdk as cdk
from aws_cdk import Duration, Stack, aws_ec2, aws_ecs, aws_logs, aws_servicediscovery
from aws_cdk.aws_ecr_assets import Platform
from constants import (
    ALLOWED_IPS,
    CONTAINER_HEALTH_INTERVAL_SECONDS,
    CONTAINER_HEALTH_RETRIES,
    CONTAINER_HEALTH_START_PERIOD_SECONDS,
    CONTAINER_HEALTH_TIMEOUT_SECONDS,
    SWEBENCH_CPU,
    SWEBENCH_MAX_TASKS,
    SWEBENCH_MEMORY,
    SWEBENCH_MIN_TASKS,
    SWEBENCH_PORT,
    SWEBENCH_SCALING_CPU_PERCENT,
)
from constructs import Construct


class SwebenchStack(Stack):
    """SWE-bench service: executes benchmark tasks and evaluations.

    This service is PRIVATE - no ALB, only accessible from tracker via service discovery.
    For debugging, you can access it directly via the task's public IP after adding
    your IP to the security group.
    """

    _SERVICE_NAME: str = "Swebench"

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc: aws_ec2.IVpc,
        cluster: aws_ecs.ICluster,
        namespace: aws_servicediscovery.IPrivateDnsNamespace,
        tracker_security_group: aws_ec2.ISecurityGroup,
        **kwargs: Any,
    ):
        super().__init__(scope, id, **kwargs)

        # fargate task
        task_def = aws_ecs.FargateTaskDefinition(
            self,
            f"{self._SERVICE_NAME}TaskDef",
            cpu=SWEBENCH_CPU,
            memory_limit_mib=SWEBENCH_MEMORY,
            runtime_platform=aws_ecs.RuntimePlatform(
                cpu_architecture=aws_ecs.CpuArchitecture.X86_64,
                operating_system_family=aws_ecs.OperatingSystemFamily.LINUX,
            ),
        )

        # docker image
        task_def.add_container(
            f"{self._SERVICE_NAME}Container",
            image=aws_ecs.ContainerImage.from_asset(
                "../services/benchmarks/swebench",
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
            port_mappings=[aws_ecs.PortMapping(container_port=SWEBENCH_PORT)],
            health_check=aws_ecs.HealthCheck(
                command=["CMD-SHELL", f"curl -f http://localhost:{SWEBENCH_PORT}/health || exit 1"],
                interval=Duration.seconds(CONTAINER_HEALTH_INTERVAL_SECONDS),
                retries=CONTAINER_HEALTH_RETRIES,
                start_period=Duration.seconds(CONTAINER_HEALTH_START_PERIOD_SECONDS),
                timeout=Duration.seconds(CONTAINER_HEALTH_TIMEOUT_SECONDS),
            ),
        )

        # accessible at http://swebench.local:{SWEBENCH_PORT}
        self.service = aws_ecs.FargateService(
            self,
            f"{self._SERVICE_NAME}Service",
            service_name=self._SERVICE_NAME,
            cluster=cluster,
            task_definition=task_def,
            desired_count=SWEBENCH_MIN_TASKS,
            assign_public_ip=True,  # For debugging access via task IP
            circuit_breaker=aws_ecs.DeploymentCircuitBreaker(rollback=True),
            cloud_map_options=aws_ecs.CloudMapOptions(
                name="swebench",
                cloud_map_namespace=namespace,
            ),
        )

        # allow inbound from tracker
        self.service.connections.allow_from(
            tracker_security_group,
            port_range=aws_ec2.Port.tcp(SWEBENCH_PORT),
            description="Allow HTTP access from Tracker only",
        )

        # allow inbound from whitelisted IPs for testing
        for ip, desc in ALLOWED_IPS:
            self.service.connections.allow_from(
                aws_ec2.Peer.ipv4(ip),
                port_range=aws_ec2.Port.tcp(SWEBENCH_PORT),
                description=f"Allow HTTP access from {desc}",
            )

        # auto-scaling
        scaling = self.service.auto_scale_task_count(
            min_capacity=SWEBENCH_MIN_TASKS,
            max_capacity=SWEBENCH_MAX_TASKS,
        )
        scaling.scale_on_cpu_utilization(
            "CpuScaling",
            target_utilization_percent=SWEBENCH_SCALING_CPU_PERCENT,
        )
