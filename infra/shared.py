"""Shared infrastructure: VPC, ECS Cluster, Service Discovery namespace, S3, ElastiCache."""

from typing import Any

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_chatbot,
    aws_ec2,
    aws_ecs,
    aws_elasticache,
    aws_events,
    aws_events_targets,
    aws_route53,
    aws_s3,
    aws_servicediscovery,
    aws_sns,
)
from constants import (
    CLUSTER_NAME,
    ELASTICACHE_NODE_TYPE,
    NAMESPACE,
    REDIS_PORT,
    S3_BUCKET_NAME,
    SLACK_CHANNEL_ID,
    SLACK_WORKSPACE_ID,
    VPC_CIDR,
    VPC_MAX_AZS,
    VPC_NAT_GATEWAYS,
)
from constructs import Construct


class SharedStack(Stack):
    """Shared infrastructure for all services."""

    def __init__(self, scope: Construct, id: str, **kwargs: Any):
        super().__init__(scope, id, **kwargs)

        # shared VPC - public subnets only, no NAT gateway (cost savings)
        self.vpc = aws_ec2.Vpc(
            self,
            "AgenticHarnessVpc",
            max_azs=VPC_MAX_AZS,
            nat_gateways=VPC_NAT_GATEWAYS,
            subnet_configuration=[
                aws_ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=aws_ec2.SubnetType.PUBLIC,
                    map_public_ip_on_launch=True,
                )
            ],
        )

        # SNS topic for deployment notifications
        notification_topic = aws_sns.Topic(
            self,
            "StackNotificationTopic",
            topic_name="agentic-harness-notifications",
        )

        # Slack channel configuration
        slack = aws_chatbot.SlackChannelConfiguration(
            self,
            "DeploymentNotificationsSlackChannel",
            slack_channel_configuration_name="deployment-notifications",
            slack_workspace_id=SLACK_WORKSPACE_ID,
            slack_channel_id=SLACK_CHANNEL_ID,
        )

        slack.add_notification_topic(notification_topic)  # type: ignore[arg-type]

        # Only notify for stacks deployed by this project
        stack_names = ["SharedStack", "TrackerStack", "WorkerStack"]

        # EventBridge rule Slack notification for successful stack deployments
        success_rule = aws_events.Rule(
            self,
            "StackDeploySuccessRule",
            event_pattern=aws_events.EventPattern(
                source=["aws.cloudformation"],
                detail_type=["CloudFormation Stack Status Change"],
                detail={
                    "stack-id": [{"wildcard": f"*:stack/{name}/*"} for name in stack_names],
                    "status-details": {
                        "status": ["CREATE_COMPLETE", "UPDATE_COMPLETE"],
                    },
                },
            ),
        )

        cfn_url = (
            "<https://"
            + aws_events.EventField.from_path("$.region")
            + ".console.aws.amazon.com/cloudformation/home?region="
            + aws_events.EventField.from_path("$.region")
            + "#/stacks|CloudFormation Stack Notification>"
        )

        success_rule.add_target(
            aws_events_targets.SnsTopic(  # type: ignore[arg-type]
                notification_topic,  # type: ignore[arg-type]
                message=aws_events.RuleTargetInput.from_object(
                    {
                        "version": "1.0",
                        "source": "custom",
                        "content": {
                            "textType": "client-markdown",
                            "title": ":white_check_mark: "
                            + cfn_url
                            + " | "
                            + aws_events.EventField.from_path("$.region")
                            + " | Account: "
                            + aws_events.EventField.from_path("$.account"),
                            "description": "CloudFormation stack deployment *SUCCEEDED*."
                            + "\n\n*Stack*\n"
                            + aws_events.EventField.from_path("$.detail.stack-id"),
                        },
                        "metadata": {
                            "threadId": aws_events.EventField.from_path("$.detail.stack-id"),
                            "summary": "Deployment succeeded",
                        },
                    }
                ),
            )
        )

        # EventBridge rule Slack notification for failed stack deployments
        failure_rule = aws_events.Rule(
            self,
            "StackDeployFailureRule",
            event_pattern=aws_events.EventPattern(
                source=["aws.cloudformation"],
                detail_type=["CloudFormation Stack Status Change"],
                detail={
                    "stack-id": [{"wildcard": f"*:stack/{name}/*"} for name in stack_names],
                    "status-details": {
                        "status": [
                            "CREATE_FAILED",
                            "UPDATE_FAILED",
                            "UPDATE_ROLLBACK_COMPLETE",
                            "ROLLBACK_COMPLETE",
                            "DELETE_FAILED",
                        ],
                    },
                },
            ),
        )

        failure_rule.add_target(
            aws_events_targets.SnsTopic(  # type: ignore[arg-type]
                notification_topic,  # type: ignore[arg-type]
                message=aws_events.RuleTargetInput.from_object(
                    {
                        "version": "1.0",
                        "source": "custom",
                        "content": {
                            "textType": "client-markdown",
                            "title": ":rotating_light: "
                            + cfn_url
                            + " | "
                            + aws_events.EventField.from_path("$.region")
                            + " | Account: "
                            + aws_events.EventField.from_path("$.account"),
                            "description": "CloudFormation stack deployment *FAILED*."
                            + "\n\n*Stack*\n"
                            + aws_events.EventField.from_path("$.detail.stack-id"),
                        },
                        "metadata": {
                            "threadId": aws_events.EventField.from_path("$.detail.stack-id"),
                            "summary": "Deployment failed",
                        },
                    }
                ),
            )
        )

        # shared ECS cluster
        self.cluster = aws_ecs.Cluster(
            self,
            "AgenticHarnessCluster",
            vpc=self.vpc,
            cluster_name=CLUSTER_NAME,
        )

        # service discovery namespace for internal communication
        # services can reach each other via: http://<service-name>.local:<port>
        self.namespace = aws_servicediscovery.PrivateDnsNamespace(
            self,
            "AgenticHarnessNamespace",
            name=NAMESPACE,
            vpc=self.vpc,
        )

        # Route53 hosted zone for vals.ai (shared by all services)
        self.hosted_zone = aws_route53.HostedZone.from_lookup(
            self,
            "HostedZone",
            domain_name="vals.ai",
        )

        # S3 bucket
        self.bucket = aws_s3.Bucket(
            self,
            "AgenticHarnessBucket",
            bucket_name=S3_BUCKET_NAME,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
        )

        # ── ElastiCache Redis ─────────────────────────────────────────────
        # Single-node Redis used as the Taskiq message broker, shared by
        # the tracker (producer) and worker (consumer).

        redis_sg = aws_ec2.SecurityGroup(
            self,
            "RedisSG",
            vpc=self.vpc,
            description="Security group for ElastiCache Redis",
            allow_all_outbound=False,
        )

        redis_sg.add_ingress_rule(
            peer=aws_ec2.Peer.ipv4(VPC_CIDR),
            connection=aws_ec2.Port.tcp(REDIS_PORT),
            description="Allow VPC services to connect to Redis",
        )

        redis_subnet_group = aws_elasticache.CfnSubnetGroup(
            self,
            "RedisSubnetGroup",
            description="Subnet group for ElastiCache Redis",
            subnet_ids=[s.subnet_id for s in self.vpc.public_subnets],
        )

        redis_cluster = aws_elasticache.CfnCacheCluster(
            self,
            "RedisCluster",
            cache_node_type=ELASTICACHE_NODE_TYPE,
            engine="redis",
            engine_version="7.1",
            num_cache_nodes=1,
            vpc_security_group_ids=[redis_sg.security_group_id],
            cache_subnet_group_name=redis_subnet_group.ref,
        )

        self.redis_url = cdk.Fn.join(
            "",
            [
                "redis://",
                redis_cluster.attr_redis_endpoint_address,
                ":",
                redis_cluster.attr_redis_endpoint_port,
            ],
        )
