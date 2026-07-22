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
    aws_ssm,
)
from constants import (
    CLUSTER_NAME,
    DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV,
    DEV_SHARED_ARTIFACT_BUCKET_PARAMETER,
    DEV_SHARED_AVAILABILITY_ZONES_PARAMETER,
    DEV_SHARED_CLUSTER_NAME_PARAMETER,
    DEV_SHARED_NAMESPACE_ARN_PARAMETER,
    DEV_SHARED_NAMESPACE_ID_PARAMETER,
    DEV_SHARED_NAMESPACE_NAME_PARAMETER,
    DEV_SHARED_PUBLIC_SUBNET_IDS_PARAMETER,
    DEV_SHARED_VPC_ID_PARAMETER,
    ELASTICACHE_NODE_TYPE,
    NAMESPACE,
    REDIS_PORT,
    S3_BUCKET_NAME,
    VPC_CIDR,
    VPC_MAX_AZS,
    VPC_NAT_GATEWAYS,
    get_slack_notification_config,
)
from constructs import Construct
from stage import Stage

DEPLOYMENT_STACK_NAMES = ("SharedStack", "TrackerStack", "WorkerStack", "MonitoringStack")
DEPLOYMENT_SUCCESS_STATUSES = ("CREATE_COMPLETE", "UPDATE_COMPLETE")
DEPLOYMENT_FAILURE_STATUSES = (
    "CREATE_FAILED",
    "UPDATE_FAILED",
    "UPDATE_ROLLBACK_COMPLETE",
    "ROLLBACK_COMPLETE",
    "DELETE_FAILED",
)


class SharedStack(Stack):
    """Shared infrastructure for all services."""

    def __init__(self, scope: Construct, id: str, stage: Stage, **kwargs: Any):
        super().__init__(scope, id, **kwargs)
        self.stage = stage

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

        slack_config = get_slack_notification_config(DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV)
        if slack_config is not None:
            self._create_deployment_notifications(*slack_config)

        # shared ECS cluster
        self.cluster = aws_ecs.Cluster(
            self,
            "AgenticHarnessCluster",
            vpc=self.vpc,
            cluster_name=self.stage.phys(CLUSTER_NAME),
            container_insights=True,
        )

        # service discovery namespace for internal communication
        # services can reach each other via: http://<service-name>.local:<port>
        self.namespace = aws_servicediscovery.PrivateDnsNamespace(
            self,
            "AgenticHarnessNamespace",
            name=self.stage.phys(NAMESPACE),
            vpc=self.vpc,
        )

        self.hosted_zone: aws_route53.IHostedZone | None = None
        if self.stage.is_prod:
            self.hosted_zone = aws_route53.HostedZone.from_lookup(
                self,
                "HostedZone",
                domain_name="vals.ai",
            )

        bucket_name = self.stage.phys(S3_BUCKET_NAME)
        self.bucket_name = bucket_name

        self.bucket = aws_s3.Bucket(
            self,
            "AgenticHarnessBucket",
            bucket_name=bucket_name,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=None if self.stage.is_prod else aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=None if self.stage.is_prod else True,
            object_ownership=None if self.stage.is_prod else aws_s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=None if self.stage.is_prod else True,
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

        self.redis_cluster = aws_elasticache.CfnCacheCluster(
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
                self.redis_cluster.attr_redis_endpoint_address,
                ":",
                self.redis_cluster.attr_redis_endpoint_port,
            ],
        )

        if not self.stage.is_prod:
            self._publish_shared_contract()

    def _publish_shared_contract(self) -> None:
        """Publish the account-local resource contract consumed by benchmark-service stacks."""
        aws_ssm.StringParameter(
            self,
            "SharedVpcIdParameter",
            parameter_name=DEV_SHARED_VPC_ID_PARAMETER,
            string_value=self.vpc.vpc_id,
        )
        aws_ssm.StringListParameter(
            self,
            "SharedAvailabilityZonesParameter",
            parameter_name=DEV_SHARED_AVAILABILITY_ZONES_PARAMETER,
            string_list_value=self.vpc.availability_zones,
        )
        aws_ssm.StringListParameter(
            self,
            "SharedPublicSubnetIdsParameter",
            parameter_name=DEV_SHARED_PUBLIC_SUBNET_IDS_PARAMETER,
            string_list_value=[subnet.subnet_id for subnet in self.vpc.public_subnets],
        )
        aws_ssm.StringParameter(
            self,
            "SharedClusterNameParameter",
            parameter_name=DEV_SHARED_CLUSTER_NAME_PARAMETER,
            string_value=self.cluster.cluster_name,
        )
        aws_ssm.StringParameter(
            self,
            "SharedNamespaceNameParameter",
            parameter_name=DEV_SHARED_NAMESPACE_NAME_PARAMETER,
            string_value=self.namespace.namespace_name,
        )
        aws_ssm.StringParameter(
            self,
            "SharedNamespaceIdParameter",
            parameter_name=DEV_SHARED_NAMESPACE_ID_PARAMETER,
            string_value=self.namespace.namespace_id,
        )
        aws_ssm.StringParameter(
            self,
            "SharedNamespaceArnParameter",
            parameter_name=DEV_SHARED_NAMESPACE_ARN_PARAMETER,
            string_value=self.namespace.namespace_arn,
        )
        aws_ssm.StringParameter(
            self,
            "SharedArtifactBucketParameter",
            parameter_name=DEV_SHARED_ARTIFACT_BUCKET_PARAMETER,
            string_value=self.bucket.bucket_name,
        )

    def _create_deployment_notifications(self, slack_workspace_id: str, slack_channel_id: str) -> None:
        notification_topic = aws_sns.Topic(
            self,
            "StackNotificationTopic",
            topic_name=self.stage.phys("agentic-harness-notifications"),
        )
        slack = aws_chatbot.SlackChannelConfiguration(
            self,
            "DeploymentNotificationsSlackChannel",
            slack_channel_configuration_name=self.stage.phys("deployment-notifications"),
            slack_workspace_id=slack_workspace_id,
            slack_channel_id=slack_channel_id,
        )
        slack.add_notification_topic(notification_topic)  # type: ignore[arg-type]

        cfn_url = (
            "<https://"
            + aws_events.EventField.from_path("$.region")
            + ".console.aws.amazon.com/cloudformation/home?region="
            + aws_events.EventField.from_path("$.region")
            + "#/stacks|CloudFormation Stack Notification>"
        )
        self._add_deployment_notification_rule(
            rule_id="StackDeploySuccessRule",
            topic=notification_topic,
            statuses=DEPLOYMENT_SUCCESS_STATUSES,
            title_prefix=":white_check_mark: ",
            description="CloudFormation stack deployment *SUCCEEDED*.",
            summary="Deployment succeeded",
            cfn_url=cfn_url,
        )
        self._add_deployment_notification_rule(
            rule_id="StackDeployFailureRule",
            topic=notification_topic,
            statuses=DEPLOYMENT_FAILURE_STATUSES,
            title_prefix=":rotating_light: ",
            description="CloudFormation stack deployment *FAILED*.",
            summary="Deployment failed",
            cfn_url=cfn_url,
        )

    def _add_deployment_notification_rule(
        self,
        *,
        rule_id: str,
        topic: aws_sns.Topic,
        statuses: tuple[str, ...],
        title_prefix: str,
        description: str,
        summary: str,
        cfn_url: str,
    ) -> None:
        rule = aws_events.Rule(
            self,
            rule_id,
            event_pattern=aws_events.EventPattern(
                source=["aws.cloudformation"],
                detail_type=["CloudFormation Stack Status Change"],
                detail={
                    "stack-id": [
                        {
                            "prefix": f"arn:aws:cloudformation:{self.region}:{self.account}:stack/{self.stage.stack_id(name)}/"
                        }
                        for name in DEPLOYMENT_STACK_NAMES
                    ],
                    "status-details": {
                        "status": list(statuses),
                    },
                },
            ),
        )
        rule.add_target(
            aws_events_targets.SnsTopic(  # type: ignore[arg-type]
                topic,  # type: ignore[arg-type]
                message=aws_events.RuleTargetInput.from_object(
                    {
                        "version": "1.0",
                        "source": "custom",
                        "content": {
                            "textType": "client-markdown",
                            "title": title_prefix
                            + cfn_url
                            + " | "
                            + aws_events.EventField.from_path("$.region")
                            + " | Account: "
                            + aws_events.EventField.from_path("$.account"),
                            "description": description
                            + "\n\n*Stack*\n"
                            + aws_events.EventField.from_path("$.detail.stack-id"),
                        },
                        "metadata": {
                            "threadId": aws_events.EventField.from_path("$.detail.stack-id"),
                            "summary": summary,
                        },
                    }
                ),
            )
        )
