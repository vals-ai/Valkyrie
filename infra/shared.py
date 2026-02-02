"""Shared infrastructure: VPC, ECS Cluster, Service Discovery namespace, S3."""

from typing import Any

import aws_cdk as cdk
from aws_cdk import Stack, aws_ec2, aws_ecs, aws_route53, aws_s3, aws_servicediscovery
from constants import CLUSTER_NAME, NAMESPACE, S3_BUCKET_NAME, VPC_MAX_AZS, VPC_NAT_GATEWAYS
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
