"""Dedicated network foundation; live service and Lambda placement is unchanged."""

from aws_cdk import CfnOutput, Environment, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct
from valsmith_network_config import NetworkInputs, validate_proxy_image


class ValSmithNetworkStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        inputs: NetworkInputs,
        proxy_image_uri: str,
        env: Environment,
    ) -> None:
        if env.account != inputs.account_id or env.region != inputs.region:
            raise ValueError("Stack environment must match the reviewed network inputs")

        validate_proxy_image(proxy_image_uri)
        super().__init__(scope, construct_id, env=env)
        self.inputs = inputs
        self.proxy_image_uri = proxy_image_uri
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr(inputs.vpc_cidr),
            availability_zones=list(inputs.availability_zones),
            nat_gateways=0,
            enable_dns_support=True,
            enable_dns_hostnames=True,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="Proxy", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=26),
                ec2.SubnetConfiguration(name="Application", subnet_type=ec2.SubnetType.PRIVATE_ISOLATED, cidr_mask=24),
            ],
        )
        self.application_subnets = self.vpc.isolated_subnets
        self.proxy_subnets = self.vpc.public_subnets
        self.generation_group = self._group("Generation", "ValSmith generation service")
        self.evaluation_group = self._group("Evaluation", "ValSmith evaluation service")
        self.view_group = self._group("DatasetView", "ValSmith dataset-view Lambda")
        self.policy_group = self._group("BucketPolicy", "ValSmith bucket-policy Lambda")
        self.proxy_group = self._group("Proxy", "ValSmith outbound proxy tasks")
        self.load_balancer_group = self._group("ProxyLoadBalancer", "ValSmith internal proxy listeners")
        self.load_balancer = elbv2.NetworkLoadBalancer(
            self,
            "ProxyLoadBalancer",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnets=self.application_subnets),
            internet_facing=False,
            cross_zone_enabled=True,
            security_groups=[self.load_balancer_group],
        )

        for name, group, port in (
            ("GenerationProxy", self.generation_group, 3128),
            ("EvaluationProxy", self.evaluation_group, 3128),
            ("DatasetViewProxy", self.view_group, 3129),
        ):
            self.connect_groups(name, group, self.load_balancer_group, port)

        for port in (3128, 3129):
            self.connect_groups(f"ProxyTarget{port}", self.load_balancer_group, self.proxy_group, port)

        ec2.CfnSecurityGroupEgress(
            self,
            "ProxyPublicHttps",
            group_id=self.proxy_group.security_group_id,
            ip_protocol="tcp",
            from_port=443,
            to_port=443,
            cidr_ip="0.0.0.0/0",
            description="HTTPS destinations are enforced by Squid and DNS Firewall",
        )
        self.peering = ec2.CfnVPCPeeringConnection(
            self,
            "TrackerPeer",
            vpc_id=self.vpc.vpc_id,
            peer_vpc_id=inputs.caller_vpc_id,
            peer_owner_id=inputs.account_id,
        )
        for index, subnet in enumerate(self.application_subnets):
            for caller_index, (table_id, caller_cidr) in enumerate(
                zip(inputs.caller_route_table_ids, inputs.caller_subnet_cidrs, strict=True)
            ):
                ec2.CfnRoute(
                    self,
                    f"Caller{caller_index}ToApplication{index}",
                    route_table_id=table_id,
                    destination_cidr_block=subnet.ipv4_cidr_block,
                    vpc_peering_connection_id=self.peering.ref,
                )
                ec2.CfnRoute(
                    self,
                    f"Application{index}ToCaller{caller_index}",
                    route_table_id=subnet.route_table.route_table_id,
                    destination_cidr_block=caller_cidr,
                    vpc_peering_connection_id=self.peering.ref,
                )

        for name, group in (("Generation", self.generation_group), ("Evaluation", self.evaluation_group)):
            ingress = ec2.CfnSecurityGroupIngress(
                self,
                f"TrackerTo{name}Ingress",
                group_id=group.security_group_id,
                ip_protocol="tcp",
                from_port=8001,
                to_port=8001,
                source_security_group_id=inputs.caller_security_group_id,
                source_security_group_owner_id=inputs.account_id,
            )
            # Standalone caller egress rules remove its existing default rule.
            # Preflight verifies access; the caller's own stack retains its rules.
            ingress.add_dependency(self.peering)

        for name, value in {
            "NetworkContractVersion": "1",
            "NetworkAccount": inputs.account_id,
            "NetworkRegion": inputs.region,
            "VpcId": self.vpc.vpc_id,
            "ApplicationSubnetAId": self.application_subnets[0].subnet_id,
            "ApplicationSubnetBId": self.application_subnets[1].subnet_id,
            "GenerationSecurityGroupId": self.generation_group.security_group_id,
            "EvaluationSecurityGroupId": self.evaluation_group.security_group_id,
            "DatasetViewSecurityGroupId": self.view_group.security_group_id,
            "BucketPolicySecurityGroupId": self.policy_group.security_group_id,
            "ServiceProxyUrl": f"http://{self.load_balancer.load_balancer_dns_name}:3128",
            "DatasetViewProxyUrl": f"http://{self.load_balancer.load_balancer_dns_name}:3129",
            "PeerConnectionId": self.peering.ref,
        }.items():
            CfnOutput(self, name, value=value)

    def _group(self, name: str, description: str) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(
            self,
            f"{name}Group",
            vpc=self.vpc,
            description=description,
            allow_all_outbound=False,
            disable_inline_rules=True,
        )

    def connect_groups(self, name: str, source: ec2.ISecurityGroup, destination: ec2.ISecurityGroup, port: int) -> None:
        ec2.CfnSecurityGroupEgress(
            self,
            f"{name}Egress",
            group_id=source.security_group_id,
            ip_protocol="tcp",
            from_port=port,
            to_port=port,
            destination_security_group_id=destination.security_group_id,
        )
        ec2.CfnSecurityGroupIngress(
            self,
            f"{name}Ingress",
            group_id=destination.security_group_id,
            ip_protocol="tcp",
            from_port=port,
            to_port=port,
            source_security_group_id=source.security_group_id,
        )
