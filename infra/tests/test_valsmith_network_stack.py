"""The network must not give applications a second Internet or shared-VPC path."""

import json
import unittest

from aws_cdk import App, Environment, assertions
from tests.test_valsmith_network_config import read_test_inputs
from valsmith_network_config import JsonValue, json_document, object_field
from valsmith_network_stack import ValSmithNetworkStack

PROXY_IMAGE = "629807611108.dkr.ecr.us-east-1.amazonaws.com/valsmith-outbound-proxy-prod@sha256:" + "a" * 64


def network_stack() -> ValSmithNetworkStack:
    return ValSmithNetworkStack(
        App(),
        "ValSmithProdNetwork",
        inputs=read_test_inputs(),
        proxy_image_uri=PROXY_IMAGE,
        env=Environment(account="629807611108", region="us-east-1"),
    )


def resources(template: assertions.Template, resource_type: str) -> dict[str, dict[str, JsonValue]]:
    document = json_document(json.dumps(template.find_resources(resource_type)))
    return {name: object_field(object_field(document, name), "Properties") for name in document}


class NetworkStackTest(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = network_stack()
        self.template = assertions.Template.from_stack(self.stack)

    def test_applications_have_no_internet_route_or_ipv6(self) -> None:
        self.template.resource_count_is("AWS::EC2::Subnet", 4)
        self.template.resource_count_is("AWS::EC2::NatGateway", 0)
        self.template.resource_count_is("AWS::EC2::EgressOnlyInternetGateway", 0)
        self.template.resource_count_is("AWS::EC2::VPCCidrBlock", 0)
        self.assertEqual(len(self.stack.application_subnets), 2)
        self.assertEqual(len(self.stack.proxy_subnets), 2)
        application_tables = [
            self.stack.resolve(subnet.route_table.route_table_id) for subnet in self.stack.application_subnets
        ]
        for route in resources(self.template, "AWS::EC2::Route").values():
            self.assertNotIn("DestinationIpv6CidrBlock", route)
            if route["RouteTableId"] in application_tables:
                self.assertNotEqual(route.get("DestinationCidrBlock"), "0.0.0.0/0")
                self.assertNotIn("GatewayId", route)

        subnets = resources(self.template, "AWS::EC2::Subnet").values()
        self.assertEqual(sorted(str(item["CidrBlock"]).split("/")[1] for item in subnets), ["24", "24", "26", "26"])
        for subnet in subnets:
            self.assertNotIn("Ipv6CidrBlock", subnet)

    def test_peer_routes_cover_only_exact_caller_and_application_subnets(self) -> None:
        self.template.resource_count_is("AWS::EC2::VPCPeeringConnection", 1)
        inputs = read_test_inputs()
        app_cidrs = {subnet.ipv4_cidr_block for subnet in self.stack.application_subnets}
        routes = [
            item for item in resources(self.template, "AWS::EC2::Route").values() if "VpcPeeringConnectionId" in item
        ]
        self.assertEqual(len(routes), 8)
        old_routes = [item for item in routes if isinstance(item["RouteTableId"], str)]
        self.assertEqual(
            {(item["RouteTableId"], item["DestinationCidrBlock"]) for item in old_routes},
            {(table, cidr) for table in inputs.caller_route_table_ids for cidr in app_cidrs},
        )
        for route in routes:
            if route not in old_routes:
                self.assertIn(route["DestinationCidrBlock"], inputs.caller_subnet_cidrs)

    def test_only_tracker_can_reach_internal_service_ports(self) -> None:
        inputs = read_test_inputs()
        rules = resources(self.template, "AWS::EC2::SecurityGroupIngress").values()
        service_rules = [item for item in rules if item.get("FromPort") == 8001]
        self.assertEqual(len(service_rules), 2)
        for rule in service_rules:
            self.assertEqual(rule["SourceSecurityGroupId"], inputs.caller_security_group_id)
            self.assertEqual(rule["SourceSecurityGroupOwnerId"], inputs.account_id)
            self.assertNotIn("CidrIp", rule)
            self.assertEqual(rule["ToPort"], 8001)

        for rule in resources(self.template, "AWS::EC2::SecurityGroupEgress").values():
            if isinstance(rule["GroupId"], str):
                self.assertEqual(rule["GroupId"], inputs.caller_security_group_id)
                self.assertEqual(rule["FromPort"], 8001)
                self.assertEqual(rule["ToPort"], 8001)
                self.assertIn("DestinationSecurityGroupId", rule)
                self.assertNotIn("CidrIp", rule)

    def test_proxy_listeners_remain_separate_and_policy_lambda_has_no_proxy(self) -> None:
        stack = self.stack
        resolve = stack.resolve
        expected = [
            (resolve(stack.generation_group.security_group_id), 3128),
            (resolve(stack.evaluation_group.security_group_id), 3128),
            (resolve(stack.view_group.security_group_id), 3129),
        ]
        nlb_id = resolve(stack.load_balancer_group.security_group_id)
        nlb_rules = [
            item
            for item in resources(self.template, "AWS::EC2::SecurityGroupIngress").values()
            if item["GroupId"] == nlb_id
        ]
        self.assertEqual(len(nlb_rules), 3)
        for rule in nlb_rules:
            self.assertIn((rule["SourceSecurityGroupId"], rule["FromPort"]), expected)
            self.assertEqual(rule["FromPort"], rule["ToPort"])
            self.assertNotIn("CidrIp", rule)

        for rule in resources(self.template, "AWS::EC2::SecurityGroupEgress").values():
            if rule.get("DestinationSecurityGroupId") == nlb_id:
                self.assertIn((rule["GroupId"], rule["FromPort"]), expected)

        caller_ids = [
            resolve(group.security_group_id)
            for group in (stack.generation_group, stack.evaluation_group, stack.view_group, stack.policy_group)
        ]
        for rule in resources(self.template, "AWS::EC2::SecurityGroupEgress").values():
            if rule["GroupId"] in caller_ids:
                self.assertNotEqual(rule.get("CidrIp"), "0.0.0.0/0")
                self.assertNotIn(rule.get("ToPort"), (53, 853, 5432, 6379))

    def test_new_stack_does_not_replace_shared_services_or_dns(self) -> None:
        for resource_type in (
            "AWS::RDS::DBCluster",
            "AWS::ElastiCache::ReplicationGroup",
            "AWS::Route53::HostedZone",
            "AWS::ServiceDiscovery::PrivateDnsNamespace",
        ):
            self.template.resource_count_is(resource_type, 0)
        self.template.resource_count_is("AWS::ElasticLoadBalancingV2::LoadBalancer", 1)
        self.template.has_resource_properties(
            "AWS::ElasticLoadBalancingV2::LoadBalancer",
            {
                "Scheme": "internal",
                "Type": "network",
                "LoadBalancerAttributes": assertions.Match.array_with(
                    [{"Key": "load_balancing.cross_zone.enabled", "Value": "true"}]
                ),
            },
        )
        self.assertNotIn("AgenticHarnessCluster-prod", json.dumps(self.template.to_json()))
