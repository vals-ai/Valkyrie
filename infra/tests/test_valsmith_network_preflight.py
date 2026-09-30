"""AWS preflight must reject missing inventory and must exclude only its own network."""

import copy
import json
import subprocess
import unittest
from unittest.mock import patch

from tests.test_valsmith_network_config import read_test_inputs, resource_fixture
from valsmith_network_config import JsonValue
from valsmith_network_preflight import AwsReader, collect_inventory


def responses() -> dict[tuple[str, str], dict[str, JsonValue]]:
    resources = resource_fixture()
    return {
        ("ec2", "describe-prefix-lists"): {"PrefixLists": resources["prefix_lists"]},
        ("sts", "get-caller-identity"): {"Account": "629807611108"},
        ("cloudformation", "list-stacks"): {"StackSummaries": []},
        ("ec2", "describe-vpcs"): {
            "Vpcs": [
                {
                    "VpcId": "vpc-0e1bfdbc090daa61a",
                    "OwnerId": "629807611108",
                    "State": "available",
                    "CidrBlockAssociationSet": [{"CidrBlock": "10.0.0.0/16"}],
                }
            ]
        },
        ("ec2", "describe-subnets"): {"Subnets": resources["subnets"]},
        ("ec2", "describe-route-tables"): {"RouteTables": resources["route_tables"]},
        ("ec2", "describe-security-groups"): {"SecurityGroups": [resources["security_group"]]},
        ("ec2", "describe-vpc-peering-connections"): {"VpcPeeringConnections": []},
        ("ec2", "describe-transit-gateway-vpc-attachments"): {"TransitGatewayVpcAttachments": []},
        ("ec2", "describe-ipam-pools"): {"IpamPools": []},
        ("ec2", "get-subnet-cidr-reservations"): {"SubnetIpv4CidrReservations": [], "SubnetIpv6CidrReservations": []},
        ("route53resolver", "list-resolver-rule-associations"): {"ResolverRuleAssociations": []},
        ("ecs", "describe-clusters"): {"clusters": [resources["cluster"]], "failures": []},
        ("servicediscovery", "get-namespace"): {"Namespace": resources["namespace"]},
        ("ecr", "describe-images"): {"imageDetails": [{"imageDigest": resources["service_image_digest"]}]},
    }


class FixtureReader(AwsReader):
    def __init__(self, data: dict[tuple[str, str], dict[str, JsonValue]]) -> None:
        super().__init__("test")
        self.data = data

    def read(self, service: str, operation: str, *arguments: str) -> dict[str, JsonValue]:
        return copy.deepcopy(self.data[(service, operation)])


class NetworkPreflightTest(unittest.TestCase):
    def test_read_only_inventory_accepts_current_resources(self) -> None:
        inventory = collect_inventory("test", read_test_inputs(), reader=FixtureReader(responses()))
        self.assertEqual(inventory.occupied_cidrs, ("10.0.0.0/16",))

    def test_cli_error_or_pagination_token_cannot_become_empty_inventory(self) -> None:
        with patch(
            "valsmith_network_preflight.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "aws", stderr="AccessDenied"),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                AwsReader("test").read("ec2", "describe-vpcs")

        results: tuple[dict[str, JsonValue], ...] = (
            {"Vpcs": [], "NextToken": "next"},
            {"Vpcs": [], "IsTruncated": True},
        )
        for result in results:
            completed = subprocess.CompletedProcess(["aws"], 0, stdout=json.dumps(result))
            with patch("valsmith_network_preflight.subprocess.run", return_value=completed):
                with self.assertRaisesRegex(ValueError, "incomplete"):
                    AwsReader("test").read("ec2", "describe-vpcs")

    def test_remote_range_or_reservation_overlap_is_rejected(self) -> None:
        variants: tuple[tuple[tuple[str, str], dict[str, JsonValue]], ...] = (
            (
                ("ec2", "describe-vpc-peering-connections"),
                {
                    "VpcPeeringConnections": [
                        {
                            "VpcPeeringConnectionId": "pcx-external",
                            "Status": {"Code": "active"},
                            "RequesterVpcInfo": {"CidrBlock": "10.0.0.0/16"},
                            "AccepterVpcInfo": {"CidrBlock": "10.64.0.0/16"},
                        }
                    ]
                },
            ),
            (("ec2", "get-subnet-cidr-reservations"), {"SubnetIpv4CidrReservations": [{"Cidr": "10.64.0.0/24"}]}),
            (
                ("ec2", "describe-transit-gateway-vpc-attachments"),
                {"TransitGatewayVpcAttachments": [{"State": "available"}]},
            ),
        )
        for key, value in variants:
            data = responses()
            data[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))

    def test_only_cloudformation_owned_vpc_is_excluded_on_update(self) -> None:
        data = responses()
        own_vpc: dict[str, JsonValue] = {
            "VpcId": "vpc-00000000000000001",
            "OwnerId": "629807611108",
            "State": "available",
            "CidrBlockAssociationSet": [{"CidrBlock": "10.64.0.0/20"}],
        }
        vpcs = data[("ec2", "describe-vpcs")]["Vpcs"]
        assert isinstance(vpcs, list)
        vpcs.append(own_vpc)
        with self.assertRaisesRegex(ValueError, "overlap"):
            collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))

        stack_id = "arn:aws:cloudformation:us-east-1:629807611108:stack/ValSmithProdNetwork/1234"
        data[("cloudformation", "list-stacks")] = {
            "StackSummaries": [
                {"StackName": "ValSmithProdNetwork", "StackId": stack_id, "StackStatus": "CREATE_COMPLETE"}
            ]
        }
        data[("cloudformation", "list-stack-resources")] = {
            "StackResourceSummaries": [{"ResourceType": "AWS::EC2::VPC", "PhysicalResourceId": own_vpc["VpcId"]}]
        }
        inventory = collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))
        self.assertEqual(inventory.occupied_cidrs, ("10.0.0.0/16",))

        peer_id = "pcx-00000000000000001"
        owned = data[("cloudformation", "list-stack-resources")]["StackResourceSummaries"]
        assert isinstance(owned, list)
        owned.append({"ResourceType": "AWS::EC2::VPCPeeringConnection", "PhysicalResourceId": peer_id})
        subnets = data[("ec2", "describe-subnets")]["Subnets"]
        assert isinstance(subnets, list)
        for index in (1, 2):
            subnet_id = f"subnet-0000000000000000{index}"
            owned.append({"ResourceType": "AWS::EC2::Subnet", "PhysicalResourceId": subnet_id})
            subnets.append({"SubnetId": subnet_id, "VpcId": own_vpc["VpcId"], "CidrBlock": f"10.64.{index}.0/24"})
        data[("ec2", "describe-vpc-peering-connections")] = {
            "VpcPeeringConnections": [
                {
                    "VpcPeeringConnectionId": peer_id,
                    "Status": {"Code": "active"},
                    "RequesterVpcInfo": {
                        "VpcId": own_vpc["VpcId"],
                        "OwnerId": "629807611108",
                        "CidrBlock": "10.64.0.0/20",
                    },
                    "AccepterVpcInfo": {
                        "VpcId": "vpc-0e1bfdbc090daa61a",
                        "OwnerId": "629807611108",
                        "CidrBlock": "10.0.0.0/16",
                    },
                }
            ]
        }
        tables = data[("ec2", "describe-route-tables")]["RouteTables"]
        assert isinstance(tables, list) and isinstance(tables[0], dict)
        tables[0]["Routes"] = [{"DestinationCidrBlock": "10.64.0.0/16", "VpcPeeringConnectionId": peer_id}]
        with self.assertRaisesRegex(ValueError, "route"):
            collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))
        tables[0]["Routes"] = [{"DestinationCidrBlock": "10.64.1.0/24", "VpcPeeringConnectionId": peer_id}]
        collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))

        own_vpc["CidrBlockAssociationSet"] = [{"CidrBlock": "10.65.0.0/20"}]
        with self.assertRaisesRegex(ValueError, "owned VPC"):
            collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))

    def test_existing_network_cannot_use_an_unreviewed_dns_forwarder(self) -> None:
        data = responses()
        vpcs = data[("ec2", "describe-vpcs")]["Vpcs"]
        assert isinstance(vpcs, list)
        vpcs.append(
            {
                "VpcId": "vpc-00000000000000001",
                "OwnerId": "629807611108",
                "State": "available",
                "CidrBlockAssociationSet": [{"CidrBlock": "10.64.0.0/20"}],
            }
        )
        data[("cloudformation", "list-stacks")] = {
            "StackSummaries": [
                {
                    "StackName": "ValSmithProdNetwork",
                    "StackId": "arn:aws:cloudformation:us-east-1:629807611108:stack/ValSmithProdNetwork/1234",
                    "StackStatus": "CREATE_COMPLETE",
                }
            ]
        }
        data[("cloudformation", "list-stack-resources")] = {
            "StackResourceSummaries": [{"ResourceType": "AWS::EC2::VPC", "PhysicalResourceId": "vpc-00000000000000001"}]
        }
        data[("route53resolver", "list-resolver-rule-associations")] = {
            "ResolverRuleAssociations": [
                {"VPCId": "vpc-00000000000000001", "Status": "COMPLETE", "ResolverRuleId": "rslvr-rr-unknown"}
            ]
        }
        with self.assertRaisesRegex(ValueError, "forwarding"):
            collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))

        rule_id = "rslvr-autodefined-rr-internet-resolver"
        data[("route53resolver", "list-resolver-rule-associations")] = {
            "ResolverRuleAssociations": [
                {"VPCId": "vpc-00000000000000001", "Status": "COMPLETE", "ResolverRuleId": rule_id}
            ]
        }
        default_rule: dict[str, JsonValue] = {
            "Id": rule_id,
            "Arn": f"arn:aws:route53resolver:us-east-1::autodefined-rule/{rule_id}",
            "OwnerId": "Route 53 Resolver",
            "DomainName": ".",
            "Status": "COMPLETE",
            "RuleType": "RECURSIVE",
        }
        data[("route53resolver", "get-resolver-rule")] = {"ResolverRule": default_rule}
        collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))

        variants: tuple[tuple[str, JsonValue], ...] = (
            ("RuleType", "FORWARD"),
            ("OwnerId", "629807611108"),
            ("DomainName", "example.com."),
            ("ResolverEndpointId", "rslvr-outbound"),
            ("TargetIps", [{"Ip": "1.1.1.1"}]),
        )
        for key, value in variants:
            data[("route53resolver", "get-resolver-rule")] = {"ResolverRule": {**default_rule, key: value}}
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "forwarding"):
                collect_inventory("test", read_test_inputs(), reader=FixtureReader(data))
