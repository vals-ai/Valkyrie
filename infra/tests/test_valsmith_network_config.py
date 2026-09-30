"""Reject wrong accounts, drifted resources, incomplete inventory, and stale deployment inputs."""

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from valsmith_network_config import (
    JsonValue,
    NetworkInputs,
    NetworkInventory,
    input_hash,
    load_inputs,
    object_field,
    validate_cidr,
    validate_inventory,
    validate_proxy_image,
)

INPUTS: dict[str, JsonValue] = {
    "account_id": "629807611108",
    "region": "us-east-1",
    "vpc_cidr": "10.64.0.0/20",
    "availability_zones": ["us-east-1a", "us-east-1b"],
    "caller_vpc_id": "vpc-0e1bfdbc090daa61a",
    "caller_subnet_ids": ["subnet-057078f4f29580c2b", "subnet-0a7a9d472d4550108"],
    "caller_subnet_cidrs": ["10.0.0.0/17", "10.0.128.0/17"],
    "caller_route_table_ids": ["rtb-05eb22d76756f3f44", "rtb-062960dd432903259"],
    "caller_security_group_id": "sg-036040bcf58e2d364",
    "cluster_arn": "arn:aws:ecs:us-east-1:629807611108:cluster/AgenticHarnessCluster-prod",
    "namespace_id": "ns-erq7ivv3x5ym4oco",
    "namespace_name": "local-prod",
    "namespace_hosted_zone_id": "Z04103512KCVYNNSTJ1Q2",
    "service_image_digest": "sha256:424cc4fb4fd8fcf0395bc7cf20b697929adbd30e3629954f397297e1c46c0e64",
    "s3_prefix_list_id": "pl-63a5400a",
    "dynamodb_prefix_list_id": "pl-02cd2c6b",
}


def read_test_inputs(document: dict[str, JsonValue] | None = None) -> NetworkInputs:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "inputs.json"
        path.write_text(json.dumps(INPUTS if document is None else document))
        return load_inputs(path)


def resource_fixture() -> dict[str, JsonValue]:
    return {
        "prefix_lists": [
            {"PrefixListId": "pl-63a5400a", "PrefixListName": "com.amazonaws.us-east-1.s3"},
            {"PrefixListId": "pl-02cd2c6b", "PrefixListName": "com.amazonaws.us-east-1.dynamodb"},
        ],
        "vpc": {"VpcId": "vpc-0e1bfdbc090daa61a", "OwnerId": "629807611108", "State": "available"},
        "subnets": [
            {
                "SubnetId": "subnet-057078f4f29580c2b",
                "VpcId": "vpc-0e1bfdbc090daa61a",
                "OwnerId": "629807611108",
                "AvailabilityZone": "us-east-1a",
                "CidrBlock": "10.0.0.0/17",
                "State": "available",
            },
            {
                "SubnetId": "subnet-0a7a9d472d4550108",
                "VpcId": "vpc-0e1bfdbc090daa61a",
                "OwnerId": "629807611108",
                "AvailabilityZone": "us-east-1b",
                "CidrBlock": "10.0.128.0/17",
                "State": "available",
            },
        ],
        "route_tables": [
            {
                "RouteTableId": "rtb-05eb22d76756f3f44",
                "VpcId": "vpc-0e1bfdbc090daa61a",
                "OwnerId": "629807611108",
                "Associations": [{"SubnetId": "subnet-057078f4f29580c2b"}],
                "Routes": [{"DestinationCidrBlock": "10.0.0.0/16", "GatewayId": "local"}],
            },
            {
                "RouteTableId": "rtb-062960dd432903259",
                "VpcId": "vpc-0e1bfdbc090daa61a",
                "OwnerId": "629807611108",
                "Associations": [{"SubnetId": "subnet-0a7a9d472d4550108"}],
                "Routes": [{"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": "igw-example"}],
            },
        ],
        "security_group": {
            "GroupId": "sg-036040bcf58e2d364",
            "VpcId": "vpc-0e1bfdbc090daa61a",
            "OwnerId": "629807611108",
            "IpPermissionsEgress": [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
        },
        "cluster": {
            "clusterArn": "arn:aws:ecs:us-east-1:629807611108:cluster/AgenticHarnessCluster-prod",
            "status": "ACTIVE",
        },
        "namespace": {
            "Id": "ns-erq7ivv3x5ym4oco",
            "Name": "local-prod",
            "Type": "DNS_PRIVATE",
            "Arn": "arn:aws:servicediscovery:us-east-1:629807611108:namespace/ns-erq7ivv3x5ym4oco",
            "Properties": {"DnsProperties": {"HostedZoneId": "Z04103512KCVYNNSTJ1Q2"}},
        },
        "service_image_digest": "sha256:424cc4fb4fd8fcf0395bc7cf20b697929adbd30e3629954f397297e1c46c0e64",
    }


class NetworkInputsTest(unittest.TestCase):
    def test_rejects_wrong_target_or_unqualified_image(self) -> None:
        cases: tuple[tuple[str, JsonValue], ...] = (
            ("account_id", "613431292675"),
            ("region", "us-west-2"),
            ("availability_zones", ["us-east-1a", "us-east-1a"]),
            ("caller_subnet_ids", ["subnet-057078f4f29580c2b"]),
            ("service_image_digest", "latest"),
            ("cluster_arn", "arn:aws:ecs:us-east-1:613431292675:cluster/AgenticHarnessCluster-prod"),
            ("unexpected_override", True),
        )
        for field, value in cases:
            with self.subTest(field=field), self.assertRaises(ValueError):
                read_test_inputs({**INPUTS, field: value})

    def test_cidr_rejects_secondary_and_remote_ranges(self) -> None:
        validate_cidr("10.64.0.0/20", ("10.0.0.0/16", "172.31.0.0/16"))
        for occupied in ("10.64.0.0/18", "10.64.1.0/24", "10.64.0.0/20"):
            with self.subTest(occupied=occupied), self.assertRaisesRegex(ValueError, "overlap"):
                validate_cidr("10.64.0.0/20", ("10.0.0.0/16", occupied))

        for candidate in ("0.0.0.0/20", "198.18.0.0/20", "10.64.0.0/16", "10.64.0.1/20", "fd00::/64"):
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                validate_cidr(candidate, ())

    def test_proxy_requires_own_repository_and_immutable_digest(self) -> None:
        repository = "629807611108.dkr.ecr.us-east-1.amazonaws.com/valsmith-outbound-proxy-prod"
        validate_proxy_image(f"{repository}@sha256:{'a' * 64}")
        for image in (f"{repository}:latest", f"{repository}@sha256:abc", f"{repository}-other@sha256:{'a' * 64}"):
            with self.subTest(image=image), self.assertRaises(ValueError):
                validate_proxy_image(image)

    def test_inventory_requires_current_identity_and_inputs(self) -> None:
        inputs = read_test_inputs()
        inventory = NetworkInventory(
            "629807611108", "us-east-1", datetime.now(UTC), input_hash(inputs), ("10.0.0.0/16",), resource_fixture()
        )
        validate_inventory(inputs, inventory)
        for invalid in (
            replace(inventory, account_id="613431292675"),
            replace(inventory, region="us-west-2"),
            replace(inventory, observed_at=datetime.now(UTC) - timedelta(minutes=16)),
            replace(inventory, observed_at=datetime.now(UTC) + timedelta(minutes=1)),
            replace(inventory, input_sha256="changed"),
            replace(inventory, resources={}),
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_inventory(inputs, invalid)

    def test_inventory_rejects_resource_drift(self) -> None:
        inputs = read_test_inputs()
        cases: tuple[tuple[str, JsonValue], ...] = (
            ("vpc", {"VpcId": inputs.caller_vpc_id, "OwnerId": "613431292675", "State": "available"}),
            (
                "security_group",
                {"GroupId": "sg-00000000000000000", "VpcId": inputs.caller_vpc_id, "OwnerId": inputs.account_id},
            ),
            ("subnets", [{"SubnetId": inputs.caller_subnet_ids[0], "VpcId": "vpc-00000000000000000"}]),
            ("route_tables", []),
            (
                "namespace",
                {
                    "Id": inputs.namespace_id,
                    "Name": "local-prod",
                    "Type": "DNS_PRIVATE",
                    "Properties": {"DnsProperties": {"HostedZoneId": "different"}},
                },
            ),
            ("service_image_digest", "sha256:" + "b" * 64),
            ("prefix_lists", [{"PrefixListId": "pl-63a5400a", "PrefixListName": "com.amazonaws.us-east-1.dynamodb"}]),
        )
        for resource, changed in cases:
            resources = resource_fixture()
            resources[resource] = changed
            inventory = NetworkInventory(
                inputs.account_id, inputs.region, datetime.now(UTC), input_hash(inputs), (), resources
            )
            with self.subTest(resource=resource), self.assertRaises(ValueError):
                validate_inventory(inputs, inventory)

    def test_caller_egress_must_already_cover_the_new_service_port(self) -> None:
        inputs = read_test_inputs()
        resources = resource_fixture()
        group = object_field(resources, "security_group")
        inventory = NetworkInventory(
            inputs.account_id, inputs.region, datetime.now(UTC), input_hash(inputs), (), resources
        )
        validate_inventory(inputs, inventory)
        group["IpPermissionsEgress"] = [
            {
                "IpProtocol": "tcp",
                "FromPort": 8001,
                "ToPort": 8001,
                "IpRanges": [{"CidrIp": "10.64.0.0/20"}],
            }
        ]
        validate_inventory(inputs, inventory)

        group["IpPermissionsEgress"] = [
            {"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": "sg-other"}]},
            {"IpProtocol": "-1", "PrefixListIds": [{"PrefixListId": "pl-other"}]},
            {"IpProtocol": "-1", "Ipv6Ranges": [{"CidrIpv6": "::/0"}]},
            {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        ]
        validate_inventory(inputs, inventory)

        invalid_rules: tuple[list[JsonValue], ...] = (
            [],
            [{"IpProtocol": "udp", "FromPort": 8001, "ToPort": 8001, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
            [{"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
            [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "10.0.0.0/16"}]}],
            [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "10.64.0.0/24"}]}],
        )
        for rules in invalid_rules:
            group["IpPermissionsEgress"] = rules
            with self.subTest(rules=rules), self.assertRaisesRegex(ValueError, "Caller egress"):
                validate_inventory(inputs, inventory)
