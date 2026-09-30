"""Read-only deployment preflight for the dedicated ValSmith network."""

import argparse
import json
import subprocess
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from deployment_target import DeploymentTarget, validate_caller_identity
from valsmith_network_config import (
    ACCOUNT,
    NETWORK_STACK,
    REGION,
    SERVICE_REPOSITORY,
    JsonValue,
    NetworkInputs,
    NetworkInventory,
    input_hash,
    json_document,
    load_inputs,
    object_field,
    object_list,
    text_field,
    validate_inventory,
)


class AwsReader:
    def __init__(self, profile: str) -> None:
        if not profile.strip():
            raise ValueError("An explicit AWS profile is required")

        self.profile = profile

    def read(self, service: str, operation: str, *arguments: str) -> dict[str, JsonValue]:
        # AWS CLI auto-pagination is required. Never pass --no-paginate or --max-items.
        result = subprocess.run(
            [
                "aws",
                service,
                operation,
                *arguments,
                "--profile",
                self.profile,
                "--region",
                REGION,
                "--output",
                "json",
                "--no-cli-pager",
            ],
            text=True,
            capture_output=True,
            check=True,
            timeout=90,
        )
        document = json_document(result.stdout)
        if any(document.get(key) for key in ("NextToken", "nextToken", "NextMarker", "Marker", "IsTruncated")):
            raise ValueError(f"AWS inventory is incomplete: {service} {operation}")

        return document


def _only(items: list[dict[str, JsonValue]], name: str) -> dict[str, JsonValue]:
    if len(items) != 1:
        raise ValueError(f"Expected exactly one {name}")

    return items[0]


def _owned_resources(reader: AwsReader) -> list[dict[str, JsonValue]]:
    stacks = object_list(reader.read("cloudformation", "list-stacks"), "StackSummaries")
    matching = [
        item
        for item in stacks
        if item.get("StackName") == NETWORK_STACK and item.get("StackStatus") != "DELETE_COMPLETE"
    ]
    if not matching:
        return []

    stack = _only(matching, NETWORK_STACK)
    stack_id = text_field(stack, "StackId")
    if not stack_id.startswith(f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{NETWORK_STACK}/"):
        raise ValueError("Network stack belongs to another account or Region")

    if stack.get("StackStatus") not in (
        "CREATE_COMPLETE",
        "UPDATE_COMPLETE",
        "UPDATE_ROLLBACK_COMPLETE",
        "IMPORT_COMPLETE",
    ):
        raise ValueError("Network stack is not in a stable completed state")

    return object_list(
        reader.read("cloudformation", "list-stack-resources", "--stack-name", stack_id), "StackResourceSummaries"
    )


def collect_inventory(profile: str, inputs: NetworkInputs, *, reader: AwsReader | None = None) -> NetworkInventory:
    reader = reader or AwsReader(profile)
    observed_at = datetime.now(UTC)
    identity = reader.read("sts", "get-caller-identity")
    validate_caller_identity(DeploymentTarget("prod", ACCOUNT, REGION), identity)
    owned = _owned_resources(reader)
    owned_vpcs = [
        text_field(item, "PhysicalResourceId") for item in owned if item.get("ResourceType") == "AWS::EC2::VPC"
    ]
    if len(owned_vpcs) > 1:
        raise ValueError("Network stack must own exactly one VPC")

    owned_vpc_id = owned_vpcs[0] if owned_vpcs else None
    owned_peer_ids = {
        text_field(item, "PhysicalResourceId")
        for item in owned
        if item.get("ResourceType") == "AWS::EC2::VPCPeeringConnection"
    }
    vpcs = object_list(reader.read("ec2", "describe-vpcs"), "Vpcs")
    subnets = object_list(reader.read("ec2", "describe-subnets"), "Subnets")
    route_tables = object_list(reader.read("ec2", "describe-route-tables"), "RouteTables")
    occupied: set[str] = set()
    for vpc in vpcs:
        cidrs = {text_field(item, "CidrBlock") for item in object_list(vpc, "CidrBlockAssociationSet")}
        if not cidrs:
            raise ValueError("VPC CIDR inventory is empty")

        if vpc.get("VpcId") == owned_vpc_id:
            if cidrs != {inputs.vpc_cidr} or vpc.get("OwnerId") != ACCOUNT or vpc.get("State") != "available":
                raise ValueError("Existing owned VPC does not match its pinned range and identity")

            continue

        occupied.update(cidrs)

    if owned_vpc_id and not any(vpc.get("VpcId") == owned_vpc_id for vpc in vpcs):
        raise ValueError("CloudFormation owned VPC is missing")

    reservations: list[JsonValue] = []
    for subnet in subnets:
        reservation = reader.read("ec2", "get-subnet-cidr-reservations", "--subnet-id", text_field(subnet, "SubnetId"))
        reservations.append(reservation)
        if subnet.get("VpcId") != owned_vpc_id:
            occupied.update(text_field(item, "Cidr") for item in object_list(reservation, "SubnetIpv4CidrReservations"))

    peerings = object_list(reader.read("ec2", "describe-vpc-peering-connections"), "VpcPeeringConnections")
    for peering in peerings:
        if object_field(peering, "Status").get("Code") in ("deleted", "rejected", "failed", "expired"):
            continue

        for side in ("RequesterVpcInfo", "AccepterVpcInfo"):
            peer = object_field(peering, side)
            peer_id = peering.get("VpcPeeringConnectionId")
            if peer_id in owned_peer_ids and peer.get("VpcId") == owned_vpc_id:
                continue

            if "CidrBlockSet" in peer:
                occupied.update(text_field(item, "CidrBlock") for item in object_list(peer, "CidrBlockSet"))
            elif "CidrBlock" in peer:
                occupied.add(text_field(peer, "CidrBlock"))
            else:
                raise ValueError("Cannot inventory a peer's address ranges")

    for route_table in route_tables:
        for route in object_list(route_table, "Routes"):
            if not (route.get("VpcPeeringConnectionId") or route.get("TransitGatewayId")):
                continue

            if route.get("VpcPeeringConnectionId") in owned_peer_ids:
                continue

            destination = route.get("DestinationCidrBlock")
            if isinstance(destination, str) and destination != "0.0.0.0/0":
                occupied.add(destination)

    transit = object_list(
        reader.read("ec2", "describe-transit-gateway-vpc-attachments"), "TransitGatewayVpcAttachments"
    )
    if any(item.get("State") not in ("deleted", "failed", "rejected") for item in transit):
        raise ValueError("Transit network ranges require a reviewed inventory before deployment")

    pools = object_list(reader.read("ec2", "describe-ipam-pools"), "IpamPools")
    pool_ranges: list[JsonValue] = []
    for pool in pools:
        response = reader.read("ec2", "get-ipam-pool-cidrs", "--ipam-pool-id", text_field(pool, "IpamPoolId"))
        pool_ranges.append(response)
        occupied.update(text_field(item, "Cidr") for item in object_list(response, "IpamPoolCidrs"))

    resolver_associations = object_list(
        reader.read("route53resolver", "list-resolver-rule-associations"), "ResolverRuleAssociations"
    )
    if owned_vpc_id and any(item.get("VPCId") == owned_vpc_id for item in resolver_associations):
        raise ValueError("New VPC has an unreviewed Resolver forwarding association")

    groups = reader.read("ec2", "describe-security-groups", "--group-ids", inputs.caller_security_group_id)
    clusters = reader.read("ecs", "describe-clusters", "--clusters", inputs.cluster_arn)
    if clusters.get("failures"):
        raise ValueError("ECS cluster inventory failed")

    namespace = reader.read("servicediscovery", "get-namespace", "--id", inputs.namespace_id)
    images = reader.read(
        "ecr",
        "describe-images",
        "--repository-name",
        SERVICE_REPOSITORY,
        "--image-ids",
        f"imageDigest={inputs.service_image_digest}",
    )
    resources: dict[str, JsonValue] = {
        "prefix_lists": list(object_list(reader.read("ec2", "describe-prefix-lists"), "PrefixLists")),
        "vpc": _only([vpc for vpc in vpcs if vpc.get("VpcId") == inputs.caller_vpc_id], "caller VPC"),
        "subnets": [subnet for subnet in subnets if subnet.get("SubnetId") in inputs.caller_subnet_ids],
        "route_tables": [table for table in route_tables if table.get("RouteTableId") in inputs.caller_route_table_ids],
        "security_group": _only(object_list(groups, "SecurityGroups"), "caller security group"),
        "cluster": _only(object_list(clusters, "clusters"), "caller cluster"),
        "namespace": object_field(namespace, "Namespace"),
        "service_image_digest": text_field(
            _only(object_list(images, "imageDetails"), "qualified service image"), "imageDigest"
        ),
        "all_vpcs": list(vpcs),
        "all_route_tables": list(route_tables),
        "peerings": list(peerings),
        "owned_resources": list(owned),
        "reservations": reservations,
        "ipam_ranges": pool_ranges,
        "resolver_associations": list(resolver_associations),
    }
    inventory = NetworkInventory(ACCOUNT, REGION, observed_at, input_hash(inputs), tuple(sorted(occupied)), resources)
    validate_inventory(inputs, inventory)
    return inventory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["preflight"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    inventory = collect_inventory(arguments.profile, load_inputs(arguments.inputs))
    arguments.output.write_text(json.dumps(asdict(inventory), default=str, indent=2) + "\n")
    print(f"Verified {inventory.account_id}/{inventory.region}; input SHA256 {inventory.input_sha256}")


if __name__ == "__main__":
    main()
