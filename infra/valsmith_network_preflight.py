"""Deployment preflight and guarded DNS verification for the dedicated network."""

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
from valsmith_network_dns import DNS_LOG_ARN, DNS_LOG_GROUP


class AwsReader:
    def __init__(self, profile: str) -> None:
        if not profile.strip():
            raise ValueError("An explicit AWS profile is required")

        self.profile = profile

    def read(self, service: str, operation: str, *arguments: str) -> dict[str, JsonValue]:
        return self._call(service, operation, *arguments)

    def disable_dns_fail_open(self, vpc_id: str) -> None:
        self._call(
            "route53resolver", "update-firewall-config", "--resource-id", vpc_id, "--firewall-fail-open", "DISABLED"
        )

    def _call(self, service: str, operation: str, *arguments: str) -> dict[str, JsonValue]:
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


def verify_dns(
    profile: str, inputs: NetworkInputs, *, disable_fail_open: bool = False, reader: AwsReader | None = None
) -> dict[str, JsonValue]:
    reader = reader or AwsReader(profile)
    inventory = collect_inventory(profile, inputs, reader=reader)
    owned = object_list(inventory.resources, "owned_resources")
    response = reader.read("cloudformation", "describe-stacks", "--stack-name", NETWORK_STACK)
    stack = _only(object_list(response, "Stacks"), NETWORK_STACK)
    if not text_field(stack, "StackId").startswith(f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{NETWORK_STACK}/"):
        raise ValueError("DNS stack identity does not match Production")

    outputs = {text_field(item, "OutputKey"): text_field(item, "OutputValue") for item in object_list(stack, "Outputs")}
    for key, expected in {
        "NetworkContractVersion": "1",
        "NetworkAccount": ACCOUNT,
        "NetworkRegion": REGION,
        "ResolverLogGroupName": DNS_LOG_GROUP,
    }.items():
        if outputs.get(key) != expected:
            raise ValueError(f"DNS output {key} does not match the network contract")

    for key, resource_type in (
        ("VpcId", "AWS::EC2::VPC"),
        ("DnsFirewallRuleGroupId", "AWS::Route53Resolver::FirewallRuleGroup"),
        ("DnsFirewallAssociationId", "AWS::Route53Resolver::FirewallRuleGroupAssociation"),
        ("ResolverQueryLogConfigId", "AWS::Route53Resolver::ResolverQueryLoggingConfig"),
        ("ResolverLogAssociationId", "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation"),
    ):
        resource = _only([item for item in owned if item.get("ResourceType") == resource_type], key)
        if outputs.get(key) != text_field(resource, "PhysicalResourceId"):
            raise ValueError(f"DNS output {key} is not owned by the network stack")

    vpc_id = outputs["VpcId"]
    if vpc_id == inputs.caller_vpc_id:
        raise ValueError("DNS operator must never modify the shared VPC")

    firewall_associations = object_list(
        reader.read("route53resolver", "list-firewall-rule-group-associations", "--vpc-id", vpc_id),
        "FirewallRuleGroupAssociations",
    )
    association = _only(firewall_associations, "DNS firewall association")
    for key, expected in {
        "Id": outputs["DnsFirewallAssociationId"],
        "VpcId": vpc_id,
        "FirewallRuleGroupId": outputs["DnsFirewallRuleGroupId"],
        "Status": "COMPLETE",
        "Priority": 101,
    }.items():
        if association.get(key) != expected:
            raise ValueError(f"DNS firewall association {key} does not match")

    log_associations = object_list(
        reader.read("route53resolver", "list-resolver-query-log-config-associations"),
        "ResolverQueryLogConfigAssociations",
    )
    log_association = _only(
        [item for item in log_associations if item.get("ResourceId") == vpc_id], "Resolver log association"
    )
    for key, expected in {
        "Id": outputs["ResolverLogAssociationId"],
        "ResolverQueryLogConfigId": outputs["ResolverQueryLogConfigId"],
        "Status": "ACTIVE",
    }.items():
        if log_association.get(key) != expected:
            raise ValueError(f"Resolver log association {key} does not match")

    if log_association.get("Error") not in (None, "NONE"):
        raise ValueError("Resolver log delivery has an error")

    log_config = object_field(
        reader.read(
            "route53resolver",
            "get-resolver-query-log-config",
            "--resolver-query-log-config-id",
            outputs["ResolverQueryLogConfigId"],
        ),
        "ResolverQueryLogConfig",
    )
    if (
        log_config.get("DestinationArn") != DNS_LOG_ARN
        or log_config.get("OwnerId") != ACCOUNT
        or log_config.get("Status") != "CREATED"
    ):
        raise ValueError("Resolver logs are not active at the reviewed destination")

    if disable_fail_open:
        reader.disable_dns_fail_open(vpc_id)

    firewall = object_field(
        reader.read("route53resolver", "get-firewall-config", "--resource-id", vpc_id), "FirewallConfig"
    )
    if (
        firewall.get("ResourceId") != vpc_id
        or firewall.get("OwnerId") != ACCOUNT
        or firewall.get("FirewallFailOpen") != "DISABLED"
    ):
        raise ValueError("DNS Firewall must fail closed on the new VPC")

    return {
        "stack_id": text_field(stack, "StackId"),
        "vpc_id": vpc_id,
        "firewall": firewall,
        "association": association,
        "log_association": log_association,
        "verified_at": datetime.now(UTC).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["preflight", "verify-dns"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stack", choices=[NETWORK_STACK], default=NETWORK_STACK)
    parser.add_argument("--disable-fail-open", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "verify-dns":
        report = verify_dns(
            arguments.profile, load_inputs(arguments.inputs), disable_fail_open=arguments.disable_fail_open
        )
        arguments.output.write_text(json.dumps(report, indent=2) + "\n")
        print("Verified the dedicated network DNS firewall and log associations")
        return

    if arguments.disable_fail_open:
        parser.error("--disable-fail-open requires verify-dns")

    inventory = collect_inventory(arguments.profile, load_inputs(arguments.inputs))
    arguments.output.write_text(json.dumps(asdict(inventory), default=str, indent=2) + "\n")
    print(f"Verified {inventory.account_id}/{inventory.region}; input SHA256 {inventory.input_sha256}")


if __name__ == "__main__":
    main()
