"""Pinned inputs and validation for the isolated Production ValSmith network."""

import hashlib
import ipaddress
import json
import re
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeAlias, cast

from deployment_target import DeploymentTarget, validate_caller_identity

JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]

ACCOUNT = "629807611108"
REGION = "us-east-1"
NETWORK_STACK = "ValSmithProdNetwork"
PROXY_REPOSITORY = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/valsmith-outbound-proxy-prod"
SERVICE_REPOSITORY = f"cdk-hnb659fds-container-assets-{ACCOUNT}-{REGION}"


def json_document(text: str) -> dict[str, JsonValue]:
    def unique_pairs(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        document: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in document:
                raise ValueError(f"Duplicate JSON key: {key}")

            document[key] = value

        return document

    # json.loads produces only JSON values; individual field shapes are checked below.
    value = cast(JsonValue, json.loads(text, object_pairs_hook=unique_pairs))
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")

    return value


def text_field(document: dict[str, JsonValue], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"Missing or invalid {name}")

    return value


def object_field(document: dict[str, JsonValue], name: str) -> dict[str, JsonValue]:
    value = document.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"Missing or invalid {name}")

    return value


def object_list(document: dict[str, JsonValue], name: str) -> list[dict[str, JsonValue]]:
    value = document.get(name)
    if not isinstance(value, list):
        raise ValueError(f"Missing or invalid {name}")

    result: list[dict[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError(f"Invalid entry in {name}")

        result.append(item)

    return result


def _pair(document: dict[str, JsonValue], name: str) -> tuple[str, str]:
    value = document.get(name)
    if not isinstance(value, list) or len(value) != 2 or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{name} requires two strings")

    first, second = value
    if not isinstance(first, str) or not isinstance(second, str) or first == second:
        raise ValueError(f"{name} requires distinct values")

    return first, second


def validate_cidr(candidate: str, occupied: tuple[str, ...]) -> None:
    network = ipaddress.ip_network(candidate, strict=True)
    private_ranges = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    if not isinstance(network, ipaddress.IPv4Network) or network.prefixlen != 20:
        raise ValueError("An explicit private IPv4 /20 is required")

    if not any(network.subnet_of(ipaddress.IPv4Network(private)) for private in private_ranges):
        raise ValueError("An RFC1918 IPv4 range is required")

    for occupied_cidr in occupied:
        other = ipaddress.ip_network(occupied_cidr, strict=True)
        if network.overlaps(other):
            raise ValueError(f"CIDR overlaps {occupied_cidr}")


def validate_proxy_image(image_uri: str) -> None:
    if not re.fullmatch(re.escape(PROXY_REPOSITORY) + r"@sha256:[a-f0-9]{64}", image_uri):
        raise ValueError("Proxy image must use its Production repository and an immutable SHA256 digest")


@dataclass(frozen=True)
class NetworkInputs:
    account_id: str
    region: str
    vpc_cidr: str
    availability_zones: tuple[str, str]
    caller_vpc_id: str
    caller_subnet_ids: tuple[str, str]
    caller_subnet_cidrs: tuple[str, str]
    caller_route_table_ids: tuple[str, str]
    caller_security_group_id: str
    cluster_arn: str
    namespace_id: str
    namespace_name: str
    namespace_hosted_zone_id: str
    service_image_digest: str
    s3_prefix_list_id: str
    dynamodb_prefix_list_id: str

    def __post_init__(self) -> None:
        if self.account_id != ACCOUNT or self.region != REGION:
            raise ValueError("ValSmith network requires Production account 629 in us-east-1")

        if self.availability_zones != ("us-east-1a", "us-east-1b"):
            raise ValueError("Use the reviewed availability zones a and b")

        validate_cidr(self.vpc_cidr, ())
        for subnet_cidr in self.caller_subnet_cidrs:
            if not isinstance(ipaddress.ip_network(subnet_cidr, strict=True), ipaddress.IPv4Network):
                raise ValueError("Caller subnets require IPv4 CIDRs")

        for prefix, identifiers in (
            ("vpc", (self.caller_vpc_id,)),
            ("subnet", self.caller_subnet_ids),
            ("rtb", self.caller_route_table_ids),
            ("sg", (self.caller_security_group_id,)),
        ):
            if len(set(identifiers)) != len(identifiers) or any(
                not re.fullmatch(prefix + r"-[a-f0-9]{17}", identifier) for identifier in identifiers
            ):
                raise ValueError(f"Invalid or duplicate {prefix} identifiers")

        if self.cluster_arn != f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/AgenticHarnessCluster-prod":
            raise ValueError("Use the existing Production ECS cluster")

        if not re.fullmatch(r"ns-[a-z0-9]+", self.namespace_id) or self.namespace_name != "local-prod":
            raise ValueError("Use the existing Production Cloud Map namespace")

        if not re.fullmatch(r"Z[A-Z0-9]+", self.namespace_hosted_zone_id):
            raise ValueError("Invalid namespace hosted zone")

        if not re.fullmatch(r"sha256:[a-f0-9]{64}", self.service_image_digest):
            raise ValueError("Service image must be an immutable SHA256 digest")

        for prefix_list in (self.s3_prefix_list_id, self.dynamodb_prefix_list_id):
            if not re.fullmatch(r"pl-(?:[a-f0-9]{8}|[a-f0-9]{17})", prefix_list):
                raise ValueError("Invalid AWS managed prefix list")


@dataclass(frozen=True)
class NetworkInventory:
    account_id: str
    region: str
    observed_at: datetime
    input_sha256: str
    occupied_cidrs: tuple[str, ...]
    resources: dict[str, JsonValue]


def load_inputs(path: Path) -> NetworkInputs:
    document = json_document(path.read_text())
    if set(document) != {field.name for field in fields(NetworkInputs)}:
        raise ValueError("Network input fields do not match the required contract")

    return NetworkInputs(
        account_id=text_field(document, "account_id"),
        region=text_field(document, "region"),
        vpc_cidr=text_field(document, "vpc_cidr"),
        availability_zones=_pair(document, "availability_zones"),
        caller_vpc_id=text_field(document, "caller_vpc_id"),
        caller_subnet_ids=_pair(document, "caller_subnet_ids"),
        caller_subnet_cidrs=_pair(document, "caller_subnet_cidrs"),
        caller_route_table_ids=_pair(document, "caller_route_table_ids"),
        caller_security_group_id=text_field(document, "caller_security_group_id"),
        cluster_arn=text_field(document, "cluster_arn"),
        namespace_id=text_field(document, "namespace_id"),
        namespace_name=text_field(document, "namespace_name"),
        namespace_hosted_zone_id=text_field(document, "namespace_hosted_zone_id"),
        service_image_digest=text_field(document, "service_image_digest"),
        s3_prefix_list_id=text_field(document, "s3_prefix_list_id"),
        dynamodb_prefix_list_id=text_field(document, "dynamodb_prefix_list_id"),
    )


def input_hash(inputs: NetworkInputs) -> str:
    return hashlib.sha256(json.dumps(asdict(inputs), sort_keys=True).encode()).hexdigest()


def _match(document: dict[str, JsonValue], expected: dict[str, str]) -> None:
    for name, value in expected.items():
        if document.get(name) != value:
            raise ValueError(f"Resource drift: {name} must be {value}")


def validate_inventory(inputs: NetworkInputs, inventory: NetworkInventory) -> None:
    validate_caller_identity(
        DeploymentTarget("prod", inputs.account_id, inputs.region), {"Account": inventory.account_id}
    )
    if inventory.region != inputs.region or inventory.input_sha256 != input_hash(inputs):
        raise ValueError("Inventory does not match the selected inputs and Region")

    if inventory.observed_at.tzinfo is None or not timedelta(0) <= datetime.now(
        UTC
    ) - inventory.observed_at <= timedelta(minutes=15):
        raise ValueError("Inventory must be no more than 15 minutes old and must not be in the future")

    validate_cidr(inputs.vpc_cidr, inventory.occupied_cidrs)
    resources = inventory.resources
    _match(
        object_field(resources, "vpc"),
        {"VpcId": inputs.caller_vpc_id, "OwnerId": inputs.account_id, "State": "available"},
    )
    subnets = object_list(resources, "subnets")
    routes = object_list(resources, "route_tables")
    if len(subnets) != 2 or len(routes) != 2:
        raise ValueError("Both caller subnets and their route tables must exist")

    for index, subnet_id in enumerate(inputs.caller_subnet_ids):
        subnet = next((item for item in subnets if item.get("SubnetId") == subnet_id), None)
        if subnet is None:
            raise ValueError("Caller subnet is missing")

        _match(
            subnet,
            {
                "SubnetId": subnet_id,
                "VpcId": inputs.caller_vpc_id,
                "OwnerId": inputs.account_id,
                "State": "available",
                "AvailabilityZone": inputs.availability_zones[index],
                "CidrBlock": inputs.caller_subnet_cidrs[index],
            },
        )
        route_table = next(
            (item for item in routes if item.get("RouteTableId") == inputs.caller_route_table_ids[index]), None
        )
        if route_table is None:
            raise ValueError("Caller route table is missing")

        _match(
            route_table,
            {
                "RouteTableId": inputs.caller_route_table_ids[index],
                "VpcId": inputs.caller_vpc_id,
                "OwnerId": inputs.account_id,
            },
        )
        if not any(
            association.get("SubnetId") == subnet_id for association in object_list(route_table, "Associations")
        ):
            raise ValueError("Caller route table association changed")

    _match(
        object_field(resources, "security_group"),
        {"GroupId": inputs.caller_security_group_id, "VpcId": inputs.caller_vpc_id, "OwnerId": inputs.account_id},
    )
    _match(object_field(resources, "cluster"), {"clusterArn": inputs.cluster_arn, "status": "ACTIVE"})
    namespace = object_field(resources, "namespace")
    _match(
        namespace,
        {
            "Id": inputs.namespace_id,
            "Name": inputs.namespace_name,
            "Type": "DNS_PRIVATE",
            "Arn": f"arn:aws:servicediscovery:{REGION}:{ACCOUNT}:namespace/{inputs.namespace_id}",
        },
    )
    _match(
        object_field(object_field(namespace, "Properties"), "DnsProperties"),
        {"HostedZoneId": inputs.namespace_hosted_zone_id},
    )
    _match(resources, {"service_image_digest": inputs.service_image_digest})

    prefix_lists = object_list(resources, "prefix_lists")
    for service, identifier in (("s3", inputs.s3_prefix_list_id), ("dynamodb", inputs.dynamodb_prefix_list_id)):
        prefix_list = next((item for item in prefix_lists if item.get("PrefixListId") == identifier), None)
        if prefix_list is None:
            raise ValueError("AWS managed prefix list is missing")

        _match(prefix_list, {"PrefixListName": f"com.amazonaws.{inputs.region}.{service}"})
