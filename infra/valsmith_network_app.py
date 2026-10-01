#!/usr/bin/env python3
"""Standalone ValSmith image or network deployment; never deploy shared stacks."""

import os
from datetime import datetime
from pathlib import Path

from aws_cdk import App, Environment, Stack
from deployment_target import DeploymentTarget, validate_caller_identity
from valsmith_network_config import (
    ACCOUNT,
    REGION,
    NetworkInputs,
    NetworkInventory,
    json_document,
    load_inputs,
    object_field,
    text_field,
    validate_inventory,
    validate_proxy_image,
)
from valsmith_network_dns import add_dns_controls
from valsmith_network_endpoints import add_endpoints, load_permissions
from valsmith_network_preflight import AwsReader
from valsmith_network_stack import ValSmithNetworkStack
from valsmith_proxy_stack import ValSmithProxyImagesStack, add_proxy_services


def build_stack(app: App, component: str, inputs: NetworkInputs, proxy_image_uri: str) -> Stack:
    app.node.set_context(
        f"availability-zones:account={inputs.account_id}:region={inputs.region}", list(inputs.availability_zones)
    )
    env = Environment(account=inputs.account_id, region=inputs.region)
    if component == "images":
        return ValSmithProxyImagesStack(app, "ValSmithProdProxyImages", env=env)

    if component != "network":
        raise ValueError("Select exactly one component: images or network")

    stack = ValSmithNetworkStack(app, "ValSmithProdNetwork", inputs=inputs, proxy_image_uri=proxy_image_uri, env=env)
    add_endpoints(stack, load_permissions())
    add_dns_controls(stack)
    add_proxy_services(stack, proxy_image_uri)
    return stack


def _context(app: App, name: str, *, required: bool = True) -> str:
    value = app.node.try_get_context(name)
    if value is None and not required:
        return ""

    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"Supply explicit CDK context {name}")

    return value


def verify_deployment_inputs(path: Path, inputs: NetworkInputs, reader: AwsReader) -> None:
    identity = reader.read("sts", "get-caller-identity")
    validate_caller_identity(DeploymentTarget("prod", ACCOUNT, REGION), identity)
    document = json_document(path.read_text())
    occupied = document.get("occupied_cidrs")
    if not isinstance(occupied, list) or not all(isinstance(cidr, str) for cidr in occupied):
        raise ValueError("Preflight report has no complete CIDR inventory")

    inventory = NetworkInventory(
        account_id=text_field(document, "account_id"),
        region=text_field(document, "region"),
        observed_at=datetime.fromisoformat(text_field(document, "observed_at")),
        input_sha256=text_field(document, "input_sha256"),
        occupied_cidrs=tuple(cidr for cidr in occupied if isinstance(cidr, str)),
        resources=object_field(document, "resources"),
    )
    validate_inventory(inputs, inventory)


def main() -> None:
    app = App()
    inputs = load_inputs(Path(_context(app, "inputs")))
    for key, expected in (("CDK_DEFAULT_ACCOUNT", ACCOUNT), ("CDK_DEFAULT_REGION", REGION)):
        value = os.environ.get(key)
        if value and value != expected:
            raise ValueError(f"{key} does not match the reviewed target")

    profile = _context(app, "profile", required=False)
    preflight_path = _context(app, "preflight_path", required=False)
    if bool(profile) != bool(preflight_path):
        raise ValueError("Checked deployment requires both profile and preflight_path")

    component = _context(app, "component")
    image_uri = _context(app, "proxy_image_uri", required=component == "network")
    if profile:
        reader = AwsReader(profile)
        verify_deployment_inputs(Path(preflight_path), inputs, reader)
        if component == "network":
            validate_proxy_image(image_uri)
            reader.read(
                "ecr",
                "describe-images",
                "--repository-name",
                "valsmith-outbound-proxy-prod",
                "--image-ids",
                f"imageDigest={image_uri.split('@')[1]}",
            )

    build_stack(app, component, inputs, image_uri)
    app.synth()


if __name__ == "__main__":
    main()
