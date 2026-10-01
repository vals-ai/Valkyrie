"""Private AWS transports with the reviewed per-role resource boundaries."""

import copy
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from valsmith_network_config import ACCOUNT, JsonValue, json_document, object_field, object_list

if TYPE_CHECKING:
    from valsmith_network_stack import ValSmithNetworkStack

EndpointPermissions: TypeAlias = dict[str, dict[str, list[dict[str, JsonValue]]]]
PROXY_EXECUTION_ROLE_NAME = "ValSmithProdProxyExecution"
PROXY_LOG_GROUP = "/vals/valsmith-prod/outbound-proxy"


def load_permissions() -> EndpointPermissions:
    document = json_document(Path(__file__).with_name("valsmith_endpoint_permissions.json").read_text())
    if set(document) != {"s3", "dynamodb", "ecr", "secretsmanager", "ssm", "logs"}:
        raise ValueError("Endpoint permissions must include exactly the reviewed services")

    permissions: EndpointPermissions = {}
    for service in document:
        roles = object_field(document, service)
        permissions[service] = {}
        for role in roles:
            if not re.fullmatch(f"arn:aws:iam::{ACCOUNT}:role/[A-Za-z0-9+=,.@_-]+", role):
                raise ValueError("Endpoint role must be an exact Production role ARN")

            permissions[service][role] = object_list(roles, role)

    return permissions


def endpoint_document(service: str, permissions: EndpointPermissions) -> dict[str, JsonValue]:
    statements: list[JsonValue] = []
    for role, source_statements in permissions[service].items():
        for original in source_statements:
            statement = copy.deepcopy(original)
            if "Principal" in statement or "NotPrincipal" in statement:
                raise ValueError("Reviewed statements must not supply a principal")

            conditions = object_field(statement, "Condition") if "Condition" in statement else {}
            for condition in conditions:
                if any(key.lower() == "aws:principalarn" for key in object_field(conditions, condition)):
                    raise ValueError("Reviewed statements must not override aws:PrincipalArn")

            principal_condition = object_field(conditions, "ArnEquals") if "ArnEquals" in conditions else {}
            principal_condition["aws:PrincipalArn"] = role
            conditions["ArnEquals"] = principal_condition
            statement["Condition"] = conditions
            statement["Principal"] = "*"
            statements.append(statement)

    if service == "s3":
        # ECR signs layer downloads with its service identity, not the task's role.
        statements.append(
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::prod-us-east-1-starport-layer-bucket/*",
            }
        )

    document: dict[str, JsonValue] = {"Version": "2012-10-17", "Statement": statements}
    if not statements or len(json.dumps(document).encode()) > 20_480:
        raise ValueError("Endpoint policy is empty or exceeds the AWS size limit")

    return document


def add_endpoints(stack: "ValSmithNetworkStack", permissions: EndpointPermissions) -> dict[str, ec2.IVpcEndpoint]:
    endpoints: dict[str, ec2.IVpcEndpoint] = {}
    for service, aws_service, subnets, prefix_list, groups in (
        (
            "s3",
            ec2.GatewayVpcEndpointAwsService.S3,
            [*stack.application_subnets, *stack.proxy_subnets],
            stack.inputs.s3_prefix_list_id,
            [stack.generation_group, stack.evaluation_group, stack.view_group, stack.policy_group, stack.proxy_group],
        ),
        (
            "dynamodb",
            ec2.GatewayVpcEndpointAwsService.DYNAMODB,
            stack.application_subnets,
            stack.inputs.dynamodb_prefix_list_id,
            [stack.generation_group, stack.evaluation_group],
        ),
    ):
        endpoint = ec2.GatewayVpcEndpoint(
            stack,
            f"{service}Endpoint",
            vpc=stack.vpc,
            service=aws_service,
            subnets=[ec2.SubnetSelection(subnets=subnets)],
        )
        for statement in object_list(endpoint_document(service, permissions), "Statement"):
            endpoint.add_to_policy(iam.PolicyStatement.from_json(statement))

        endpoints[service] = endpoint
        for index, group in enumerate(groups):
            ec2.CfnSecurityGroupEgress(
                stack,
                f"{service}EndpointCaller{index}",
                group_id=group.security_group_id,
                ip_protocol="tcp",
                from_port=443,
                to_port=443,
                destination_prefix_list_id=prefix_list,
            )

    services = (
        (
            "ecr-api",
            "ecr",
            ec2.InterfaceVpcEndpointAwsService.ECR,
            [stack.generation_group, stack.evaluation_group, stack.proxy_group],
        ),
        (
            "ecr-dkr",
            "ecr",
            ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
            [stack.generation_group, stack.evaluation_group, stack.proxy_group],
        ),
        (
            "secretsmanager",
            "secretsmanager",
            ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            [stack.generation_group, stack.evaluation_group, stack.view_group],
        ),
        ("ssm", "ssm", ec2.InterfaceVpcEndpointAwsService.SSM, [stack.generation_group, stack.evaluation_group]),
        (
            "logs",
            "logs",
            ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            [stack.generation_group, stack.evaluation_group, stack.proxy_group],
        ),
    )
    for name, service, aws_service, groups in services:
        endpoint_group = ec2.SecurityGroup(
            stack,
            f"{name}EndpointGroup",
            vpc=stack.vpc,
            allow_all_outbound=False,
            disable_inline_rules=True,
            description=f"ValSmith {name} endpoint callers",
        )
        for index, group in enumerate(groups):
            stack.connect_groups(f"{name}EndpointCaller{index}", group, endpoint_group, 443)

        endpoint = ec2.InterfaceVpcEndpoint(
            stack,
            f"{name}Endpoint",
            vpc=stack.vpc,
            service=aws_service,
            subnets=ec2.SubnetSelection(subnets=stack.application_subnets),
            private_dns_enabled=True,
            open=False,
            security_groups=[endpoint_group],
        )
        for statement in object_list(endpoint_document(service, permissions), "Statement"):
            endpoint.add_to_policy(iam.PolicyStatement.from_json(statement))

        endpoints[name] = endpoint

    return endpoints
