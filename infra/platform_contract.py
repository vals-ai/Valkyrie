"""Valkyrie platform contract — discovery handles for co-deployed benchmark services.

Valkyrie advertises its shared-infra handles (VPC, ECS cluster, Cloud Map namespace,
tracker security group) so any co-deployed benchmark service can discover them WITHOUT
referencing Valkyrie's internal stack/construct names. Consumers read the VPC by tag
(a synth-time lookup cannot use SSM) and the rest by SSM parameter (deploy-time refs).

CONSUMER-AGNOSTIC: this module must never name or assume any specific consumer. It
publishes Valkyrie's own handles; who reads them is not Valkyrie's concern. The
benchmark-services-registry mirrors this key schema in its own platform_contract.py —
keep the two in sync.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_ec2, aws_ssm
from constructs import Construct

# VPC discovery tag; value is the stage name (e.g. "prod", "dev").
VPC_STAGE_TAG = "valkyrie:shared-vpc-stage"


def _key(stage_name: str, leaf: str) -> str:
    return f"/valkyrie/platform/{stage_name}/{leaf}"


def ecs_cluster_name_key(stage_name: str) -> str:
    return _key(stage_name, "ecs-cluster-name")


def cloudmap_namespace_name_key(stage_name: str) -> str:
    return _key(stage_name, "cloudmap-namespace-name")


def cloudmap_namespace_id_key(stage_name: str) -> str:
    return _key(stage_name, "cloudmap-namespace-id")


def cloudmap_namespace_arn_key(stage_name: str) -> str:
    return _key(stage_name, "cloudmap-namespace-arn")


def tracker_security_group_id_key(stage_name: str) -> str:
    return _key(stage_name, "tracker-security-group-id")


def tag_shared_vpc(vpc: aws_ec2.IVpc, stage_name: str) -> None:
    cdk.Tags.of(vpc).add(VPC_STAGE_TAG, stage_name)


def _param(scope: Construct, construct_id: str, key: str, value: str) -> None:
    aws_ssm.StringParameter(scope, construct_id, parameter_name=key, string_value=value)


def publish_shared_handles(
    scope: Construct,
    stage_name: str,
    *,
    cluster_name: str,
    namespace_name: str,
    namespace_id: str,
    namespace_arn: str,
) -> None:
    _param(scope, "PlatformClusterNameParam", ecs_cluster_name_key(stage_name), cluster_name)
    _param(scope, "PlatformNamespaceNameParam", cloudmap_namespace_name_key(stage_name), namespace_name)
    _param(scope, "PlatformNamespaceIdParam", cloudmap_namespace_id_key(stage_name), namespace_id)
    _param(scope, "PlatformNamespaceArnParam", cloudmap_namespace_arn_key(stage_name), namespace_arn)


def publish_tracker_security_group(scope: Construct, stage_name: str, security_group_id: str) -> None:
    _param(
        scope,
        "PlatformTrackerSgIdParam",
        tracker_security_group_id_key(stage_name),
        security_group_id,
    )
