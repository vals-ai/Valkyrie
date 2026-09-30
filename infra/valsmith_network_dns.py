"""Reviewed DNS destinations and local query logs for the dedicated VPC."""

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

from aws_cdk import CfnOutput, RemovalPolicy, Token
from aws_cdk import aws_logs as logs
from aws_cdk import aws_route53resolver as resolver
from valsmith_network_config import ACCOUNT, REGION

if TYPE_CHECKING:
    from valsmith_network_stack import ValSmithNetworkStack

DNS_LOG_GROUP = "/vals/security/valsmith-prod/dns"
DNS_LOG_ARN = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{DNS_LOG_GROUP}"
REGIONAL_S3_SUFFIX = f"*.s3.{REGION}.amazonaws.com"


def normalize_dns_names(names: tuple[str, ...]) -> tuple[str, ...]:
    normalized: set[str] = set()
    for name in names:
        if Token.is_unresolved(name):
            normalized.add(name)
            continue

        value = name.lower().removesuffix(".")
        labels = value.split(".")
        if (
            len(value) > 253
            or len(labels) < 2
            or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in labels)
        ):
            raise ValueError("DNS destinations must be exact hostnames without wildcards")

        normalized.add(value)

    return tuple(sorted(normalized))


def dns_names(load_balancer_name: str) -> tuple[str, ...]:
    directory = Path(__file__).parent / "outbound_proxy"
    application_names = tuple(
        host
        for filename in ("service-hosts.txt", "view-hosts.txt")
        for host in (directory / filename).read_text().splitlines()
        if host
    )
    dependencies = (
        "7a5f089d15810552.vercel-dns-016.com",
        load_balancer_name,
        f"api.ecr.{REGION}.amazonaws.com",
        f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com",
        f"secretsmanager.{REGION}.amazonaws.com",
        f"ssm.{REGION}.amazonaws.com",
        f"logs.{REGION}.amazonaws.com",
        f"dynamodb.{REGION}.amazonaws.com",
        f"s3.{REGION}.amazonaws.com",
        f"s3-r-w.{REGION}.amazonaws.com",
        "agentic-harness.s3.amazonaws.com",
    )
    return (*normalize_dns_names((*application_names, *dependencies)), REGIONAL_S3_SUFFIX)


def dns_rules(
    approved_list_id: str, all_domains_list_id: str
) -> list[resolver.CfnFirewallRuleGroup.FirewallRuleProperty]:
    return [
        *[
            resolver.CfnFirewallRuleGroup.FirewallRuleProperty(
                action="ALLOW",
                priority=priority,
                firewall_domain_list_id=approved_list_id,
                firewall_domain_redirection_action="INSPECT_REDIRECTION_DOMAIN",
                qtype=query_type,
            )
            for priority, query_type in ((100, "A"), (200, "AAAA"))
        ],
        resolver.CfnFirewallRuleGroup.FirewallRuleProperty(
            action="BLOCK",
            priority=9900,
            firewall_domain_list_id=all_domains_list_id,
            block_response="NODATA",
        ),
    ]


def add_dns_controls(stack: "ValSmithNetworkStack") -> None:
    approved = resolver.CfnFirewallDomainList(
        stack,
        "DnsApprovedDomains",
        name="valsmith-prod-approved",
        domains=list(dns_names(stack.load_balancer.load_balancer_dns_name)),
    )
    all_domains = resolver.CfnFirewallDomainList(stack, "DnsAllDomains", name="valsmith-prod-all", domains=["*"])
    rule_group = resolver.CfnFirewallRuleGroup(
        stack,
        "DnsRuleGroup",
        name="valsmith-prod-boundary",
        firewall_rules=dns_rules(approved.attr_id, all_domains.attr_id),
    )
    association = resolver.CfnFirewallRuleGroupAssociation(
        stack,
        "DnsFirewallAssociation",
        firewall_rule_group_id=rule_group.attr_id,
        vpc_id=stack.vpc.vpc_id,
        priority=101,
        name="valsmith-prod-boundary",
        mutation_protection="DISABLED",
    )
    log_group = logs.LogGroup(
        stack,
        "ResolverLogs",
        log_group_name=DNS_LOG_GROUP,
        retention=logs.RetentionDays.ONE_WEEK,
        removal_policy=RemovalPolicy.RETAIN,
    )
    delivery_policy = logs.CfnResourcePolicy(
        stack,
        "ResolverLogDeliveryPolicy",
        policy_name="ValSmithProdResolverLogDelivery",
        policy_document=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "delivery.logs.amazonaws.com"},
                        "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                        "Resource": DNS_LOG_ARN + ":log-stream:*",
                        "Condition": {
                            "StringEquals": {"aws:SourceAccount": ACCOUNT},
                            "ArnLike": {"aws:SourceArn": f"arn:aws:logs:{REGION}:{ACCOUNT}:*"},
                        },
                    }
                ],
            }
        ),
    )
    query_config = resolver.CfnResolverQueryLoggingConfig(
        stack, "ResolverQueryLogConfig", destination_arn=DNS_LOG_ARN, name="valsmith-prod-dns"
    )
    query_config.node.add_dependency(log_group, delivery_policy)
    log_association = resolver.CfnResolverQueryLoggingConfigAssociation(
        stack,
        "ResolverLogAssociation",
        resolver_query_log_config_id=query_config.attr_id,
        resource_id=stack.vpc.vpc_id,
    )
    for name, value in {
        "DnsFirewallRuleGroupId": rule_group.attr_id,
        "DnsFirewallAssociationId": association.attr_id,
        "ResolverLogGroupName": log_group.log_group_name,
        "ResolverQueryLogConfigId": query_config.attr_id,
        "ResolverLogAssociationId": log_association.attr_id,
    }.items():
        CfnOutput(stack, name, value=value)
