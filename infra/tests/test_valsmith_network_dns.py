"""DNS aliases and query types must not bypass the new VPC's boundary."""

import copy
import unittest
from unittest.mock import patch

from aws_cdk import assertions
from tests.test_valsmith_network_config import read_test_inputs
from tests.test_valsmith_network_preflight import FixtureReader, responses
from tests.test_valsmith_network_stack import network_stack, resources
from valsmith_network_config import JsonValue, text_field
from valsmith_network_dns import add_dns_controls, dns_names, normalize_dns_names
from valsmith_network_preflight import verify_dns

VPC = "vpc-00000000000000001"
RULE_GROUP = "rslvr-frg-test"
FIREWALL_ASSOCIATION = "rslvr-frgassoc-test"
QUERY_CONFIG = "rqlc-test"
QUERY_ASSOCIATION = "rqlca-test"


def dns_responses() -> dict[tuple[str, str], dict[str, JsonValue]]:
    data = responses()
    stack_id = "arn:aws:cloudformation:us-east-1:629807611108:stack/ValSmithProdNetwork/1234"
    data[("cloudformation", "list-stacks")] = {
        "StackSummaries": [{"StackName": "ValSmithProdNetwork", "StackId": stack_id, "StackStatus": "CREATE_COMPLETE"}]
    }
    data[("cloudformation", "list-stack-resources")] = {
        "StackResourceSummaries": [
            {"ResourceType": kind, "PhysicalResourceId": identifier}
            for kind, identifier in (
                ("AWS::EC2::VPC", VPC),
                ("AWS::Route53Resolver::FirewallRuleGroup", RULE_GROUP),
                ("AWS::Route53Resolver::FirewallRuleGroupAssociation", FIREWALL_ASSOCIATION),
                ("AWS::Route53Resolver::ResolverQueryLoggingConfig", QUERY_CONFIG),
                ("AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation", QUERY_ASSOCIATION),
            )
        ]
    }
    vpcs = data[("ec2", "describe-vpcs")]["Vpcs"]
    assert isinstance(vpcs, list)
    vpcs.append(
        {
            "VpcId": VPC,
            "OwnerId": "629807611108",
            "State": "available",
            "CidrBlockAssociationSet": [{"CidrBlock": "10.64.0.0/20"}],
        }
    )
    data[("cloudformation", "describe-stacks")] = {
        "Stacks": [
            {
                "StackId": stack_id,
                "Outputs": [
                    {"OutputKey": key, "OutputValue": value}
                    for key, value in {
                        "NetworkContractVersion": "1",
                        "NetworkAccount": "629807611108",
                        "NetworkRegion": "us-east-1",
                        "VpcId": VPC,
                        "DnsFirewallRuleGroupId": RULE_GROUP,
                        "DnsFirewallAssociationId": FIREWALL_ASSOCIATION,
                        "ResolverQueryLogConfigId": QUERY_CONFIG,
                        "ResolverLogAssociationId": QUERY_ASSOCIATION,
                        "ResolverLogGroupName": "/vals/security/valsmith-prod/dns",
                    }.items()
                ],
            }
        ]
    }
    data[("route53resolver", "list-firewall-rule-group-associations")] = {
        "FirewallRuleGroupAssociations": [
            {
                "Id": FIREWALL_ASSOCIATION,
                "VpcId": VPC,
                "FirewallRuleGroupId": RULE_GROUP,
                "Status": "COMPLETE",
                "Priority": 101,
            }
        ]
    }
    data[("route53resolver", "list-resolver-query-log-config-associations")] = {
        "ResolverQueryLogConfigAssociations": [
            {
                "Id": QUERY_ASSOCIATION,
                "ResourceId": VPC,
                "ResolverQueryLogConfigId": QUERY_CONFIG,
                "Status": "ACTIVE",
                "Error": "NONE",
            }
        ]
    }
    data[("route53resolver", "get-resolver-query-log-config")] = {
        "ResolverQueryLogConfig": {
            "Id": QUERY_CONFIG,
            "OwnerId": "629807611108",
            "Status": "CREATED",
            "DestinationArn": "arn:aws:logs:us-east-1:629807611108:log-group:/vals/security/valsmith-prod/dns",
        }
    }
    data[("route53resolver", "get-firewall-config")] = {
        "FirewallConfig": {"ResourceId": VPC, "OwnerId": "629807611108", "FirewallFailOpen": "DISABLED"}
    }
    return data


class DnsReader(FixtureReader):
    def __init__(self, data: dict[tuple[str, str], dict[str, JsonValue]]) -> None:
        super().__init__(data)
        self.mutations: list[str] = []

    def disable_dns_fail_open(self, vpc_id: str) -> None:
        self.mutations.append(vpc_id)


class DnsControlsTest(unittest.TestCase):
    def test_public_names_are_exact_and_regional_s3_is_the_only_wildcard(self) -> None:
        self.assertEqual(normalize_dns_names(("VALSMITH.VALS.AI.", "valsmith.vals.ai")), ("valsmith.vals.ai",))
        for forbidden in ("*.vals.ai", "*.amazonaws.com", "*", "https://vals.ai", "a..vals.ai", " vals.ai"):
            with self.subTest(name=forbidden), self.assertRaises(ValueError):
                normalize_dns_names((forbidden,))

        names = dns_names("internal-test.elb.us-east-1.amazonaws.com")
        self.assertEqual([name for name in names if "*" in name], ["*.s3.us-east-1.amazonaws.com"])
        self.assertIn("7a5f089d15810552.vercel-dns-016.com", names)
        self.assertIn("s3-r-w.us-east-1.amazonaws.com", names)
        self.assertNotIn("child.valsmith.vals.ai", names)
        self.assertNotIn("prod.benchmarks.vals.ai", names)

    def test_allow_rules_inspect_aliases_and_catch_all_covers_every_query_type(self) -> None:
        stack = network_stack()
        add_dns_controls(stack)
        template = assertions.Template.from_stack(stack)
        groups = resources(template, "AWS::Route53Resolver::FirewallRuleGroup")
        self.assertEqual(len(groups), 1)
        rules = next(iter(groups.values()))["FirewallRules"]
        assert isinstance(rules, list)
        allows = [rule for rule in rules if isinstance(rule, dict) and rule["Action"] == "ALLOW"]
        self.assertEqual({text_field(rule, "Qtype") for rule in allows}, {"A", "AAAA"})
        for rule in allows:
            self.assertEqual(rule["FirewallDomainRedirectionAction"], "INSPECT_REDIRECTION_DOMAIN")
            priority = rule["Priority"]
            assert isinstance(priority, int)
            self.assertLess(priority, 9900)

        blocks = [rule for rule in rules if isinstance(rule, dict) and rule["Action"] == "BLOCK"]
        self.assertEqual(len(blocks), 1)
        self.assertNotIn("Qtype", blocks[0])
        self.assertEqual(blocks[0]["BlockResponse"], "NODATA")
        self.assertEqual(blocks[0]["Priority"], 9900)
        lists = resources(template, "AWS::Route53Resolver::FirewallDomainList")
        self.assertEqual(sum(item["Domains"] == ["*"] for item in lists.values()), 1)
        self.assertTrue(
            any(stack.resolve(stack.load_balancer.load_balancer_dns_name) in item["Domains"] for item in lists.values())
        )

    def test_firewall_and_short_lived_logs_apply_only_to_the_new_vpc(self) -> None:
        stack = network_stack()
        add_dns_controls(stack)
        template = assertions.Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Route53Resolver::FirewallRuleGroupAssociation",
            {"VpcId": stack.resolve(stack.vpc.vpc_id), "Priority": 101},
        )
        template.has_resource_properties(
            "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation",
            {"ResourceId": stack.resolve(stack.vpc.vpc_id)},
        )
        template.has_resource_properties(
            "AWS::Logs::LogGroup", {"LogGroupName": "/vals/security/valsmith-prod/dns", "RetentionInDays": 7}
        )
        template.resource_count_is("AWS::Logs::SubscriptionFilter", 0)
        policy = next(iter(resources(template, "AWS::Logs::ResourcePolicy").values()))["PolicyDocument"]
        self.assertIn("delivery.logs.amazonaws.com", str(policy))
        self.assertIn("aws:SourceAccount", str(policy))
        self.assertNotIn("logs:GetLogEvents", str(policy))
        self.assertNotIn("logs:FilterLogEvents", str(policy))

    def test_fail_closed_verification_requires_correct_live_associations(self) -> None:
        reader = DnsReader(dns_responses())
        verify_dns("test", read_test_inputs(), disable_fail_open=True, reader=reader)
        self.assertEqual(reader.mutations, [VPC])
        cases: tuple[tuple[tuple[str, str], dict[str, JsonValue]], ...] = (
            (
                ("route53resolver", "get-firewall-config"),
                {"FirewallConfig": {"ResourceId": VPC, "OwnerId": "629807611108", "FirewallFailOpen": "ENABLED"}},
            ),
            (
                ("route53resolver", "list-firewall-rule-group-associations"),
                {
                    "FirewallRuleGroupAssociations": [
                        {
                            "VpcId": VPC,
                            "Id": FIREWALL_ASSOCIATION,
                            "FirewallRuleGroupId": "wrong",
                            "Status": "COMPLETE",
                            "Priority": 100,
                        }
                    ]
                },
            ),
            (
                ("route53resolver", "list-resolver-query-log-config-associations"),
                {
                    "ResolverQueryLogConfigAssociations": [
                        {
                            "ResourceId": VPC,
                            "Id": QUERY_ASSOCIATION,
                            "ResolverQueryLogConfigId": QUERY_CONFIG,
                            "Status": "FAILED",
                            "Error": "ACCESS_DENIED",
                        }
                    ]
                },
            ),
        )
        for operation, response in cases:
            data = dns_responses()
            data[operation] = response
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                verify_dns("test", read_test_inputs(), reader=DnsReader(data))

    def test_operator_cannot_mutate_shared_vpc_or_invent_a_stack(self) -> None:
        data = dns_responses()
        altered = copy.deepcopy(data[("cloudformation", "describe-stacks")])
        stacks = altered["Stacks"]
        assert isinstance(stacks, list) and isinstance(stacks[0], dict)
        outputs = stacks[0]["Outputs"]
        assert isinstance(outputs, list)
        for output in outputs:
            if isinstance(output, dict) and output["OutputKey"] == "VpcId":
                output["OutputValue"] = read_test_inputs().caller_vpc_id

        data[("cloudformation", "describe-stacks")] = altered
        reader = DnsReader(data)
        with self.assertRaises(ValueError):
            verify_dns("test", read_test_inputs(), disable_fail_open=True, reader=reader)
        self.assertEqual(reader.mutations, [])

    def test_failed_aws_update_or_wrong_readback_never_reports_success(self) -> None:
        reader = DnsReader(dns_responses())
        with patch.object(reader, "disable_dns_fail_open", side_effect=PermissionError("AccessDenied")):
            with self.assertRaises(PermissionError):
                verify_dns("test", read_test_inputs(), disable_fail_open=True, reader=reader)

        data = dns_responses()
        data[("route53resolver", "get-firewall-config")] = {
            "FirewallConfig": {
                "ResourceId": read_test_inputs().caller_vpc_id,
                "OwnerId": "629807611108",
                "FirewallFailOpen": "DISABLED",
            }
        }
        with self.assertRaisesRegex(ValueError, "fail closed"):
            verify_dns("test", read_test_inputs(), disable_fail_open=True, reader=DnsReader(data))
