"""Tracker and its runners must reach services in the isolated ValSmith VPC."""

import os
import unittest
from typing import cast
from unittest import mock

from stage import BENCH, DEV, PROD, RELEASE_TEST
from test_monitoring_stack import (
    TEST_BENCH_ENV,
    TEST_DEV_ENV,
    TEST_PROD_ENV,
    TEST_RELEASE_TEST_ENV,
    JsonObject,
    service_templates,
)


class TrackerServiceEgressTest(unittest.TestCase):
    def test_service_calls_reach_the_private_valsmith_vpc_only_in_prod(self) -> None:
        cases = (
            (PROD, TEST_PROD_ENV, {"10.0.0.0/16", "10.64.0.0/20"}),
            (DEV, TEST_DEV_ENV, {"10.0.0.0/16"}),
            (BENCH, TEST_BENCH_ENV, {"10.0.0.0/16"}),
            (RELEASE_TEST, TEST_RELEASE_TEST_ENV, {"10.0.0.0/16"}),
        )
        for stage_name, environment, expected_destinations in cases:
            with self.subTest(stage=stage_name), mock.patch.dict(os.environ, environment, clear=True):
                tracker, _, _ = service_templates(stage_name)
                services = cast(dict[str, JsonObject], tracker.find_resources("AWS::ECS::Service"))
                service = next(iter(services.values()))["Properties"]
                group_reference = service["NetworkConfiguration"]["AwsvpcConfiguration"]["SecurityGroups"][0]
                groups = cast(dict[str, JsonObject], tracker.find_resources("AWS::EC2::SecurityGroup"))
                group = groups[group_reference["Fn::GetAtt"][0]]["Properties"]
                rules = cast(list[JsonObject], group["SecurityGroupEgress"])
                service_rules = [
                    rule for rule in rules if rule["IpProtocol"] == "tcp" and rule["FromPort"] <= 8001 <= rule["ToPort"]
                ]

                self.assertEqual({rule["CidrIp"] for rule in service_rules}, expected_destinations)
                for rule in service_rules:
                    self.assertEqual((rule["FromPort"], rule["ToPort"]), (8001, 8001))

                self.assertFalse(any(rule["IpProtocol"] == "-1" for rule in rules))
