"""Retained DNS resources owned by the Valkyrie development account."""

from typing import Any

import aws_cdk as cdk
from aws_cdk import Stack, aws_route53, aws_ssm
from constructs import Construct

TRACKER_DEV_ZONE_NAME = "benchmark-tracker-dev.vals.ai"
TRACKER_DEV_ZONE_ID_PARAMETER = "/valkyrie/dev/dns/tracker/hosted-zone-id"


class DnsStack(Stack):
    """Create the retained development tracker child hosted zone."""

    def __init__(self, scope: Construct, id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, **kwargs)

        self.hosted_zone = aws_route53.HostedZone(
            self,
            "TrackerHostedZone",
            zone_name=TRACKER_DEV_ZONE_NAME,
        )
        self.hosted_zone.apply_removal_policy(cdk.RemovalPolicy.RETAIN)

        aws_ssm.StringParameter(
            self,
            "TrackerHostedZoneIdParameter",
            parameter_name=TRACKER_DEV_ZONE_ID_PARAMETER,
            string_value=self.hosted_zone.hosted_zone_id,
        )
