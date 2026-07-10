"""AWS resources and authentication selected for one tracker operation."""

from __future__ import annotations

from dataclasses import dataclass

from tracker.aws.clients import AwsClientProvider, ExplicitCredentialsAwsClientProvider
from tracker.types import HarnessConfig


@dataclass(frozen=True)
class AwsResources:
    region: str
    s3_bucket: str
    log_group: str
    log_retention_days: int


@dataclass(frozen=True)
class AwsRuntime:
    resources: AwsResources
    clients: AwsClientProvider
    managed: bool

    @classmethod
    def from_harness_config(cls, harness_config: HarnessConfig) -> AwsRuntime:
        """Convert the legacy access-key wire contract into an internal runtime."""
        return cls(
            resources=AwsResources(
                region=harness_config.aws.aws_default_region,
                s3_bucket=harness_config.s3_bucket,
                log_group=harness_config.log_group,
                log_retention_days=harness_config.log_retention_policy,
            ),
            clients=ExplicitCredentialsAwsClientProvider(harness_config.aws),
            managed=False,
        )
