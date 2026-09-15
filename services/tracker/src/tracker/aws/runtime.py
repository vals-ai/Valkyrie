"""AWS resources and authentication selected for one tracker operation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tracker.aws.clients import AWSClientProvider, ExplicitCredentialsAWSClientProvider

if TYPE_CHECKING:
    from tracker.types import HarnessConfig


@dataclass(frozen=True)
class AWSResources:
    region: str
    s3_bucket: str
    log_group: str
    log_retention_days: int


@dataclass(frozen=True)
class AWSRuntime:
    resources: AWSResources
    clients: AWSClientProvider

    @classmethod
    def from_harness_config(cls, harness_config: HarnessConfig) -> AWSRuntime:
        """Convert access-key request configuration into an internal runtime."""
        return cls(
            resources=AWSResources(
                region=harness_config.aws.aws_default_region,
                s3_bucket=harness_config.s3_bucket,
                log_group=harness_config.log_group,
                log_retention_days=harness_config.log_retention_policy,
            ),
            clients=ExplicitCredentialsAWSClientProvider(harness_config.aws),
        )
