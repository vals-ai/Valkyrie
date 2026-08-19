"""AWS resources and authentication selected for one tracker operation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tracker.aws.clients import AWSClientProvider, ExplicitCredentialsAWSClientProvider
from tracker.aws.models import RunAWSResources

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


def identify_run_aws_resources(runtime: AWSRuntime) -> RunAWSResources:
    """Resolve the physical AWS namespace and locations selected by a runtime."""
    response = runtime.clients.sts_client().get_caller_identity()
    account_id = response.get("Account")
    if not isinstance(account_id, str):
        raise ValueError("AWS did not return an account ID for the selected credentials")
    return RunAWSResources(
        account_id=account_id,
        region=runtime.resources.region,
        s3_bucket=runtime.resources.s3_bucket,
        log_group=runtime.resources.log_group,
        log_retention_days=runtime.resources.log_retention_days,
    )


def bind_runtime_to_run(runtime: AWSRuntime, resources: RunAWSResources) -> AWSRuntime:
    """Use a run's canonical resources with the currently selected AWS clients."""
    return AWSRuntime(
        resources=resources_for_run(resources),
        clients=runtime.clients,
    )


def resources_for_run(resources: RunAWSResources) -> AWSResources:
    """Return the runtime resource locations persisted with a run."""
    return AWSResources(
        region=resources.region,
        s3_bucket=resources.s3_bucket,
        log_group=resources.log_group,
        log_retention_days=resources.log_retention_days,
    )
