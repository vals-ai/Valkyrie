"""Shared API dependencies."""

from fastapi import Depends

from tracker.aws.runtime import AWSRuntime
from tracker.types import HarnessConfig
from tracker.utils import fetch_harness_config


def get_access_key_aws_runtime(
    harness_config: HarnessConfig = Depends(fetch_harness_config),
) -> AWSRuntime:
    """Build the AWS runtime selected by the request headers."""
    return AWSRuntime.from_harness_config(harness_config)
