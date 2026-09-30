from functools import lru_cache
from typing import Any

import click
from tracker.aws.clients import LocalChainAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime

from valkyrie.sdk import ValkyrieConfig, ValkyrieConfigError
from valkyrie.cli.runtime_config import config_location


@lru_cache(maxsize=4)
def _aws_runtime(resources: AWSResources) -> AWSRuntime:
    return AWSRuntime(
        resources=resources,
        clients=LocalChainAWSClientProvider(resources.region),
    )


def aws_runtime() -> AWSRuntime:
    """Build the AWS runtime configured for local CLI operations."""
    try:
        config = ValkyrieConfig.from_yaml(config_location())
    except ValkyrieConfigError as error:
        raise click.ClickException(str(error)) from error
    if config.aws is None:
        raise click.ClickException("AWS resources are not configured. Run 'valkyrie config init' first.")
    aws = config.aws
    return _aws_runtime(
        AWSResources(
            region=aws.aws_default_region,
            s3_bucket=aws.s3_bucket,
            log_group=aws.log_group,
            log_retention_days=aws.log_retention_policy,
        )
    )


def fetch_bucket_name() -> str:
    return aws_runtime().resources.s3_bucket


def s3_client() -> Any:
    """Open an async S3 client using the local CLI runtime."""
    return aws_runtime().clients.s3_client()
