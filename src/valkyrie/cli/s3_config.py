from functools import lru_cache
from typing import Any

import click
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider, LocalChainAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.types import AWSCredentials

from valkyrie.sdk import ValkyrieConfig, ValkyrieConfigError
from valkyrie.cli.runtime_config import config_location


@lru_cache(maxsize=4)
def _aws_runtime(resources: AWSResources, credentials: AWSCredentials | None) -> AWSRuntime:
    return AWSRuntime(
        resources=resources,
        clients=LocalChainAWSClientProvider(resources.region)
        if credentials is None
        else ExplicitCredentialsAWSClientProvider(credentials),
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
    resources = AWSResources(
        region=aws.aws_default_region,
        s3_bucket=aws.s3_bucket,
        log_group=aws.log_group,
        log_retention_days=aws.log_retention_policy,
    )
    credentials = None
    if aws.credentials is not None:
        credentials = AWSCredentials(
            aws_access_key_id=aws.credentials.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=aws.credentials.aws_secret_access_key.get_secret_value(),
            aws_session_token=aws.credentials.aws_session_token.get_secret_value()
            if aws.credentials.aws_session_token
            else None,
            aws_default_region=resources.region,
        )
    return _aws_runtime(resources, credentials)


def fetch_bucket_name() -> str:
    return aws_runtime().resources.s3_bucket


def s3_client() -> Any:
    """Open an async S3 client using the local CLI runtime."""
    return aws_runtime().clients.s3_client()
