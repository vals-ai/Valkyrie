from functools import lru_cache
from typing import Any

import click
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider, LocalChainAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.types import AWSCredentials

from valkyrie.sdk import ValkyrieConfig, ValkyrieConfigError
from valkyrie.cli.runtime_config import config_location


@lru_cache(maxsize=4)
def _aws_runtime(
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
    region: str,
    s3_bucket: str,
    log_group: str,
    log_retention_days: int,
) -> AWSRuntime:
    if access_key_id is None:
        clients = LocalChainAWSClientProvider(region)
    else:
        if secret_access_key is None:
            raise click.ClickException("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together.")
        clients = ExplicitCredentialsAWSClientProvider(
            AWSCredentials(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                aws_session_token=session_token,
                aws_default_region=region,
            )
        )

    return AWSRuntime(
        resources=AWSResources(
            region=region,
            s3_bucket=s3_bucket,
            log_group=log_group,
            log_retention_days=log_retention_days,
        ),
        clients=clients,
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
    credentials = aws.credentials
    return _aws_runtime(
        access_key_id=credentials.aws_access_key_id.get_secret_value() if credentials else None,
        secret_access_key=credentials.aws_secret_access_key.get_secret_value() if credentials else None,
        session_token=credentials.aws_session_token.get_secret_value()
        if credentials and credentials.aws_session_token
        else None,
        region=aws.aws_default_region,
        s3_bucket=aws.s3_bucket,
        log_group=aws.log_group,
        log_retention_days=aws.log_retention_policy,
    )


def fetch_bucket_name() -> str:
    return aws_runtime().resources.s3_bucket


def s3_client() -> Any:
    """Open an async S3 client using the local CLI runtime."""
    return aws_runtime().clients.s3_client()
