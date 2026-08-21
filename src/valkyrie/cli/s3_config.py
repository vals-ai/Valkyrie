from functools import lru_cache
from typing import Any

import click
from tracker.aws.clients import DefaultChainAWSClientProvider, ExplicitCredentialsAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.types import AWSCredentials

from valkyrie.cli.config.state import load_config


def _bucket_name(config: dict[str, Any]) -> str:
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'valkyrie config set' first.")

    return str(bucket_name)


def _region(config: dict[str, Any]) -> str:
    region = config.get("AWS_DEFAULT_REGION")
    if not region:
        raise click.ClickException("AWS_DEFAULT_REGION key not found. Add it using 'valkyrie config set' first.")

    return str(region)


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
        clients = DefaultChainAWSClientProvider(region)
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
    config = load_config()
    access_key_id = str(config.get("AWS_ACCESS_KEY_ID") or "") or None
    secret_access_key = str(config.get("AWS_SECRET_ACCESS_KEY") or "") or None
    if (access_key_id is None) != (secret_access_key is None):
        raise click.ClickException("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together.")

    return _aws_runtime(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=str(config.get("AWS_SESSION_TOKEN") or "") or None,
        region=_region(config),
        s3_bucket=_bucket_name(config),
        log_group=str(config.get("LOG_GROUP") or ""),
        log_retention_days=int(config.get("LOG_RETENTION_POLICY") or 30),
    )


def fetch_bucket_name() -> str:
    return _bucket_name(load_config())


def s3_client() -> Any:
    """Open an async S3 client using the local CLI runtime."""
    return aws_runtime().clients.s3_client()
