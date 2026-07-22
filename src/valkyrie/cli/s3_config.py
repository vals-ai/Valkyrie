from functools import lru_cache
from typing import Any

import click
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.types import AWSCredentials

from valkyrie.cli.config.state import load_config


def _bucket_name(config: dict[str, Any]) -> str:
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'valkyrie config set' first.")

    return str(bucket_name)


@lru_cache(maxsize=4)
def _aws_runtime(
    access_key_id: str,
    secret_access_key: str,
    region: str,
    s3_bucket: str,
    log_group: str,
    log_retention_days: int,
) -> AWSRuntime:
    credentials = AWSCredentials(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_default_region=region,
    )
    return AWSRuntime(
        resources=AWSResources(
            region=region,
            s3_bucket=s3_bucket,
            log_group=log_group,
            log_retention_days=log_retention_days,
        ),
        clients=ExplicitCredentialsAWSClientProvider(credentials),
    )


def aws_runtime() -> AWSRuntime:
    """Build the AWS runtime configured for local CLI operations."""
    config = load_config()
    return _aws_runtime(
        access_key_id=config["AWS_ACCESS_KEY_ID"],
        secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        region=config["AWS_DEFAULT_REGION"],
        s3_bucket=_bucket_name(config),
        log_group=str(config.get("LOG_GROUP") or ""),
        log_retention_days=int(config.get("LOG_RETENTION_POLICY") or 30),
    )


def fetch_bucket_name() -> str:
    return _bucket_name(load_config())


def s3_client() -> Any:
    """Open an async S3 client using the local CLI runtime."""
    return aws_runtime().clients.s3_client()
