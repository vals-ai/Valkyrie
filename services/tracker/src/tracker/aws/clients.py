"""AWS client providers for tracker runtimes."""

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Protocol, cast

import aioboto3
import boto3
from botocore.config import Config

from tracker.types import AWSCredentials

_HIGH_CONCURRENCY_CLIENT_CONFIG = Config(max_pool_connections=200)
_S3_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})
_DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS = 3600


def _boto3_client(service_name: str, **kwargs: Any) -> Any:
    client_factory = cast(Any, boto3.client)  # pyright: ignore[reportUnknownMemberType]
    return client_factory(service_name, **kwargs)


class AwsClientProvider(Protocol):
    """Construct AWS service clients for one authentication source."""

    def s3_client(self) -> Any:
        """Open an async S3 client for use as a context manager."""
        ...

    def cloudwatch_logs_client(self) -> Any:
        """Return a synchronous CloudWatch Logs client."""
        ...

    def secretsmanager_client(self) -> Any:
        """Return a synchronous Secrets Manager client."""
        ...

    def lambda_client(self, config: Config | None = None) -> Any:
        """Return a synchronous Lambda client using the requested config."""
        ...

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        """Return the longest safe presigned URL lifetime."""
        ...


@dataclass(frozen=True)
class ExplicitCredentialsAwsClientProvider:
    """Construct AWS clients from caller-supplied credentials."""

    credentials: AWSCredentials = field(repr=False)

    @lru_cache(maxsize=32)
    def _s3_session(self) -> aioboto3.Session:
        return aioboto3.Session(
            aws_access_key_id=self.credentials.aws_access_key_id,
            aws_secret_access_key=self.credentials.aws_secret_access_key,
            aws_session_token=self.credentials.aws_session_token,
            region_name=self.credentials.aws_default_region,
        )

    def s3_client(self) -> Any:
        return self._s3_session().client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            config=_S3_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def cloudwatch_logs_client(self) -> Any:
        return _boto3_client(
            "logs",
            aws_access_key_id=self.credentials.aws_access_key_id,
            aws_secret_access_key=self.credentials.aws_secret_access_key,
            aws_session_token=self.credentials.aws_session_token,
            region_name=self.credentials.aws_default_region,
            config=_HIGH_CONCURRENCY_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def secretsmanager_client(self) -> Any:
        return _boto3_client(
            "secretsmanager",
            aws_access_key_id=self.credentials.aws_access_key_id,
            aws_secret_access_key=self.credentials.aws_secret_access_key,
            aws_session_token=self.credentials.aws_session_token,
            region_name=self.credentials.aws_default_region,
        )

    @lru_cache(maxsize=32)
    def lambda_client(self, config: Config | None = None) -> Any:
        return _boto3_client(
            "lambda",
            aws_access_key_id=self.credentials.aws_access_key_id,
            aws_secret_access_key=self.credentials.aws_secret_access_key,
            aws_session_token=self.credentials.aws_session_token,
            region_name=self.credentials.aws_default_region,
            config=config,
        )

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return requested_seconds


@dataclass(frozen=True)
class DefaultChainAwsClientProvider:
    """Construct AWS clients through the SDK default credential chain."""

    region: str

    @lru_cache(maxsize=32)
    def _s3_session(self) -> aioboto3.Session:
        return aioboto3.Session(region_name=self.region)

    def s3_client(self) -> Any:
        return self._s3_session().client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            config=_S3_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def cloudwatch_logs_client(self) -> Any:
        return _boto3_client(
            "logs",
            region_name=self.region,
            config=_HIGH_CONCURRENCY_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def secretsmanager_client(self) -> Any:
        return _boto3_client(
            "secretsmanager",
            region_name=self.region,
        )

    @lru_cache(maxsize=32)
    def lambda_client(self, config: Config | None = None) -> Any:
        return _boto3_client(
            "lambda",
            region_name=self.region,
            config=config,
        )

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return min(requested_seconds, _DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS)
