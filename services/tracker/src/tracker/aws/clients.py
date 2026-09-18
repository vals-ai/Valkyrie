"""AWS client providers for tracker runtimes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

import aioboto3
import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from tracker.types import AWSCredentials

_HIGH_CONCURRENCY_CLIENT_CONFIG = Config(max_pool_connections=200)
_S3_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})
_DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS = 3600


def _boto3_client(service_name: str, **kwargs: Any) -> Any:
    client_factory = cast(Any, boto3.client)  # pyright: ignore[reportUnknownMemberType]
    return client_factory(service_name, **kwargs)


class AWSClientProvider(ABC):
    """Construct AWS service clients for one authentication source."""

    credential_source: ClassVar[Literal["access_key", "local", "managed"]]

    @abstractmethod
    def with_region(self, region: str) -> "AWSClientProvider":
        """Select a resource region while retaining this authentication source."""
        raise NotImplementedError

    @abstractmethod
    def _client_kwargs(self) -> dict[str, Any]:
        """Return SDK arguments for this credential source."""
        raise NotImplementedError

    def _s3_session(self) -> aioboto3.Session:
        return aioboto3.Session(**self._client_kwargs())

    def s3_client(self) -> Any:
        return self._s3_session().client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            config=_S3_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def cloudwatch_logs_client(self) -> Any:
        return _boto3_client(
            "logs",
            config=_HIGH_CONCURRENCY_CLIENT_CONFIG,
            **self._client_kwargs(),
        )

    def secretsmanager_async_client(self) -> Any:
        return self._s3_session().client("secretsmanager")  # pyright: ignore[reportUnknownMemberType]

    def cloudwatch_logs_async_client(self) -> Any:
        return cast(Any, self._s3_session().client("logs", config=_HIGH_CONCURRENCY_CLIENT_CONFIG))  # pyright: ignore[reportUnknownMemberType]

    def lambda_client(self, config: Config | None = None) -> Any:
        return cast(Any, self._s3_session().client("lambda", config=config))  # pyright: ignore[reportUnknownMemberType]

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return requested_seconds


@dataclass(frozen=True)
class ExplicitCredentialsAWSClientProvider(AWSClientProvider):
    """Construct AWS clients from caller-supplied credentials."""

    credential_source: ClassVar[Literal["access_key", "local", "managed"]] = "access_key"
    credentials: AWSCredentials = field(repr=False)

    def with_region(self, region: str) -> AWSClientProvider:
        return ExplicitCredentialsAWSClientProvider(self.credentials.model_copy(update={"aws_default_region": region}))

    def _client_kwargs(self) -> dict[str, Any]:
        return {
            "aws_access_key_id": self.credentials.aws_access_key_id,
            "aws_secret_access_key": self.credentials.aws_secret_access_key,
            "aws_session_token": self.credentials.aws_session_token,
            "region_name": self.credentials.aws_default_region,
        }


@dataclass(frozen=True)
class DefaultChainAWSClientProvider(AWSClientProvider):
    """Construct AWS clients through the SDK default credential chain."""

    credential_source: ClassVar[Literal["access_key", "local", "managed"]] = "managed"
    region: str

    def with_region(self, region: str) -> AWSClientProvider:
        return DefaultChainAWSClientProvider(region)

    def _client_kwargs(self) -> dict[str, Any]:
        return {"region_name": self.region}

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return min(requested_seconds, _DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS)


@dataclass(frozen=True)
class LocalChainAWSClientProvider(DefaultChainAWSClientProvider):
    """Use local SDK credentials without deployment-managed storage authority."""

    credential_source: ClassVar[Literal["access_key", "local", "managed"]] = "local"

    def with_region(self, region: str) -> AWSClientProvider:
        return LocalChainAWSClientProvider(region)
