"""AWS client providers for tracker runtimes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, cast

import aioboto3
import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from tracker.types import AWSCredentials

_HIGH_CONCURRENCY_CLIENT_CONFIG = Config(max_pool_connections=200)
_S3_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})
_DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS = 3600


def _session_client(session: boto3.Session, service_name: str, **kwargs: Any) -> Any:
    client_factory = cast(Any, session.client)  # pyright: ignore[reportUnknownMemberType]
    return client_factory(service_name, **kwargs)


class AWSClientProvider(ABC):
    """Construct AWS service clients for one authentication source."""

    @abstractmethod
    def _client_kwargs(self) -> dict[str, Any]:
        """Return SDK arguments for this credential source."""
        raise NotImplementedError

    @lru_cache(maxsize=32)
    def _s3_session(self) -> aioboto3.Session:
        return aioboto3.Session(**self._client_kwargs())

    # profile_name is a Session argument the client() call does not accept, so
    # synchronous clients must be built from a Session rather than boto3.client().
    @lru_cache(maxsize=32)
    def _sync_session(self) -> boto3.Session:
        return boto3.Session(**self._client_kwargs())

    def s3_client(self) -> Any:
        return self._s3_session().client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            config=_S3_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def cloudwatch_logs_client(self) -> Any:
        return _session_client(self._sync_session(), "logs", config=_HIGH_CONCURRENCY_CLIENT_CONFIG)

    @lru_cache(maxsize=32)
    def secretsmanager_client(self) -> Any:
        return _session_client(self._sync_session(), "secretsmanager")

    @lru_cache(maxsize=32)
    def lambda_client(self, config: Config | None = None) -> Any:
        return _session_client(self._sync_session(), "lambda", config=config)

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return requested_seconds


@dataclass(frozen=True)
class ExplicitCredentialsAWSClientProvider(AWSClientProvider):
    """Construct AWS clients from caller-supplied credentials."""

    credentials: AWSCredentials = field(repr=False)

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

    region: str
    profile_name: str | None = None

    def _client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"region_name": self.region}
        if self.profile_name:
            kwargs["profile_name"] = self.profile_name

        return kwargs

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return min(requested_seconds, _DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS)
