"""AWS client providers for tracker runtimes."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, ClassVar, Literal, cast

import aioboto3
import boto3
from botocore.config import Config

_HIGH_CONCURRENCY_CLIENT_CONFIG = Config(max_pool_connections=200)
_S3_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})
_DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS = 3600


class AWSClientProvider(ABC):
    """Construct AWS service clients for one authentication source."""

    credential_source: ClassVar[Literal["local", "managed"]]

    @abstractmethod
    def with_region(self, region: str) -> "AWSClientProvider":
        """Select a resource region while retaining this authentication source."""
        raise NotImplementedError

    @abstractmethod
    def _client_kwargs(self) -> dict[str, Any]:
        """Return SDK arguments for this credential source."""
        raise NotImplementedError

    @lru_cache(maxsize=32)
    def _loop_session(self, loop: asyncio.AbstractEventLoop) -> aioboto3.Session:
        """Share one session per event loop so loop-bound credential state never crosses loops."""
        return aioboto3.Session(**self._client_kwargs())

    def _s3_session(self) -> aioboto3.Session:
        return self._loop_session(asyncio.get_running_loop())

    def s3_client(self) -> Any:
        return self._s3_session().client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            config=_S3_CLIENT_CONFIG,
        )

    @lru_cache(maxsize=32)
    def cloudwatch_logs_client(self) -> Any:
        client_factory = cast(Any, boto3.client)  # pyright: ignore[reportUnknownMemberType]
        return client_factory(
            "logs",
            config=_HIGH_CONCURRENCY_CLIENT_CONFIG,
            **self._client_kwargs(),
        )

    def secretsmanager_async_client(self) -> Any:
        return self._s3_session().client("secretsmanager")  # pyright: ignore[reportUnknownMemberType]

    def cloudwatch_logs_async_client(self) -> Any:
        return cast(Any, self._s3_session().client("logs"))  # pyright: ignore[reportUnknownMemberType]

    def lambda_client(self, config: Config | None = None) -> Any:
        return cast(Any, self._s3_session().client("lambda", config=config))  # pyright: ignore[reportUnknownMemberType]

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return requested_seconds


@dataclass(frozen=True)
class DefaultChainAWSClientProvider(AWSClientProvider):
    """Construct AWS clients through the SDK default credential chain."""

    credential_source: ClassVar[Literal["local", "managed"]] = "managed"
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

    credential_source: ClassVar[Literal["local", "managed"]] = "local"

    def with_region(self, region: str) -> AWSClientProvider:
        return LocalChainAWSClientProvider(region)
