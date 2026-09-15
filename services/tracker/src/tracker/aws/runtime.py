"""AWS resources and authentication selected for one tracker operation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

from tracker.aws.clients import AWSClientProvider, ExplicitCredentialsAWSClientProvider

if TYPE_CHECKING:
    from tracker.types import HarnessConfig
    from tracker.runtime.services import RuntimeServices


@dataclass(frozen=True)
class AWSResources:
    region: str
    s3_bucket: str
    log_group: str
    log_retention_days: int


@dataclass(frozen=True)
class AWSRuntime:
    resources: AWSResources
    clients: AWSClientProvider

    @classmethod
    def from_harness_config(cls, harness_config: HarnessConfig) -> AWSRuntime:
        """Convert access-key request configuration into an internal runtime."""
        return cls(
            resources=AWSResources(
                region=harness_config.aws.aws_default_region,
                s3_bucket=harness_config.s3_bucket,
                log_group=harness_config.log_group,
                log_retention_days=harness_config.log_retention_policy,
            ),
            clients=ExplicitCredentialsAWSClientProvider(harness_config.aws),
        )


class CloudRuntimeConfig(BaseModel):
    """Non-secret configuration for services using resolved AWS authority."""

    environment: Literal["aws"] = "aws"
    properties: AWSResources

    @asynccontextmanager
    async def create_runtime(
        self,
        *,
        clients: AWSClientProvider,
        sandbox_provider: str = "daytona",
        sandbox_provider_secret_name: str | None = None,
    ) -> AsyncGenerator[RuntimeServices]:
        """Compose existing AWS adapters without resolving credentials again."""
        from tracker.aws.cloudwatch_logs import (
            CloudWatchBenchmarkLogLocations,
            CloudWatchBenchmarkLogSink,
            CloudWatchLogProvider,
        )
        from tracker.aws.s3 import S3ArtifactLocations, S3ObjectStore
        from tracker.aws.secrets import SecretsManagerStore
        from tracker.runtime.services import RuntimeServices

        runtime = AWSRuntime(resources=self.properties, clients=clients)
        secrets = SecretsManagerStore(clients)

        services = RuntimeServices(
            objects=S3ObjectStore(runtime),
            secrets=secrets,
            async_secrets=secrets,
            logs=CloudWatchBenchmarkLogSink(clients, self.properties.log_group),
            log_reader=CloudWatchLogProvider(clients, self.properties.log_group),
            log_locations=CloudWatchBenchmarkLogLocations(self.properties),
            artifacts=S3ArtifactLocations(self.properties),
            sandbox_provider=sandbox_provider,
            sandbox_provider_secret_name=sandbox_provider_secret_name,
        )

        try:
            yield services
        finally:
            await services.close()
