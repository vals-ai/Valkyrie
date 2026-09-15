"""Compose AWS-backed runtime services."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Literal

from pydantic import BaseModel

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.cloudwatch_logs import (
    CloudWatchBenchmarkLogLocations,
    CloudWatchBenchmarkLogSink,
    CloudWatchLogProvider,
)
from tracker.aws.s3 import S3ArtifactLocations, S3ObjectStore
from tracker.aws.secrets import SecretsManagerStore
from tracker.runtime.services import RuntimeServices


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
