"""Compose AWS-backed runtime services."""

from asyncio import to_thread
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from uuid import UUID
from contextlib import asynccontextmanager
from typing import Literal

from pydantic import BaseModel

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.resolver import deployment_aws_runtime
from tracker._lambda import dry_run_lambda
from tracker.types import StartBenchmarkRequest
from tracker.aws.cloudwatch_logs import (
    CloudWatchBenchmarkLogLocations,
    CloudWatchBenchmarkLogSink,
    CloudWatchLogProvider,
)
from tracker.aws.s3 import S3ArtifactLocations, S3ObjectStore
from tracker.aws.secrets import SecretsManagerStore
from tracker.runtime.services import RuntimeServices


@dataclass(kw_only=True)
class CloudRuntimeServices(RuntimeServices):
    """AWS execution setup behind the shared runtime interface."""

    aws_runtime: AWSRuntime

    async def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Prepare logs and verify managed credentials before sandbox work."""
        await to_thread(
            self.logs.create_benchmark,
            str(benchmark_id),
            retention_days=self.aws_runtime.resources.log_retention_days,
        )
        if request.harness_config is not None:
            return

        await self.get_sandbox_provider_config()
        await self.resolve_secrets(request.contract.secrets)
        if request.webhook_secret_name and request.webhook_intervals:
            await self.async_secrets.get_async(request.webhook_secret_name)
        if request.lambda_function:
            await to_thread(dry_run_lambda, self.aws_runtime.clients, request.lambda_function)


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
    ) -> AsyncGenerator[CloudRuntimeServices]:
        """Compose existing AWS adapters without resolving credentials again."""
        runtime = AWSRuntime(resources=self.properties, clients=clients)
        secrets = SecretsManagerStore(clients)

        services = CloudRuntimeServices(
            aws_runtime=runtime,
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

    @classmethod
    @asynccontextmanager
    async def create_execution_runtime(
        cls,
        request: StartBenchmarkRequest,
        org_id: UUID,
        benchmark_id: UUID,
    ) -> AsyncGenerator[CloudRuntimeServices]:
        """Select AWS access and keep execution services alive for one dispatch."""
        aws_runtime = (
            deployment_aws_runtime(org_id)
            if request.harness_config is None
            else AWSRuntime.from_harness_config(request.harness_config)
        )
        config = cls(properties=aws_runtime.resources)

        async with config.create_runtime(
            clients=aws_runtime.clients,
            sandbox_provider=request.sandbox_provider,
            sandbox_provider_secret_name=request.sandbox_provider_secret_reference,
        ) as runtime:
            await runtime.prepare_execution(request, benchmark_id)
            yield runtime
