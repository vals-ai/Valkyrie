"""Compose AWS-backed runtime services."""

from asyncio import to_thread
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from tracker._lambda import dry_run_lambda
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import (
    CloudWatchBenchmarkLogLocations,
    CloudWatchBenchmarkLogSink,
    CloudWatchLogProvider,
)
from tracker.aws.resolver import deployment_aws_runtime
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.s3 import S3ArtifactLocations, S3ObjectStore
from tracker.aws.secrets import SecretsManagerStore
from tracker.runtime.services import RuntimeServices
from tracker.runtime.secrets import resolve_secrets
from tracker.types import StartBenchmarkRequest


@dataclass(kw_only=True)
class CloudRuntimeServices(RuntimeServices):
    """AWS execution setup behind the shared runtime interface."""

    aws_runtime: AWSRuntime

    def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Run synchronous AWS preflight checks together, before sandbox work."""
        self.logs.create_benchmark(str(benchmark_id), retention_days=self.aws_runtime.resources.log_retention_days)
        if request.harness_config is not None:
            return

        resolve_secrets(request.contract.secrets, self.secrets)
        if request.webhook_secret_name and request.webhook_intervals:
            self.secrets.get(request.webhook_secret_name)
        if request.lambda_function:
            dry_run_lambda(self.aws_runtime.clients, request.lambda_function)


class CloudRuntimeConfig(BaseModel):
    """Non-secret configuration for AWS runtime services."""

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
        *,
        properties: AWSResources | None = None,
    ) -> AsyncGenerator[CloudRuntimeServices]:
        """Select AWS access and keep execution services alive for one dispatch."""
        properties = request.properties or properties
        aws_runtime = (
            deployment_aws_runtime(org_id, properties)
            if request.harness_config is None
            else AWSRuntime.from_harness_config(request.harness_config).with_resources(properties)
        )
        config = cls(properties=aws_runtime.resources)

        async with config.create_runtime(
            clients=aws_runtime.clients,
            sandbox_provider=request.sandbox_provider,
            sandbox_provider_secret_name=request.sandbox_provider_secret_reference,
        ) as runtime:
            await to_thread(runtime.prepare_execution, request, benchmark_id)
            yield runtime
