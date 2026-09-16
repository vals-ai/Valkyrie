"""Compose AWS-backed runtime services."""

from asyncio import to_thread
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, ClassVar, Literal
from uuid import UUID

from botocore.config import Config
from pydantic import BaseModel, Field, TypeAdapter

from tracker._lambda import dry_run_lambda, invoke_lambda
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
from tracker.types import FinalViewResponse, StartBenchmarkRequest


# Limit non-idempotent completion callbacks to one attempt and a 60-second read.
_COMPLETION_CALLBACK_CONFIG = Config(read_timeout=60, retries={"total_max_attempts": 1})


@dataclass(kw_only=True)
class CloudRuntimeServices(RuntimeServices):
    """AWS execution setup behind the shared runtime interface."""

    aws_runtime: AWSRuntime

    def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Prepare logs before sandbox work."""
        self.logs.create_benchmark(str(benchmark_id), retention_days=self.aws_runtime.resources.log_retention_days)

    async def run_completion_callback(self, final_view: FinalViewResponse) -> None:
        arguments = final_view.benchmark_arguments
        if not arguments.lambda_function:
            return

        payload = arguments.model_dump()
        payload["benchmark_id"] = str(final_view.benchmark_id)
        payload["benchmark_name"] = final_view.benchmark_name
        payload["bucket"] = self.aws_runtime.resources.s3_bucket
        await to_thread(
            invoke_lambda,
            self.aws_runtime.clients,
            arguments.lambda_function,
            payload,
            config=_COMPLETION_CALLBACK_CONFIG,
        )


class ManagedCloudRuntimeServices(CloudRuntimeServices):
    """Verify deployment AWS access before starting managed execution."""

    def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        super().prepare_execution(request, benchmark_id)

        resolve_secrets(request.contract.secrets, self.secrets)
        if request.webhook_secret_name and request.webhook_intervals:
            self.secrets.get(request.webhook_secret_name)
        if request.lambda_function:
            dry_run_lambda(self.aws_runtime.clients, request.lambda_function)


class CloudRuntimeConfig(BaseModel):
    """Non-secret configuration for AWS runtime services."""

    environment: Literal["aws"] = "aws"
    properties: AWSResources
    services_type: ClassVar[type[CloudRuntimeServices]]

    @staticmethod
    def from_aws_runtime(runtime: AWSRuntime) -> "CloudRuntimeConfig":
        return _RUNTIME_CONFIG_ADAPTER.validate_python(
            {"credential_source": runtime.clients.credential_source, "properties": runtime.resources}
        )

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

        services = self.services_type(
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
    ) -> AsyncGenerator[RuntimeServices]:
        """Select AWS access and keep execution services alive for one dispatch."""
        aws_runtime = (
            deployment_aws_runtime(org_id)
            if request.harness_config is None
            else AWSRuntime.from_harness_config(request.harness_config)
        )
        config = cls.from_aws_runtime(aws_runtime)

        async with config.create_runtime(
            clients=aws_runtime.clients,
            sandbox_provider=request.sandbox_provider,
            sandbox_provider_secret_name=request.sandbox_provider_secret_reference,
        ) as runtime:
            await to_thread(runtime.prepare_execution, request, benchmark_id)
            yield runtime


class AccessKeyRuntimeConfig(CloudRuntimeConfig):
    credential_source: Literal["access_key"] = "access_key"
    services_type: ClassVar[type[CloudRuntimeServices]] = CloudRuntimeServices


class ManagedRuntimeConfig(CloudRuntimeConfig):
    credential_source: Literal["managed"] = "managed"
    services_type: ClassVar[type[CloudRuntimeServices]] = ManagedCloudRuntimeServices


RuntimeConfig = Annotated[AccessKeyRuntimeConfig | ManagedRuntimeConfig, Field(discriminator="credential_source")]
_RUNTIME_CONFIG_ADAPTER: TypeAdapter[RuntimeConfig] = TypeAdapter(RuntimeConfig)
