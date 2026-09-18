"""Compose AWS-backed runtime services."""

from asyncio import to_thread
from dataclasses import dataclass
from uuid import UUID

from botocore.config import Config

from tracker._lambda import dry_run_lambda, invoke_lambda
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

    async def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Prepare logs before sandbox work."""
        await to_thread(
            self.logs.create_benchmark, str(benchmark_id), retention_days=self.aws_runtime.resources.log_retention_days
        )
        if self.aws_runtime.clients.credential_source != "managed":
            return

        await resolve_secrets(request.contract.secrets, self.secrets)
        if request.webhook_secret_name and request.webhook_intervals:
            await self.secrets.get(request.webhook_secret_name)
        if request.lambda_function:
            await to_thread(dry_run_lambda, self.aws_runtime.clients, request.lambda_function)

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


class CloudRuntimeFactory:
    """Compose AWS services from resolved resources and credentials."""

    @staticmethod
    def create_runtime(
        runtime: AWSRuntime,
        *,
        sandbox_provider: str = "daytona",
        sandbox_provider_secret_name: str | None = None,
    ) -> RuntimeServices:
        """Compose existing AWS adapters without resolving credentials again."""
        clients = runtime.clients
        resources = runtime.resources
        secrets = SecretsManagerStore(clients)

        return CloudRuntimeServices(
            aws_runtime=runtime,
            objects=S3ObjectStore(runtime),
            secrets=secrets,
            logs=CloudWatchBenchmarkLogSink(clients, resources.log_group),
            log_reader=CloudWatchLogProvider(clients, resources.log_group),
            log_locations=CloudWatchBenchmarkLogLocations(resources),
            artifacts=S3ArtifactLocations(resources),
            sandbox_provider=sandbox_provider,
            sandbox_provider_secret_name=sandbox_provider_secret_name,
        )

    @classmethod
    async def create_execution_runtime(
        cls,
        request: StartBenchmarkRequest,
        org_id: UUID,
        benchmark_id: UUID,
        *,
        properties: AWSResources | None = None,
    ) -> RuntimeServices:
        """Select AWS access and prepare services for one dispatch."""
        assert request.properties is None or isinstance(request.properties, AWSResources)
        properties = request.properties or properties
        aws_runtime = (
            deployment_aws_runtime(org_id, properties)
            if request.harness_config is None
            else AWSRuntime.from_harness_config(request.harness_config).with_resources(properties)
        )
        runtime = cls.create_runtime(
            aws_runtime,
            sandbox_provider=request.sandbox_provider,
            sandbox_provider_secret_name=request.sandbox_provider_secret_reference,
        )
        await runtime.prepare_execution(request, benchmark_id)
        return runtime
