"""Dependencies scoped to one executor dispatch."""

from asyncio import to_thread
from contextlib import AsyncExitStack, closing

from tracker.aws.runtime import AWSResources
from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, Org
from tracker.exceptions import SecretsError, TrackerServiceError
from tracker.local.resources import LocalResources
from tracker.local.runtime import LocalRuntimeFactory
from tracker.local.secrets import InMemorySecretStore, load_execution_secrets
from tracker.runtime.services import RuntimeServices
from tracker.types import StartBenchmarkRequest


async def get_execution_runtime(
    request: StartBenchmarkRequest,
    benchmark: Benchmark,
    org: Org,
    *,
    context_version: int | None = None,
    runtime_stack: AsyncExitStack,
) -> RuntimeServices:
    if request.environment != benchmark.arguments.environment:
        raise SecretsError("Queued runtime environment does not match the saved run")
    properties = benchmark.arguments.properties
    if request.environment == "local":
        if not isinstance(properties, LocalResources):
            raise SecretsError("Saved local run has no filesystem resource configuration")
        values = await to_thread(load_execution_secrets, properties.secrets_file, request.contract.secrets)
        try:
            secrets = runtime_stack.enter_context(closing(InMemorySecretStore(request.contract.secrets, values)))
            runtime = LocalRuntimeFactory.create_runtime(properties.data_root, org.id, secrets=secrets)
        finally:
            values.clear()
        await runtime.prepare_execution(request, benchmark.id)
        return runtime

    assert properties is None or isinstance(properties, AWSResources)
    assert request.properties is None or isinstance(request.properties, AWSResources)
    stored = properties
    queued = request.properties
    if context_version == 3 and stored is None:
        raise TrackerServiceError("Managed execution has no saved AWS resources")

    if stored is not None and (queued is not None or context_version == 3) and queued != stored:
        raise TrackerServiceError("Queued AWS resources differ from the saved run")

    if context_version == 2:
        resources = stored or queued
        if resources is not None and resources.s3_bucket.startswith(("vs-dev-", "vs-prod-")):
            raise TrackerServiceError("Protocol 2 cannot execute owner storage")

    return await CloudRuntimeFactory.create_execution_runtime(
        request, org.id, benchmark.id, properties=stored, context_version=context_version
    )
