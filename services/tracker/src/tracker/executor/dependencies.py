"""Dependencies scoped to one executor dispatch."""

from asyncio import to_thread

from tracker.aws.runtime import AWSResources
from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, Org
from tracker.exceptions import SecretsError, TrackerServiceError
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
) -> RuntimeServices:
    arguments = benchmark.arguments
    if request.environment != arguments.environment:
        raise SecretsError("Queued runtime environment does not match the saved run")
    if arguments.environment == "local":
        properties = arguments.properties
        values = await to_thread(load_execution_secrets, properties.secrets_file, request.contract.secrets)
        secrets = InMemorySecretStore(request.contract.secrets, values)
        runtime = LocalRuntimeFactory.create_runtime(properties.data_root, org.id, secrets=secrets)
        await runtime.prepare_execution(request, benchmark.id)
        return runtime

    assert request.properties is None or isinstance(request.properties, AWSResources)
    stored = arguments.properties
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
