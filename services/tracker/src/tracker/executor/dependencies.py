"""Dependencies scoped to one executor dispatch."""

from asyncio import to_thread
from contextlib import AsyncExitStack

from tracker.aws.runtime import AWSResources
from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, Org
from tracker.exceptions import SecretsError
from tracker.local.resources import LocalResources
from tracker.local.runtime import LocalRuntimeFactory
from tracker.local.secrets import load_execution_secrets
from tracker.runtime.services import RuntimeServices
from tracker.types import StartBenchmarkRequest


async def get_execution_runtime(
    request: StartBenchmarkRequest,
    benchmark: Benchmark,
    org: Org,
    *,
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
            runtime = runtime_stack.enter_context(
                LocalRuntimeFactory.open(
                    properties.data_root, org.id, secret_references=request.contract.secrets, execution_secrets=values
                )
            )
        finally:
            values.clear()
        await runtime.prepare_execution(request, benchmark.id)
        return runtime
    assert properties is None or isinstance(properties, AWSResources)
    return await CloudRuntimeFactory.create_execution_runtime(request, org.id, benchmark.id, properties=properties)
