"""Dependencies scoped to one executor dispatch."""

from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import Benchmark, Org
from tracker.types import StartBenchmarkRequest


async def get_execution_runtime(
    request: StartBenchmarkRequest,
    benchmark: Benchmark,
    org: Org,
) -> RuntimeServices:
    return await CloudRuntimeFactory.create_execution_runtime(
        request, org.id, benchmark.id, properties=benchmark.arguments.properties
    )
