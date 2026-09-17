"""Dependencies scoped to one executor dispatch."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import Benchmark, Org
from tracker.types import StartBenchmarkRequest


@asynccontextmanager
async def get_execution_runtime(
    request: StartBenchmarkRequest,
    benchmark: Benchmark,
    org: Org,
) -> AsyncGenerator[RuntimeServices, None]:
    async with CloudRuntimeFactory.create_execution_runtime(
        request, org.id, benchmark.id, properties=benchmark.arguments.properties
    ) as runtime:
        yield runtime
