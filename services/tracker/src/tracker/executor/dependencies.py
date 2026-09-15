"""Dependencies scoped to one executor dispatch."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from benchmark_service.client import BenchmarkServiceClient
from taskiq_dependencies import Depends

from tracker.aws.services import CloudRuntimeConfig, CloudRuntimeServices
from tracker.database.models import Benchmark, Org
from tracker.types import StartBenchmarkRequest


@asynccontextmanager
async def get_execution_runtime(
    request: StartBenchmarkRequest = Depends(),
    benchmark: Benchmark = Depends(),
    org: Org = Depends(),
) -> AsyncGenerator[CloudRuntimeServices, None]:
    async with CloudRuntimeConfig.create_execution_runtime(request, org.id, benchmark.id) as runtime:
        yield runtime


@asynccontextmanager
async def get_benchmark_service(
    request: StartBenchmarkRequest = Depends(),
) -> AsyncGenerator[BenchmarkServiceClient, None]:
    async with request.benchmark_service as service:
        yield service
