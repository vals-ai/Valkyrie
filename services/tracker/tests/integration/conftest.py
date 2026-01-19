import os
from collections.abc import AsyncGenerator

import pytest
from daytona import AsyncDaytona, DaytonaConfig
from dotenv import load_dotenv

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import *  # noqa: F403 # type: ignore[attr-defined]

_ = load_dotenv()


@pytest.fixture(scope="function")
async def benchmark_service() -> AsyncGenerator[BenchmarkService, None]:
    service_ip = os.getenv("BENCHMARK_SERVICE_URL")
    if not service_ip:
        raise ValueError("BENCHMARK_SERVICE_URL is not set")

    service = BenchmarkService(name="swebench", url=service_ip)

    yield service

    await service.daytona_client.close()


@pytest.fixture
async def daytona_client(benchmark_service: BenchmarkService) -> AsyncGenerator[AsyncDaytona, None]:
    daytona_config = DaytonaConfig(
        api_key=benchmark_service.environment_keys["DAYTONA_API_KEY"],
        api_url=benchmark_service.environment_keys["DAYTONA_API_URL"],
        target=benchmark_service.environment_keys["DAYTONA_TARGET"],
    )

    async with AsyncDaytona(config=daytona_config) as client:
        yield client
