import os

import pytest
from daytona import AsyncDaytona, DaytonaConfig
from dotenv import load_dotenv

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import *  # noqa: F403

_ = load_dotenv()


@pytest.fixture(scope="session")
def benchmark_service() -> BenchmarkService:
    service_ip = os.getenv("SWEBENCH_SERVICE_IP")
    if not service_ip:
        raise ValueError("SWEBENCH_SERVICE_IP is not set")

    return BenchmarkService(name="swebench", url=f"http://{service_ip}:8000")


@pytest.fixture
def daytona_client(benchmark_service: BenchmarkService) -> AsyncDaytona:
    return AsyncDaytona(
        config=DaytonaConfig(
            api_key=benchmark_service.environment_keys["DAYTONA_API_KEY"],
            api_url=benchmark_service.environment_keys["DAYTONA_API_URL"],
            target=benchmark_service.environment_keys["DAYTONA_TARGET"],
        )
    )
