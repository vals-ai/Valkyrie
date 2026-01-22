import os
from typing import Any

import httpx
from daytona import AsyncDaytona, DaytonaConfig

from tracker.database.models import Benchmark
from tracker.exceptions import BenchmarkServiceError
from tracker.logger import get_logger
from tracker.types import (
    BenchmarkArguments,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskResponse,
    StartRunRequest,
    VerifyTaskIdsResponse,
)

logger = get_logger(__name__)


class BenchmarkService:
    _name: str
    _url: str
    _environment_keys: dict[str, str]

    def __init__(self, name: str, url: str):
        self._name = name
        self._url = url
        self._environment_keys = self.daytona_keys()

    @property
    def name(self) -> str:
        return self._name

    @property
    def environment_keys(self) -> dict[str, str]:
        return self._environment_keys

    @property
    def daytona_client(self) -> AsyncDaytona:
        config = DaytonaConfig(
            api_key=self.environment_keys["DAYTONA_API_KEY"],
            api_url=self.environment_keys["DAYTONA_API_URL"],
            target=self.environment_keys["DAYTONA_TARGET"],
        )
        return AsyncDaytona(config=config)

    @staticmethod
    def daytona_keys() -> dict[str, str]:
        environment_keys: dict[str, str] = {
            "DAYTONA_API_KEY": os.getenv("DAYTONA_API_KEY") or "",
            "DAYTONA_API_URL": os.getenv("DAYTONA_API_URL") or "",
            "DAYTONA_TARGET": os.getenv("DAYTONA_TARGET") or "",
        }

        missing_keys: list[str] = []
        for key, value in environment_keys.items():
            if not value:
                missing_keys.append(key)

        if missing_keys:
            raise ValueError(
                f"The following environment variables are not set: {', '.join(missing_keys)}. Please set them in your `.env` file so that they can be sourced."
            )

        return environment_keys

    @staticmethod
    def start_run_request_to_benchmark_object(request: StartRunRequest) -> Benchmark:
        return Benchmark(
            name=request.benchmark_name,
            arguments=BenchmarkArguments(
                contract=request.contract,
                concurrency=request.concurrency,
                task_ids=request.task_ids,
                slice_str=request.slice_str,
            ),
        )

    async def request_health_check(self) -> HealthCheckResponse:
        """
        Requests health check from benchmark service
        """
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            response = await client.get(f"{self._url}/health")

        logger.debug(f"Health check response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Health check failed with status code {response.status_code}, response: {response.text}"
            )

        return HealthCheckResponse.model_validate(response.json())

    async def request_verify_task_ids(self, task_ids: list[str] | None, slice_str: str | None) -> VerifyTaskIdsResponse:
        """
        Requests verify task ids from benchmark service
        """

        params: dict[str, list[str] | str] = {}
        if task_ids is not None:
            params["task_ids"] = task_ids

        if slice_str is not None:
            params["slice"] = slice_str

        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            response = await client.get(f"{self._url}/verify-task-ids/", params=params)

        logger.debug(f"Verify task ids response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Verify task ids failed with status code {response.status_code}, response: {response.text}"
            )

        return VerifyTaskIdsResponse.model_validate(response.json())

    async def request_retrieve_task(self, task_id: str, skip_validation: bool = False) -> RetrieveTaskResponse:
        """
        Requests retrieve task from benchmark service for a single task
        """

        params = {"task_id": task_id, "skip_validation": skip_validation}
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            response = await client.get(f"{self._url}/retrieve-task/", params=params)

        logger.debug(f"Retrieve task response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve task failed with status code {response.status_code}, response: {response.text}"
            )

        return RetrieveTaskResponse.model_validate(response.json())

    async def request_setup_task(self, task_id: str, instance_id: str) -> SetupTaskResponse:
        """
        Requests setup task from benchmark service
        """

        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            response = await client.post(
                f"{self._url}/setup-task",
                json={"task_id": task_id, "instance_id": instance_id},
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": self._environment_keys["DAYTONA_API_KEY"],
                    "X-Api-Url": self._environment_keys["DAYTONA_API_URL"],
                    "X-Target": self._environment_keys["DAYTONA_TARGET"],
                },
            )

        logger.debug(f"Setup task response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Setup task failed with status code {response.status_code}, response: {response.text}"
            )

        return SetupTaskResponse.model_validate(response.json())

    async def request_evaluate_instance(self, task_id: str, instance_id: str) -> dict[str, str]:
        """
        Requests evaluate instance from benchmark service
        """

        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            response = await client.post(
                f"{self._url}/evaluate-instance/",
                json={"task_id": task_id, "instance_id": instance_id},
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": self._environment_keys["DAYTONA_API_KEY"],
                    "X-Api-Url": self._environment_keys["DAYTONA_API_URL"],
                    "X-Target": self._environment_keys["DAYTONA_TARGET"],
                },
            )

        logger.debug(f"Evaluate instance response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Evaluate instance failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json()

    async def request_final_score(self, evaluation_results: dict[str, dict[str, Any] | None]) -> FinalScoreResponse:
        """
        Requests final score from benchmark service
        """
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            response = await client.post(
                f"{self._url}/final-score/",
                json={"evaluation_results": evaluation_results},
                headers={"Content-Type": "application/json"},
            )

        logger.debug(f"Final score response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Final score failed with status code {response.status_code}, response: {response.text}"
            )

        return FinalScoreResponse.model_validate(response.json())
