import json
import os
from collections.abc import AsyncGenerator, Callable
from typing import Any

import httpx
import websockets
from daytona import AsyncDaytona, DaytonaConfig
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from tracker.database.models import Benchmark, BenchmarkArguments
from tracker.exceptions import BenchmarkServiceError
from tracker.logger import get_logger
from tracker.types import (
    EvaluateInstanceRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskRequest,
    SetupTaskResponse,
    StartBenchmarkRequest,
    VerifyTaskIdsResponse,
)

logger = get_logger(__name__)


class BenchmarkService:
    _name: str
    _url: str
    _environment_keys: dict[str, str]
    _daytona_client: AsyncDaytona | None = None
    _timeout: int = 60

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
        if self._daytona_client:
            return self._daytona_client

        config = DaytonaConfig(
            api_key=self.environment_keys["DAYTONA_API_KEY"],
            api_url=self.environment_keys["DAYTONA_API_URL"],
            target=self.environment_keys["DAYTONA_TARGET"],
        )

        self._daytona_client = AsyncDaytona(config=config)

        return self._daytona_client

    @property
    def ws_url(self) -> str:
        return self._url.replace("http://", "ws://").replace("https://", "wss://")

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "x-api-key": self._environment_keys["DAYTONA_API_KEY"],
            "x-api-url": self._environment_keys["DAYTONA_API_URL"],
            "x-target": self._environment_keys["DAYTONA_TARGET"],
        }

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
    def start_benchmark_request_to_benchmark_object(request: StartBenchmarkRequest) -> Benchmark:
        return Benchmark(
            name=request.benchmark_name,
            arguments=BenchmarkArguments(
                contract=request.contract,
                concurrency=request.concurrency,
                task_ids=request.task_ids,
                slice_str=request.slice_str,
            ),
        )

    async def _return_websocket_result(self, websocket: ClientConnection) -> AsyncGenerator[str | dict[str, Any]]:
        """
        Returns the result from a websock connection, logs and handles other types of responses returned

        Yields:
            str | dict[str, Any]: The message that we can log or the result object that we can return
        """
        try:
            async for message in websocket:
                parsed_message: dict[str, Any] = json.loads(message)
                data: str | dict[str, Any] = parsed_message["data"]
                if parsed_message["type"] == "error":
                    raise BenchmarkServiceError(data)

                if not data:
                    continue

                yield data
        except ConnectionClosed:
            pass

    async def request_health_check(self) -> HealthCheckResponse:
        """
        Requests health check from benchmark service
        """
        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
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

        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
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
        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
            response = await client.get(f"{self._url}/retrieve-task/", params=params)

        logger.debug(f"Retrieve task response: {response.text}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve task failed with status code {response.status_code}, response: {response.text}"
            )

        return RetrieveTaskResponse.model_validate(response.json())

    async def request_setup_task(
        self, task_id: str, instance_id: str, on_message: Callable[[str], None] | None = None
    ) -> SetupTaskResponse:
        """
        Requests setup task from benchmark service
        """

        request = SetupTaskRequest(
            task_id=task_id,
            instance_id=instance_id,
        )

        async with websockets.connect(
            f"{self.ws_url}/ws/setup-task",
            additional_headers=self._headers,
            open_timeout=60,
        ) as websocket:
            await websocket.send(request.model_dump_json())

            async for data in self._return_websocket_result(websocket):
                if isinstance(data, dict):
                    return SetupTaskResponse.model_validate(data)

                if on_message:
                    on_message(data)

        raise BenchmarkServiceError("Exited websocket without returning final result")

    async def request_evaluate_instance(
        self, task_id: str, instance_id: str, on_message: Callable[[str], None] | None = None
    ) -> dict[str, str]:
        """
        Requests evaluate instance from benchmark service
        """

        request = EvaluateInstanceRequest(
            task_id=task_id,
            instance_id=instance_id,
        )

        async with websockets.connect(
            f"{self.ws_url}/ws/evaluate-instance",
            additional_headers=self._headers,
            open_timeout=60,
        ) as websocket:
            await websocket.send(request.model_dump_json())

            async for data in self._return_websocket_result(websocket):
                if isinstance(data, dict):
                    return data

                if on_message:
                    on_message(data)

        raise BenchmarkServiceError("Exited websocket without returning final result")

    async def request_final_score(self, evaluation_results: dict[str, dict[str, Any] | None]) -> FinalScoreResponse:
        """
        Requests final score from benchmark service
        """
        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
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
