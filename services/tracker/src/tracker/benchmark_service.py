import logging
import os
from typing import Any

import httpx
from daytona import AsyncDaytona, DaytonaConfig

from tracker.exceptions import BenchmarkServiceError

logger = logging.getLogger(__name__)


class BenchmarkService:
    _name: str
    _url: str
    _environment_keys: dict[str, str]
    _daytona_client: AsyncDaytona

    def __init__(self, name: str, url: str):
        self._name = name
        self._url = url
        self._environment_keys = self.daytona_keys()
        self._daytona_client = self.create_daytona_client()

    @property
    def name(self) -> str:
        return self._name

    @property
    def environment_keys(self) -> dict[str, str]:
        return self._environment_keys

    @property
    def daytona_client(self) -> AsyncDaytona:
        return self._daytona_client

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

    def create_daytona_client(self) -> AsyncDaytona:
        return AsyncDaytona(
            config=DaytonaConfig(
                api_key=self.environment_keys["DAYTONA_API_KEY"],
                api_url=self.environment_keys["DAYTONA_API_URL"],
                target=self.environment_keys["DAYTONA_TARGET"],
            )
        )

    async def request_health_check(self) -> dict[str, str]:
        """
        Requests health check from benchmark service
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/health")

        logger.debug(f"Health check response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Health check failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json()

    async def request_verify_task_ids(self, task_ids: list[str] | None) -> list[str]:
        """
        Requests verify task ids from benchmark service
        """

        params: dict[str, list[str]] = {}
        if task_ids is not None:
            params["task_ids"] = task_ids

        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/verify-task-ids", params=params)

        logger.debug(f"Verify task ids response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Verify task ids failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json().get("task_ids", [])

    async def request_retrieve_tasks(
        self, task_ids: list[str], skip_validation: bool = False
    ) -> dict[str, dict[str, str]]:
        """
        Requests retrieve tasks from benchmark service
        """

        params = {"task_ids": task_ids, "skip_validation": skip_validation}
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/retrieve-tasks", params=params)

        logger.debug(f"Retrieve tasks response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve tasks failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json()

    async def request_setup_task(self, task_id: str, instance_id: str) -> dict[str, str]:
        """
        Requests setup task from benchmark service
        """

        async with httpx.AsyncClient() as client:
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

        logger.debug(f"Setup task response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Setup task failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json()

    async def request_evaluate_instance(self, task_id: str, instance_id: str) -> dict[str, str]:
        """
        Requests evaluate instance from benchmark service
        """

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._url}/evaluate-instance",
                json={"task_id": task_id, "instance_id": instance_id},
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": self._environment_keys["DAYTONA_API_KEY"],
                    "X-Api-Url": self._environment_keys["DAYTONA_API_URL"],
                    "X-Target": self._environment_keys["DAYTONA_TARGET"],
                },
            )

        logger.debug(f"Evaluate instance response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Evaluate instance failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json()

    async def request_final_score(self, evaluation_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """
        Requests final score from benchmark service
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._url}/final-score",
                json={"evaluation_results": evaluation_results},
                headers={"Content-Type": "application/json"},
            )

        logger.debug(f"Final score response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Final score failed with status code {response.status_code}, response: {response.text}"
            )

        return response.json()
