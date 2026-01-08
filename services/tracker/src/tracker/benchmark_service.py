import os
from typing import Any

import httpx
from daytona import AsyncDaytona, DaytonaConfig
from dotenv import load_dotenv

from tracker.exceptions import BenchmarkServiceError
from tracker.logger import get_logger

load_dotenv()

logger = get_logger(__name__)


class BenchmarkService:
    _name: str
    _url: str
    _environment_keys: dict[str, str]

    def __init__(self, name: str, url: str):
        logger.info(f"Initializing benchmark service for {name} at {url}")

        self._name = name
        self._url = url
        self._environment_keys = self.daytona_keys()

        self._daytona = AsyncDaytona(
            config=DaytonaConfig(
                api_key=self._environment_keys["DAYTONA_API_KEY"],
                api_url=self._environment_keys["DAYTONA_API_URL"],
                target=self._environment_keys["DAYTONA_TARGET"],
            )
        )

        logger.info(f"Benchmark service initialized for {name} at {url}")

    @staticmethod
    def daytona_keys() -> dict[str, str]:
        environment_keys: dict[str, str] = {
            "DAYTONA_API_KEY": os.getenv("DAYTONA_API_KEY") or "",
            "DAYTONA_API_URL": os.getenv("DAYTONA_API_URL") or "",
            "DAYTONA_TARGET": os.getenv("DAYTONA_TARGET") or "",
        }

        missing_keys = [key for key, value in environment_keys.items() if not value]

        if missing_keys:
            raise BenchmarkServiceError(
                f"The following environment variables are not set: {', '.join(missing_keys)}. Please set them in your `.env` file so that they can be sourced."
            )

        return environment_keys

    async def request_health_check(self) -> dict[str, str]:
        """
        Requests health check from benchmark service
        """
        logger.info(f"Performing health check for {self._name} benchmark service")

        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/health")

        logger.debug(f"Health check response: {response.json()}")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Health check failed with status code {response.status_code}, response: {response.text}"
            )

        logger.info(f"Health check passed for {self._name} benchmark service")

        return response.json()

    async def request_verify_task_ids(self, task_ids: list[str]) -> list[str]:
        """
        Requests verify task ids from benchmark service
        Returns a list of verified task ids
        """
        logger.info(f"Verifying task IDs: {task_ids}")

        params = {"task_ids": task_ids}
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/verify-task-ids", params=params)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Verify task ids failed with status code {response.status_code}, response: {response.text}"
            )

        response_json = response.json()
        verified_task_ids: list[str] = response_json["task_ids"]

        logger.info(f"Task IDs verified successfully: {verified_task_ids}")

        return verified_task_ids

    async def request_retrieve_tasks(
        self, task_ids: list[str], skip_validation: bool = False
    ) -> dict[str, dict[str, str]]:
        """
        Requests retrieve tasks from benchmark service
        """
        logger.info(f"Retrieving tasks for verified task IDs: {task_ids}")

        params = {"task_ids": task_ids, "skip_validation": skip_validation}
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._url}/retrieve-tasks", params=params)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve tasks failed with status code {response.status_code}, response: {response.text}"
            )

        logger.info("Tasks retrieved successfully")
        return response.json()

    async def request_setup_task(self, task_id: str, instance_id: str) -> dict[str, str]:
        """
        Requests setup task from benchmark service
        """
        logger.info(f"Setting up task {task_id} with instance {instance_id}")

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

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Setup task failed with status code {response.status_code}, response: {response.text}"
            )

        logger.info(f"Task {task_id} setup successfully")

        return response.json()

    async def request_evaluate_instance(self, task_id: str, instance_id: str) -> dict[str, str]:
        """
        Requests evaluate instance from benchmark service
        """
        logger.info(f"Evaluating instance {instance_id} for task {task_id}")

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

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Evaluate instance failed with status code {response.status_code}, response: {response.text}"
            )

        logger.info(f"Instance {instance_id} evaluated successfully")

        return response.json()

    async def request_final_score(self, evaluation_results: dict[str, dict[str, Any]]) -> dict[str, str]:
        """
        Requests final score from benchmark service
        """
        logger.info(f"Producing final score for tasks {evaluation_results.keys()}")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._url}/final-score",
                json={"evaluation_results": evaluation_results},
                headers={"Content-Type": "application/json"},
            )

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Final score failed with status code {response.status_code}, response: {response.text}"
            )

        logger.info("Final score produced successfully")

        return response.json()

    @property
    def daytona(self) -> AsyncDaytona:
        """
        Returns the Daytona client instance.
        """
        return self._daytona
