"""Stop intake when ECS replaces this host, then wait for its executor processes."""

import asyncio
import logging
import os
import signal
from typing import Any, Protocol, cast

import aiohttp
import boto3
from botocore.config import Config
from pydantic import BaseModel, Field
from taskiq.receiver import Receiver

logger = logging.getLogger(__name__)
_POLL_SECONDS = 10


class HostGenerationError(Exception):
    """ECS did not identify exactly one current host task definition."""


class _TaskMetadata(BaseModel):
    cluster: str = Field(alias="Cluster")
    family: str = Field(alias="Family")
    revision: str = Field(alias="Revision")


class _Deployment(BaseModel):
    status: str
    task_definition: str = Field(alias="taskDefinition")


class _Service(BaseModel):
    deployments: list[_Deployment]


class _ServiceResponse(BaseModel):
    services: list[_Service]


class _ECSClient(Protocol):
    def describe_services(self, *, cluster: str, services: list[str]) -> dict[str, Any]: ...
    def close(self) -> None: ...


def _describe_service(cluster: str, service_name: str) -> dict[str, Any]:
    ecs = cast(
        _ECSClient,
        boto3.client("ecs", config=Config(connect_timeout=5, read_timeout=5, retries={"total_max_attempts": 1})),
    )
    try:
        return ecs.describe_services(cluster=cluster, services=[service_name])
    finally:
        ecs.close()


async def is_current_generation() -> bool:
    """Compare this task's definition with the service's primary deployment."""
    metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    if metadata_uri is None:
        return True
    service_name = os.environ["EXECUTOR_HOST_SERVICE_NAME"]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as client:
        async with client.get(f"{metadata_uri}/task", allow_redirects=False) as response:
            response.raise_for_status()
            metadata = _TaskMetadata.model_validate(await response.json())
    described = _ServiceResponse.model_validate(
        await asyncio.to_thread(_describe_service, metadata.cluster, service_name)
    )
    if len(described.services) != 1:
        raise HostGenerationError("ECS did not return the executor host service")
    primary = [deployment for deployment in described.services[0].deployments if deployment.status == "PRIMARY"]
    if len(primary) != 1:
        raise HostGenerationError("ECS did not return one primary executor host deployment")

    return primary[0].task_definition.rsplit("/", 1)[-1] == f"{metadata.family}:{metadata.revision}"


async def _check_generation(finish_event: asyncio.Event) -> None:
    if finish_event.is_set():
        return
    if not await is_current_generation():
        logger.info("Executor host was replaced; stopping intake while current runs finish")
        finish_event.set()


async def _watch_generation(finish_event: asyncio.Event) -> None:
    while not finish_event.is_set():
        await asyncio.sleep(_POLL_SECONDS)
        try:
            await _check_generation(finish_event)
        except Exception:
            logger.exception("Could not check executor host deployment; retaining current runs")


class DrainingReceiver(Receiver):
    """Use Taskiq's unbounded task drain without cancelling active executor processes."""

    async def listen(self, finish_event: asyncio.Event) -> None:
        if self.wait_tasks_timeout is not None:
            raise HostGenerationError("Executor hosts must wait for active runs without a shutdown task timeout")
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGUSR1, finish_event.set)
        watcher: asyncio.Task[None] | None = None
        try:
            while not finish_event.is_set():
                try:
                    await _check_generation(finish_event)
                    break
                except Exception:
                    logger.exception("Waiting to verify executor host deployment before accepting runs")
                    await asyncio.sleep(_POLL_SECONDS)
            watcher = asyncio.create_task(_watch_generation(finish_event))
            await super().listen(finish_event)
        finally:
            loop.remove_signal_handler(signal.SIGUSR1)
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
