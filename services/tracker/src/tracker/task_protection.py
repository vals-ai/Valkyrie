"""ECS Task Protection middleware for Taskiq.

Prevents ECS from stopping a Fargate task during deployments while
benchmarks are actively running.  Uses reference counting so protection
is only disabled once *all* concurrent tasks on the container have
finished — not when the first one completes.
"""

import asyncio
import logging
import os

import httpx
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

logger = logging.getLogger(__name__)

ECS_AGENT_URI = os.environ.get("ECS_AGENT_URI")

_PROTECTION_EXPIRY_MINUTES = 1440  # Max 24 hours (Prevents cloud waste)


async def _set_task_protection(*, enabled: bool) -> None:
    """Enables task protection for a ecs task when we deploy (if there are tasks still running inside)"""
    if not ECS_AGENT_URI:
        return

    url = f"{ECS_AGENT_URI}/task-protection/v1/state"
    body: dict[str, object] = {"ProtectionEnabled": enabled}

    if enabled:
        body["ExpiresInMinutes"] = _PROTECTION_EXPIRY_MINUTES

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(url, json=body)
            resp.raise_for_status()

    except Exception:
        logger.exception("Failed to set ECS task protection to %s", enabled)


class TaskProtectionMiddleware(TaskiqMiddleware):
    """
    Use middleware to enable ECS task protection while any task is running and release task protection when no benchmarks are running

    NOTE: Use a lock to count ongoing benchmarks across the whole worker service
    """

    def __init__(self) -> None:
        super().__init__()
        self._active: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        async with self._lock:
            self._active += 1
            if self._active == 1:
                await _set_task_protection(enabled=True)
        return message

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult) -> None:
        await self._release()

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
        exception: BaseException,
    ) -> None:
        await self._release()

    async def _release(self) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            if self._active == 0:
                await _set_task_protection(enabled=False)
