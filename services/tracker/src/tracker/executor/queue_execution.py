"""Serialize provider admission through Tracker without retaining a database connection."""

import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass

from benchmark_service import Resources, Sandbox, SandboxProvider, SandboxSource

from tracker.exceptions import TrackerServiceError
from tracker.executor.task_persistence import ApiTaskPersistence, settle_task_io
from tracker.executor_api.v1.task_schemas import BuildTask, RunTask

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApiSandboxQueueContext:
    provider: SandboxProvider
    poll_interval_seconds: float = 1

    async def enter(
        self,
        *,
        stack: AsyncExitStack,
        persistence: ApiTaskPersistence,
        source: SandboxSource,
        resources: Resources,
        create: Callable[[], AbstractAsyncContextManager[Sandbox]],
    ) -> Sandbox | None:
        """Release after confirmed creation; retain the reservation when the provider outcome is unknown."""
        while await persistence.current():
            creation_started = False
            creation_confirmed = False
            try:
                if await persistence.reserve_pool():
                    if await self.provider.check_admission(source, resources):
                        if not await persistence.write(BuildTask()):
                            return None
                        creation_started = True
                        try:
                            sandbox = await stack.enter_async_context(create())
                        except Exception as error:
                            raise TrackerServiceError(
                                "Sandbox creation outcome is unknown; the pool reservation requires reconciliation"
                            ) from error
                        creation_confirmed = True
                        if not await persistence.write(RunTask()):
                            await settle_task_io(stack.aclose())
                            return None

                        return sandbox
            finally:
                if not creation_started or creation_confirmed:
                    await persistence.release_pool()
                else:
                    logger.error(
                        "Sandbox creation outcome is unknown; retaining the pool reservation until reconciliation"
                    )
            await asyncio.sleep(self.poll_interval_seconds)

        return None
