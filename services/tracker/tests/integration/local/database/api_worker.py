"""Database-free executor client process for the API restart integration test."""

import asyncio
import json
import os
import sys
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, SecretStr

from tracker.executor.api_execution import run_with_dispatch_lease
from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.finalization_schemas import CompleteRun
from tracker.executor_api.v1.schemas import ClaimRequest
from tracker.executor_api.v1.task_schemas import BuildTask, CompleteTask, EvaluateTask, RunTask


class WorkerInput(BaseModel):
    endpoint: str
    token: SecretStr
    dispatch_id: UUID
    claim: ClaimRequest
    task_ids: list[str]


async def main() -> None:
    configuration = WorkerInput.model_validate_json(await asyncio.to_thread(sys.stdin.readline))
    async with httpx.AsyncClient(base_url=configuration.endpoint, trust_env=False) as http:
        api = ExecutorClient(
            ExecutorTransport(http, configuration.token), configuration.dispatch_id, configuration.claim.claimant_id
        )

        async def execute() -> None:
            state = await api.run_state(configuration.task_ids)
            task = state.tasks[0]
            owned = await api.claim_task(task.id, task.started_at, command_id=uuid4())
            built = await api.write_task(
                task.id, task.started_at, BuildTask(), command_id=uuid4(), expected_revision=owned.revision
            )
            running = await api.write_task(
                task.id, task.started_at, RunTask(), command_id=uuid4(), expected_revision=built.revision
            )
            forbidden = [
                module
                for module in sys.modules
                if module.startswith(("tracker.database", "tracker.config")) or module == "sqlmodel"
            ]
            assert not forbidden, forbidden
            print(
                json.dumps({"event": "started", "pid": os.getpid(), "started_at": task.started_at.isoformat()}),
                flush=True,
            )
            command = (await asyncio.to_thread(sys.stdin.readline)).strip()
            assert command == "read"
            print(json.dumps({"event": "reading"}), flush=True)
            state = await api.run_state(configuration.task_ids)
            assert state.current
            assert state.tasks[0].started_at == task.started_at
            print(json.dumps({"event": "resumed", "pid": os.getpid()}), flush=True)

            evaluated = await api.write_task(
                task.id, task.started_at, EvaluateTask(), command_id=uuid4(), expected_revision=running.revision
            )
            await api.write_task(
                task.id,
                task.started_at,
                CompleteTask(result={"score": 1}),
                command_id=uuid4(),
                expected_revision=evaluated.revision,
            )
            finalization = await api.finalization_state()
            assert finalization.snapshot_digest is not None
            await api.finalize_run(finalization.snapshot_digest, CompleteRun(final_score=1), command_id=uuid4())

        await run_with_dispatch_lease(api, configuration.claim, execute, heartbeat_interval_seconds=0.05)
        await api.finish()
    print(json.dumps({"event": "finished", "pid": os.getpid()}), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
