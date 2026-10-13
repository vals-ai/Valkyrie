"""Launch one pinned executor runner for an admitted dispatch."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from uuid import UUID

import boto3
from botocore.config import Config
from sqlmodel import Session

from tracker.database.models import ExecutorDispatch
from tracker.database.session import engine

logger = logging.getLogger(__name__)
_ECS_RUNNER_PYTHON = "/app/.venv/bin/python"


class DefinitiveLaunchFailure(Exception):
    """ECS rejected the launch without creating a task."""


def _record_task_arn(dispatch_id: UUID, task_arn: str) -> None:
    with Session(engine) as session:
        dispatch = session.get(ExecutorDispatch, dispatch_id)
        if dispatch is None:
            raise RuntimeError(f"Executor dispatch {dispatch_id} disappeared after launch")
        dispatch.ecs_task_arn = task_arn
        session.add(dispatch)
        session.commit()


async def _record_task_arn_after_launch(dispatch_id: UUID, task_arn: str) -> None:
    # ECS already accepted the task, so a failed diagnostic write must not fail the launch.
    for attempt in range(3):
        try:
            await asyncio.to_thread(_record_task_arn, dispatch_id, task_arn)
            return
        except Exception:
            if attempt == 2:
                logger.exception("Failed to record ECS task %s for executor dispatch %s", task_arn, dispatch_id)
                return
            await asyncio.sleep(0.2 * (2**attempt))


async def launch_dispatch(dispatch: ExecutorDispatch) -> None:
    launcher = os.environ["EXECUTOR_LAUNCHER"]
    dispatch_id = str(dispatch.id)
    command = [sys.executable, "-m", "tracker.executor.runner", "--dispatch-id", dispatch_id]
    if launcher == "local":
        subprocess.Popen(command, start_new_session=True, env=os.environ)
        return
    if launcher != "ecs":
        raise ValueError("EXECUTOR_LAUNCHER must be ecs or local")

    params = {
        "cluster": os.environ["EXECUTOR_RUNNER_CLUSTER"],
        "taskDefinition": os.environ["EXECUTOR_RUNNER_TASK_DEFINITION"],
        "launchType": "FARGATE",
        "count": 1,
        "clientToken": dispatch_id,
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": os.environ["EXECUTOR_RUNNER_SUBNETS"].split(","),
                "securityGroups": [os.environ["EXECUTOR_RUNNER_SECURITY_GROUP"]],
                "assignPublicIp": "ENABLED",
            }
        },
        "overrides": {
            "containerOverrides": [
                {
                    "name": os.environ["EXECUTOR_RUNNER_CONTAINER"],
                    "command": [_ECS_RUNNER_PYTHON, "-m", "tracker.executor.runner", "--dispatch-id", dispatch_id],
                }
            ]
        },
    }
    ecs = boto3.client("ecs", config=Config(connect_timeout=3, read_timeout=8, retries={"max_attempts": 1}))
    for attempt in range(3):
        try:
            response = await asyncio.to_thread(ecs.run_task, **params)
            tasks = response["tasks"]
            if response["failures"] and not tasks:
                raise DefinitiveLaunchFailure(f"ECS rejected executor dispatch {dispatch_id}")
            if len(tasks) != 1:
                raise RuntimeError(f"Expected one ECS task for executor dispatch {dispatch_id}")
            task_arn = tasks[0]["taskArn"]
            logger.info("Launched executor dispatch %s as ECS task %s", dispatch_id, task_arn)
            await _record_task_arn_after_launch(dispatch.id, task_arn)
            return
        except DefinitiveLaunchFailure:
            raise
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(0.2 * (2**attempt))
