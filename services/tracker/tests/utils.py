from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from daytona import AsyncDaytona, AsyncSandbox, CreateSandboxFromImageParams, Resources


@asynccontextmanager
async def build_task_environment(
    daytona: AsyncDaytona,
    task_id: str,
    docker_image: str,
) -> AsyncIterator[AsyncSandbox]:
    """
    Builds the task environment using the docker image, once we are finished using the sandbox, we delete it

    Args:
        daytona: The daytona client
        task_id: The id of the task (usually a string name of the task)
        docker_image: The docker image to build the sandbox from

    Returns:
        A context manager that yields the sandbox
    """

    sandbox = await daytona.create(
        CreateSandboxFromImageParams(
            image=docker_image,
            name=task_id,
            network_block_all=False,
            resources=Resources(cpu=4, memory=8, disk=10),
        ),
        timeout=360,
    )

    await sandbox.process.create_session(sandbox.id)

    try:
        yield sandbox
    finally:
        try:
            await daytona.delete(sandbox)
        except Exception:
            try:
                sandbox = await daytona.get(task_id)

                await daytona.delete(sandbox)
            except Exception:
                pass
