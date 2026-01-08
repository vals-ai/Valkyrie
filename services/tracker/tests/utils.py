import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from daytona import AsyncDaytona, AsyncSandbox, CreateSandboxFromImageParams, DaytonaNotFoundError, Image, Resources


async def validate_docker_image(image_name: str) -> bool:
    try:
        result = await asyncio.create_subprocess_exec(
            "docker",
            "manifest",
            "inspect",
            image_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        _, stderr = await result.communicate()

        return result.returncode == 0 or (len(stderr) > 0 and "unsupported manifest format" in stderr.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Error validating docker image: {e}")


@asynccontextmanager
async def build_task_environment(
    daytona: AsyncDaytona,
    task_id: str,
    docker_image: str,
) -> AsyncIterator[AsyncSandbox]:
    """
    Builds the task environment using the docker image, once we are finished using the sandbox, we delete it
    """

    task_image = Image.base(docker_image)
    sandbox = await daytona.create(
        CreateSandboxFromImageParams(
            env_vars={"TEST_DIR": "/tests"},
            image=task_image,
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
            except DaytonaNotFoundError:
                pass
