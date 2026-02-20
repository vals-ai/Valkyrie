"""Sandbox management utilities for the tracker service."""

import base64
import io
import shlex
import uuid
import zipfile
from asyncio import Semaphore
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator

from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    DaytonaNotFoundError,
    FileUpload,
    Resources,
    SandboxState,
    SessionExecuteRequest,
)
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from tracker.database.models import AgentContractRequest
from tracker.exceptions import SandboxError
from tracker.logger import get_logger
from tracker.s3 import download_from_s3, get_contract_s3_key, upload_to_s3
from benchmark_service.schemas import Resources as TrackerResources

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / contract_name


async def delete_sandbox(sandbox: AsyncSandbox, daytona: AsyncDaytona) -> None:
    """Delete sandbox if it is not already destroyed or being destroyed"""
    try:
        await sandbox.refresh_data()
        if sandbox.state not in [SandboxState.DESTROYING, SandboxState.DESTROYED]:
            await daytona.delete(sandbox)
    except DaytonaNotFoundError:
        # If we error here that means the sandbox has just been deleted before we could refresh the state
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
        pass
    except Exception as e:
        logger.error(f"Unexpected error deleting sandbox {sandbox.name}: {e}")


_SANBDOX_CREATION_CAP: int = 10
_sandbox_creation_semaphore = Semaphore(_SANBDOX_CREATION_CAP)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(120),
    before_sleep=before_sleep_log(logger, logger.level),
    reraise=True,
)
async def _create_sandbox(
    daytona: AsyncDaytona,
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> AsyncSandbox:
    """
    Creates a sandbox and takes into account timeouts and retries.

    This retry only works in the following case:
    - Client times out while sandbox is being created
    """

    # If the container already exists we reuse it
    try:
        sandbox = await daytona.get(sandbox_name)

        await sandbox.wait_for_sandbox_start(timeout=0)

        return sandbox
    except DaytonaNotFoundError:
        pass

    # Create a new sandbox from scratch, if it stops we delete it within a minute
    return await daytona.create(
        CreateSandboxFromImageParams(
            auto_delete_interval=60,
            name=sandbox_name,
            labels=labels,
            image=image,
            network_block_all=False,
            resources=Resources(
                cpu=resources.vcpu,
                memory=resources.memory,
                disk=resources.disk,
            ),
            env_vars=env_vars,
        ),
        timeout=360,
    )


@asynccontextmanager
async def create_sandbox(
    daytona: AsyncDaytona,
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> AsyncGenerator[AsyncSandbox, Any]:
    """
    Yeild a sandbox to be used within a context manager.

    Args:
        daytona: The daytona client
        sandbox_name: The name of the sandbox
        image: The image to use for the sandbox
        resources: The resources to use for the sandbox
        labels: The labels to use for the sandbox
        env_vars: The environment variables to use for the sandbox

    Returns:
        A context manager that yields the sandbox
    """
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    # If we run too many at once it can cause hanging issues
    # NOTE does not block how many context managers we can have open, just how many sandboxes we can create at once
    async with _sandbox_creation_semaphore:
        sandbox = await _create_sandbox(daytona, sandbox_name, image, resources, labels, env_vars)

    try:
        yield sandbox
    except Exception as e:
        logger.error(f"Error creating sandbox {sandbox.name}: {e}")
        raise e from e
    finally:
        await delete_sandbox(sandbox, daytona)


async def upload_agent_artifacts(sandbox: AsyncSandbox, contract: AgentContractRequest) -> None:
    """
    Upload contract from S3 to the sandbox.

    Args:
        sandbox: The sandbox to upload files to
        contract_name: Name of the contract (without extension)

    Raises:
        SandboxError: If directory creation or file upload fails
    """
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = get_contract_s3_key(contract.name)
    contract_content = download_from_s3(contract_s3_key)

    # Unzip contract and collect files and directories
    with zipfile.ZipFile(io.BytesIO(contract_content), "r") as zip_ref:
        files_to_upload = [
            FileUpload(
                source=zip_ref.read(file_info),
                destination=str(bundle_path / file_info.filename),
            )
            for file_info in zip_ref.infolist()
            if not file_info.is_dir()
        ]

    await sandbox.fs.upload_files(files_to_upload)


async def install_agent_dependencies(
    sandbox: AsyncSandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
) -> None:
    """Install agent dependencies in the sandbox."""
    if not contract.install_cmd:
        return

    log_output(f"Installing dependencies for contract: {contract.name}")

    contract_path = get_contract_path(contract.name)

    await stream_command_output(sandbox, f"cd {str(contract_path)} && {contract.install_cmd}", log_output)

    log_output(f"Finished installing dependencies for contract: {contract.name}")


async def stream_command_output(
    sandbox: AsyncSandbox,
    command: str,
    on_output: Callable[[str], None],
) -> None:
    """
    Execute a command inside of a sandbox using a session and stream the output to the given callbacks.
    Falls back to polling if WebSocket streaming fails.
    """
    session_id = f"{sandbox.id}:{str(uuid.uuid4())}"
    try:
        await sandbox.process.create_session(session_id)

        session_exec_resp = await sandbox.process.execute_session_command(
            session_id, SessionExecuteRequest(command=command, run_async=True)
        )

        cmd_id = session_exec_resp.cmd_id

        if not cmd_id:
            raise SandboxError(f"Failed to execute command {command} in session {session_id}")

        await sandbox.process.get_session_command_logs_async(
            session_id=session_id,
            command_id=cmd_id,
            on_stdout=on_output,
            on_stderr=on_output,
        )

        try:
            cmd = await sandbox.process.get_session_command(session_id, cmd_id)

            if cmd.exit_code != 0:
                raise SandboxError(f"Failed to run command {command}, exit code: {cmd.exit_code}")

        except SandboxError:
            raise
        except Exception as e:
            logger.warning(f"Failed to get session command for {session_id}: {e}")
            pass

    finally:
        try:
            await sandbox.process.delete_session(session_id)
        except Exception:
            logger.warning(f"Caught failure to delete session `{session_id}`")
            pass


async def archive_and_upload_output(sandbox: AsyncSandbox, output_path: str, agent_output_s3_key: str) -> None:
    """Compress a file in the sandbox into a tar.gz and upload it to S3"""
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"

    tar_result = await sandbox.process.exec(f"tar -czf {shlex.quote(archive_path)} {shlex.quote(output_path)}")
    if tar_result.exit_code != 0:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        b64_result = await sandbox.process.exec(f"base64 {shlex.quote(archive_path)}")
        if b64_result.exit_code != 0:
            raise SandboxError(f"Failed to read archive from {output_path}")

        upload_to_s3(base64.b64decode(b64_result.result), agent_output_s3_key)
    finally:
        # Check if file exists and remove it if it does
        result = await sandbox.process.exec(f"test -e {shlex.quote(archive_path)}")
        if result.exit_code == 0:
            await sandbox.process.exec(f"rm -f {shlex.quote(archive_path)}")
        else:
            logger.warning(f"File {archive_path} does not exist, skipping removal")


async def run_agent(
    sandbox: AsyncSandbox,
    contract: AgentContractRequest,
    problem_statement: str,
    task_id: str,
    log_output: Callable[[str], None],
    cwd: str,
    agent_output_s3_key: str | None = None,
) -> None:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract: The agent contract configuration
        problem_statement: Problem statement to pass to the agent
        log_output: Callback to log output
        cwd: Working directory to run the agent in
        agent_output_s3_key: S3 key to where we will upload the final output archive to

    Returns:
        Agent output as a dictionary

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    log_output(f"Running agent {contract.name}")

    await install_agent_dependencies(sandbox, contract, log_output)

    problem_statement_path = "/tmp/problem_statement.txt"
    await sandbox.fs.upload_file(problem_statement.encode(), problem_statement_path)

    run_cmd = contract.run_cmd.replace("{problem_statement_path}", problem_statement_path).replace("{task_id}", task_id)

    # Run the agent without including task directory dependencies
    await stream_command_output(sandbox, f"cd {cwd} && PYTHONSAFEPATH=1 {run_cmd}", log_output)

    if not contract.final_output:
        return

    result = await sandbox.process.exec(f"test -e {shlex.quote(contract.final_output)}")
    if result.exit_code != 0:
        return

    if agent_output_s3_key:
        await archive_and_upload_output(sandbox, contract.final_output, agent_output_s3_key)
