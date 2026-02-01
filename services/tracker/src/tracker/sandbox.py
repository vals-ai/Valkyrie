"""Sandbox management utilities for the tracker service."""

import asyncio
import io
import shlex
import zipfile
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

from tracker.database.models import AgentContractRequest
from tracker.exceptions import SandboxError
from tracker.logger import get_logger
from tracker.s3 import download_from_s3, get_contract_s3_key

logger = get_logger(__name__)

stream_logger = get_logger(__name__, stream=True)


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


@asynccontextmanager
async def create_sandbox(
    daytona: AsyncDaytona,
    sandbox_name: str,
    image: str,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> AsyncGenerator[AsyncSandbox, Any]:
    """
    Create a sandbox with the given name, image, and labels.
    Automatically cleans up the sandbox when the context manager exits.
    """
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    sandbox = await daytona.create(
        CreateSandboxFromImageParams(
            name=sandbox_name,
            labels=labels,
            image=image,
            network_block_all=False,
            resources=Resources(
                cpu=4,
                memory=8,
                disk=10,
            ),
            env_vars=env_vars,
        ),
        timeout=360,
    )

    try:
        yield sandbox
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


async def install_agent_dependencies(sandbox: AsyncSandbox, contract: AgentContractRequest) -> None:
    """Install agent dependencies in the sandbox."""
    logger.info(f"Installing dependencies for contract: {contract.name}")

    contract_path = get_contract_path(contract.name)

    response = await sandbox.process.exec(contract.install_cmd, cwd=str(contract_path))

    if response.exit_code != 0:
        error_msg = f"Failed to install dependencies for contract {contract.name}: {response.result}"
        logger.error(error_msg)
        raise SandboxError(error_msg)

    logger.info(response.result)
    logger.info(f"Finished running installing dependencies for contract: {contract.name}")


async def run_agent(
    sandbox: AsyncSandbox, contract: AgentContractRequest, problem_statement: str, task_id: str, cwd: str
) -> str:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract_name: Name of the contract
        problem_statement: Problem statement to pass to the agent

    Retruns:
        Agent stdout and stderr

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    logger.info(f"Running agent {contract.name} on task {task_id}")

    run_cmd = contract.run_cmd.replace("{problem_statement}", shlex.quote(problem_statement))

    def on_data(data: str) -> None:
        stream_logger.info(data.strip("\n"))

    session_id = f"{contract.name}-{task_id.replace(' ', '_')}"

    try:
        await sandbox.process.create_session(session_id)

        session_exec_resp = await sandbox.process.execute_session_command(
            session_id, SessionExecuteRequest(command=f"cd {cwd} && {run_cmd}", run_async=True)
        )

        cmd_id = session_exec_resp.cmd_id

        if not cmd_id:
            raise SandboxError(f"Failed to execute command {run_cmd} in session {session_id}")

        log_task = asyncio.create_task(
            sandbox.process.get_session_command_logs_async(
                session_id=session_id,
                command_id=cmd_id,
                on_stdout=on_data,
                on_stderr=on_data,
            )
        )

        await log_task

        cmd = await sandbox.process.get_session_command(session_id, cmd_id)

        if cmd.exit_code != 0:
            raise SandboxError(f"Failed to run agent {contract.name}, exit code: {cmd.exit_code}")

        return ""
    finally:
        try:
            await sandbox.process.delete_session(session_id)
        except Exception:
            # NOTE: If we kill the sandbox this sometimes errors
            logger.error(f"Caught failure to delete session `{session_id}`")
            pass
