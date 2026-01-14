"""Sandbox management utilities for the tracker service."""

import asyncio
import io
import shlex
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from typing import Any, AsyncGenerator

from agentic_harness.base.contract import AgentContract
from agentic_harness.logger import get_logger
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    FileUpload,
    Image,
    Resources,
    SessionExecuteRequest,
)

from tracker.exceptions import SandboxError
from tracker.s3 import download_from_s3, get_contract_s3_key

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / contract_name


@asynccontextmanager
async def create_sandbox(
    daytona: AsyncDaytona,
    sandbox_name: str,
    image: str,
    env_vars: dict[str, str] = {},
) -> AsyncGenerator[AsyncSandbox, Any]:
    """
    Create a sandbox with the given name and image.
    Automatically cleans up the sandbox when the context manager exits.
    """
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    sandbox = await daytona.create(
        CreateSandboxFromImageParams(
            env_vars=env_vars,
            name=sandbox_name,
            image=Image.base(image),
            network_block_all=False,
            resources=Resources(
                cpu=4,
                memory=8,
                disk=10,
            ),
        ),
        timeout=360,
    )

    try:
        yield sandbox
    finally:
        logger.info(f"Deleting sandbox {sandbox.name}")
        await daytona.delete(sandbox)


async def upload_agent_artifacts(sandbox: AsyncSandbox, contract: AgentContract) -> None:
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
    files_to_upload: list[FileUpload] = []
    dirs_to_create: set[str] = set()

    # Unzip contract and collect files and directories
    with zipfile.ZipFile(io.BytesIO(contract_content), "r") as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.is_dir():
                continue

            file_content = zip_ref.read(file_info.filename)

            files_to_upload.append(
                FileUpload(
                    source=file_content,
                    destination=str(bundle_path / file_info.filename),
                )
            )

            parent = Path(file_info.filename).parent
            dirs_to_create.add(str(parent))

    # Create all necessary directories
    if dirs_to_create:
        mkdir_cmd = "mkdir -p " + " ".join(sorted(dirs_to_create))
        result = await sandbox.process.exec(mkdir_cmd)
        if result.exit_code != 0:
            raise SandboxError(f"Failed to create directories: {result.result}")

    await sandbox.fs.upload_files(files_to_upload)


async def install_agent_dependencies(sandbox: AsyncSandbox, contract: AgentContract) -> None:
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


async def run_agent(sandbox: AsyncSandbox, contract: AgentContract, problem_statement: str) -> None:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract_name: Name of the contract
        problem_statement: Problem statement to pass to the agent

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    logger.info(f"Starting agent {contract.name}")

    session_id = contract.name
    await sandbox.process.create_session(session_id)

    run_cmd = contract.run_cmd.replace("{{problem_statement}}", shlex.quote(problem_statement))
    run_agent_request = SessionExecuteRequest(command=run_cmd)
    run_agent_response = await sandbox.process.execute_session_command(session_id, run_agent_request)
    command_id = run_agent_response.cmd_id

    if command_id is None:
        raise SandboxError("Session command didn't return a command ID")

    command = await sandbox.process.get_session_command(session_id, command_id)

    timeout = 60
    polling_interval = 5
    while command.exit_code is None and timeout > 0:
        await asyncio.sleep(polling_interval)
        timeout -= polling_interval
        command = await sandbox.process.get_session_command(session_id, command_id)

    # TODO: Stream logs in a separate thread using the get_session_command_logs_async method
    logs = await sandbox.process.get_session_command_logs(session_id, command_id)
    logger.info(logs.output)

    if command.exit_code != 0:
        raise SandboxError(f"Failed to run agent {contract.name}: {run_agent_response.output}")

    logger.info(f"Ran agent {contract.name} successfully")
