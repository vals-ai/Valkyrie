"""Sandbox management utilities for the tracker service."""

import io
import shlex
import zipfile
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator

from tracker.logger import get_logger
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    FileUpload,
    Image,
    Resources,
)

from tracker.exceptions import SandboxError
from tracker.types import AgentContractRequest
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
) -> AsyncGenerator[AsyncSandbox, Any]:
    """
    Create a sandbox with the given name and image.
    Automatically cleans up the sandbox when the context manager exits.
    """
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    sandbox = await daytona.create(
        CreateSandboxFromImageParams(
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


async def run_agent(sandbox: AsyncSandbox, contract: AgentContractRequest, problem_statement: str, task_id: str) -> str:
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

    def on_data(data: bytes) -> None:
        # TODO: save logs to disk/s3
        data_str = data.decode("utf-8")
        print(data_str, end="")

    pty_handle = await sandbox.process.create_pty_session(
        id=contract.name,
        on_data=on_data,
        envs=contract.env,
    )

    await pty_handle.wait_for_connection()

    await pty_handle.send_input(run_cmd)

    await pty_handle.send_input("\nexit\n")

    result = await pty_handle.wait()

    if result.exit_code != 0:
        raise SandboxError(f"Failed to run agent {contract.name}, exit code: {result.error}")

    return ""
