"""Sandbox management utilities for the tracker service."""

import asyncio
import io
import os
import shlex
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from typing import Any, AsyncGenerator

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
from tracker.logger import get_logger
from tracker.s3 import download_from_s3, get_contract_s3_key

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")


def _strip_bundle_prefix(filename: str) -> str:
    """
    Strip bundle_* prefix from filename if present.

    Args:
        filename: Filename from zip archive (e.g., "bundle_abc123/contracts/mycontract/contract.py")

    Returns:
        Filename with bundle prefix removed (e.g., "contracts/mycontract/contract.py")
    """
    if "/" in filename and filename.split("/")[0].startswith("bundle"):
        return "/".join(filename.split("/")[1:])

    return filename


def _collect_parent_directories(filename: str, base_path: PurePosixPath) -> set[str]:
    """
    Collect all parent directories that need to be created for a file.

    Args:
        filename: Relative filename (e.g., "contracts/mycontract/contract.py")
        base_path: Base path to prepend (e.g., "/bundle")

    Returns:
        Set of directory paths that need to be created
    """
    dirs_to_create: set[str] = set()
    parent = str(Path(filename).parent)

    if parent and parent != ".":
        # Add all parent directories in the path
        parts = Path(filename).parts[:-1]  # Exclude the file itself
        for i in range(1, len(parts) + 1):
            dir_path = str(base_path.joinpath(*parts[:i]))
            dirs_to_create.add(dir_path)

    return dirs_to_create


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / "contracts" / contract_name


def get_submodules_dir(contract_name: str) -> PurePosixPath:
    """Get the path to submodules directory for a contract."""
    return get_contract_path(contract_name) / "submodules"


@asynccontextmanager
async def create_sandbox(daytona: AsyncDaytona, sandbox_name: str, image: str) -> AsyncGenerator[AsyncSandbox, Any]:
    """
    Create a sandbox with the given name and image.
    Automatically cleans up the sandbox when the context manager exits.
    """
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    sandbox = await daytona.create(
        CreateSandboxFromImageParams(
            env_vars=dict(os.environ),
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


async def upload_contract_to_sandbox(sandbox: AsyncSandbox, contract_name: str) -> None:
    """
    Upload contract from S3 to the sandbox.

    Args:
        sandbox: The sandbox to upload files to
        contract_name: Name of the contract (without extension)

    Raises:
        SandboxError: If directory creation or file upload fails
    """
    logger.info(f"Uploading contract {contract_name} to sandbox {sandbox.name}")

    contract_s3_key = get_contract_s3_key(contract_name)
    contract_content = download_from_s3(contract_s3_key)
    files_to_upload: list[FileUpload] = []
    dirs_to_create: set[str] = set()

    # Unzip contract and collect files and directories
    with zipfile.ZipFile(io.BytesIO(contract_content), "r") as zip_ref:
        for file_info in zip_ref.filelist:
            if not file_info.is_dir():
                file_content = zip_ref.read(file_info.filename)
                filename = _strip_bundle_prefix(file_info.filename)

                files_to_upload.append(
                    FileUpload(
                        source=file_content,
                        destination=str(bundle_path / filename),
                    )
                )

                dirs_to_create.update(_collect_parent_directories(filename, bundle_path))

    # Create all necessary directories
    if dirs_to_create:
        mkdir_cmd = "mkdir -p " + " ".join(sorted(dirs_to_create))
        result = await sandbox.process.exec(mkdir_cmd)
        if result.exit_code != 0:
            raise SandboxError(f"Failed to create directories: {result.result}")

    await sandbox.fs.upload_files(files_to_upload)


async def install_dependencies(sandbox: AsyncSandbox, contract_name: str) -> None:
    """
    Install agent dependencies in the sandbox.

    This function:
    1. Installs the agentic_harness package
    2. Installs all packages in the submodules/ directory (if it exists)
    3. Runs the setup.sh script from the contract (if it exists)

    Args:
        sandbox: The sandbox to install dependencies in
        contract_name: Name of the contract (used to locate setup.sh)

    Raises:
        SandboxError: If dependency installation fails

    TODO: add integration test
    """
    logger.info(f"Installing dependencies for contract: {contract_name}")

    contract_path = get_contract_path(contract_name)

    contract_files = await sandbox.fs.list_files(str(contract_path))
    setup_exists = any(file.name == "setup.sh" for file in contract_files)
    submodules_exist = any(file.name == "submodules" for file in contract_files)

    logger.info("Installing agentic_harness dependencies")
    response = await sandbox.process.exec("pip install -e .", cwd=str(bundle_path))
    if response.exit_code != 0:
        error_msg = f"Failed to install agentic_harness dependencies: {response.result}"
        logger.error(error_msg)
        raise SandboxError(error_msg)
    logger.info(response.result)
    logger.info("Finished installing agentic_harness dependencies")

    if submodules_exist:
        submodules_dir = get_submodules_dir(contract_name)
        submodule_files = await sandbox.fs.list_files(str(submodules_dir))

        for submodule in submodule_files:
            if submodule.is_dir:
                submodule_dir = submodules_dir / submodule.name
                logger.info(f"Installing dependencies for submodule: {submodule.name}")
                response = await sandbox.process.exec("pip install -e .", cwd=str(submodule_dir))

                if response.exit_code != 0:
                    error_msg = f"Failed to install dependencies for submodule {submodule.name}: {response.result}"
                    logger.error(error_msg)
                    raise SandboxError(error_msg)

                logger.info(response.result)
                logger.info(f"Finished installing dependencies for submodule: {submodule.name}")

    if setup_exists:
        logger.info(f"Running setup.sh for contract: {contract_name}")
        response = await sandbox.process.exec("bash setup.sh", cwd=str(contract_path))

        if response.exit_code != 0:
            logger.error(f"Failed to run setup.sh for contract {contract_name}: {response.result}")

        logger.info(response.result)
        logger.info(f"Finished running setup.sh for contract: {contract_name}")


async def run_agent(sandbox: AsyncSandbox, contract_name: str, task_id: str, problem_statement: str) -> None:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract_name: Name of the contract
        task_id: ID of the task being run
        problem_statement: Problem statement to pass to the agent

    Raises:
        SandboxError: If the agent fails to run or times out

    TODO: add integration tests
    """
    logger.info(f"Starting agent {contract_name} for task {task_id}")

    contract_path = get_contract_path(contract_name)
    run_cmd = f"python -m agentic_harness.entry --contract {contract_path} --problem_statement {shlex.quote(problem_statement)}"

    session_id = contract_name
    await sandbox.process.create_session(session_id)

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
        raise SandboxError(f"Failed to run agent {contract_name} for task {task_id}")

    logger.info(f"Agent ran successfully for task {task_id}")
