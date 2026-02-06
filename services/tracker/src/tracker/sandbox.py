"""Sandbox management utilities for the tracker service."""

import io
import json
import shlex
import uuid
import zipfile
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

from tracker.database.models import AgentContractRequest
from tracker.exceptions import SandboxError
from tracker.logger import get_logger
from tracker.s3 import download_from_s3, get_contract_s3_key
from tracker.types import Resources as TrackerResources

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
                cpu=resources.vcpu,
                memory=resources.memory,
                disk=resources.disk,
            ),
            env_vars=env_vars,
        ),
        timeout=360,
    )

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

        cmd = await sandbox.process.get_session_command(session_id, cmd_id)

        if cmd.exit_code != 0:
            raise SandboxError(f"Failed to run command {command}, exit code: {cmd.exit_code}")

    finally:
        try:
            await sandbox.process.delete_session(session_id)
        except Exception:
            logger.error(f"Caught failure to delete session `{session_id}`")
            pass


async def run_agent(
    sandbox: AsyncSandbox,
    contract: AgentContractRequest,
    problem_statement: str,
    log_output: Callable[[str], None],
    cwd: str,
) -> dict[str, Any]:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract: The agent contract configuration
        problem_statement: Problem statement to pass to the agent
        log_output: Callback to log output
        cwd: Working directory to run the agent in

    Returns:
        Agent output as a dictionary

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    log_output(f"Running agent {contract.name}")

    await install_agent_dependencies(sandbox, contract, log_output)

    run_cmd = contract.run_cmd.replace("{problem_statement}", shlex.quote(problem_statement))

    await stream_command_output(sandbox, f"cd {cwd} && {run_cmd}", log_output)

    if not contract.final_output:
        return {}

    # Check if a agent_output.json file exists
    agent_output_exists = await sandbox.process.exec(f"[ -f {contract.final_output} ]")

    # If agent output does not exist, return an empty result
    if agent_output_exists.exit_code != 0:
        return {}

    # If agent output exists, try to load it as a json, if not, return the result as a string
    agent_output = await sandbox.process.exec(f"cat {contract.final_output}")

    try:
        return json.loads(agent_output.result)
    except Exception:
        logger.warning("Failed to load agent output as a json, creating a fallback result")
        pass

    return {
        "result": agent_output.result,
    }
