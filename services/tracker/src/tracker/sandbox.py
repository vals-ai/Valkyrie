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

from benchmark_service.schemas import Resources as TrackerResources
from benchmark_service.sandbox import (
    Sandbox,
    SandboxCreateRequest,
    SandboxFile,
    SandboxProvider,
    SandboxResources,
    SandboxSourceType,
)

from tenacity import retry, retry_if_exception_type, stop_after_attempt

from tracker.database.models import AgentContractRequest
from tracker.exceptions import InvalidSandboxConfigurationError, SandboxError
from tracker.logger import get_logger
from tracker.s3 import download_from_s3, get_contract_s3_key, upload_to_s3
from tracker.types import AWSCredentials

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")
SNAPSHOT_IMAGE_PREFIX = "snapshot:"


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / contract_name


_SANBDOX_CREATION_CAP: int = 10
_sandbox_creation_semaphore = Semaphore(_SANBDOX_CREATION_CAP)


def _build_create_request(
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> SandboxCreateRequest:
    source_id = image
    source_type = SandboxSourceType.IMAGE
    if image.startswith(SNAPSHOT_IMAGE_PREFIX):
        source_id = image[len(SNAPSHOT_IMAGE_PREFIX):].strip()
        if not source_id:
            raise InvalidSandboxConfigurationError("Snapshot-based sandbox requested without a snapshot name")
        source_type = SandboxSourceType.SNAPSHOT


    return SandboxCreateRequest(
        source_id=source_id,
        source_type=source_type,
        name=sandbox_name,
        labels=labels or {},
        env_vars=env_vars or {},
        network_blocked=False,
        auto_delete_interval=60,
        resources=SandboxResources(
            cpu=resources.vcpu,
            memory=resources.memory,
            disk=resources.disk,
        ),
    )


@asynccontextmanager
async def create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> AsyncGenerator[Sandbox, Any]:
    """Yield a sandbox to be used within a context manager."""
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    request = _build_create_request(sandbox_name, image, resources, labels, env_vars)

    # Limit concurrent sandbox creation to avoid hanging issues
    # TODO: this value should be provider specific
    async with _sandbox_creation_semaphore:
        sandbox = await provider.create_sandbox(request)

    try:
        yield sandbox
    except Exception as e:
        logger.error(f"Error creating sandbox {sandbox.name}: {e}")
        raise e from e
    finally:
        await provider.delete_sandbox(sandbox)


async def upload_agent_artifacts(
    sandbox: Sandbox, contract: AgentContractRequest, aws: AWSCredentials, s3_bucket: str
) -> None:
    """
    Upload contract from S3 to the sandbox.

    Args:
        sandbox: The sandbox to upload files to
        contract: The agent contract configuration
        aws: AWS credentials for S3 access
        s3_bucket: S3 bucket name

    Raises:
        SandboxError: If directory creation or file upload fails
    """
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = get_contract_s3_key(contract.name)
    contract_content = download_from_s3(contract_s3_key, aws, s3_bucket)

    # Unzip contract and collect files, excluding contract.py
    with zipfile.ZipFile(io.BytesIO(contract_content), "r") as zip_ref:
        files_to_upload = [
            SandboxFile(
                content=zip_ref.read(file_info),
                remote_path=str(bundle_path / file_info.filename),
            )
            for file_info in zip_ref.infolist()
            if not file_info.is_dir() and not file_info.filename.endswith("contract.py")
        ]

    if files_to_upload:
        await sandbox.upload_files(files_to_upload)
    else:
        await sandbox.create_folder(str(bundle_path / contract.name))


@retry(retry=retry_if_exception_type(SandboxError), reraise=True, stop=stop_after_attempt(3))
async def install_agent_dependencies(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
) -> None:
    """Install agent dependencies in the sandbox."""
    if not contract.install_cmd:
        return

    log_output(f"Installing dependencies for contract: {contract.name}")

    contract_path = get_contract_path(contract.name)

    result = await sandbox.exec(
        contract.install_cmd,
        cwd=str(contract_path),
        on_stdout=log_output,
        on_stderr=log_output,
    )
    if result.exit_code is None:
        logger.warning(f"Streamed install command for {contract.name} finished without an exit code")
    elif result.exit_code not in (0, 124):
        raise SandboxError(f"Failed to install dependencies, exit code: {result.exit_code}")

    log_output(f"Finished installing dependencies for contract: {contract.name}")


async def stream_command_output(
    sandbox: Sandbox,
    command: str,
    on_output: Callable[[str], None],
) -> None:
    """Execute a command inside of a sandbox and stream the output."""
    result = await sandbox.exec(command, on_stdout=on_output, on_stderr=on_output)

    # Exit code 124 = timeout(1) killed the process; treat as success so evaluation still runs
    if result.exit_code is None:
        logger.warning(f"Streamed command `{command}` finished without an exit code")
    elif result.exit_code not in (0, 124):
        raise SandboxError(f"Failed to run command {command}, exit code: {result.exit_code}")


async def archive_and_upload_output(
    sandbox: Sandbox, output_path: str, agent_output_s3_key: str, aws: AWSCredentials, s3_bucket: str
) -> None:
    """Compress a file in the sandbox into a tar.gz and upload it to S3"""
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"

    tar_result = await sandbox.exec(f"tar -czf {shlex.quote(archive_path)} {shlex.quote(output_path)}")
    if tar_result.exit_code != 0:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        b64_result = await sandbox.exec(f"base64 {shlex.quote(archive_path)}")
        if b64_result.exit_code != 0:
            raise SandboxError(f"Failed to read archive from {output_path}")

        upload_to_s3(base64.b64decode(b64_result.stdout), agent_output_s3_key, aws, s3_bucket)
    finally:
        # Check if file exists and remove it if it does
        result = await sandbox.exec(f"test -e {shlex.quote(archive_path)}")
        if result.exit_code == 0:
            await sandbox.exec(f"rm -f {shlex.quote(archive_path)}")
        else:
            logger.warning(f"File {archive_path} does not exist, skipping removal")


async def run_agent(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    problem_path: str,
    task_id: str,
    log_output: Callable[[str], None],
    cwd: str,
    aws: AWSCredentials,
    s3_bucket: str,
    agent_output_s3_key: str | None = None,
    agent_timeout: float | None = None,
) -> None:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract: The agent contract configuration
        problem_path: Path inside the sandbox where the problem statement file was written during setup
        task_id: The task ID
        log_output: Callback to log output
        cwd: Working directory to run the agent in
        aws: AWS credentials
        s3_bucket: S3 bucket name
        agent_output_s3_key: S3 key to where we will upload the final output archive to
        agent_timeout: Optional timeout in seconds to enforce on the agent command

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    log_output(f"Running agent {contract.name}")

    await install_agent_dependencies(sandbox, contract, log_output)

    run_cmd = contract.run_cmd.replace("{problem_statement_path}", problem_path).replace("{task_id}", task_id)

    # Apply timeout if specified
    if agent_timeout is not None:
        run_cmd = f"timeout {agent_timeout} {run_cmd}"

    # Create cwd if it does not already exist
    await sandbox.exec(f"mkdir -p {shlex.quote(cwd)}")

    # Run the agent without including task directory dependencies
    await stream_command_output(sandbox, f"cd {cwd} && PYTHONSAFEPATH=1 {run_cmd}", log_output)

    if not contract.final_output:
        return

    result = await sandbox.exec(f"test -e {shlex.quote(contract.final_output)}")
    if result.exit_code != 0:
        return

    if agent_output_s3_key:
        await archive_and_upload_output(sandbox, contract.final_output, agent_output_s3_key, aws, s3_bucket)
