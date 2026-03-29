"""Sandbox management utilities for the tracker service."""

import asyncio
import base64
import io
import shlex
import uuid
import zipfile
from asyncio import Semaphore
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator

from benchmark_service.schemas import Resources as TrackerResources
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaNotFoundError,
    FileUpload,
    Resources,
    SandboxState,
    SessionExecuteRequest,
)
from daytona.common.errors import DaytonaError
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry,
    retry_if_exception,
    retry_if_exception_type,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_fixed,
)

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


_SANDBOX_CREATION_CAP: int = 10
_sandbox_creation_semaphore = Semaphore(_SANDBOX_CREATION_CAP)


@retry(
    retry=retry_if_not_exception_type(InvalidSandboxConfigurationError),
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

    if image.startswith(SNAPSHOT_IMAGE_PREFIX):
        snapshot_name = image[len(SNAPSHOT_IMAGE_PREFIX) :].strip()
        if not snapshot_name:
            raise InvalidSandboxConfigurationError("Snapshot-based sandbox requested without a snapshot name")

        return await daytona.create(
            CreateSandboxFromSnapshotParams(
                auto_delete_interval=60,
                name=sandbox_name,
                labels=labels,
                snapshot=snapshot_name,
                language="python",
                network_block_all=False,
                env_vars=env_vars,
            ),
            timeout=360,
        )

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


async def upload_agent_artifacts(
    sandbox: AsyncSandbox, contract: AgentContractRequest, aws: AWSCredentials, s3_bucket: str
) -> None:
    """
    Upload contract from S3 to the sandbox.

    Args:
        sandbox: The sandbox to upload files to
        contract_name: Name of the contract (without extension)
        harness_config: Harness config for S3 access

    Raises:
        SandboxError: If directory creation or file upload fails
    """
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = get_contract_s3_key(contract.name)
    contract_content = download_from_s3(contract_s3_key, aws, s3_bucket)

    # Unzip contract and collect files and directories, excluding contract.py
    with zipfile.ZipFile(io.BytesIO(contract_content), "r") as zip_ref:
        files_to_upload = [
            FileUpload(
                source=zip_ref.read(file_info),
                destination=str(bundle_path / file_info.filename),
            )
            for file_info in zip_ref.infolist()
            if not file_info.is_dir() and not file_info.filename.endswith("contract.py")
        ]

    if files_to_upload:
        await sandbox.fs.upload_files(files_to_upload)
    else:
        await sandbox.fs.create_folder(str(bundle_path / contract.name), "755")


@retry(retry=retry_if_exception_type(SandboxError), reraise=True, stop=stop_after_attempt(3))
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


LOG_STREAM_RETRY_DELAY_SECONDS = 1.0
WEBSOCKET_STREAM_ERRORS = ("websocket", "http 502", "opening handshake", "no close frame", "1011")


def is_websocket_stream_error(error: BaseException) -> bool:
    return isinstance(error, DaytonaError) and any(pattern in str(error).lower() for pattern in WEBSOCKET_STREAM_ERRORS)


async def start_session_command(
    sandbox: AsyncSandbox,
    session_id: str,
    command: str,
) -> str:
    session_exec_resp = await sandbox.process.execute_session_command(
        session_id, SessionExecuteRequest(command=command, run_async=True)
    )
    if not session_exec_resp.cmd_id:
        raise SandboxError(f"Failed to execute command {command} in session {session_id}")
    return session_exec_resp.cmd_id


async def stream_session_command_logs(
    sandbox: AsyncSandbox,
    *,
    session_id: str,
    command_id: str,
    on_output: Callable[[str], None],
) -> None:
    try:
        async for attempt in AsyncRetrying(
            retry=retry_if_exception(is_websocket_stream_error),
            stop=stop_after_attempt(10),
            wait=wait_fixed(LOG_STREAM_RETRY_DELAY_SECONDS),
            before_sleep=before_sleep_log(logger, logger.level),
            reraise=True,
        ):
            with attempt:
                await sandbox.process.get_session_command_logs_async(
                    session_id=session_id,
                    command_id=command_id,
                    on_stdout=on_output,
                    on_stderr=on_output,
                )
        return
    except Exception as error:
        warning = f"Log streaming unavailable; continuing without live logs: {error}"
        logger.warning(warning)
        on_output(f"[WARNING] {warning}\n")


async def stream_command_output(
    sandbox: AsyncSandbox,
    command: str,
    on_output: Callable[[str], None],
) -> None:
    """
    Execute a command inside of a sandbox using a session and stream the output to the given callbacks.
    Log streaming is best-effort. Command status is checked once before cleanup.
    """
    session_id = f"{sandbox.id}:{str(uuid.uuid4())}"
    log_task: asyncio.Task[None] | None = None
    try:
        await sandbox.process.create_session(session_id)
        cmd_id = await start_session_command(
            sandbox,
            session_id,
            command,
        )
        log_task = asyncio.create_task(
            stream_session_command_logs(
                sandbox,
                session_id=session_id,
                command_id=cmd_id,
                on_output=on_output,
            )
        )
        if log_task is not None and not log_task.done():
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(log_task), timeout=1)

        try:
            cmd = await sandbox.process.get_session_command(session_id, cmd_id)

            # Exit code 124 = timeout(1) killed the process; treat as success so evaluation still runs
            if cmd.exit_code not in (0, 124):
                raise SandboxError(f"Failed to run command {command}, exit code: {cmd.exit_code}")
        except SandboxError:
            raise
        except Exception as error:
            logger.warning(f"Failed to get session command for {session_id}: {error}")

    finally:
        if log_task is not None and not log_task.done():
            log_task.cancel()
            try:
                await log_task
            except asyncio.CancelledError:
                pass
        try:
            await sandbox.process.delete_session(session_id)
        except Exception:
            logger.warning(f"Caught failure to delete session `{session_id}`")
            pass


async def archive_and_upload_output(
    sandbox: AsyncSandbox, output_path: str, agent_output_s3_key: str, aws: AWSCredentials, s3_bucket: str
) -> None:
    """Compress a file in the sandbox into a tar.gz and upload it to S3"""
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"

    tar_result = await sandbox.process.exec(f"tar -czf {shlex.quote(archive_path)} {shlex.quote(output_path)}")
    if tar_result.exit_code != 0:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        b64_result = await sandbox.process.exec(f"base64 {shlex.quote(archive_path)}")
        if b64_result.exit_code != 0:
            raise SandboxError(f"Failed to read archive from {output_path}")

        upload_to_s3(base64.b64decode(b64_result.result), agent_output_s3_key, aws, s3_bucket)
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
        log_output: Callback to log output
        cwd: Working directory to run the agent in
        agent_output_s3_key: S3 key to where we will upload the final output archive to
        agent_timeout: Optional timeout in seconds to enforce on the agent command

    Returns:
        Agent output as a dictionary

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
    await sandbox.process.exec(f"mkdir -p {shlex.quote(cwd)}")

    # Run the agent without including task directory dependencies
    await stream_command_output(sandbox, f"cd {cwd} && PYTHONSAFEPATH=1 {run_cmd}", log_output)

    if not contract.final_output:
        return

    result = await sandbox.process.exec(f"test -e {shlex.quote(contract.final_output)}")
    if result.exit_code != 0:
        return

    if agent_output_s3_key:
        await archive_and_upload_output(sandbox, contract.final_output, agent_output_s3_key, aws, s3_bucket)
