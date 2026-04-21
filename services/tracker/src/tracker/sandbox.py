"""Provider-generic sandbox utilities for tracker runs."""

import logging
import shlex
import uuid
from asyncio import Semaphore
from collections import deque
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator

from benchmark_service.sandbox import (
    ExecResult,
    ImageSandboxCreateRequest,
    Sandbox,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxResources,
    SnapshotSandboxCreateRequest,
)
from benchmark_service.sandbox import SandboxError as ProviderSandboxError
from benchmark_service.schemas import Resources as TrackerResources
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from tracker.database.models import AgentCausedExitReason, AgentContractRequest
from tracker.exceptions import InvalidSandboxConfigurationError, SandboxError
from tracker.logging import get_logger
from tracker.s3 import create_presigned_url, get_contract_s3_key, upload_to_s3
from tracker.types import AWSCredentials

logger = get_logger(__name__)

bundle_path = PurePosixPath("/bundle")
SNAPSHOT_IMAGE_PREFIX = "snapshot:"
_TIMEOUT_EXIT_CODE = 124
_OS_KILL_EXIT_CODE = 137
_SUCCESS_EXIT_CODE = 0


def get_contract_path(contract_name: str) -> PurePosixPath:
    return bundle_path / contract_name


def _resources(resources: TrackerResources) -> SandboxResources:
    return SandboxResources(cpu=resources.vcpu, memory=resources.memory, disk=resources.disk)


@retry(
    retry=retry_if_not_exception_type(InvalidSandboxConfigurationError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=5, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def _create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
    labels: dict[str, str],
    env_vars: dict[str, str],
) -> Sandbox:
    try:
        sandbox = await provider.get_sandbox(sandbox_name)
        await sandbox.wait_until_ready()
        return sandbox
    except SandboxNotFoundError:
        pass

    sandbox_resources = _resources(resources)
    if image.startswith(SNAPSHOT_IMAGE_PREFIX):
        snapshot = image[len(SNAPSHOT_IMAGE_PREFIX) :].strip()
        if not snapshot:
            raise InvalidSandboxConfigurationError("Snapshot-based sandbox requested without a snapshot name")
        return await provider.create_sandbox(
            SnapshotSandboxCreateRequest(
                snapshot=snapshot,
                name=sandbox_name,
                labels=labels,
                env_vars=env_vars,
                resources=sandbox_resources,
                auto_delete_interval=60,
                creation_timeout=360,
            )
        )

    return await provider.create_sandbox(
        ImageSandboxCreateRequest(
            image=image,
            name=sandbox_name,
            labels=labels,
            env_vars=env_vars,
            resources=sandbox_resources,
            auto_delete_interval=60,
            creation_timeout=360,
        )
    )


@asynccontextmanager
async def create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
    creation_semaphore: Semaphore,
    labels: dict[str, str],
    env_vars: dict[str, str],
) -> AsyncGenerator[Sandbox, Any]:
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    async with creation_semaphore:
        sandbox = await _create_sandbox(provider, sandbox_name, image, resources, labels, env_vars)

    try:
        yield sandbox
    finally:
        await provider.delete_sandbox(sandbox)


@retry(
    retry=retry_if_exception_type(ProviderSandboxError), reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(2)
)
async def _exec(sandbox: Sandbox, command: str) -> ExecResult:
    return await sandbox.exec(command)


def _tail(result: ExecResult, output: deque[str]) -> str:
    lines = "".join(output).strip().splitlines()
    if not lines:
        lines = f"{result.stdout}{result.stderr}".strip().splitlines()
    return "\n".join(lines[-10:]) or "(no output)"


@retry(retry=retry_if_exception_type(SandboxError), reraise=True, stop=stop_after_attempt(3))
async def upload_agent_artifacts(
    sandbox: Sandbox, contract: AgentContractRequest, aws: AWSCredentials, s3_bucket: str
) -> None:
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = get_contract_s3_key(contract.name)
    presigned_url = create_presigned_url(contract_s3_key, aws, s3_bucket)

    zip_path = shlex.quote(f"/tmp/{contract.name}.zip")
    contract_dir = shlex.quote(str(bundle_path / contract.name))
    bundle_dir = shlex.quote(str(bundle_path))
    quoted_url = shlex.quote(presigned_url)

    install_deps = (
        "NEED='';"
        " command -v curl >/dev/null 2>&1 || NEED='curl';"
        ' command -v unzip >/dev/null 2>&1 || NEED="$NEED unzip";'
        ' if [ -n "$NEED" ]; then'
        "  if command -v apt-get >/dev/null 2>&1; then DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq $NEED;"
        "  elif command -v apk >/dev/null 2>&1; then apk add --no-cache $NEED;"
        "  elif command -v yum >/dev/null 2>&1; then yum install -y $NEED;"
        "  elif command -v dnf >/dev/null 2>&1; then dnf install -y $NEED;"
        "  elif command -v pacman >/dev/null 2>&1; then pacman -Sy --noconfirm $NEED;"
        "  elif command -v zypper >/dev/null 2>&1; then zypper install -y $NEED;"
        "  fi;"
        " fi"
    )
    steps = [
        install_deps,
        f"curl -sfL -o {zip_path} {quoted_url}",
        f"mkdir -p {bundle_dir}",
        f"unzip -o -d {bundle_dir} {zip_path} -x contract.py",
        f"rm -f {zip_path}",
        f"mkdir -p {contract_dir}",
    ]
    result = await _exec(sandbox, " && ".join(steps))
    if result.exit_code != _SUCCESS_EXIT_CODE:
        raise SandboxError(f"Failed to upload contract {contract.name}: {result.stdout}{result.stderr}")


@retry(retry=retry_if_exception_type(SandboxError), reraise=True, stop=stop_after_attempt(3))
async def install_agent_dependencies(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
) -> None:
    if not contract.install_cmd:
        return

    log_output(f"Installing dependencies for contract: {contract.name}")
    contract_path = get_contract_path(contract.name)
    exit_reason = await stream_command_output(
        sandbox, f"cd {shlex.quote(str(contract_path))} && {contract.install_cmd}", log_output
    )
    if exit_reason is not None:
        raise SandboxError(f"Install command exited with {exit_reason}")
    log_output(f"Finished installing dependencies for contract: {contract.name}")


async def stream_command_output(
    sandbox: Sandbox, command: str, on_output: Callable[[str], None]
) -> AgentCausedExitReason | None:
    output: deque[str] = deque(maxlen=50)

    def collect(data: str) -> None:
        on_output(data)
        output.append(data)

    result = await sandbox.exec(command, on_stdout=collect, on_stderr=collect)
    if result.exit_code == _SUCCESS_EXIT_CODE:
        return None
    if result.exit_code == _TIMEOUT_EXIT_CODE:
        return AgentCausedExitReason.TIMEOUT
    if result.exit_code == _OS_KILL_EXIT_CODE:
        return AgentCausedExitReason.OS_KILLED
    raise SandboxError(
        f"Failed to run command {command}, exit code: {result.exit_code}\nLast output:\n{_tail(result, output)}"
    )


async def archive_and_upload_output(
    sandbox: Sandbox, output_path: str, agent_output_s3_key: str, aws: AWSCredentials, s3_bucket: str
) -> None:
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"
    tar_result = await _exec(sandbox, f"tar -czf {shlex.quote(archive_path)} {shlex.quote(output_path)}")
    if tar_result.exit_code != _SUCCESS_EXIT_CODE:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        await upload_to_s3(await sandbox.download_file(archive_path), agent_output_s3_key, aws, s3_bucket)
    finally:
        await _exec(sandbox, f"rm -f {shlex.quote(archive_path)}")


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
) -> AgentCausedExitReason | None:
    log_output(f"Running agent {contract.name}")

    await install_agent_dependencies(sandbox, contract, log_output)

    run_cmd = contract.run_cmd.replace("{problem_statement_path}", problem_path).replace("{task_id}", task_id)
    if agent_timeout is not None:
        run_cmd = f"timeout {agent_timeout} {run_cmd}"

    await _exec(sandbox, f"mkdir -p {shlex.quote(cwd)}")
    exit_reason = await stream_command_output(
        sandbox, f"cd {shlex.quote(cwd)} && PYTHONSAFEPATH=1 {run_cmd}", log_output
    )

    if exit_reason == AgentCausedExitReason.TIMEOUT:
        log_output(
            f"[WARNING]:`{contract.name}` reached the task timeout `{agent_timeout}`. The process was terminated and evaluation will proceed."
        )
    if exit_reason == AgentCausedExitReason.OS_KILLED:
        log_output(f"[WARNING]:`{contract.name}` was killed by the sandbox OS. Evaluation will proceed.")

    if contract.final_output:
        result = await _exec(sandbox, f"test -e {shlex.quote(contract.final_output)}")
        if result.exit_code == _SUCCESS_EXIT_CODE and agent_output_s3_key:
            await archive_and_upload_output(sandbox, contract.final_output, agent_output_s3_key, aws, s3_bucket)

    return exit_reason
