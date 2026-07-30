"""Sandbox management utilities for the tracker service."""

import asyncio
import shlex
import time
import uuid
from asyncio import Semaphore
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator, assert_never

import logfire
import sentry_sdk
from benchmark_service import (
    ComposeSandbox,
    ComposeSource,
    ExecResult,
    ImageSource,
    Sandbox,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
)
from benchmark_service import (
    Resources as TrackerResources,
)
from benchmark_service.sandbox import SandboxCommandError as ProviderSandboxCommandError
from benchmark_service.sandbox import SandboxError as ProviderSandboxError
from opentelemetry import trace
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
    wait_none,
)

from tracker.aws.s3 import (
    create_presigned_url,
    get_agent_result_s3_key,
    get_benchmark_contract_s3_key,
    upload_stream_to_s3,
    upload_to_s3,
)
from tracker.database.models import (
    MAX_OUTPUT_ARTIFACT_BYTES,
    AgentCausedExitReason,
    AgentContractRequest,
    OutputArtifactSpec,
)
from tracker.exceptions import (
    AgentRunFailedError,
    DependencySetupExhaustedError,
    OutputArtifactError,
    SandboxError,
    SandboxSetupError,
    SSLConnectionError,
)
from tracker.logging import get_logger
from tracker.observability import (
    distribution,
    elapsed_ms,
    incr,
    retry_callback,
    set_sandbox_context,
)
from tracker.types import AWSCredentials

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")
SANDBOX_AUTO_STOP_INTERVAL = 10 * 60
SANDBOX_CREATE_TIMEOUT = 360
AGENT_INSTALL_TIMEOUT_SECONDS = 10 * 60
CONTRACT_DOWNLOAD_URL_EXPIRES_SECONDS = 24 * 60 * 60


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / contract_name


async def delete_sandbox(sandbox: Sandbox, provider: SandboxProvider) -> None:
    """Delete sandbox through its provider."""
    try:
        await provider.delete_sandbox(sandbox.id)
    except SandboxNotFoundError:
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
    except ProviderSandboxError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error deleting sandbox {sandbox.name}: {e}")


def _source_name(source: SandboxSource) -> str:
    match source:
        case ComposeSource(outer=outer):
            return _source_name(outer)
        case ImageSource(image=image):
            return image
        case SnapshotSource() | TargetedSnapshotSource():
            return "snapshot"
        case _:
            assert_never(source)


def _provider_source(source: SandboxSource) -> SandboxSource:
    if isinstance(source, ComposeSource):
        return source.outer
    return source


def runtime_sandbox(sandbox: Sandbox, source: SandboxSource) -> Sandbox:
    if isinstance(source, ComposeSource):
        return ComposeSandbox(sandbox, source)
    return sandbox


def _metric_source_name(source: SandboxSource) -> str:
    image = _source_name(source)
    if image == "snapshot":
        return "snapshot"

    without_digest = image.split("@", maxsplit=1)[0]
    last_slash = without_digest.rfind("/")
    last_colon = without_digest.rfind(":")
    if last_colon > last_slash:
        without_digest = without_digest[:last_colon]

    return without_digest[:80]


def _set_sandbox_create_span_attributes(
    sandbox_name: str,
    source: SandboxSource,
    resources: TrackerResources,
) -> None:
    span = trace.get_current_span()
    span.set_attribute("valkyrie.sandbox_name", sandbox_name)
    span.set_attribute("valkyrie.image", _source_name(source))
    span.set_attribute("valkyrie.resources.vcpu", resources.vcpu)
    span.set_attribute("valkyrie.resources.memory", resources.memory)
    span.set_attribute("valkyrie.resources.disk", resources.disk)


def _set_sandbox_span_attributes(sandbox: Sandbox) -> None:
    span = trace.get_current_span()
    span.set_attribute("valkyrie.sandbox_id", sandbox.id)
    span.set_attribute("valkyrie.sandbox_name", sandbox.name)
    span.set_attribute("valkyrie.sandbox_state", sandbox.state)


@logfire.instrument("sandbox.create", extract_args=False)
async def _create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    source: SandboxSource,
    resources: TrackerResources,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> Sandbox:
    """Create a sandbox through its provider."""
    provider_source = _provider_source(source)
    _set_sandbox_create_span_attributes(sandbox_name, provider_source, resources)
    return await provider.create_sandbox(
        SandboxCreateRequest(
            source=provider_source,
            resources=resources,
            name=sandbox_name,
            labels=labels or {},
            env_vars=env_vars or {},
            auto_stop_interval=SANDBOX_AUTO_STOP_INTERVAL,
            create_timeout=SANDBOX_CREATE_TIMEOUT,
        )
    )


@asynccontextmanager
async def create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    source: SandboxSource,
    resources: TrackerResources,
    creation_semaphore: Semaphore,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> AsyncGenerator[Sandbox, Any]:
    """
    Yeild a sandbox to be used within a context manager.

    Args:
        provider: The sandbox provider
        sandbox_name: The name of the sandbox
        source: The sandbox source image or snapshot
        resources: The resources to use for the sandbox
        labels: The labels to use for the sandbox
        env_vars: The environment variables to use for the sandbox
        creation_semaphore: Per-benchmark semaphore to limit concurrent sandbox creation.

    Returns:
        A context manager that yields the sandbox
    """
    sandbox_name = f"{sandbox_name}_{uuid.uuid4().hex[:6]}"
    source_name = _source_name(source)
    logger.info(f"Creating sandbox {sandbox_name} with source {source_name}")

    # If we run too many at once it can cause hanging issues
    # NOTE does not block how many context managers we can have open, just how many sandboxes we can create at once
    try:
        async with creation_semaphore:
            start = time.monotonic()
            creation_task = asyncio.create_task(
                _create_sandbox(provider, sandbox_name, source, resources, labels, env_vars)
            )
            try:
                sandbox = await asyncio.shield(creation_task)
            except asyncio.CancelledError:
                sandbox = await creation_task
                await delete_sandbox(sandbox, provider)
                raise
    except Exception as e:
        incr("valkyrie.sandbox.create.errors", tags={"error_class": type(e).__name__})
        raise

    distribution(
        "valkyrie.sandbox.create.duration",
        time.monotonic() - start,
        tags={"image": _metric_source_name(source)},
    )
    set_sandbox_context(sandbox, image=source_name)

    try:
        yield sandbox
    except Exception as e:
        logger.error(f"Error during sandbox execution {sandbox.name}: {e}")
        raise
    finally:
        await delete_sandbox(sandbox, provider)


@retry(
    retry=retry_if_exception_type(SandboxError) & retry_if_not_exception_type(SandboxSetupError),
    reraise=True,
    stop=stop_after_attempt(3),
    before_sleep=retry_callback("valkyrie.sandbox.upload"),
)
async def upload_agent_artifacts(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    benchmark_id: str,
    aws: AWSCredentials,
    s3_bucket: str,
) -> None:
    """
    Download and extract the agent contract zip directly inside the sandbox. We generate a presigned S3 URL and have the sandbox curl + unzip it directly.

    Reads from benchmarks/<benchmark_id>/<name>.zip so edits to the shared agent don't affect runs in flight.

    Args:
        sandbox: The sandbox to download and extract files in
        contract: The agent contract configuration
        benchmark_id: The benchmark run id, used to locate the agent
        aws: AWS credentials for presigned URL generation
        s3_bucket: S3 bucket name

    Raises:
        SandboxError: If download or extraction fails inside the sandbox
    """
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = get_benchmark_contract_s3_key(benchmark_id, contract.name)
    presigned_url = await create_presigned_url(
        contract_s3_key, aws, s3_bucket, expiration=CONTRACT_DOWNLOAD_URL_EXPIRES_SECONDS
    )

    zip_path = shlex.quote(f"/tmp/{contract.name}.zip")
    contract_dir = shlex.quote(str(bundle_path / contract.name))
    bundle_dir = shlex.quote(str(bundle_path))
    quoted_url = shlex.quote(presigned_url)

    # Install required dependencies inside of the instance
    # Tracks if curl or unzip are missing and installs them if needed
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
        f"unzip -o -d {bundle_dir} {zip_path}",
        f"rm -f {zip_path}",
        f"mkdir -p {contract_dir}",
    ]

    script = " && ".join(steps)

    try:
        result = await _exec(sandbox, script)
    except Exception as e:
        raise SandboxError(f"Failed to upload contract {contract.name} to sandbox {sandbox.name}: {e}") from e

    error_message: str = (
        f"Failed to upload contract {contract.name} to sandbox {sandbox.name}: "
        f"Command failed with exit code {result.exit_code}: {result.stdout}"
    )
    if result.exit_code == 35:
        raise SSLConnectionError(error_message)

    if result.exit_code != 0:
        raise SandboxError(error_message)


class DependencySetupMode(str, Enum):
    """Select whether dependency setup can retry inside the current sandbox."""

    IN_PLACE_RETRIES = "in_place_retries"
    FINAL_FRESH_SANDBOX = "final_fresh_sandbox"


async def _install_agent_dependencies_once(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
) -> None:
    """Run one bounded dependency installation attempt."""
    if not contract.install_cmd:
        return

    log_output(f"Installing dependencies for contract: {contract.name}")

    contract_path = get_contract_path(contract.name)
    install_cmd = f"timeout {AGENT_INSTALL_TIMEOUT_SECONDS:g} sh -c {shlex.quote(contract.install_cmd)}"

    exit_reason, _duration = await stream_command_output(
        sandbox,
        f"cd {shlex.quote(str(contract_path))} && {install_cmd}",
        log_output,
    )
    if exit_reason == AgentCausedExitReason.TIMEOUT:
        raise SandboxError(
            f"Dependency installation for contract {contract.name} timed out after "
            f"{AGENT_INSTALL_TIMEOUT_SECONDS:g} seconds"
        )

    log_output(f"Finished installing dependencies for contract: {contract.name}")


@retry(
    retry=retry_if_exception_type(SandboxError),
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_chain(wait_none(), wait_fixed(10), wait_fixed(60)),
    before_sleep=retry_callback("valkyrie.sandbox.deps"),
)
async def _install_agent_dependencies_with_retries(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
) -> None:
    await _install_agent_dependencies_once(sandbox, contract, log_output)


async def install_agent_dependencies(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
    mode: DependencySetupMode = DependencySetupMode.IN_PLACE_RETRIES,
) -> None:
    """Install dependencies using the policy selected for this sandbox."""
    if mode is DependencySetupMode.FINAL_FRESH_SANDBOX:
        await _install_agent_dependencies_once(sandbox, contract, log_output)
        return

    try:
        await _install_agent_dependencies_with_retries(sandbox, contract, log_output)
    except SandboxError as error:
        raise DependencySetupExhaustedError(
            f"Dependency installation for contract {contract.name} failed after 4 attempts"
        ) from error


# NOTE: If this gets too big move it into a mapping
# these are decoupled since its just 2 exit codes we need to track
_TIMEOUT_EXIT_CODE: int = 124
_OS_KILL_EXIT_CODE: int = 137
_SUCCESS_EXIT_CODE: int = 0
_STATUS_DIR = "/tmp/.valkyrie"
_OUTPUT_TAIL_MAX_CHARS = 64 * 1024
_EGRESS_RETRY = retry(
    retry=retry_if_exception_type(ProviderSandboxError) & retry_if_not_exception_type(SandboxNotFoundError),
    reraise=True,
    stop=stop_after_attempt(3),
    before_sleep=retry_callback("valkyrie.sandbox.egress"),
)


@logfire.instrument("sandbox.exec", extract_args=False)
async def _exec(sandbox: Sandbox, command: str) -> ExecResult:
    _set_sandbox_span_attributes(sandbox)
    try:
        return await sandbox.exec(command)
    except SandboxNotFoundError:
        raise
    except ProviderSandboxError as e:
        raise SandboxError(str(e)) from e


@_EGRESS_RETRY
async def _run_egress_operation(operation: Callable[[], Awaitable[None]]) -> None:
    await operation()


async def _apply_egress_allowlist(sandbox: Sandbox, allowed_addresses: list[str]) -> None:
    try:
        await _run_egress_operation(lambda: sandbox.modify_egress_rules(allowed_addresses))
    except SandboxNotFoundError:
        raise
    except ValueError as e:
        raise SandboxSetupError(f"Failed to apply egress rules: {e}") from e
    except ProviderSandboxError as e:
        raise SandboxError(str(e)) from e


async def _clear_egress_allowlist(sandbox: Sandbox, fail_on_error: bool) -> None:
    try:
        # Clearing restores unrestricted egress because sandboxes have no baseline restriction today.
        await _run_egress_operation(sandbox.clear_egress_rules)
    except Exception as e:
        logger.warning("failed to clear egress rules for sandbox %s", sandbox.id, exc_info=True)
        if fail_on_error:
            raise SandboxSetupError("Failed to clear egress rules") from e


async def _stream_command_output_with_egress_allowlist(
    sandbox: Sandbox,
    command: str,
    on_output: Callable[[str], None],
    allowed_addresses: list[str],
) -> tuple[AgentCausedExitReason | None, float]:
    if not allowed_addresses:
        return await stream_command_output(sandbox, command, on_output)

    command_completed = False
    try:
        await _apply_egress_allowlist(sandbox, allowed_addresses)
        result = await stream_command_output(sandbox, command, on_output)
        command_completed = True
        return result
    finally:
        # After a clean agent run, stale egress rules would affect evaluation; otherwise preserve the original error.
        await _clear_egress_allowlist(sandbox, fail_on_error=command_completed)


async def stream_command_output(
    sandbox: Sandbox,
    command: str,
    on_output: Callable[[str], None],
) -> tuple[AgentCausedExitReason | None, float]:
    # Bounded tail of recent output, kept only for error messages; capped by characters
    # rather than chunk count since a single chunk can be arbitrarily large.
    output: deque[str] = deque()
    output_chars = 0
    run_id = uuid.uuid4().hex
    start_ns_path = f"{_STATUS_DIR}/{run_id}.start_ns"
    end_ns_path = f"{_STATUS_DIR}/{run_id}.end_ns"
    # Timing is embedded in the sandbox command so it excludes the tracker->sandbox
    # request round-trip and program cold-start (~5-6s), keeping the measurement accurate.
    timed_command = (
        f"mkdir -p {shlex.quote(_STATUS_DIR)}"
        f" && date +%s%N > {shlex.quote(start_ns_path)}"
        f"; {command}"
        f"; exit_code=$?"
        f"; date +%s%N > {shlex.quote(end_ns_path)}"
        f'; sh -c "exit $exit_code"'
    )

    exit_code = _SUCCESS_EXIT_CODE
    monotonic_start = time.monotonic()
    try:
        try:
            async for data in sandbox.command(timed_command):
                on_output(data)
                output.append(data)
                output_chars += len(data)
                while output_chars > _OUTPUT_TAIL_MAX_CHARS and len(output) > 1:
                    output_chars -= len(output.popleft())
        except ProviderSandboxCommandError as e:
            exit_code = e.exit_code
        except SandboxNotFoundError:
            raise
        except ProviderSandboxError as e:
            raise SandboxError(str(e)) from e

        # Prefer the sandbox-measured duration; fall back to the tracker-side monotonic
        # duration if the timing files are missing/unparseable (e.g. the agent removed
        # /tmp/.valkyrie), rather than crashing on `int()` of a `cat` error string.
        duration = await _read_sandbox_duration(
            sandbox, start_ns_path, end_ns_path, fallback=time.monotonic() - monotonic_start
        )

        if exit_code == _SUCCESS_EXIT_CODE:
            return None, duration
        if exit_code == _TIMEOUT_EXIT_CODE:
            return AgentCausedExitReason.TIMEOUT, duration
        if exit_code == _OS_KILL_EXIT_CODE:
            return AgentCausedExitReason.OS_KILLED, duration

        tail = "".join(output).strip().splitlines()
        recent = "\n".join(tail[-10:]) if tail else "(no output)"
        sentry_sdk.set_tag("agent_exit_code", str(exit_code))
        raise AgentRunFailedError(f"Failed to run command {command}, exit code: {exit_code}\nLast output:\n{recent}")
    finally:
        try:
            await _exec(sandbox, f"rm -f {shlex.quote(start_ns_path)} {shlex.quote(end_ns_path)}")
        except Exception:
            pass


async def _read_sandbox_duration(sandbox: Sandbox, start_ns_path: str, end_ns_path: str, fallback: float) -> float:
    """Read the sandbox-side command duration, degrading to ``fallback`` on any failure."""
    start_result = await _exec(sandbox, f"cat {shlex.quote(start_ns_path)}")
    end_result = await _exec(sandbox, f"cat {shlex.quote(end_ns_path)}")
    if start_result.exit_code != _SUCCESS_EXIT_CODE or end_result.exit_code != _SUCCESS_EXIT_CODE:
        return fallback
    try:
        return (int(end_result.stdout.strip()) - int(start_result.stdout.strip())) / 1e9
    except ValueError:
        return fallback


@logfire.instrument(
    "agent_output.archive_and_upload",
    extract_args=("output_path", "agent_output_s3_key", "benchmark_id", "task_id"),
)
async def archive_and_upload_output(
    sandbox: Sandbox,
    output_path: str,
    agent_output_s3_key: str,
    aws: AWSCredentials,
    s3_bucket: str,
    *,
    benchmark_id: str | None = None,
    task_id: str | None = None,
    execution_is_current: Callable[[], bool] | None = None,
) -> None:
    """Compress a file in the sandbox into a tar.gz and upload it to S3"""
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"
    start = time.monotonic()

    tar_result = await _exec(sandbox, f"tar -czf {shlex.quote(archive_path)} {shlex.quote(output_path)}")
    if tar_result.exit_code != 0:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        archive_bytes = await upload_stream_to_s3(
            sandbox.stream_download(archive_path),
            agent_output_s3_key,
            aws,
            s3_bucket,
            should_continue=execution_is_current,
        )

        logger.info(
            "agent_output.archive_and_upload.complete",
            extra={
                "sandbox_id": sandbox.id,
                "sandbox_name": sandbox.name,
                "output_path": output_path,
                "s3_key": agent_output_s3_key,
                "benchmark_id": benchmark_id,
                "task_id": task_id,
                "archive_bytes": archive_bytes,
                "duration_ms": elapsed_ms(start),
            },
        )
    finally:
        # `-f` exits silently if the file does not exist
        try:
            await _exec(sandbox, f"rm -f {shlex.quote(archive_path)}")
        except Exception:
            pass


OUTPUT_ARTIFACTS_SANDBOX_ROOT = PurePosixPath("/tmp/valkyrie")
OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES = 50 * 1024 * 1024


def _output_artifact_path(artifact: OutputArtifactSpec) -> str:
    return artifact if isinstance(artifact, str) else artifact.path


def _output_artifact_is_required(artifact: OutputArtifactSpec) -> bool:
    return isinstance(artifact, str) or artifact.required


def _output_artifact_source(artifact: OutputArtifactSpec) -> str:
    artifact_path = _output_artifact_path(artifact)
    source = artifact.source if not isinstance(artifact, str) else None
    return source or str(OUTPUT_ARTIFACTS_SANDBOX_ROOT / artifact_path)


def _format_output_artifact_source(source: str, task_id: str) -> str:
    return source.replace("{task_id}", task_id)


def _has_glob(source: str) -> bool:
    return any(char in source for char in "*?[")


def _find_root_for_glob(source: str) -> str:
    glob_indices = [source.find(char) for char in "*?[" if source.find(char) != -1]
    first_glob_index = min(glob_indices)
    root = source[:first_glob_index].rsplit("/", 1)[0]
    if not root or root == "/":
        raise OutputArtifactError(f"Output artifact glob source must include a non-root directory prefix: {source}")
    return root


async def _resolve_output_artifact_sandbox_path(sandbox: Sandbox, artifact: OutputArtifactSpec, task_id: str) -> str:
    source = _format_output_artifact_source(_output_artifact_source(artifact), task_id)
    if _has_glob(source):
        find_root = _find_root_for_glob(source)
        find_command = f"find {shlex.quote(find_root)} -type f -path {shlex.quote(source)} | sort | head -n 1"
        source_result = await _exec(sandbox, find_command)
        source_path = source_result.stdout.strip()
        if source_result.exit_code == _SUCCESS_EXIT_CODE and source_path:
            return source_path
    else:
        quoted_source = shlex.quote(source)
        exists_command = f"test -f {quoted_source}"
        if not _output_artifact_is_required(artifact):
            exists_command += f" && ! test -L {quoted_source}"
        exists = await _exec(sandbox, exists_command)
        if exists.exit_code == _SUCCESS_EXIT_CODE:
            return source

    artifact_label = "Required output artifact" if _output_artifact_is_required(artifact) else "Output artifact"
    raise OutputArtifactError(f"{artifact_label} missing: {source}")


async def upload_output_artifacts(
    sandbox: Sandbox,
    artifacts: list[OutputArtifactSpec],
    benchmark_id: str,
    task_id: str,
    aws: AWSCredentials,
    s3_bucket: str,
    execution_is_current: Callable[[], bool] | None = None,
) -> None:
    """Upload declared small output artifacts from the sandbox directly to task S3 keys."""
    total_bytes = 0
    required_artifacts = [artifact for artifact in artifacts if _output_artifact_is_required(artifact)]
    optional_artifacts = [artifact for artifact in artifacts if not _output_artifact_is_required(artifact)]

    for artifact in [*required_artifacts, *optional_artifacts]:
        artifact_path = _output_artifact_path(artifact)
        try:
            updated_total_bytes = await _upload_output_artifact(
                sandbox,
                artifact,
                benchmark_id,
                task_id,
                aws,
                s3_bucket,
                total_bytes,
                execution_is_current,
            )
            if updated_total_bytes is None:
                return
            total_bytes = updated_total_bytes
        except Exception:
            if _output_artifact_is_required(artifact):
                raise
            logger.warning(
                "output_artifact.optional_skip",
                extra={
                    "sandbox_id": sandbox.id,
                    "sandbox_name": sandbox.name,
                    "artifact_path": artifact_path,
                    "benchmark_id": benchmark_id,
                    "task_id": task_id,
                },
                exc_info=True,
            )


async def _upload_output_artifact(
    sandbox: Sandbox,
    artifact: OutputArtifactSpec,
    benchmark_id: str,
    task_id: str,
    aws: AWSCredentials,
    s3_bucket: str,
    total_bytes: int,
    execution_is_current: Callable[[], bool] | None = None,
) -> int | None:
    artifact_path = _output_artifact_path(artifact)
    sandbox_path = await _resolve_output_artifact_sandbox_path(sandbox, artifact, task_id)
    quoted_path = shlex.quote(sandbox_path)

    size_result = await _exec(sandbox, f"stat -c%s {quoted_path}")
    if size_result.exit_code != _SUCCESS_EXIT_CODE:
        raise OutputArtifactError(f"Failed to stat output artifact: {sandbox_path}")

    try:
        artifact_bytes = int(size_result.stdout.strip())
    except ValueError as e:
        raise OutputArtifactError(
            f"Failed to parse output artifact size for {sandbox_path}: {size_result.stdout!r}"
        ) from e

    if artifact_bytes > MAX_OUTPUT_ARTIFACT_BYTES:
        raise OutputArtifactError(
            f"Output artifact {sandbox_path} is too large: {artifact_bytes} bytes > {MAX_OUTPUT_ARTIFACT_BYTES} bytes"
        )

    new_total_bytes = total_bytes + artifact_bytes
    if new_total_bytes > OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES:
        raise OutputArtifactError(
            f"Output artifacts are too large: {new_total_bytes} bytes > {OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES} bytes"
        )

    if execution_is_current is not None and not execution_is_current():
        return None

    s3_key = get_agent_result_s3_key(benchmark_id, task_id, artifact_path)
    file_content = await sandbox.download_file(sandbox_path)
    if execution_is_current is not None and not execution_is_current():
        return None
    await upload_to_s3(file_content, s3_key, aws, s3_bucket)

    logger.info(
        "output_artifact.upload.complete",
        extra={
            "sandbox_id": sandbox.id,
            "sandbox_name": sandbox.name,
            "sandbox_path": sandbox_path,
            "s3_key": s3_key,
            "benchmark_id": benchmark_id,
            "task_id": task_id,
            "artifact_bytes": artifact_bytes,
        },
    )
    return new_total_bytes


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
    benchmark_id: str | None = None,
    runtime_source: SandboxSource | None = None,
    dependency_setup_mode: DependencySetupMode = DependencySetupMode.IN_PLACE_RETRIES,
    execution_is_current: Callable[[], bool] | None = None,
) -> tuple[AgentCausedExitReason | None, float]:
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
        runtime_source: Optional source used to adapt agent commands to the task runtime
        execution_is_current: Optional execution-authority check before output uploads

    Returns:
        AgentCausedExitReason if the agent was terminated abnormally but recoverably
        (timeout or OS kill), None on clean exit.

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    log_output(f"Running agent {contract.name}")
    if runtime_source is not None:
        sandbox = runtime_sandbox(sandbox, runtime_source)

    await install_agent_dependencies(sandbox, contract, log_output, dependency_setup_mode)

    run_cmd = contract.run_cmd.replace("{problem_statement_path}", problem_path).replace("{task_id}", task_id)

    for kwarg_key, kwarg_value in contract.kwargs.items():
        run_cmd = run_cmd.replace(f"{{{kwarg_key}}}", kwarg_value)

    # Apply timeout if specified
    if agent_timeout is not None:
        run_cmd = f"timeout {agent_timeout:g} sh -c {shlex.quote(run_cmd)}"

    # Create cwd if it does not already exist
    await _exec(sandbox, f"mkdir -p {shlex.quote(cwd)}")

    # Run the agent without including task directory dependencies
    exit_reason, agent_run_time = await _stream_command_output_with_egress_allowlist(
        sandbox,
        f"cd {shlex.quote(cwd)} && PYTHONSAFEPATH=1 {run_cmd}",
        log_output,
        contract.egress_allowlist,
    )

    if exit_reason == AgentCausedExitReason.TIMEOUT:
        log_output(
            f"[WARNING]:`{contract.name}` has reached the designated timeout provided by the benchmark service for this task: `{agent_timeout}`. The process has been terminated and evaluation will proceed."
        )
    elif exit_reason == AgentCausedExitReason.OS_KILLED:
        log_output(
            f"[WARNING]:`{contract.name}` was killed by the OS (exit code {_OS_KILL_EXIT_CODE}, likely out-of-memory). The process has been terminated and evaluation will proceed."
        )

    # Upload any output from the agent to S3
    if contract.final_output:
        result = await _exec(sandbox, f"test -e {shlex.quote(contract.final_output)}")
        if (
            result.exit_code == _SUCCESS_EXIT_CODE
            and agent_output_s3_key
            and (execution_is_current is None or execution_is_current())
        ):
            await archive_and_upload_output(
                sandbox,
                contract.final_output,
                agent_output_s3_key,
                aws,
                s3_bucket,
                benchmark_id=benchmark_id,
                task_id=task_id,
                execution_is_current=execution_is_current,
            )

    if contract.output_artifacts:
        if benchmark_id is None:
            raise SandboxError("benchmark_id is required to upload output artifacts")
        await upload_output_artifacts(
            sandbox,
            contract.output_artifacts,
            benchmark_id,
            task_id,
            aws,
            s3_bucket,
            execution_is_current,
        )

    # Return why the agent terminated abnormally, or None on clean exit
    return exit_reason, agent_run_time
