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
from typing import Any, AsyncGenerator

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

from tracker.agent.schemas import bind_shell_variables, prepare_shell_command
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import (
    copy_s3_object,
    create_presigned_url,
    delete_from_s3,
    get_agent_result_s3_key,
    get_benchmark_contract_s3_key,
    s3_object_exists,
    upload_stream_to_s3,
    upload_to_s3,
    upload_to_s3_if_absent,
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
    S3Error,
    S3ObjectExistsError,
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
from tracker.task_artifacts import (
    MAX_ARTIFACT_FILES,
    MAX_ARTIFACT_INDEX_BYTES,
    MAX_ARTIFACT_PACK_BYTES,
    build_artifact_index,
    serialize_artifact_index,
    task_artifact_generation_key,
    task_artifact_index_key,
)

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
        case SnapshotSource():
            return "snapshot"


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
    aws_runtime: AWSRuntime,
) -> None:
    """
    Download and extract the agent contract zip directly inside the sandbox. We generate a presigned S3 URL and have the sandbox curl + unzip it directly.

    Reads from benchmarks/<benchmark_id>/<name>.zip so edits to the shared agent don't affect runs in flight.

    Args:
        sandbox: The sandbox to download and extract files in
        contract: The agent contract configuration
        benchmark_id: The benchmark run id, used to locate the agent
        aws_runtime: AWS resources and client provider

    Raises:
        SandboxError: If download or extraction fails inside the sandbox
    """
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = get_benchmark_contract_s3_key(benchmark_id, contract.name)
    presigned_url = await create_presigned_url(
        contract_s3_key,
        aws_runtime,
        expiration=CONTRACT_DOWNLOAD_URL_EXPIRES_SECONDS,
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


class ArtifactCollectionMode(str, Enum):
    REQUIRED = "required"
    AVAILABLE = "available"


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
    timed_command = (
        f"mkdir -p {shlex.quote(_STATUS_DIR)}"
        f" && date +%s%N > {shlex.quote(start_ns_path)}"
        f"; {command}"
        f"; exit_code=$?"
        f"; date +%s%N > {shlex.quote(end_ns_path)}"
        f'; sh -c "exit $exit_code"'
    )

    exit_code = _SUCCESS_EXIT_CODE
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

        start_ns = (await _exec(sandbox, f"cat {shlex.quote(start_ns_path)}")).stdout
        end_ns = (await _exec(sandbox, f"cat {shlex.quote(end_ns_path)}")).stdout
        duration = (int(end_ns.strip()) - int(start_ns.strip())) / 1e9

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


@logfire.instrument(
    "agent_output.archive_and_upload",
    extract_args=("output_path", "agent_output_s3_key", "benchmark_id", "task_id"),
)
async def archive_and_upload_output(
    sandbox: Sandbox,
    output_path: str,
    agent_output_s3_key: str,
    aws_runtime: AWSRuntime,
    *,
    benchmark_id: str | None = None,
    task_id: str | None = None,
) -> None:
    """Compress a file in the sandbox into a tar.gz and upload it to S3"""
    await _archive_and_upload_output(
        sandbox,
        output_path,
        agent_output_s3_key,
        aws_runtime,
        benchmark_id=benchmark_id,
        task_id=task_id,
    )


async def _archive_and_upload_output(
    sandbox: Sandbox,
    output_path: str,
    s3_key: str,
    aws_runtime: AWSRuntime,
    *,
    benchmark_id: str | None,
    task_id: str | None,
) -> None:
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"
    start = time.monotonic()

    tar_result = await _exec(
        sandbox,
        f"tar -czf {shlex.quote(archive_path)} -- {shlex.quote(output_path)}",
    )
    if tar_result.exit_code != 0:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        archive_bytes = await upload_stream_to_s3(
            sandbox.stream_download(archive_path),
            s3_key,
            aws_runtime,
        )
        logger.info(
            "agent_output.archive_and_upload.complete",
            extra={
                "sandbox_id": sandbox.id,
                "sandbox_name": sandbox.name,
                "output_path": output_path,
                "s3_key": s3_key,
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


async def upload_task_artifacts(
    sandbox: Sandbox,
    final_output: str | None,
    artifacts: list[OutputArtifactSpec],
    benchmark_id: str,
    task_id: str,
    attempt_id: str,
    aws_runtime: AWSRuntime,
    mode: ArtifactCollectionMode,
) -> None:
    """Upload an immutable per-attempt archive, content pack, and index."""
    index_key = task_artifact_index_key(benchmark_id, task_id, attempt_id)
    if await s3_object_exists(index_key, aws_runtime):
        return

    generation = uuid.uuid4().hex
    archive_key = (
        task_artifact_generation_key(
            benchmark_id,
            task_id,
            attempt_id,
            generation,
            "agent_output.tar.gz",
        )
        if final_output
        else None
    )
    pack_key = task_artifact_generation_key(
        benchmark_id,
        task_id,
        attempt_id,
        generation,
        "content.pack",
    )
    staging_dir = f"/tmp/valkyrie-artifacts-{uuid.uuid4().hex}"
    pack_path = f"{staging_dir}/content.pack"
    manifest_path = f"{staging_dir}/manifest"
    generation_keys: list[str] = []
    published = False

    try:
        if final_output and archive_key:
            await _reject_artifact_root_symlink(sandbox, final_output)
            await _archive_and_upload_output(
                sandbox,
                final_output,
                archive_key,
                aws_runtime,
                benchmark_id=benchmark_id,
                task_id=task_id,
            )
            generation_keys.append(archive_key)

        if artifacts and mode is ArtifactCollectionMode.REQUIRED:
            await upload_output_artifacts(
                sandbox,
                artifacts,
                benchmark_id,
                task_id,
                aws_runtime,
            )

        sources: list[tuple[str, str]] = []
        if final_output:
            final_output = str(PurePosixPath(final_output))
            is_directory = (await _exec(sandbox, f"test -d {shlex.quote(final_output)}")).exit_code == 0
            logical_path = "agent_output" if is_directory else f"agent_output/{PurePosixPath(final_output).name}"
            sources.append((final_output, logical_path))
        for artifact in artifacts:
            sandbox_path = await _resolve_output_artifact_sandbox_path(
                sandbox,
                artifact,
                task_id,
                mode,
            )
            if sandbox_path is None:
                continue
            sources.append((str(PurePosixPath(sandbox_path)), _output_artifact_path(artifact)))

        setup = await _exec(
            sandbox,
            f"mkdir -p {shlex.quote(staging_dir)} && : > {shlex.quote(pack_path)} && : > {shlex.quote(manifest_path)}",
        )
        if setup.exit_code != 0:
            raise SandboxError("Failed to create artifact staging directory")

        file_count = 0
        for source, _ in sources:
            count_result = await _exec(
                sandbox,
                f"find -P -- {shlex.quote(source)} -path '*/.valkyrie' -prune -o -type f -printf . | wc -c",
            )
            if count_result.exit_code != 0:
                raise SandboxError("Failed to enumerate task artifacts")
            file_count += int(count_result.stdout.strip())
        if file_count > MAX_ARTIFACT_FILES:
            raise OutputArtifactError(f"Task artifacts exceed {MAX_ARTIFACT_FILES} files")

        build_script = (
            "source=$1; logical_root=$2; pack=$3; manifest=$4; shift 4; "
            "for file do "
            'if [ "$file" = "$source" ]; then logical=$logical_root; '
            'else relative=${file#"$source"}; relative=${relative#/}; '
            "logical=$logical_root/$relative; fi; "
            'offset=$(stat -c%s -- "$pack") || exit 1; '
            f"remaining=$(({MAX_ARTIFACT_PACK_BYTES} - offset)); "
            'limit=$((remaining + 1)); head -c "$limit" -- "$file" >> "$pack" || exit 1; '
            'end=$(stat -c%s -- "$pack") || exit 1; size=$((end - offset)); '
            '[ "$size" -le "$remaining" ] || exit 1; '
            'printf "%s\\0%s\\0%s\\0" "$logical" "$size" "$offset" >> "$manifest"; '
            "done"
        )
        for source, logical_root in sources:
            build_command = (
                f"find -P -- {shlex.quote(source)} -path '*/.valkyrie' -prune -o -type f"
                f" -exec sh -c {shlex.quote(build_script)} sh"
                f" {shlex.quote(source)} {shlex.quote(logical_root)}"
                f" {shlex.quote(pack_path)} {shlex.quote(manifest_path)} {{}} +"
            )
            if (await _exec(sandbox, build_command)).exit_code != 0:
                raise OutputArtifactError("Failed to build task artifact pack")

        manifest_size_result = await _exec(sandbox, f"stat -c%s -- {shlex.quote(manifest_path)}")
        if manifest_size_result.exit_code != 0:
            raise SandboxError("Failed to stat task artifact manifest")
        if int(manifest_size_result.stdout.strip()) > MAX_ARTIFACT_INDEX_BYTES:
            raise OutputArtifactError("Task artifact manifest is too large")

        pack_size_result = await _exec(sandbox, f"stat -c%s -- {shlex.quote(pack_path)}")
        if pack_size_result.exit_code != 0:
            raise SandboxError("Failed to stat task artifact pack")
        manifest = await sandbox.download_file(manifest_path)
        index = build_artifact_index(
            manifest,
            int(pack_size_result.stdout.strip()),
            generation,
            archive_key is not None,
        )
        index_content = serialize_artifact_index(index)
        pack_size_bytes = await upload_stream_to_s3(
            sandbox.stream_download(pack_path),
            pack_key,
            aws_runtime,
        )
        generation_keys.append(pack_key)
        if pack_size_bytes != index.pack_size_bytes:
            raise OutputArtifactError("Uploaded task artifact pack size changed")
        try:
            await upload_to_s3_if_absent(index_content, index_key, aws_runtime)
        except S3ObjectExistsError:
            return
        published = True

        if archive_key and mode is ArtifactCollectionMode.REQUIRED:
            canonical_archive_key = get_agent_result_s3_key(
                benchmark_id,
                task_id,
                "agent_output.tar.gz",
            )
            try:
                await copy_s3_object(archive_key, canonical_archive_key, aws_runtime)
            except S3Error:
                logger.exception("Failed to refresh canonical task artifact archive")
    finally:
        try:
            await _exec(sandbox, f"rm -rf {shlex.quote(staging_dir)}")
        except Exception:
            pass
        if not published:
            for key in generation_keys:
                try:
                    await delete_from_s3(key, aws_runtime)
                except Exception:
                    logger.exception("Failed to clean unpublished task artifact generation")


OUTPUT_ARTIFACTS_SANDBOX_ROOT = PurePosixPath("/tmp/valkyrie")
OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES = 50 * 1024 * 1024


def _output_artifact_path(artifact: OutputArtifactSpec) -> str:
    return artifact if isinstance(artifact, str) else artifact.path


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


async def _reject_artifact_root_symlink(sandbox: Sandbox, source: str) -> None:
    if (await _exec(sandbox, f"test -L {shlex.quote(source)}")).exit_code == _SUCCESS_EXIT_CODE:
        raise OutputArtifactError(f"Task artifact source cannot be a symlink: {source}")


async def _resolve_output_artifact_sandbox_path(
    sandbox: Sandbox,
    artifact: OutputArtifactSpec,
    task_id: str,
    mode: ArtifactCollectionMode,
) -> str | None:
    source = _format_output_artifact_source(_output_artifact_source(artifact), task_id)
    if _has_glob(source):
        find_root = _find_root_for_glob(source)
        await _reject_artifact_root_symlink(sandbox, find_root)
        find_command = f"find -P {shlex.quote(find_root)} -type f -path {shlex.quote(source)} | sort | head -n 1"
        source_result = await _exec(sandbox, find_command)
        source_path = source_result.stdout.strip()
        if source_result.exit_code == _SUCCESS_EXIT_CODE and source_path:
            return source_path
    else:
        await _reject_artifact_root_symlink(sandbox, source)
        exists = await _exec(sandbox, f"test -f {shlex.quote(source)}")
        if exists.exit_code == _SUCCESS_EXIT_CODE:
            return source

    if mode is ArtifactCollectionMode.AVAILABLE:
        return None
    raise OutputArtifactError(f"Required output artifact missing: {source}")


async def upload_output_artifacts(
    sandbox: Sandbox,
    artifacts: list[OutputArtifactSpec],
    benchmark_id: str,
    task_id: str,
    aws_runtime: AWSRuntime,
) -> None:
    """Upload declared small output artifacts from the sandbox directly to task S3 keys."""
    total_bytes = 0

    for artifact in artifacts:
        artifact_path = _output_artifact_path(artifact)
        sandbox_path = await _resolve_output_artifact_sandbox_path(
            sandbox,
            artifact,
            task_id,
            ArtifactCollectionMode.REQUIRED,
        )
        assert sandbox_path is not None
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

        total_bytes += artifact_bytes
        if total_bytes > OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES:
            raise OutputArtifactError(
                f"Output artifacts are too large: {total_bytes} bytes > {OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES} bytes"
            )

        s3_key = get_agent_result_s3_key(benchmark_id, task_id, artifact_path)
        file_content = await sandbox.download_file(sandbox_path)
        await upload_to_s3(file_content, s3_key, aws_runtime)

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


async def run_agent(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    problem_path: str,
    task_id: str,
    log_output: Callable[[str], None],
    cwd: str,
    aws_runtime: AWSRuntime,
    benchmark_id: str,
    artifact_attempt_id: str,
    agent_timeout: float | None = None,
    runtime_source: SandboxSource | None = None,
    dependency_setup_mode: DependencySetupMode = DependencySetupMode.IN_PLACE_RETRIES,
) -> tuple[AgentCausedExitReason | None, float]:
    """
    Run the agent inside the sandbox for a given task.

    Args:
        sandbox: The sandbox to run the agent in
        contract: The agent contract configuration
        problem_path: Path inside the sandbox where the problem statement file was written during setup
        log_output: Callback to log output
        cwd: Working directory to run the agent in
        agent_timeout: Optional timeout in seconds to enforce on the agent command
        runtime_source: Optional source used to adapt agent commands to the task runtime

    Returns:
        AgentCausedExitReason if the agent was terminated abnormally but recoverably
        (timeout or OS kill), None on clean exit.

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    log_output(f"Running agent {contract.name}")
    if runtime_source is not None:
        sandbox = runtime_sandbox(sandbox, runtime_source)

    runtime_arguments = {
        **contract.kwargs,
        "problem_statement_path": problem_path,
        "task_id": task_id,
    }
    try:
        run_cmd = prepare_shell_command(contract.run_cmd, runtime_arguments)
        run_cmd = bind_shell_variables(run_cmd, runtime_arguments)
    except ValueError as exc:
        raise SandboxError(str(exc)) from exc

    # Apply timeout if specified
    if agent_timeout is not None:
        run_cmd = f"timeout {agent_timeout:g} sh -c {shlex.quote(run_cmd)}"

    async def collect_artifacts(mode: ArtifactCollectionMode) -> None:
        final_output = None
        if contract.final_output:
            result = await _exec(sandbox, f"test -e {shlex.quote(contract.final_output)}")
            if result.exit_code == _SUCCESS_EXIT_CODE:
                final_output = contract.final_output
        if final_output is None and not contract.output_artifacts:
            return
        await upload_task_artifacts(
            sandbox,
            final_output,
            contract.output_artifacts,
            benchmark_id,
            task_id,
            artifact_attempt_id,
            aws_runtime,
            mode,
        )

    try:
        await install_agent_dependencies(sandbox, contract, log_output, dependency_setup_mode)
        await _exec(sandbox, f"mkdir -p {shlex.quote(cwd)}")
        exit_reason, agent_run_time = await _stream_command_output_with_egress_allowlist(
            sandbox,
            f"cd {shlex.quote(cwd)} && PYTHONSAFEPATH=1 {run_cmd}",
            log_output,
            contract.egress_allowlist,
        )
    except (Exception, asyncio.CancelledError):
        try:
            await asyncio.shield(collect_artifacts(ArtifactCollectionMode.AVAILABLE))
        except Exception:
            logger.exception("Failed to preserve artifacts after agent failure")
        raise

    if exit_reason == AgentCausedExitReason.TIMEOUT:
        log_output(
            f"[WARNING]:`{contract.name}` has reached the designated timeout provided by the benchmark service for this task: `{agent_timeout}`. The process has been terminated and evaluation will proceed."
        )
    elif exit_reason == AgentCausedExitReason.OS_KILLED:
        log_output(
            f"[WARNING]:`{contract.name}` was killed by the OS (exit code {_OS_KILL_EXIT_CODE}, likely out-of-memory). The process has been terminated and evaluation will proceed."
        )

    await collect_artifacts(ArtifactCollectionMode.REQUIRED)

    # Return why the agent terminated abnormally, or None on clean exit
    return exit_reason, agent_run_time
