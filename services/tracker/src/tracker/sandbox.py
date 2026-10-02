"""Sandbox management utilities for the tracker service."""

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import shlex
import time
import uuid
from dataclasses import dataclass
from asyncio import Semaphore
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator, Literal, Never, assert_never

import logfire
import sentry_sdk
from benchmark_service import (
    ControlledWorkload,
    ControlledWorkloadResult,
    CreditedGeneration,
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
    VolumeMount,
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

from tracker.runtime.artifacts import benchmark_agent_bundle_key, task_artifact_key
from tracker.runtime.storage import ObjectStore
from tracker.egress import EgressPolicy
from tracker.external_service_gateway import (
    ArbitrationDecision,
    ExternalServiceAccountingSummary,
    ExternalServiceDeadlineController,
)
from tracker.database.models import (
    MAX_OUTPUT_ARTIFACT_BYTES,
    AgentCausedExitReason,
    AgentContractRequest,
    OutputArtifactSpec,
)
from tracker.exceptions import (
    AgentRunFailedError,
    ControlledGenerationError,
    ControlledGenerationTerminationUnconfirmedError,
    DependencySetupExhaustedError,
    GenerationTerminationUnconfirmedError,
    InvalidSandboxConfigurationError,
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

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")
SANDBOX_AUTO_STOP_INTERVAL = 10 * 60
SANDBOX_CREATE_TIMEOUT = 360
AGENT_INSTALL_TIMEOUT_SECONDS = 10 * 60
# The 20-second post-deadline window includes at most 10 seconds for gateway arbitration;
# the remaining time is reserved for confirmed workload termination.
GENERATION_ARBITRATION_GRACE_SECONDS = 10.0
GENERATION_TERMINATION_GRACE_SECONDS = 20.0
EXTERNAL_SERVICE_REFRESH_LEAD_SECONDS = 60.0
EXTERNAL_SERVICE_REFRESH_RETRY_SECONDS = 1.0
CONTRACT_DOWNLOAD_URL_EXPIRES_SECONDS = 24 * 60 * 60
_STAGE_DIR = "/run/valkyrie-stage"
_STAGE_PREFIX = "VALKYRIE-STAGE/1 "
# Compact JSON with a 255-character Docker name and a 20-digit sequence, base64url, and a 64-hex MAC fits.
_STAGE_MAX_FRAME_LENGTH = 512
_STAGE_FRAME = re.compile(r"VALKYRIE-STAGE/1 ([A-Za-z0-9_-]+) ([0-9a-f]{64})")


@dataclass(frozen=True)
class _StageFrame:
    seq: int
    event: str
    container: str | None
    wire: str
    received_at: float


class _StageOutput:
    def __init__(self, key: bytes, on_output: Callable[[str], None]) -> None:
        self.key = key
        self.on_output = on_output
        self.frames: asyncio.Queue[_StageFrame] = asyncio.Queue()
        self.pending = ""

    def feed(self, chunk: str) -> None:
        self.pending += chunk
        while "\n" in self.pending:
            line, self.pending = self.pending.split("\n", 1)
            wire = line.removesuffix("\r")
            match = _STAGE_FRAME.fullmatch(wire) if len(wire) <= _STAGE_MAX_FRAME_LENGTH else None
            if match is None:
                self.on_output(line + "\n")
                continue
            encoded, mac = match.groups()
            try:
                payload = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
            except binascii.Error:
                self.on_output(line + "\n")
                continue
            expected = hmac.new(self.key, encoded.encode("ascii"), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(mac, expected):
                self.on_output(line + "\n")
                continue
            try:
                message = json.loads(payload)
                seq, event, container = message["seq"], message["event"], message["container"]
                if (
                    type(seq) is not int
                    or seq < 1
                    or event not in ("begin", "end")
                    or (container is not None and not isinstance(container, str))
                ):
                    raise ValueError("Invalid stage frame")
            except (ValueError, KeyError, TypeError) as error:
                raise ControlledGenerationError("Invalid authenticated stage frame") from error
            self.frames.put_nowait(_StageFrame(seq, event, container, wire, asyncio.get_running_loop().time()))
        if self.pending and not (
            len(self.pending) <= _STAGE_MAX_FRAME_LENGTH
            and (_STAGE_PREFIX.startswith(self.pending) or self.pending.startswith(_STAGE_PREFIX))
        ):
            self.on_output(self.pending)
            self.pending = ""

    def finish(self) -> None:
        if self.pending:
            self.on_output(self.pending)
            self.pending = ""


async def _provision_stage(sandbox: Sandbox) -> bytes:
    key = os.urandom(32)
    result = await _exec(sandbox, f"mkdir -m 700 {_STAGE_DIR} && mkdir -m 700 {_STAGE_DIR}/ack")
    if result.exit_code != 0:
        raise SandboxError("Could not provision stage control directory")
    await sandbox.upload_file(f"{_STAGE_DIR}/key", key.hex().encode())
    result = await _exec(sandbox, f"chmod 600 {_STAGE_DIR}/key")
    if result.exit_code != 0:
        raise SandboxError("Could not restrict stage signing key")
    return key


async def _confirm_inner_stopped(sandbox: Sandbox, container: str | None) -> None:
    if container is None:
        return
    quoted = shlex.quote(container)
    await _exec(sandbox, f"docker rm -f {quoted}")
    listed = await _exec(sandbox, "docker container ls -a --format '{{.Names}}'")
    if listed.exit_code != 0 or container in listed.output.splitlines():
        raise ControlledGenerationTerminationUnconfirmedError("Nested generation container absence is unconfirmed")


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / contract_name


SandboxDeleteInitiator = Literal["create_cancelled", "force_stop", "orphan_cleanup", "task_teardown"]
SandboxDeleteOutcome = Literal["deleted", "already_gone", "cancelled", "failed"]


def audit_sandbox_delete(
    sandbox: Sandbox,
    initiated_by: SandboxDeleteInitiator,
    org_id: str | None,
    outcome: SandboxDeleteOutcome,
    error: str | None = None,
) -> None:
    # Labels attached at sandbox creation (utils/task_execution.py):
    # {"Benchmark": name, "Id": benchmark id, "Task": task id}.
    labels = sandbox.labels or {}
    logger.info(
        "sandbox.delete",
        extra={
            "sandbox_id": sandbox.id,
            "sandbox_name": sandbox.name,
            "benchmark_id": labels.get("Id"),
            "benchmark_name": labels.get("Benchmark"),
            "task_id": labels.get("Task"),
            "org_id": org_id,
            "initiated_by": initiated_by,
            "outcome": outcome,
            "error": error,
        },
    )


async def delete_sandbox(
    sandbox: Sandbox,
    provider: SandboxProvider,
    *,
    initiated_by: SandboxDeleteInitiator,
    org_id: str | None = None,
) -> None:
    """Delete sandbox through its provider."""
    try:
        await provider.delete_sandbox(sandbox.id)
    except SandboxNotFoundError:
        audit_sandbox_delete(sandbox, initiated_by, org_id, "already_gone")
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
    except ProviderSandboxError as e:
        audit_sandbox_delete(sandbox, initiated_by, org_id, "failed", f"{type(e).__name__}: {e}")
        raise
    except asyncio.CancelledError:
        # Caught only to audit: a cancelled delete may still have reached the provider.
        audit_sandbox_delete(sandbox, initiated_by, org_id, "cancelled")
        raise
    except Exception as e:
        audit_sandbox_delete(sandbox, initiated_by, org_id, "failed", f"{type(e).__name__}: {e}")
        logger.error(f"Unexpected error deleting sandbox {sandbox.name}: {e}")
    else:
        audit_sandbox_delete(sandbox, initiated_by, org_id, "deleted")


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


def _reject_plaintext_secret_collisions(
    env_vars: dict[str, str] | None,
    sandbox_secrets: dict[str, str] | None,
) -> None:
    overlapping_env_names = sorted(set(env_vars or {}) & set(sandbox_secrets or {}))
    if overlapping_env_names:
        raise InvalidSandboxConfigurationError(
            "Sandbox environment variables cannot be both plaintext and provider-managed secrets: "
            f"{', '.join(overlapping_env_names)}"
        )


@logfire.instrument("sandbox.create", extract_args=False)
async def _create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    source: SandboxSource,
    resources: TrackerResources,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
    volumes: list[VolumeMount] | None = None,
    sandbox_secrets: dict[str, str] | None = None,
) -> Sandbox:
    """Create a sandbox through its provider."""
    _reject_plaintext_secret_collisions(env_vars, sandbox_secrets)
    provider_source = _provider_source(source)
    _set_sandbox_create_span_attributes(sandbox_name, provider_source, resources)
    sandbox = await provider.create_sandbox(
        SandboxCreateRequest(
            source=provider_source,
            resources=resources,
            name=sandbox_name,
            labels=labels or {},
            env_vars=env_vars or {},
            sandbox_secrets=sandbox_secrets or {},
            volumes=volumes or [],
            auto_stop_interval=SANDBOX_AUTO_STOP_INTERVAL,
            create_timeout=SANDBOX_CREATE_TIMEOUT,
            network_block_all=False,
        )
    )
    _set_sandbox_span_attributes(sandbox)
    return sandbox


@asynccontextmanager
async def create_sandbox(
    provider: SandboxProvider,
    sandbox_name: str,
    source: SandboxSource,
    resources: TrackerResources,
    creation_semaphore: Semaphore,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
    volumes: list[VolumeMount] | None = None,
    sandbox_secrets: dict[str, str] | None = None,
    *,
    unique_name: bool = True,
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
        volumes: Persistent volumes to mount in the sandbox
        sandbox_secrets: Provider-managed secret references keyed by environment variable name
        creation_semaphore: Per-benchmark semaphore to limit concurrent sandbox creation.
        unique_name: Whether to append a random suffix to the supplied name.

    Returns:
        A context manager that yields the sandbox
    """
    if unique_name:
        sandbox_name = f"{sandbox_name}_{uuid.uuid4().hex[:6]}"
    source_name = _source_name(source)
    logger.info(f"Creating sandbox {sandbox_name} with source {source_name}")

    # If we run too many at once it can cause hanging issues
    # NOTE does not block how many context managers we can have open, just how many sandboxes we can create at once
    try:
        async with creation_semaphore:
            start = time.monotonic()
            creation_task = asyncio.create_task(
                _create_sandbox(
                    provider,
                    sandbox_name,
                    source,
                    resources,
                    labels=labels,
                    env_vars=env_vars,
                    volumes=volumes,
                    sandbox_secrets=sandbox_secrets,
                )
            )
            try:
                sandbox = await asyncio.shield(creation_task)
            except asyncio.CancelledError:
                sandbox = await creation_task
                await delete_sandbox(sandbox, provider, initiated_by="create_cancelled")
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
        try:
            await delete_sandbox(sandbox, provider, initiated_by="task_teardown")
        except ProviderSandboxError:
            # The failed delete is audited by delete_sandbox and must not replace the task outcome.
            pass


@retry(
    retry=retry_if_exception_type(SandboxError) & retry_if_not_exception_type(SandboxSetupError),
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_chain(wait_none(), wait_fixed(30)),
    before_sleep=retry_callback("valkyrie.sandbox.upload"),
)
async def upload_agent_artifacts(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    benchmark_id: str,
    object_store: ObjectStore,
) -> None:
    """
    Transfer the frozen agent bundle using a signed URL or local provider file upload.

    Reads from benchmarks/<benchmark_id>/<name>.zip so edits to the shared agent don't affect runs in flight.

    Args:
        sandbox: The sandbox to download and extract files in
        contract: The agent contract configuration
        benchmark_id: The benchmark run id, used to locate the agent
        object_store: storage provider for the already-resolved run authority

    Raises:
        SandboxError: If download or extraction fails inside the sandbox
    """
    logger.info(f"Uploading contract {contract.name} to sandbox {sandbox.name}")

    contract_s3_key = benchmark_agent_bundle_key(benchmark_id, contract.name)
    presigned_url = await object_store.temporary_download_url(
        contract_s3_key,
        expires_in=CONTRACT_DOWNLOAD_URL_EXPIRES_SECONDS,
    )
    if presigned_url is None:
        from tracker.local.artifacts import upload_local_agent_artifacts

        await upload_local_agent_artifacts(sandbox, await object_store.get_bytes(contract_s3_key))
        return

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


async def apply_egress_policy(sandbox: Sandbox, policy: EgressPolicy) -> None:
    """Replace the sandbox egress policy, failing before the next lifecycle stage."""
    try:
        if policy == "*":
            await _run_egress_operation(sandbox.clear_egress_rules)
        elif policy:
            await _run_egress_operation(lambda: sandbox.modify_egress_rules(policy))
        else:
            await _run_egress_operation(sandbox.block_all_egress)
    except SandboxNotFoundError:
        raise
    except ValueError as error:
        raise SandboxSetupError(f"Failed to apply egress policy: {error}") from error
    except ProviderSandboxError as error:
        raise SandboxError(str(error)) from error


async def stream_command_output(
    sandbox: Sandbox,
    command: str,
    on_output: Callable[[str], None],
) -> tuple[AgentCausedExitReason | None, float]:
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

        sentry_sdk.set_tag("agent_exit_code", str(exit_code))
        raise AgentRunFailedError(f"Agent command failed with exit code {exit_code}")
    finally:
        try:
            await _exec(sandbox, f"rm -f {shlex.quote(start_ns_path)} {shlex.quote(end_ns_path)}")
        except Exception:
            pass


def _controlled_completion_precedes_deadline(result: ControlledWorkloadResult, deadline: float) -> bool:
    return result.absence_confirmed_at < deadline


def _controlled_result_outcome(
    completed: ControlledWorkloadResult, started_at: float
) -> tuple[AgentCausedExitReason | None, float]:
    duration = completed.absence_confirmed_at - started_at
    exit_code = completed.result.exit_code
    if exit_code == _SUCCESS_EXIT_CODE:
        return None, duration
    if exit_code == _OS_KILL_EXIT_CODE:
        return AgentCausedExitReason.OS_KILLED, duration
    sentry_sdk.set_tag("agent_exit_code", str(exit_code))
    raise AgentRunFailedError(f"Agent command failed with exit code {exit_code}")


async def _raise_controlled_failure_after_close(workload: ControlledWorkload, error: BaseException) -> Never:
    try:
        await workload.kill()
    except BaseException as kill_error:
        raise ControlledGenerationTerminationUnconfirmedError(
            "Controlled generation failed and workload termination could not be confirmed"
        ) from kill_error
    raise ControlledGenerationError("Controlled generation failed after workload construction") from error


async def _finish_controlled_output(workload: ControlledWorkload, output_task: asyncio.Task[None]) -> None:
    try:
        # A naturally completed producer has a finite buffered tail to drain.
        await output_task
    except Exception as error:
        await _raise_controlled_failure_after_close(workload, error)


async def _controlled_wait_result(
    workload: ControlledWorkload, wait_task: asyncio.Task[ControlledWorkloadResult]
) -> ControlledWorkloadResult:
    try:
        return wait_task.result()
    except BaseException as error:
        await _raise_controlled_failure_after_close(workload, error)


async def _cancel_and_join_controlled_tasks(*tasks: asyncio.Task[Any]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _stream_controlled_output(
    sandbox: Sandbox,
    command: str,
    cwd: str,
    on_output: Callable[[str], None],
    timeout: float,
    deadline_controller: ExternalServiceDeadlineController | None = None,
    on_accounting_sealed: (Callable[[ExternalServiceAccountingSummary], Awaitable[None]] | None) = None,
    *,
    stage_key: bytes | None = None,
) -> tuple[AgentCausedExitReason | None, float]:
    loop = asyncio.get_running_loop()
    controller = deadline_controller or ExternalServiceDeadlineController(base_allowance_seconds=timeout)
    stage = _StageOutput(stage_key, on_output) if stage_key is not None else None
    try:
        if stage is None:
            await controller.begin_generation(now=loop.time())
        started_at = loop.time()
        workload = sandbox.controlled_workload(command, cwd=cwd)
    except BaseException as original:
        try:
            if controller.client is not None:
                snapshot = await controller.seal_after_confirmed_stop(loop.time())
                assert on_accounting_sealed is not None
                await on_accounting_sealed(controller.summary(snapshot))
            elif controller.active_since is not None:
                await controller.end_generation()
        except BaseException as control_error:
            original.add_note(f"Gateway cleanup before workload start failed: {control_error!r}")
            logger.exception("Could not seal gateway before workload start")
        raise

    async def pump() -> None:
        async for chunk in workload.output():
            if stage is None:
                on_output(chunk)
            else:
                stage.feed(chunk)
        if stage is not None:
            stage.finish()

    output_task = asyncio.create_task(pump())
    wait_task = asyncio.create_task(workload.wait())
    event_task = asyncio.create_task(stage.frames.get()) if stage is not None else None
    pending_acks: set[asyncio.Task[None]] = set()
    deadline = controller.deadline(started_at) if stage is None else 0.0
    deadline_task: asyncio.Task[None] | None = None
    stop_at: float | None = None
    active_container: str | None = None
    last_frame: _StageFrame | None = None
    outer_stopped = False
    inner_stopped = False
    absence_confirmed_at: float | None = None
    sealed_once = False

    async def wait_deadline() -> None:
        nonlocal deadline
        while True:
            if controller.client is None:
                await asyncio.sleep(max(0.0, deadline - loop.time()))
                return
            await asyncio.sleep(max(0.0, deadline - EXTERNAL_SERVICE_REFRESH_LEAD_SECONDS - loop.time()))
            if loop.time() >= deadline:
                return
            try:
                async with asyncio.timeout_at(deadline):
                    snapshot = await controller.refresh()
            except TimeoutError:
                return
            except Exception:
                await asyncio.sleep(min(EXTERNAL_SERVICE_REFRESH_RETRY_SECONDS, max(0.0, deadline - loop.time())))
                continue
            refreshed_deadline = controller.deadline(loop.time(), snapshot)
            if refreshed_deadline <= deadline:
                await asyncio.sleep(max(0.0, deadline - loop.time()))
                return
            deadline = refreshed_deadline

    async def disarm() -> None:
        nonlocal deadline_task
        if deadline_task is not None:
            if not deadline_task.done():
                deadline_task.cancel()
            await asyncio.gather(deadline_task, return_exceptions=True)
            deadline_task = None

    async def stop(termination_deadline: float | None = None) -> None:
        nonlocal outer_stopped, inner_stopped, absence_confirmed_at
        try:
            async with asyncio.timeout_at(termination_deadline or loop.time() + GENERATION_TERMINATION_GRACE_SECONDS):
                if not outer_stopped:
                    await workload.kill()
                    outer_stopped = True
                if not inner_stopped:
                    await _confirm_inner_stopped(sandbox, active_container)
                    inner_stopped = True
                absence_confirmed_at = loop.time()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if termination_deadline is not None:
                raise GenerationTerminationUnconfirmedError(
                    "Generation deadline expired without confirmed workload termination"
                ) from error
            raise

    async def completed_result() -> ControlledWorkloadResult:
        nonlocal outer_stopped, absence_confirmed_at
        try:
            completed = await _controlled_wait_result(workload, wait_task)
            outer_stopped = True
            absence_confirmed_at = completed.absence_confirmed_at
            return completed
        except ControlledGenerationError as error:
            if not isinstance(error, ControlledGenerationTerminationUnconfirmedError):
                outer_stopped = True
                absence_confirmed_at = loop.time()
            raise

    async def finish_output() -> None:
        nonlocal outer_stopped, absence_confirmed_at
        try:
            await _finish_controlled_output(workload, output_task)
        except ControlledGenerationError as error:
            if not isinstance(error, ControlledGenerationTerminationUnconfirmedError):
                outer_stopped = True
                absence_confirmed_at = loop.time()
            raise

    async def seal() -> None:
        nonlocal sealed_once
        if sealed_once:
            return
        if controller.client is not None:
            assert absence_confirmed_at is not None
            snapshot = await controller.seal_after_confirmed_stop(absence_confirmed_at)
            sealed_once = True
            assert on_accounting_sealed is not None
            await on_accounting_sealed(controller.summary(snapshot))
        else:
            if controller.active_since is not None:
                assert absence_confirmed_at is not None
                await controller.end_generation(now=absence_confirmed_at)
            sealed_once = True

    async def exhausted_at(ended_at: float) -> bool:
        nonlocal stop_at
        if controller.elapsed_seconds(ended_at) < controller.effective_allowance_seconds():
            return False
        stop_at = deadline + GENERATION_TERMINATION_GRACE_SECONDS
        if controller.client is not None:
            async with asyncio.timeout_at(deadline + GENERATION_ARBITRATION_GRACE_SECONDS):
                frozen = await controller.begin_arbitration()
                if controller.elapsed_seconds(ended_at) < controller.effective_allowance_seconds(frozen):
                    await controller.resolve(ArbitrationDecision.RESUME)
                    stop_at = None
                    return False
        return True

    async def acknowledge(seq: int) -> None:
        await sandbox.upload_file(f"{_STAGE_DIR}/ack/{seq}", b"")

    if stage is None:
        deadline_task = asyncio.create_task(wait_deadline())
    try:
        while True:
            if stage is None and wait_task.done() and not output_task.done():
                completed = await completed_result()
                if _controlled_completion_precedes_deadline(completed, deadline):
                    await disarm()
                    await finish_output()
                    continue
            if (
                wait_task.done()
                and output_task.done()
                and (stage is None or (stage.frames.empty() and event_task is not None and not event_task.done()))
            ):
                completed = await completed_result()
                await finish_output()
                await _cancel_and_join_controlled_tasks(*pending_acks)
                pending_acks.clear()
                exhausted = False
                if controller.active_since is not None:
                    ended_at = completed.absence_confirmed_at
                    if active_container is not None:

                        async def confirm_nested() -> float:
                            await _confirm_inner_stopped(sandbox, active_container)
                            return loop.time()

                        nested_task = asyncio.create_task(confirm_nested())
                        try:
                            while True:
                                assert deadline_task is not None
                                if not nested_task.done() and not deadline_task.done():
                                    await asyncio.wait(
                                        {nested_task, deadline_task}, return_when=asyncio.FIRST_COMPLETED
                                    )
                                if not deadline_task.done():
                                    await nested_task
                                    break
                                apparent_deadline = deadline
                                stop_at = apparent_deadline + GENERATION_TERMINATION_GRACE_SECONDS
                                await deadline_task
                                if controller.client is not None:
                                    arbitration_deadline = apparent_deadline + GENERATION_ARBITRATION_GRACE_SECONDS
                                    async with asyncio.timeout_at(arbitration_deadline):
                                        frozen = await controller.begin_arbitration()
                                        if nested_task.done():
                                            confirmed_at = await nested_task
                                            can_resume = controller.elapsed_seconds(
                                                confirmed_at
                                            ) < controller.effective_allowance_seconds(frozen)
                                        else:
                                            can_resume = controller.deadline(loop.time(), frozen) > loop.time()
                                        if can_resume:
                                            await controller.resolve(ArbitrationDecision.RESUME)
                                            deadline = controller.deadline(loop.time(), frozen)
                                            stop_at = None
                                            deadline_task = asyncio.create_task(wait_deadline())
                                            continue
                                async with asyncio.timeout_at(stop_at):
                                    ended_at = await nested_task
                                inner_stopped = True
                                absence_confirmed_at = ended_at
                                await seal()
                                return AgentCausedExitReason.TIMEOUT, controller.effective_allowance_seconds()
                        except TimeoutError as error:
                            raise ControlledGenerationTerminationUnconfirmedError(
                                "Nested generation container absence was not confirmed by the stop deadline"
                            ) from error
                        finally:
                            await _cancel_and_join_controlled_tasks(nested_task)
                        inner_stopped = True
                        ended_at = nested_task.result()
                        absence_confirmed_at = ended_at
                        exhausted = await exhausted_at(ended_at)
                    await disarm()
                    if not exhausted:
                        await controller.end_generation(now=ended_at)
                        if active_container is None:
                            exhausted = await exhausted_at(ended_at)
                    if stage is None and controller.client is None:
                        exhausted = not _controlled_completion_precedes_deadline(completed, deadline)
                if exhausted and stage is None:
                    await stop(deadline + GENERATION_TERMINATION_GRACE_SECONDS)
                await seal()
                if exhausted:
                    return AgentCausedExitReason.TIMEOUT, controller.effective_allowance_seconds()
                return _controlled_result_outcome(completed, started_at)

            watched: set[asyncio.Task[Any]] = {event_task} if event_task is not None else set()
            watched.update(pending_acks)
            if not wait_task.done():
                watched.add(wait_task)
            if not output_task.done():
                watched.add(output_task)
            if deadline_task is not None:
                watched.add(deadline_task)
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            if output_task in done:
                await finish_output()
            if not (wait_task.done() and output_task.done()):
                for ack_task in done & pending_acks:
                    pending_acks.remove(ack_task)
                    await ack_task
            if stage is None and wait_task.done() and output_task.done():
                continue
            if (
                event_task is not None
                and event_task in done
                and not (
                    deadline_task is not None and deadline_task in done and event_task.result().received_at > deadline
                )
            ):
                frame = event_task.result()
                assert stage is not None
                event_task = asyncio.create_task(stage.frames.get())
                if last_frame is not None and frame.wire == last_frame.wire:
                    continue
                if frame.seq != (last_frame.seq + 1 if last_frame else 1):
                    raise ControlledGenerationError("Stage frames arrived out of order")
                if frame.event == "begin":
                    if controller.active_since is not None:
                        raise ControlledGenerationError("Nested generation interval")
                    if controller.elapsed_seconds(frame.received_at) >= controller.effective_allowance_seconds():
                        await stop()
                        await seal()
                        return AgentCausedExitReason.TIMEOUT, controller.effective_allowance_seconds()
                    await controller.begin_generation()
                    active_container = frame.container
                    deadline = controller.deadline(controller.active_since)
                    deadline_task = asyncio.create_task(wait_deadline())
                    last_frame = frame
                    pending_acks.add(asyncio.create_task(acknowledge(frame.seq)))
                    continue
                else:
                    if controller.active_since is None or frame.container != active_container:
                        raise ControlledGenerationError("Stage END does not match an active container")
                    if deadline_task is not None:
                        deadline_task.cancel()
                    ended_at = frame.received_at
                    await controller.end_generation(now=ended_at)
                    await disarm()
                    if await exhausted_at(ended_at):
                        await stop(stop_at)
                        await seal()
                        return AgentCausedExitReason.TIMEOUT, controller.effective_allowance_seconds()
                    active_container = None
                last_frame = frame
                pending_acks.add(asyncio.create_task(acknowledge(frame.seq)))
                continue
            if deadline_task is not None and deadline_task in done:
                apparent_deadline = deadline
                stop_at = apparent_deadline + GENERATION_TERMINATION_GRACE_SECONDS
                await deadline_task
                if controller.client is not None:
                    arbitration_deadline = apparent_deadline + GENERATION_ARBITRATION_GRACE_SECONDS
                    async with asyncio.timeout_at(arbitration_deadline):
                        frozen = await controller.begin_arbitration()
                    if stage is None and wait_task.done():
                        completed = await completed_result()
                        if controller.elapsed_seconds(
                            completed.absence_confirmed_at
                        ) < controller.effective_allowance_seconds(frozen):
                            await controller.end_generation(now=completed.absence_confirmed_at)
                            await finish_output()
                            await seal()
                            return _controlled_result_outcome(completed, started_at)
                    if controller.deadline(loop.time(), frozen) > loop.time():
                        async with asyncio.timeout_at(arbitration_deadline):
                            await controller.resolve(ArbitrationDecision.RESUME)
                        deadline = controller.deadline(loop.time(), frozen)
                        stop_at = None
                        deadline_task = asyncio.create_task(wait_deadline())
                        continue
                await stop(stop_at)
                await _cancel_and_join_controlled_tasks(wait_task)
                await finish_output()
                await seal()
                return AgentCausedExitReason.TIMEOUT, controller.effective_allowance_seconds()
    except BaseException as error:
        await disarm()
        if isinstance(error, ControlledGenerationTerminationUnconfirmedError):
            raise
        try:
            if not (wait_task.done() and not wait_task.cancelled() and wait_task.exception() is None):
                await stop(stop_at)
            else:
                await completed_result()
                if controller.active_since is not None and not inner_stopped:
                    await _confirm_inner_stopped(sandbox, active_container)
                    inner_stopped = True
                    absence_confirmed_at = loop.time()
        except BaseException as stop_error:
            raise ControlledGenerationTerminationUnconfirmedError(
                "Controlled generation failed without confirmed nested and outer workload absence"
            ) from stop_error
        if not isinstance(error, ControlledGenerationError):
            try:
                await seal()
            except BaseException as seal_error:
                error.add_note(f"Gateway cleanup after confirmed stop failed: {seal_error!r}")
                logger.exception("Could not seal gateway after confirmed workload stop")
        raise
    finally:
        await disarm()
        await _cancel_and_join_controlled_tasks(*pending_acks)
        if event_task is not None:
            await _cancel_and_join_controlled_tasks(event_task)
        await _cancel_and_join_controlled_tasks(wait_task, output_task)


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
    object_store: ObjectStore,
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
        archive_bytes = await object_store.put_stream(
            agent_output_s3_key,
            sandbox.stream_download(archive_path),
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
OUTPUT_ARTIFACTS_MAX_TOTAL_BYTES = 250 * 1024 * 1024


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
    object_store: ObjectStore,
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
                object_store,
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
    object_store: ObjectStore,
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

    s3_key = task_artifact_key(benchmark_id, task_id, artifact_path)
    # Providers may report an empty file as a failed transfer; Daytona raises "No file data received".
    chunks = _no_chunks() if artifact_bytes == 0 else sandbox.stream_download(sandbox_path)
    await object_store.put_stream(s3_key, chunks, should_continue=execution_is_current)

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


async def _no_chunks() -> AsyncGenerator[bytes, None]:
    return
    yield


async def run_agent(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    problem_path: str,
    task_id: str,
    log_output: Callable[[str], None],
    cwd: str,
    object_store: ObjectStore,
    agent_output_s3_key: str | None = None,
    agent_timeout: float | None = None,
    task_credited_generation: CreditedGeneration | None = None,
    benchmark_id: str | None = None,
    execution_is_current: Callable[[], bool] | None = None,
    external_service_deadline: ExternalServiceDeadlineController | None = None,
    on_external_service_sealed: (Callable[[ExternalServiceAccountingSummary], Awaitable[None]] | None) = None,
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
        agent_timeout: Published timeout envelope; enforced by the shell only for unselected tasks
        task_credited_generation: Benchmark generation allowance and optional stage protocol
        execution_is_current: Optional execution-authority check before output uploads
        external_service_deadline: Optional Tracker-owned cumulative deadline controller
        on_external_service_sealed: Awaited persistence callback after workload absence

    Returns:
        AgentCausedExitReason if the agent was terminated abnormally but recoverably
        (timeout or OS kill), None on clean exit.

    Raises:
        SandboxError: If the agent fails to run or times out
    """
    log_output(f"Running agent {contract.name}")
    controlled_generation = task_credited_generation is not None
    if controlled_generation and external_service_deadline is None:
        assert task_credited_generation is not None
        external_service_deadline = ExternalServiceDeadlineController(
            base_allowance_seconds=task_credited_generation.allowance_seconds
        )

    if controlled_generation:
        runtime_containment = sandbox.generation_containment
        if (
            runtime_containment is None
            or runtime_containment.type != "linux_pid_namespace"
            or runtime_containment.version != 1
        ):
            raise InvalidSandboxConfigurationError("Effective sandbox does not support linux_pid_namespace v1")
        await sandbox.probe_generation_containment()

    run_cmd = contract.run_cmd.replace("{problem_statement_path}", problem_path).replace("{task_id}", task_id)

    for kwarg_key, kwarg_value in contract.kwargs.items():
        run_cmd = run_cmd.replace(f"{{{kwarg_key}}}", kwarg_value)

    # Legacy paths preserve the benchmark-provided shell timeout.
    if agent_timeout is not None and not controlled_generation:
        run_cmd = f"timeout {agent_timeout:g} sh -c {shlex.quote(run_cmd)}"

    # Create cwd if it does not already exist
    await _exec(sandbox, f"mkdir -p {shlex.quote(cwd)}")
    stage_key = (
        await _provision_stage(sandbox)
        if task_credited_generation is not None and task_credited_generation.stage_protocol is not None
        else None
    )

    async def upload_outputs(*, preserve_agent_error: bool = False) -> None:
        if execution_is_current is not None and not execution_is_current():
            return
        errors: list[Exception] = []
        if contract.final_output:
            try:
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
                        object_store,
                        benchmark_id=benchmark_id,
                        task_id=task_id,
                        execution_is_current=execution_is_current,
                    )
            except Exception as error:
                errors.append(error)
                logger.exception(
                    "Failed to collect final agent output",
                    extra={
                        "sandbox_id": sandbox.id,
                        "sandbox_name": sandbox.name,
                        "benchmark_id": benchmark_id,
                        "task_id": task_id,
                    },
                )

        if contract.output_artifacts:
            try:
                if benchmark_id is None:
                    raise SandboxError("benchmark_id is required to upload output artifacts")
                await upload_output_artifacts(
                    sandbox,
                    contract.output_artifacts,
                    benchmark_id,
                    task_id,
                    object_store,
                    execution_is_current,
                )
            except Exception as error:
                errors.append(error)
                logger.exception(
                    "Failed to collect declared agent artifacts",
                    extra={
                        "sandbox_id": sandbox.id,
                        "sandbox_name": sandbox.name,
                        "benchmark_id": benchmark_id,
                        "task_id": task_id,
                    },
                )

        if errors and not preserve_agent_error:
            raise errors[0]

    # A nonzero exit is terminal evidence; collect declared outputs while the
    # sandbox is still available.
    try:
        if controlled_generation:
            assert task_credited_generation is not None and external_service_deadline is not None
            command = f"PYTHONSAFEPATH=1 {run_cmd}"
            if stage_key is not None:
                command = f"export VALKYRIE_STAGE_DIR={_STAGE_DIR}; {command}"
            exit_reason, agent_run_time = await _stream_controlled_output(
                sandbox,
                command,
                cwd,
                log_output,
                task_credited_generation.allowance_seconds,
                external_service_deadline,
                on_external_service_sealed,
                stage_key=stage_key,
            )
        else:
            exit_reason, agent_run_time = await stream_command_output(
                sandbox,
                f"cd {shlex.quote(cwd)} && PYTHONSAFEPATH=1 {run_cmd}",
                log_output,
            )
    except ControlledGenerationError:
        raise
    except AgentRunFailedError:
        await upload_outputs(preserve_agent_error=True)
        raise
    except Exception:
        if external_service_deadline is not None:
            raise
        await upload_outputs(preserve_agent_error=True)
        raise

    if exit_reason == AgentCausedExitReason.TIMEOUT:
        timeout_limit = (
            task_credited_generation.allowance_seconds if task_credited_generation is not None else agent_timeout
        )
        log_output(
            f"[WARNING]:`{contract.name}` has reached the designated generation allowance for this task: `{timeout_limit}`. The process has been terminated and evaluation will proceed."
        )
    elif exit_reason == AgentCausedExitReason.OS_KILLED:
        log_output(
            f"[WARNING]:`{contract.name}` was killed by the OS (exit code {_OS_KILL_EXIT_CODE}, likely out-of-memory). The process has been terminated and evaluation will proceed."
        )

    await upload_outputs()

    # Return why the agent terminated abnormally, or None on clean exit
    return exit_reason, agent_run_time
