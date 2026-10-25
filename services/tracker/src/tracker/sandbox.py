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
from asyncio import Semaphore
from collections.abc import Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator, Literal, Never, assert_never

import logfire
import sentry_sdk
from benchmark_service import (
    ControlledWorkload,
    ControlledWorkloadResult,
    Episode,
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
from benchmark_service.sandbox.episode import controlled_episode_workload
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
from tracker.runtime.lifecycle import finish_cleanup
from tracker.generation_plan import GenerationPlan
from tracker.runtime.storage import ObjectStore
from tracker.egress import EgressPolicy
from tracker.external_service_gateway import (
    GenerationAccountingSummary,
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
from tracker.sandbox_cost import SandboxLink, record_sandbox
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
# The 20-second post-deadline window includes at most 10 seconds for a counter refresh;
# the rest permits either confirmed termination or predeadline agent result collection.
GENERATION_CREDIT_REFRESH_GRACE_SECONDS = 10.0
GENERATION_TERMINATION_GRACE_SECONDS = 20.0
EPISODE_END_GRACE_SECONDS = 15.0
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


@dataclass
class SandboxCreationState:
    cleanup_pending: bool


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
) -> bool:
    """Delete sandbox through its provider and report acknowledged cleanup."""
    try:
        await provider.delete_sandbox(sandbox.id)
    except SandboxNotFoundError:
        audit_sandbox_delete(sandbox, initiated_by, org_id, "already_gone")
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
        return True
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
        return False
    else:
        audit_sandbox_delete(sandbox, initiated_by, org_id, "deleted")
        return True


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
    creation_state: SandboxCreationState,
    unique_name: bool = True,
    sandbox_link: SandboxLink | None = None,
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
        creation_state: Shared attempt state tracking unacknowledged provider cleanup.
        unique_name: Whether to append a random suffix to the supplied name.
        sandbox_link: The attempt to save the sandbox against once the provider names it.

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
            creation_state.cleanup_pending = True
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
                if sandbox_link is not None:
                    await asyncio.to_thread(record_sandbox, sandbox_link, sandbox.id)
                if await delete_sandbox(sandbox, provider, initiated_by="create_cancelled"):
                    creation_state.cleanup_pending = False
                raise
            except SandboxNotFoundError:
                raise
            except ProviderSandboxError as e:
                incr("valkyrie.sandbox.create.errors", tags={"error_class": type(e).__name__})
                # Nothing has run yet, so a fresh creation attempt costs no model spend.
                raise SandboxSetupError(f"Failed to create sandbox {sandbox_name}: {e}") from e
    except SandboxSetupError:
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
        if sandbox_link is not None:
            link_write = asyncio.ensure_future(asyncio.to_thread(record_sandbox, sandbox_link, sandbox.id))
            try:
                await asyncio.shield(link_write)
            except asyncio.CancelledError:
                await link_write
                raise
        yield sandbox
    except Exception as e:
        logger.error(f"Error during sandbox execution {sandbox.name}: {e}")
        raise
    finally:
        try:
            if await delete_sandbox(sandbox, provider, initiated_by="task_teardown"):
                creation_state.cleanup_pending = False
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
    except SandboxNotFoundError:
        # Keep the provider type so the sandbox-loss recovery path can recognise it.
        raise
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
    *,
    redact_error: Callable[[str], str] | None = None,
    secret_env_names: tuple[str, ...] = (),
) -> None:
    """Run one bounded dependency installation attempt."""
    if not contract.install_cmd:
        return

    log_output(f"Installing dependencies for contract: {contract.name}")

    contract_path = get_contract_path(contract.name)
    # Reuse the provider shell's foreground process group so terminal setup cannot suspend the installer.
    install_cmd = f"exec timeout {AGENT_INSTALL_TIMEOUT_SECONDS:g} sh -c {shlex.quote(contract.install_cmd)}"

    exit_reason = await stream_command_output(
        sandbox,
        f"cd {shlex.quote(str(contract_path))} && {install_cmd}",
        log_output,
        failure_subject=f"Dependency installation for contract {contract.name}",
        redact_error=redact_error,
        secret_env_names=secret_env_names,
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
    *,
    redact_error: Callable[[str], str] | None = None,
    secret_env_names: tuple[str, ...] = (),
) -> None:
    await _install_agent_dependencies_once(
        sandbox, contract, log_output, redact_error=redact_error, secret_env_names=secret_env_names
    )


async def install_agent_dependencies(
    sandbox: Sandbox,
    contract: AgentContractRequest,
    log_output: Callable[[str], None],
    mode: DependencySetupMode = DependencySetupMode.IN_PLACE_RETRIES,
    *,
    redact_error: Callable[[str], str] | None = None,
    secret_env_names: tuple[str, ...] = (),
) -> None:
    """Install dependencies using the policy selected for this sandbox."""
    if mode is DependencySetupMode.FINAL_FRESH_SANDBOX:
        await _install_agent_dependencies_once(
            sandbox, contract, log_output, redact_error=redact_error, secret_env_names=secret_env_names
        )
        return

    try:
        await _install_agent_dependencies_with_retries(
            sandbox, contract, log_output, redact_error=redact_error, secret_env_names=secret_env_names
        )
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
AGENT_ERROR_PATH_ENV = "VALKYRIE_ERROR_PATH"
_AGENT_ERROR_MAX_BYTES = 2048
_POST_EXIT_READ_TIMEOUT_SECONDS = 5
_DNS_CHECK_TIMEOUT_SECONDS = 30
# Provider network-settings updates race with the sandbox's own start-up (HTTP 409 on
# Daytona), so give the conflict time to clear instead of retrying back-to-back.
_EGRESS_RETRY = retry(
    retry=retry_if_exception_type(ProviderSandboxError) & retry_if_not_exception_type(SandboxNotFoundError),
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_chain(wait_fixed(2), wait_fixed(5)),
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


async def sandbox_resolves(sandbox: Sandbox, host: str) -> bool:
    """Whether the sandbox resolves `host`, answered by the tracker rather than the agent.

    Only getent's own "not found" exit (2) counts as a failure. A sandbox without
    /usr/bin/getent, one that is gone, a check that times out, or any other exit counts as
    resolving, so the check reports only a failure it observed.
    """
    command = f"[ -x /usr/bin/getent ] || exit 0; /usr/bin/getent ahosts {shlex.quote(host)}"
    try:
        result = await asyncio.wait_for(_exec(sandbox, command), timeout=_DNS_CHECK_TIMEOUT_SECONDS)
    except (SandboxError, SandboxNotFoundError, TimeoutError):
        return True
    return result.exit_code != 2


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
        # The provider rejected the allowlist itself, so a fresh sandbox would fail the same way.
        raise InvalidSandboxConfigurationError(f"Failed to apply egress policy: {error}") from error
    except ProviderSandboxError as error:
        # The sandbox still exists; only the provider's network API failed.
        raise SandboxError(str(error), transient=True) from error


async def stream_command_output(
    sandbox: Sandbox,
    command: str,
    on_output: Callable[[str], None],
    *,
    failure_subject: str = "Agent command",
    redact_error: Callable[[str], str] | None = None,
    secret_env_names: tuple[str, ...] = (),
) -> AgentCausedExitReason | None:
    error_path: str | None = None
    if redact_error is not None:
        error_path = f"{_STATUS_DIR}/{uuid.uuid4().hex}.error"
        command = (
            f"mkdir -p {shlex.quote(_STATUS_DIR)}; export {AGENT_ERROR_PATH_ENV}={shlex.quote(error_path)}; {command}"
        )
    exit_code = _SUCCESS_EXIT_CODE
    try:
        try:
            async for data in sandbox.command(command):
                on_output(data)
        except ProviderSandboxCommandError as e:
            exit_code = e.exit_code
        except SandboxNotFoundError:
            raise
        except ProviderSandboxError as e:
            raise SandboxError(str(e)) from e

        if exit_code == _SUCCESS_EXIT_CODE:
            return None
        if exit_code == _TIMEOUT_EXIT_CODE:
            return AgentCausedExitReason.TIMEOUT
        if exit_code == _OS_KILL_EXIT_CODE:
            return AgentCausedExitReason.OS_KILLED

        sentry_sdk.set_tag("agent_exit_code", str(exit_code))
        message = f"{failure_subject} failed with exit code {exit_code}"
        if redact_error is not None and error_path is not None:
            reported_error = await _read_agent_error(
                sandbox, error_path, redact_error, secret_env_names=secret_env_names
            )
            if reported_error:
                message = f"{message}: {reported_error}"
        raise AgentRunFailedError(message)
    finally:
        if error_path is not None:
            try:
                await asyncio.wait_for(
                    _exec(sandbox, f"rm -f {shlex.quote(error_path)}"),
                    timeout=_POST_EXIT_READ_TIMEOUT_SECONDS,
                )
            except Exception:
                pass


async def _read_post_exit_file(sandbox: Sandbox, command: str) -> str | None:
    """Best-effort read of a file the command wrote before exiting.

    Runs after the command's exit code is already known, so nothing raised here may
    replace the original outcome: any failure (sandbox gone, transport error, hang)
    yields ``None``.
    """
    try:
        result = await asyncio.wait_for(_exec(sandbox, command), timeout=_POST_EXIT_READ_TIMEOUT_SECONDS)
    except Exception:
        return None
    if result.exit_code != _SUCCESS_EXIT_CODE:
        return None
    return result.output


def create_agent_error_redactor(secret_values: Iterable[str]) -> Callable[[str], str]:
    """Mask literal configured values without putting them in caller-frame arguments."""
    values = sorted({value for value in secret_values if value}, key=len, reverse=True)
    pattern = re.compile("|".join(re.escape(value) for value in values)) if values else None

    def redact(content: str) -> str:
        return pattern.sub("[REDACTED]", content) if pattern else content

    return redact


# Provider secrets are references, not values, in Tracker. Read and mask the
# values visible to the agent inside the sandbox; never return them separately.
# Python is optional in arbitrary images: a missing interpreter retains the
# exit-code fallback via _read_post_exit_file, just like any failed report read.
_MASK_PROVIDER_ERROR_SCRIPT = """\
import os, re, sys
try:
    limit = int(sys.argv[2])
    with open(sys.argv[1], 'rb') as report:
        content = report.read(limit + 1)
    values = {os.environ[name].encode() for name in sys.argv[3:]}
    if len(content) > limit or b'' in values:
        sys.exit(1)
    pattern = b'|'.join(re.escape(value) for value in sorted(values, key=len, reverse=True))
    content = re.sub(pattern, b'[REDACTED]', content)
    content.decode('utf-8')
    content = content[:limit].decode('utf-8', errors='ignore').encode('utf-8')
except (OSError, KeyError, UnicodeError):
    sys.exit(1)
sys.stdout.buffer.write(content)
"""


async def _read_agent_error(
    sandbox: Sandbox,
    error_path: str,
    redact_error: Callable[[str], str] | None,
    *,
    secret_env_names: tuple[str, ...] = (),
) -> str:
    """Read a bounded Type: message report only when configured-secret redaction is available."""
    if redact_error is None:
        return ""
    # An extra byte detects oversized reports; do not truncate through a secret.
    command = f"head -c {_AGENT_ERROR_MAX_BYTES + 1} {shlex.quote(error_path)}"
    if secret_env_names:
        command = shlex.join(
            [
                "python3",
                "-I",
                "-S",
                "-c",
                _MASK_PROVIDER_ERROR_SCRIPT,
                error_path,
                str(_AGENT_ERROR_MAX_BYTES),
                *secret_env_names,
            ]
        )
    content = await _read_post_exit_file(sandbox, command)
    if content is None or len(content.encode()) > _AGENT_ERROR_MAX_BYTES:
        return ""
    message = " ".join(redact_error(content).split())
    if not message.isprintable() or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}: .+", message):
        return ""
    return message.encode()[:_AGENT_ERROR_MAX_BYTES].decode(errors="ignore")


def _controlled_completion_precedes_deadline(ended_at: float, deadline: float) -> bool:
    return ended_at < deadline


def _controlled_result_outcome(
    completed: ControlledWorkloadResult, started_at: float, ended_at: float, *, episode: bool
) -> tuple[AgentCausedExitReason | None, float]:
    duration = ended_at - started_at
    exit_code = completed.result.exit_code
    if exit_code == _SUCCESS_EXIT_CODE:
        return None, duration
    if exit_code == _OS_KILL_EXIT_CODE:
        return AgentCausedExitReason.OS_KILLED, duration
    sentry_sdk.set_tag("agent_exit_code", str(exit_code))
    command = "Episode orchestrator command" if episode else "Agent command"
    raise AgentRunFailedError(f"{command} failed with exit code {exit_code}")


async def _raise_controlled_failure_after_close(workload: ControlledWorkload, error: BaseException) -> Never:
    if isinstance(error, SandboxNotFoundError):
        raise error
    try:
        async with asyncio.timeout(GENERATION_TERMINATION_GRACE_SECONDS):
            await workload.kill()
    except SandboxNotFoundError:
        raise
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
    deadline_controller: ExternalServiceDeadlineController,
    on_accounting_finalized: (Callable[[GenerationAccountingSummary], Awaitable[None]] | None) = None,
    *,
    on_accounting_failure: Callable[[], Awaitable[None]],
    stage_key: bytes | None = None,
    episode: Episode | None = None,
    generation_gateway_url: str | None = None,
    redact_error: Callable[[str], str] | None = None,
    secret_env_names: tuple[str, ...] = (),
) -> tuple[AgentCausedExitReason | None, float]:
    loop = asyncio.get_running_loop()
    controller = deadline_controller
    stage = _StageOutput(stage_key, on_output) if stage_key is not None else None
    try:
        env_name = "MODEL_GATEWAY_URL" if episode is None and stage is None else "VALKYRIE_GENERATION_MODEL_GATEWAY_URL"
        env_vars = {env_name: generation_gateway_url} if generation_gateway_url is not None else {}
        error_path = f"{_STATUS_DIR}/{uuid.uuid4().hex}.error" if redact_error is not None else None
        if error_path is not None:
            await _exec(sandbox, f"mkdir -p {shlex.quote(_STATUS_DIR)}")
            env_vars[AGENT_ERROR_PATH_ENV] = error_path
        if stage is None:
            controller.begin_generation(now=loop.time())
        if episode is None:
            started_at = loop.time()
            workload = sandbox.controlled_workload(command, cwd=cwd, env_vars=env_vars or None)
        else:
            workload = await controlled_episode_workload(sandbox, command, cwd=cwd, env_vars=env_vars or None)
            started_at = loop.time()
    except BaseException as original:

        async def finalize_prelaunch() -> None:
            await controller.finalize_after_confirmed_stop(loop.time())
            if on_accounting_finalized is not None:
                await on_accounting_finalized(controller.summary())

        finalize_task = asyncio.create_task(finalize_prelaunch())
        try:
            await finish_cleanup(finalize_task)
        except BaseException as control_error:
            original.add_note(f"Generation accounting cleanup before workload start failed: {control_error!r}")
            logger.exception("Could not finalize generation accounting before workload start")
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
    finalize_task: asyncio.Task[None] | None = None
    notification_task: asyncio.Task[None] | None = None
    episode_exhausted = False
    finalized_running = False
    accounting_error: Exception | None = None

    def completed_end(completed: ControlledWorkloadResult) -> float:
        if stage is not None:
            return completed.absence_confirmed_at
        ended_at = workload.agent_absence_confirmed_at
        assert ended_at is not None
        return ended_at

    def stopped_end() -> float:
        if stage is not None:
            return loop.time()
        ended_at = workload.agent_absence_confirmed_at
        assert ended_at is not None
        return ended_at

    async def completed_outcome(completed: ControlledWorkloadResult) -> tuple[AgentCausedExitReason | None, float]:
        exit_code = completed.result.exit_code
        if error_path is not None and exit_code not in (_SUCCESS_EXIT_CODE, _OS_KILL_EXIT_CODE):
            sentry_sdk.set_tag("agent_exit_code", str(exit_code))
            message = f"Agent command failed with exit code {exit_code}"
            reported_error = await _read_agent_error(
                sandbox, error_path, redact_error, secret_env_names=secret_env_names
            )
            if reported_error:
                message = f"{message}: {reported_error}"
            raise AgentRunFailedError(message)
        return _controlled_result_outcome(completed, started_at, completed_end(completed), episode=episode is not None)

    def timeout_result() -> tuple[AgentCausedExitReason, float]:
        assert absence_confirmed_at is not None
        return AgentCausedExitReason.TIMEOUT, absence_confirmed_at - started_at

    async def wait_deadline() -> None:
        nonlocal deadline
        assert deadline is not None
        while True:
            if controller.client is None:
                await asyncio.sleep(max(0.0, deadline - loop.time()))
                return
            await asyncio.sleep(max(0.0, deadline - EXTERNAL_SERVICE_REFRESH_LEAD_SECONDS - loop.time()))
            if loop.time() >= deadline:
                return
            try:
                async with asyncio.timeout_at(deadline):
                    await controller.refresh()
            except TimeoutError:
                return
            except Exception:
                await asyncio.sleep(min(EXTERNAL_SERVICE_REFRESH_RETRY_SECONDS, max(0.0, deadline - loop.time())))
                continue
            refreshed_deadline = controller.deadline(loop.time())
            assert refreshed_deadline is not None
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
                absence_confirmed_at = stopped_end()
        except asyncio.CancelledError:
            raise
        except SandboxNotFoundError:
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
            absence_confirmed_at = completed_end(completed)
            return completed
        except ControlledGenerationError as error:
            if not isinstance(error, ControlledGenerationTerminationUnconfirmedError):
                outer_stopped = True
                absence_confirmed_at = stopped_end()
            raise

    async def finish_output() -> None:
        nonlocal outer_stopped, absence_confirmed_at
        try:
            await _finish_controlled_output(workload, output_task)
        except ControlledGenerationError as error:
            if not isinstance(error, ControlledGenerationTerminationUnconfirmedError):
                outer_stopped = True
                absence_confirmed_at = stopped_end()
            raise

    async def finalize() -> None:
        nonlocal finalize_task
        if finalize_task is None:

            async def finalize_and_persist() -> None:
                nonlocal accounting_error
                await disarm()
                assert absence_confirmed_at is not None
                try:
                    await controller.finalize_after_confirmed_stop(absence_confirmed_at)
                except Exception as error:
                    accounting_error = error
                    raise
                if on_accounting_finalized is not None:
                    summary = controller.summary()
                    try:
                        await on_accounting_finalized(summary)
                    except Exception as error:
                        accounting_error = error
                        raise

            finalize_task = asyncio.create_task(finalize_and_persist())
        await asyncio.shield(finalize_task)

    async def stop_and_finalize_timeout() -> tuple[AgentCausedExitReason, float]:
        await stop(stop_at)
        await _cancel_and_join_controlled_tasks(wait_task)
        await finish_output()
        await finalize()
        return timeout_result()

    async def exhausted_at(ended_at: float, apparent_deadline: float) -> bool:
        nonlocal stop_at
        allowance = controller.effective_allowance_seconds()
        if allowance is None or controller.elapsed_seconds(ended_at) < allowance:
            return False
        stop_at = apparent_deadline + GENERATION_TERMINATION_GRACE_SECONDS
        if controller.client is not None:
            async with asyncio.timeout_at(apparent_deadline + GENERATION_CREDIT_REFRESH_GRACE_SECONDS):
                await controller.refresh()
            allowance = controller.effective_allowance_seconds()
            assert allowance is not None
            if controller.elapsed_seconds(ended_at) < allowance:
                stop_at = None
                return False
        controller.freeze_credit()
        return True

    async def acknowledge(seq: int) -> None:
        await sandbox.upload_file(f"{_STAGE_DIR}/ack/{seq}", b"")

    async def notify_exhausted(origin: float) -> bool:
        nonlocal notification_task, episode_exhausted
        notification_cutoff = origin + EPISODE_END_GRACE_SECONDS
        if loop.time() >= notification_cutoff:
            return False
        completion_at: float | None = None

        async def upload_notification() -> None:
            nonlocal completion_at
            try:
                await sandbox.upload_file(f"{_STAGE_DIR}/exhausted", b"")
            finally:
                completion_at = loop.time()

        notification_task = asyncio.create_task(upload_notification())
        done, _ = await asyncio.wait({notification_task}, timeout=max(0.0, notification_cutoff - loop.time()))
        if notification_task not in done:
            notification_task.cancel()
            return False
        assert completion_at is not None
        if completion_at >= notification_cutoff:
            return False
        notification_task.result()
        episode_exhausted = True
        return True

    def end_received_stage(frame: _StageFrame) -> float:
        if controller.active_since is None or frame.container != active_container:
            raise ControlledGenerationError("Stage END does not match an active container")
        if deadline_task is not None:
            deadline_task.cancel()
        controller.end_generation(now=frame.received_at)
        return frame.received_at

    if stage is None and deadline is not None:
        deadline_task = asyncio.create_task(wait_deadline())
    try:
        while True:
            if (
                stage is not None
                and wait_task.done()
                and not wait_task.cancelled()
                and isinstance(wait_task.exception(), SandboxNotFoundError)
            ):
                if controller.active_since is not None:
                    buffered_frame = event_task.result() if event_task is not None and event_task.done() else None
                    while buffered_frame is not None or not stage.frames.empty():
                        frame = buffered_frame if buffered_frame is not None else stage.frames.get_nowait()
                        buffered_frame = None
                        if last_frame is not None and frame.wire == last_frame.wire:
                            continue
                        if (
                            last_frame is not None
                            and frame.seq == last_frame.seq + 1
                            and frame.event == "end"
                            and frame.container == active_container
                        ):
                            end_received_stage(frame)
                        break
                await completed_result()
            if stage is None and wait_task.done() and not output_task.done():
                await completed_result()
                if deadline is None:
                    await disarm()
                    await finish_output()
                    continue
            if (
                stage is not None
                and wait_task.done()
                and not wait_task.cancelled()
                and wait_task.exception() is not None
                and not output_task.done()
                and stage.frames.empty()
                and event_task is not None
                and not event_task.done()
                and (deadline_task is None or not deadline_task.done())
            ):
                await completed_result()
            if (
                wait_task.done()
                and output_task.done()
                and (stage is None or (stage.frames.empty() and event_task is not None and not event_task.done()))
            ):
                completed = await completed_result()
                await finish_output()
                if episode is not None:
                    await asyncio.gather(*pending_acks)
                else:
                    await _cancel_and_join_controlled_tasks(*pending_acks)
                pending_acks.clear()
                if episode_exhausted and not finalized_running:
                    async with asyncio.timeout_at(stop_at):
                        await _confirm_inner_stopped(sandbox, active_container)
                    inner_stopped = True
                    absence_confirmed_at = loop.time()
                    controller.end_generation(absence_confirmed_at)
                    await finalize()
                    return AgentCausedExitReason.TIMEOUT, completed.absence_confirmed_at - started_at
                if finalized_running:
                    return AgentCausedExitReason.TIMEOUT, completed.absence_confirmed_at - started_at
                exhausted = False
                if controller.active_since is not None:
                    ended_at = completed_end(completed)
                    if active_container is not None:

                        async def confirm_nested() -> float:
                            await _confirm_inner_stopped(sandbox, active_container)
                            return loop.time()

                        nested_task = asyncio.create_task(confirm_nested())
                        nested_exhausted = False
                        try:
                            while True:
                                if deadline_task is None:
                                    await nested_task
                                    break
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
                                    credit_read_deadline = apparent_deadline + GENERATION_CREDIT_REFRESH_GRACE_SECONDS
                                    async with asyncio.timeout_at(credit_read_deadline):
                                        await controller.refresh()
                                    if nested_task.done():
                                        confirmed_at = await nested_task
                                        allowance = controller.effective_allowance_seconds()
                                        assert allowance is not None
                                        can_resume = controller.elapsed_seconds(confirmed_at) < allowance
                                    else:
                                        next_deadline = controller.deadline(loop.time())
                                        can_resume = next_deadline is not None and next_deadline > loop.time()
                                    if can_resume:
                                        deadline = controller.deadline(loop.time())
                                        stop_at = None
                                        deadline_task = asyncio.create_task(wait_deadline())
                                        continue
                                controller.freeze_credit()
                                async with asyncio.timeout_at(stop_at):
                                    ended_at = await nested_task
                                inner_stopped = True
                                absence_confirmed_at = ended_at
                                nested_exhausted = True
                                break
                        except TimeoutError as error:
                            raise ControlledGenerationTerminationUnconfirmedError(
                                "Nested generation container absence was not confirmed by the stop deadline"
                            ) from error
                        finally:
                            await _cancel_and_join_controlled_tasks(nested_task)
                        if nested_exhausted:
                            await finalize()
                            return timeout_result()
                        inner_stopped = True
                        ended_at = nested_task.result()
                        absence_confirmed_at = ended_at
                        if deadline is not None:
                            await disarm()
                            exhausted = await exhausted_at(ended_at, deadline)
                    await disarm()
                    if not exhausted:
                        controller.end_generation(now=ended_at)
                        if active_container is None:
                            if deadline is not None:
                                exhausted = await exhausted_at(ended_at, deadline)
                    if stage is None and controller.client is None and deadline is not None:
                        exhausted = not _controlled_completion_precedes_deadline(ended_at, deadline)
                if exhausted and stage is None and deadline is not None:
                    await stop(deadline + GENERATION_TERMINATION_GRACE_SECONDS)
                await finalize()
                if exhausted:
                    return timeout_result()
                return await completed_outcome(completed)

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
                    if episode_exhausted:
                        last_frame = frame
                        pending_acks.add(asyncio.create_task(acknowledge(frame.seq)))
                        continue
                    if controller.active_since is not None:
                        raise ControlledGenerationError("Nested generation interval")
                    controller.begin_generation()
                    active_container = frame.container
                    deadline = controller.deadline(loop.time())
                    deadline_task = asyncio.create_task(wait_deadline())
                    last_frame = frame
                    pending_acks.add(asyncio.create_task(acknowledge(frame.seq)))
                    continue
                else:
                    if controller.active_since is None or frame.container != active_container:
                        raise ControlledGenerationError("Stage END does not match an active container")
                    ended_at = frame.received_at
                    if episode_exhausted:
                        controller.end_generation(ended_at)
                        await disarm()
                        async with asyncio.timeout_at(stop_at):
                            await _confirm_inner_stopped(sandbox, active_container)
                        inner_stopped = True
                        absence_confirmed_at = loop.time()
                        await finalize()
                        finalized_running = True
                    else:
                        if deadline_task is not None:
                            deadline_task.cancel()
                        controller.end_generation(now=ended_at)
                        await disarm()
                        if await exhausted_at(ended_at, deadline):
                            if episode is None:
                                await stop(stop_at)
                                await finalize()
                                return timeout_result()
                            if not await notify_exhausted(deadline):
                                return await stop_and_finalize_timeout()
                            async with asyncio.timeout_at(stop_at):
                                await _confirm_inner_stopped(sandbox, active_container)
                            inner_stopped = True
                            absence_confirmed_at = loop.time()
                            await finalize()
                            finalized_running = True
                    active_container = None
                last_frame = frame
                pending_acks.add(asyncio.create_task(acknowledge(frame.seq)))
                continue
            if deadline_task is not None and deadline_task in done:
                await deadline_task
                if episode_exhausted:
                    return await stop_and_finalize_timeout()
                apparent_deadline = deadline
                stop_at = apparent_deadline + GENERATION_TERMINATION_GRACE_SECONDS
                agent_ended_at = workload.agent_absence_confirmed_at if stage is None else None
                if controller.client is not None:
                    credit_read_deadline = apparent_deadline + GENERATION_CREDIT_REFRESH_GRACE_SECONDS
                    async with asyncio.timeout_at(credit_read_deadline):
                        await controller.refresh()
                    agent_ended_at = workload.agent_absence_confirmed_at if stage is None else None
                    next_deadline = controller.deadline(loop.time())
                    assert next_deadline is not None
                    if next_deadline > loop.time():
                        deadline = next_deadline
                        stop_at = None
                        deadline_task = asyncio.create_task(wait_deadline())
                        continue
                    effective_cutoff = next_deadline
                else:
                    effective_cutoff = apparent_deadline
                controller.freeze_credit()
                if agent_ended_at is not None and _controlled_completion_precedes_deadline(
                    agent_ended_at, effective_cutoff
                ):
                    result_deadline = effective_cutoff + GENERATION_TERMINATION_GRACE_SECONDS
                    try:
                        async with asyncio.timeout_at(result_deadline):
                            if loop.time() >= result_deadline:
                                raise TimeoutError
                            if output_task.done():
                                await finish_output()
                            while not wait_task.done():
                                watched: set[asyncio.Task[Any]] = {wait_task}
                                if not output_task.done():
                                    watched.add(output_task)
                                done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
                                if output_task in done:
                                    await finish_output()
                            await completed_result()
                            await finish_output()
                            if loop.time() >= result_deadline:
                                raise TimeoutError
                    except TimeoutError as error:
                        raise ControlledGenerationTerminationUnconfirmedError(
                            "Agent result, output, or workload cleanup unconfirmed after collection window"
                        ) from error
                    continue
                if episode is not None and stage is not None:
                    if not await notify_exhausted(apparent_deadline):
                        return await stop_and_finalize_timeout()
                    deadline = apparent_deadline + EPISODE_END_GRACE_SECONDS
                    deadline_task = asyncio.create_task(asyncio.sleep(max(0.0, deadline - loop.time())))
                    continue
                return await stop_and_finalize_timeout()
    except BaseException as error:
        primary_accounting_failure = error is accounting_error
        if notification_task is not None and not notification_task.done():
            notification_task.cancel()

        async def failure_cleanup(error: BaseException) -> None:
            nonlocal outer_stopped, inner_stopped, absence_confirmed_at
            stop_error: BaseException | None = None
            loss_error: SandboxNotFoundError | None = None
            if isinstance(error, SandboxNotFoundError):
                outer_stopped = inner_stopped = True
                absence_confirmed_at = loop.time()
            elif isinstance(error, ControlledGenerationTerminationUnconfirmedError):
                stop_error = error
            elif error is accounting_error and outer_stopped and (active_container is None or inner_stopped):
                pass
            else:
                try:
                    if not (wait_task.done() and not wait_task.cancelled() and wait_task.exception() is None):
                        await stop(stop_at)
                    else:
                        await completed_result()
                        if active_container is not None and not inner_stopped:
                            async with asyncio.timeout_at(stop_at):
                                await _confirm_inner_stopped(sandbox, active_container)
                            inner_stopped = True
                            absence_confirmed_at = loop.time()
                except SandboxNotFoundError as absent:
                    outer_stopped = inner_stopped = True
                    absence_confirmed_at = loop.time()
                    loss_error = absent
                except BaseException as failed_stop:
                    stop_error = ControlledGenerationTerminationUnconfirmedError(
                        "Controlled generation failed without confirmed nested and outer workload absence"
                    )
                    stop_error.__cause__ = failed_stop
            await disarm()
            if stop_error is None:
                await _cancel_and_join_controlled_tasks(wait_task)
                if isinstance(error, SandboxNotFoundError) or loss_error is not None:
                    await _cancel_and_join_controlled_tasks(output_task)
                elif not output_task.done():
                    try:
                        await finish_output()
                    except BaseException as output_error:
                        error.add_note(f"Generation output cleanup after confirmed stop failed: {output_error!r}")
            if finalize_task is not None or stop_error is None:
                try:
                    if finalize_task is None:
                        await finalize()
                    else:
                        await finish_cleanup(finalize_task)
                except BaseException as cleanup_accounting_error:
                    error.add_note(
                        f"Generation accounting cleanup after confirmed stop failed: {cleanup_accounting_error!r}"
                    )
                    logger.exception("Could not finalize generation accounting after confirmed workload stop")
            if stop_error is not None:
                raise stop_error
            if loss_error is not None:
                raise loss_error

        cleanup_task = asyncio.create_task(failure_cleanup(error))
        await finish_cleanup(cleanup_task)
        # A terminal accounting failure may collect only after both workloads are absent.
        if primary_accounting_failure and outer_stopped and (active_container is None or inner_stopped):
            task = asyncio.current_task()
            assert task is not None
            if not task.cancelling():
                try:
                    await on_accounting_failure()
                except Exception:
                    logger.exception("Could not collect outputs after terminal accounting failure")
        raise
    finally:

        async def finish_children() -> None:
            try:
                await disarm()
                await _cancel_and_join_controlled_tasks(*pending_acks)
                if event_task is not None:
                    await _cancel_and_join_controlled_tasks(event_task)
                await _cancel_and_join_controlled_tasks(wait_task, output_task)
            finally:
                if notification_task is not None:
                    try:
                        await finish_cleanup(notification_task)
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        logger.exception("Exhausted notification cleanup failed after generation stop")
                if error_path is not None:
                    try:
                        await asyncio.wait_for(
                            _exec(sandbox, f"rm -f {shlex.quote(error_path)}"),
                            timeout=_POST_EXIT_READ_TIMEOUT_SECONDS,
                        )
                    except Exception:
                        pass

        await finish_cleanup(asyncio.create_task(finish_children()))


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
    plan: GenerationPlan,
    deadline_controller: ExternalServiceDeadlineController,
    agent_output_s3_key: str | None = None,
    benchmark_id: str | None = None,
    execution_is_current: Callable[[], bool] | None = None,
    on_external_service_finalized: (Callable[[GenerationAccountingSummary], Awaitable[None]] | None) = None,
    generation_gateway_url: str | None = None,
    *,
    redact_error: Callable[[str], str] | None = None,
    secret_env_names: tuple[str, ...] = (),
) -> tuple[AgentCausedExitReason | None, float]:
    """
    Run the agent or episode orchestrator inside the sandbox for a given task.

    Args:
        sandbox: The sandbox in which the workload runs
        contract: The agent contract configuration
        problem_path: Path inside the sandbox where the problem statement file was written during setup
        log_output: Callback to log output
        cwd: Working directory to run the agent in
        agent_output_s3_key: S3 key to where we will upload the final output archive to
        plan: Task generation execution plan
        execution_is_current: Optional execution-authority check before output uploads
        deadline_controller: Tracker-owned generation deadline controller
        on_external_service_finalized: Awaited persistence callback after confirmed generation absence

    Returns:
        AgentCausedExitReason if the workload exits abnormally but recoverably
        (timeout or OS kill), None on clean exit.

    Raises:
        SandboxError: If the workload fails to run or times out
    """
    log_output(
        f"Running episode orchestrator for {contract.name}"
        if plan.episode is not None
        else f"Running agent {contract.name}"
    )
    if plan.episode is not None and plan.episode.parallel_agents > 1:
        if "{slot_dir}" not in contract.run_cmd:
            raise InvalidSandboxConfigurationError("Parallel episode agent run_cmd requires {slot_dir}")
        if contract.final_output is not None and "{slot_dir}" not in contract.final_output:
            raise InvalidSandboxConfigurationError("Parallel episode final_output requires {slot_dir}")
        if contract.turns is not None and contract.turns.container_name is not None:
            raise InvalidSandboxConfigurationError("Parallel episode does not support container_name")

    runtime_containment = sandbox.generation_containment
    if (
        runtime_containment is None
        or runtime_containment.type not in ("linux_cgroup_v2", "linux_process_group")
        or runtime_containment.version != 1
    ):
        raise InvalidSandboxConfigurationError(
            "Effective sandbox does not support controlled generation containment v1"
        )
    await sandbox.probe_generation_containment()

    static_bindings = {**contract.kwargs, "task_id": task_id}
    if plan.episode is None:
        bindings = {**static_bindings, "problem_statement_path": problem_path}
        run_cmd = re.sub(
            "|".join(re.escape("{" + name + "}") for name in bindings),
            lambda match: bindings[match.group(0)[1:-1]],
            contract.run_cmd,
        )
    else:
        run_cmd = plan.episode.command

    await _exec(sandbox, f"mkdir -p {shlex.quote(cwd)}")
    stage_key = await _provision_stage(sandbox) if plan.staged else None
    if plan.episode is not None:
        turns = contract.turns
        await sandbox.upload_file(
            f"{_STAGE_DIR}/agent.json",
            json.dumps(
                {
                    "run_cmd": contract.run_cmd,
                    "continue_cmd": turns.continue_cmd if turns and turns.continue_cmd else None,
                    "command_bindings": static_bindings,
                    "interrupt_grace_seconds": turns.interrupt_grace_seconds if turns else None,
                    "container_name": turns.container_name if turns else None,
                    "final_output": contract.final_output,
                    "parallel_agents": plan.episode.parallel_agents,
                    "slots_root": "/workspace/.valkyrie/slots",
                }
            ).encode(),
        )

    async def upload_outputs(*, preserve_agent_error: bool = False) -> None:
        if execution_is_current is not None and not execution_is_current():
            return
        errors: list[Exception] = []
        output_path = "/workspace/.valkyrie/slots" if plan.episode is not None else contract.final_output
        if output_path:
            try:
                result = await _exec(sandbox, f"test -e {shlex.quote(output_path)}")
                if (
                    result.exit_code == _SUCCESS_EXIT_CODE
                    and agent_output_s3_key
                    and (execution_is_current is None or execution_is_current())
                ):
                    await archive_and_upload_output(
                        sandbox,
                        output_path,
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

        if plan.episode is None and contract.output_artifacts:
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
        command = f"PYTHONSAFEPATH=1 {run_cmd}"
        if stage_key is not None:
            command = f"export VALKYRIE_STAGE_DIR={_STAGE_DIR}; {command}"
        exit_reason, agent_run_time = await _stream_controlled_output(
            sandbox,
            command,
            cwd,
            log_output,
            deadline_controller,
            on_external_service_finalized,
            on_accounting_failure=lambda: upload_outputs(preserve_agent_error=True),
            stage_key=stage_key,
            episode=plan.episode,
            generation_gateway_url=generation_gateway_url,
            redact_error=redact_error if plan.episode is None else None,
            secret_env_names=secret_env_names,
        )
    except AgentRunFailedError:
        await upload_outputs(preserve_agent_error=True)
        raise

    if exit_reason == AgentCausedExitReason.TIMEOUT:
        suffix = (
            "The orchestrator has exited and evaluation will proceed."
            if plan.episode is not None
            else "The process has been terminated and evaluation will proceed."
        )
        log_output(
            f"[WARNING]:`{contract.name}` has reached the designated generation allowance for this task: `{deadline_controller.effective_allowance_seconds()}`. {suffix}"
        )
    elif exit_reason == AgentCausedExitReason.OS_KILLED:
        log_output(
            f"[WARNING]:`{contract.name}` was killed by the OS (exit code {_OS_KILL_EXIT_CODE}, likely out-of-memory). The process has been terminated and evaluation will proceed."
        )

    await upload_outputs()

    # Return why the agent terminated abnormally, or None on clean exit
    return exit_reason, agent_run_time
