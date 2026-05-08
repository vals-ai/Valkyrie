"""Sandbox management utilities for the tracker service."""

import base64
import shlex
import time
import uuid
from asyncio import Semaphore
from collections import deque
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from typing import Any, AsyncGenerator

import logfire
import sentry_sdk
from benchmark_service.schemas import Resources as TrackerResources
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaNotFoundError,
    ExecuteResponse,
    Resources,
    SandboxState,
)
from daytona.common.errors import DaytonaError
from daytona.handle.async_pty_handle import AsyncPtyHandle
from opentelemetry import trace
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from tracker.daytona_retry import daytona_retry_callback, wait_daytona_rate_limit
from tracker.database.models import AgentCausedExitReason, AgentContractRequest
from tracker.exceptions import (
    AgentRunFailedError,
    InvalidSandboxConfigurationError,
    PtyCreationError,
    SandboxError,
    SandboxSetupError,
    SSLConnectionError,
)
from tracker.logging import get_logger
from tracker.observability import (
    distribution,
    elapsed_ms,
    gauge,
    incr,
    retry_callback,
    set_pty_context,
    set_sandbox_context,
    tag_daytona_error,
)
from tracker.aws.s3 import create_presigned_url, get_benchmark_contract_s3_key, upload_to_s3
from tracker.types import AWSCredentials

logger = get_logger(__name__)


bundle_path = PurePosixPath("/bundle")
SNAPSHOT_IMAGE_PREFIX = "snapshot:"


def get_contract_path(contract_name: str) -> PurePosixPath:
    """Get the path to a contract in the sandbox."""
    return bundle_path / contract_name


@retry(
    retry=retry_if_exception_type(DaytonaError),
    stop=stop_after_attempt(3),
    wait=wait_daytona_rate_limit(non_rate_limit_wait=wait_fixed(2)),
    before_sleep=daytona_retry_callback("valkyrie.sandbox.delete", op="sandbox.delete"),
    reraise=True,
)
async def delete_sandbox(sandbox: AsyncSandbox, daytona: AsyncDaytona) -> None:
    """Delete sandbox if it is not already destroyed or being destroyed"""
    try:
        await sandbox.refresh_data()

        if sandbox.state not in [SandboxState.DESTROYING, SandboxState.DESTROYED]:
            # Set auto-stop interval in-case we fail to delete the sandbox
            await sandbox.set_autostop_interval(interval=1)
            await daytona.delete(sandbox)
    except DaytonaNotFoundError:
        # If we error here that means the sandbox has just been deleted before we could refresh the state
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
    except DaytonaError as e:
        tag_daytona_error(e, op="sandbox.delete")
        raise
    except Exception as e:
        logger.error(f"Unexpected error deleting sandbox {sandbox.name}: {e}")


def _metric_image_name(image: str) -> str:
    if image.startswith(SNAPSHOT_IMAGE_PREFIX):
        return "snapshot"

    without_digest = image.split("@", maxsplit=1)[0]
    last_slash = without_digest.rfind("/")
    last_colon = without_digest.rfind(":")
    if last_colon > last_slash:
        without_digest = without_digest[:last_colon]

    return without_digest[:80]


def _set_sandbox_create_span_attributes(
    sandbox_name: str,
    image: str,
    resources: TrackerResources,
) -> None:
    span = trace.get_current_span()
    span.set_attribute("valkyrie.sandbox_name", sandbox_name)
    span.set_attribute("valkyrie.image", image)
    span.set_attribute("valkyrie.resources.vcpu", resources.vcpu)
    span.set_attribute("valkyrie.resources.memory", resources.memory)
    span.set_attribute("valkyrie.resources.disk", resources.disk)


def _set_sandbox_span_attributes(sandbox: AsyncSandbox) -> None:
    span = trace.get_current_span()
    span.set_attribute("valkyrie.sandbox_id", sandbox.id)
    span.set_attribute("valkyrie.sandbox_name", sandbox.name)
    state = getattr(sandbox, "state", None)
    if state is not None:
        span.set_attribute("valkyrie.sandbox_state", str(state))


@retry(
    retry=retry_if_not_exception_type(InvalidSandboxConfigurationError),
    stop=stop_after_attempt(3),
    wait=wait_daytona_rate_limit(non_rate_limit_wait=wait_exponential(multiplier=1, min=5, max=30)),
    before_sleep=daytona_retry_callback("valkyrie.sandbox.create", op="sandbox.create"),
    reraise=True,
)
@logfire.instrument("sandbox.create", extract_args=False)
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
    _set_sandbox_create_span_attributes(sandbox_name, image, resources)

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
                auto_stop_interval=0,
                auto_delete_interval=0,
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
            auto_stop_interval=0,
            auto_delete_interval=0,
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
    creation_semaphore: Semaphore,
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
        creation_semaphore: Per-benchmark semaphore to limit concurrent sandbox creation.

    Returns:
        A context manager that yields the sandbox
    """
    logger.info(f"Creating sandbox {sandbox_name} with image {image}")

    # If we run too many at once it can cause hanging issues
    # NOTE does not block how many context managers we can have open, just how many sandboxes we can create at once
    try:
        async with creation_semaphore:
            start = time.monotonic()
            sandbox = await _create_sandbox(daytona, sandbox_name, image, resources, labels, env_vars)
    except Exception as e:
        incr("valkyrie.sandbox.create.errors", tags={"error_class": type(e).__name__})
        if isinstance(e, DaytonaError):
            tag_daytona_error(e, op="sandbox.create")
        raise

    distribution(
        "valkyrie.sandbox.create.duration",
        time.monotonic() - start,
        tags={"image": _metric_image_name(image)},
    )
    set_sandbox_context(sandbox, image=image)

    try:
        yield sandbox
    except Exception as e:
        logger.error(f"Error during sandbox execution {sandbox.name}: {e}")
        raise
    finally:
        await delete_sandbox(sandbox, daytona)


@retry(
    retry=retry_if_exception_type(SandboxError) & retry_if_not_exception_type(SandboxSetupError),
    reraise=True,
    stop=stop_after_attempt(3),
    before_sleep=retry_callback("valkyrie.sandbox.upload"),
)
async def upload_agent_artifacts(
    sandbox: AsyncSandbox,
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
    presigned_url = create_presigned_url(contract_s3_key, aws, s3_bucket)

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
        f"unzip -o -d {bundle_dir} {zip_path} -x contract.py",
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
        f"Command failed with exit code {result.exit_code}: {result.result}"
    )
    if result.exit_code == 35:
        raise SSLConnectionError(error_message)

    if result.exit_code != 0:
        raise SandboxError(error_message)


@retry(
    retry=retry_if_exception_type(SandboxError),
    reraise=True,
    stop=stop_after_attempt(3),
    before_sleep=retry_callback("valkyrie.sandbox.deps"),
)
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

    await stream_command_output(sandbox, f"cd {shlex.quote(str(contract_path))} && {contract.install_cmd}", log_output)

    log_output(f"Finished installing dependencies for contract: {contract.name}")


# NOTE: If this gets too big move it into a mapping
# these are decoupled since its just 2 exit codes we need to track
_TIMEOUT_EXIT_CODE: int = 124
_OS_KILL_EXIT_CODE: int = 137
_SUCCESS_EXIT_CODE: int = 0

# Process exec retry settings
_EXEC_MAX_ATTEMPTS: int = 3
_EXEC_DELAY_SECONDS: float = 2.0

# Reconnect to the PTY retry settings
_PTY_RECONNECT_MAX_ATTEMPTS: int = 10
_PTY_RECONNECT_DELAY_SECONDS: float = 1.0

# Creating a PTY retry settings
_PTY_CREATE_MAX_ATTEMPTS: int = 5
_PTY_CREATE_DELAY_SECONDS: float = 2.0

# Process-global cap on concurrent PTY WebSocket handshakes.
# Daytona starts failing above ~500 concurrent handshakes; 100 held 800 PTYs cleanly in testing.
# Temporarily effectively unbounded while validating the new Daytona SDK.
_PTY_HANDSHAKE_CAP: int = 1_000_000
_PTY_HANDSHAKE_SLOW_LOG_THRESHOLD: float = 2.0

_pty_handshake_semaphore: Semaphore = Semaphore(_PTY_HANDSHAKE_CAP)
_pty_handshake_in_flight_count: int = 0

# States that determine if the sandbox has been killed
_DEAD_SANDBOX_STATES = (SandboxState.DESTROYING, SandboxState.DESTROYED, SandboxState.STOPPED)


def _set_pty_span_attributes(sandbox: AsyncSandbox, session_id: str) -> None:
    span = trace.get_current_span()
    span.set_attribute("valkyrie.sandbox_id", sandbox.id)
    span.set_attribute("valkyrie.sandbox_name", sandbox.name)
    span.set_attribute("valkyrie.pty_session_id", session_id)


def _log_pty_event(event: str, sandbox: AsyncSandbox, session_id: str, **extra: Any) -> None:
    logger.info(
        f"pty.{event}",
        extra={
            "pty_event": event,
            "session_id": session_id,
            "sandbox_id": sandbox.id,
            **extra,
        },
    )


@asynccontextmanager
async def _pty_handshake_slot(operation: str, session_id: str) -> AsyncGenerator[None, None]:
    """
    Gate a PTY WebSocket handshake with a process-global semaphore. Released as soon as the
    handshake call returns, so the PTY's subsequent lifetime (wait, send_input, etc.) is not gated.

    Logs when the gate is saturated on entry or when the handshake itself is slow.
    """
    gate_full_on_entry = _pty_handshake_semaphore.locked()

    wait_start = time.monotonic()
    async with _pty_handshake_semaphore:
        global _pty_handshake_in_flight_count

        wait_duration = time.monotonic() - wait_start
        distribution("valkyrie.pty.handshake.wait_duration", wait_duration, tags={"operation": operation})

        _pty_handshake_in_flight_count += 1
        gauge(
            "valkyrie.pty.handshake.in_flight",
            _pty_handshake_in_flight_count,
            tags={"operation": operation},
        )

        if gate_full_on_entry:
            logger.info(
                f"PTY handshake gate full: {operation} session={session_id} "
                f"cap={_PTY_HANDSHAKE_CAP} waited={wait_duration:.2f}s"
            )

        handshake_start = time.monotonic()
        try:
            yield
        finally:
            handshake_duration = time.monotonic() - handshake_start
            distribution("valkyrie.pty.handshake.duration", handshake_duration, tags={"operation": operation})
            _pty_handshake_in_flight_count = max(0, _pty_handshake_in_flight_count - 1)
            gauge(
                "valkyrie.pty.handshake.in_flight",
                _pty_handshake_in_flight_count,
                tags={"operation": operation},
            )
            if handshake_duration > _PTY_HANDSHAKE_SLOW_LOG_THRESHOLD:
                logger.warning(
                    f"PTY handshake slow: {operation} session={session_id} duration={handshake_duration:.2f}s"
                )


@retry(
    retry=retry_if_exception_type(DaytonaError),
    stop=stop_after_attempt(_EXEC_MAX_ATTEMPTS),
    wait=wait_daytona_rate_limit(non_rate_limit_wait=wait_fixed(_EXEC_DELAY_SECONDS)),
    before_sleep=daytona_retry_callback("valkyrie.sandbox.exec", op="sandbox.exec"),
    reraise=True,
)
@logfire.instrument("sandbox.exec", extract_args=False)
async def _exec(sandbox: AsyncSandbox, command: str) -> ExecuteResponse:
    """
    Execute a command inside the sandbox with retries for transient network failures.

    Raises:
        DaytonaError: If all retry attempts are exhausted
    """
    _set_sandbox_span_attributes(sandbox)
    try:
        return await sandbox.process.exec(command)
    except DaytonaError as e:
        tag_daytona_error(e, op="sandbox.exec")
        raise


@retry(
    retry=retry_if_exception_type(DaytonaError),
    stop=stop_after_attempt(_PTY_CREATE_MAX_ATTEMPTS),
    wait=wait_daytona_rate_limit(non_rate_limit_wait=wait_fixed(_PTY_CREATE_DELAY_SECONDS)),
    before_sleep=daytona_retry_callback("valkyrie.pty.create", op="pty.create"),
    reraise=True,
)
@logfire.instrument("pty.create", extract_args=False)
async def _create_pty_session(
    sandbox: AsyncSandbox,
    session_id: str,
    on_data: Callable[[bytes], None],
    envs: dict[str, str] | None = None,
) -> tuple[AsyncPtyHandle, str]:
    """
    Create a PTY session, retried since network errors do happen.

    Raises:
        DaytonaError: If all retry attempts are exhausted
    """

    # Salt the session id to ensure that no collisions occur
    salted_id = f"{session_id}-{uuid.uuid4().hex[:8]}"

    # Each time we run this we want it to be logged, makes debugging easier
    on_data(f"[Debug]: Creating PTY session with the following id {salted_id}\n".encode())
    _set_pty_span_attributes(sandbox, salted_id)
    set_pty_context(session_id=salted_id)
    _log_pty_event("create", sandbox, salted_id)

    # Attempt to make the PTY session, timeouts occur under load
    async with _pty_handshake_slot("create", salted_id):
        handle = await sandbox.process.create_pty_session(id=salted_id, on_data=on_data, envs=envs)

    # Handle to access the PTY and the session id to pass on
    return handle, salted_id


@logfire.instrument("sandbox.health_check", extract_args=False)
async def _check_sandbox_health(sandbox: AsyncSandbox) -> None:
    """
    Checks if we can connect to a sandbox

    Raises:
         SandboxError: if the sandbox cannot be connected to
    """
    try:
        await sandbox.refresh_data()
        _set_sandbox_span_attributes(sandbox)
        if sandbox.state in _DEAD_SANDBOX_STATES:
            sentry_sdk.set_tag("sandbox_state", str(sandbox.state))
            incr("valkyrie.sandbox.unhealthy", tags={"state": str(sandbox.state)})
            raise SandboxError(f"Sandbox {sandbox.name} crashed during command execution (state: {sandbox.state})")
    except SandboxError:
        raise
    except Exception as e:
        if isinstance(e, DaytonaError):
            tag_daytona_error(e, op="sandbox.health_check")
        raise SandboxError(f"Failed to check sandbox {sandbox.name} health: {e}") from e


@retry(
    retry=retry_if_not_exception_type(SandboxError),
    stop=stop_after_attempt(_PTY_RECONNECT_MAX_ATTEMPTS),
    wait=wait_daytona_rate_limit(non_rate_limit_wait=wait_fixed(_PTY_RECONNECT_DELAY_SECONDS)),
    before_sleep=daytona_retry_callback("valkyrie.pty.reconnect", op="pty.reconnect"),
    reraise=True,
)
@logfire.instrument("pty.reconnect", extract_args=False)
async def _reconnect_and_wait_pty(
    sandbox: AsyncSandbox,
    session_id: str,
    on_data: Callable[[bytes], None],
    on_output: Callable[[str], None],
) -> None:
    """
    Reconnect to a PTY session and wait for it to close,
    Used to determine if a connection issue happened or if the sandbox was killed while the run was in progress

    Raises:
        SandboxError: If we cannot successfully check the sandbox health status
    """
    incr("valkyrie.pty.reconnect.count", tags={"operation": "reconnect"})
    _set_pty_span_attributes(sandbox, session_id)
    set_pty_context(session_id=session_id)

    # Check if the sandbox has been closed
    await _check_sandbox_health(sandbox)

    # Log so the user can see we have seen a disconnection from the websocket (easier to pickup in logs)
    on_output("[Debug]: Disconnected from websocket, creating a new reader and reconnecting\n")
    _log_pty_event("reconnect_start", sandbox, session_id)

    # Reconnect to the PTY. Only the connect handshake is gated; handle.wait() runs ungated below.
    async with _pty_handshake_slot("reconnect", session_id):
        handle = await sandbox.process.connect_pty_session(session_id, on_data)

    # Wait until the command has finished running
    await handle.wait()


@logfire.instrument("pty.wait", extract_args=False)
async def _wait_for_pty(
    sandbox: AsyncSandbox,
    session_id: str,
    handle: Any,
    on_data: Callable[[bytes], None],
    on_output: Callable[[str], None],
    status_path: str,
) -> None:
    """
    Wait for the PTY to close, reconnecting on WebSocket failures.
    If the PTY closes prematurely (e.g. WebSocket idle timeout) but the status file
    hasn't been written yet, reconnect and wait again until the command actually finishes.

    Raises:
        SandboxError: Failed to wait until the command has been completed
    """
    _set_pty_span_attributes(sandbox, session_id)
    try:
        await handle.wait()
        on_output("[Debug]: PTY has been disconnected, handler has stopped polling\n")
        _log_pty_event("stream_disconnect", sandbox, session_id)
    except Exception as e:
        on_output(f"[Debug]: PTY stream has been disconnected (Attempting reconnection): {e}\n")
        _log_pty_event("stream_disconnect_with_error", sandbox, session_id, error_class=type(e).__name__)
        try:
            await _reconnect_and_wait_pty(sandbox, session_id, on_data, on_output)
        except SandboxError:
            raise
        except Exception as e:
            raise SandboxError(f"PTY reconnect failed after {_PTY_RECONNECT_MAX_ATTEMPTS} attempts") from e

    # If the PTY closed cleanly but the command is still running (e.g. idle WebSocket timeout),
    # breaks when the file with the command status code is made
    while True:
        await _check_sandbox_health(sandbox)
        if (await _exec(sandbox, f"test -e {status_path}")).exit_code == 0:
            break

        on_output("[Debug]: PTY closed but status file not written yet, reconnecting\n")
        _log_pty_event("reconnect_status_missing", sandbox, session_id)
        try:
            await _reconnect_and_wait_pty(sandbox, session_id, on_data, on_output)
        except SandboxError:
            raise
        except Exception as e:
            raise SandboxError(f"PTY reconnect failed after {_PTY_RECONNECT_MAX_ATTEMPTS} attempts") from e


async def _read_exit_code(sandbox: AsyncSandbox, status_path: str) -> int:
    """
    Read the command exit code from the status file (Important to determine reason for exiting)

    Raises:
        SandboxError: If we fail to read the exit code from the status file
    """
    try:
        # Read the status file, extracting the exit code
        result = await _exec(sandbox, f"cat {status_path}")

        # If file does not exist or we have no content the command has not produced an exit code
        # That would suggest the sandbox was killed or something interrupted the program
        if result.exit_code != 0 or not result.result.strip():
            raise SandboxError(f"Failed to read exit status from {status_path}")

        # Should always be a integer
        return int(result.result.strip())

    except SandboxError:
        raise
    except Exception as e:
        raise SandboxError(f"Failed to read exit status from {status_path}: {e}") from e


@logfire.instrument("pty_disconnect")
async def _disconnect_pty(handle: AsyncPtyHandle | None, sandbox: AsyncSandbox) -> None:
    """Disconnect from the PTY, ignoring exit errors (typically network errors)."""
    if not handle:
        return

    trace.get_current_span().set_attribute("sandbox_id", sandbox.id)
    try:
        await handle.disconnect()
    except Exception:
        logfire.exception(f"PTY disconnect failed on sandbox {sandbox.id}")


@logfire.instrument("kill_pty_session", extract_args=("session_id",))
async def _kill_pty_session(sandbox: AsyncSandbox, session_id: str | None) -> None:
    """Kill a PTY session, ignoring errors if raised or if session was never created."""
    if not session_id:
        return

    trace.get_current_span().set_attribute("sandbox_id", sandbox.id)
    try:
        await sandbox.process.kill_pty_session(session_id)
    except Exception:
        logfire.exception(f"Failed to kill PTY session {session_id} on sandbox {sandbox.id}")


@logfire.instrument("pty.stream_command_output", extract_args=False)
async def stream_command_output(
    sandbox: AsyncSandbox,
    command: str,
    on_output: Callable[[str], None],
) -> AgentCausedExitReason | None:
    """
    Execute a command inside a sandbox using a PTY session, reconnecting on errors

    The exit code is written to a status file rather than relying on
    the PTY WebSocket close frame, which doesn't reliably propagate it.

    Return:
        AgentCausedExitReason if the command terminated abnormally but recoverably
        (e.g., timeout or OS kill), None on clean exit.
    """
    pty_id = uuid.uuid4().hex
    session_id = f"{sandbox.id}:pty-{pty_id}"
    status_dir = "/tmp/.valkyrie"
    status_path = f"{status_dir}/{pty_id}.status"
    handle: AsyncPtyHandle | None = None
    last_output: deque[str] = deque(maxlen=50)
    _set_pty_span_attributes(sandbox, session_id)

    def on_data(data: bytes) -> None:
        text = data.decode("utf-8", errors="replace")
        on_output(text)
        last_output.append(text)

    try:
        # Create a PTY session, this is where our agent will be running
        # Set term to DUMB and LANG to UTF-8 to account for terminal colors and unsupported unicode characters
        try:
            handle, session_id = await _create_pty_session(
                sandbox, session_id, on_data, envs={"TERM": "dumb", "LANG": "C.UTF-8"}
            )
        except DaytonaError as e:
            tag_daytona_error(e, op="pty.create")
            raise PtyCreationError(
                f"Failed to create PTY session after {_PTY_CREATE_MAX_ATTEMPTS} attempts: {e}"
            ) from e

        # Disable echo to suppress command line noise in the output
        await handle.send_input("stty -echo\n")

        # Capture exit code in a status file
        await handle.send_input(f"mkdir -p {status_dir} && {command}; echo $? > {status_path}; exit\n")

        # Wait for the PTY to finish running the agent, logging data returned
        await _wait_for_pty(sandbox, session_id, handle, on_data, on_output, status_path)

        # Verify sandbox is still alive before reading the status file
        await _check_sandbox_health(sandbox)

        # Read the exit code of the process running the agent
        exit_code = await _read_exit_code(sandbox, status_path)

        # Timeout error has a special handle since its caused by the benchmark service
        # Exit codes are waterfalled
        if exit_code == _TIMEOUT_EXIT_CODE:
            return AgentCausedExitReason.TIMEOUT

        # OS killed the process
        if exit_code == _OS_KILL_EXIT_CODE:
            return AgentCausedExitReason.OS_KILLED

        # Failed error code handling (truncate error shown to the user)
        # Final log is shown to the user, ignore traceback since it provides no insight as to what actually happened in the sandbox
        if exit_code != _SUCCESS_EXIT_CODE:
            tail = "".join(last_output).strip().splitlines()
            recent = "\n".join(tail[-10:]) if tail else "(no output)"
            sentry_sdk.set_tag("agent_exit_code", str(exit_code))
            raise AgentRunFailedError(
                f"Failed to run command {command}, exit code: {exit_code}\nLast output:\n{recent}"
            )

        return None

    finally:
        # Disconnect form PTY, ignoring exception if raised
        await _disconnect_pty(handle, sandbox)

        # Kill the PTY session, ignoring exception if raised
        await _kill_pty_session(sandbox, session_id)

        # Remove the status file, ignoring exception if raised
        try:
            await _exec(sandbox, f"rm -f {status_path}")
        except Exception:
            pass


@logfire.instrument(
    "agent_output.archive_and_upload",
    extract_args=("output_path", "agent_output_s3_key", "benchmark_id", "task_id"),
)
async def archive_and_upload_output(
    sandbox: AsyncSandbox,
    output_path: str,
    agent_output_s3_key: str,
    aws: AWSCredentials,
    s3_bucket: str,
    *,
    benchmark_id: str | None = None,
    task_id: str | None = None,
) -> None:
    """Compress a file in the sandbox into a tar.gz and upload it to S3"""
    archive_path = f"/tmp/{uuid.uuid4().hex}.tar.gz"
    start = time.monotonic()

    tar_result = await _exec(sandbox, f"tar -czf {shlex.quote(archive_path)} {shlex.quote(output_path)}")
    if tar_result.exit_code != 0:
        raise SandboxError(f"Failed to create archive from {output_path}")

    try:
        b64_result = await _exec(sandbox, f"base64 {shlex.quote(archive_path)}")
        if b64_result.exit_code != 0:
            raise SandboxError(f"Failed to read archive from {output_path}")

        file_content = base64.b64decode(b64_result.result)
        await upload_to_s3(file_content, agent_output_s3_key, aws, s3_bucket)

        logger.info(
            "agent_output.archive_and_upload.complete",
            extra={
                "sandbox_id": sandbox.id,
                "sandbox_name": sandbox.name,
                "output_path": output_path,
                "s3_key": agent_output_s3_key,
                "benchmark_id": benchmark_id,
                "task_id": task_id,
                "archive_bytes": len(file_content),
                "duration_ms": elapsed_ms(start),
            },
        )
    finally:
        # `-f` exits silently if the file does not exist
        try:
            await _exec(sandbox, f"rm -f {shlex.quote(archive_path)}")
        except Exception:
            pass


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
    benchmark_id: str | None = None,
) -> AgentCausedExitReason | None:
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
        AgentCausedExitReason if the agent was terminated abnormally but recoverably
        (timeout or OS kill), None on clean exit.

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
    await _exec(sandbox, f"mkdir -p {shlex.quote(cwd)}")

    # Run the agent without including task directory dependencies
    exit_reason = await stream_command_output(
        sandbox, f"cd {shlex.quote(cwd)} && PYTHONSAFEPATH=1 {run_cmd}", log_output
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
        if result.exit_code == _SUCCESS_EXIT_CODE and agent_output_s3_key:
            await archive_and_upload_output(
                sandbox,
                contract.final_output,
                agent_output_s3_key,
                aws,
                s3_bucket,
                benchmark_id=benchmark_id,
                task_id=task_id,
            )

    # Return why the agent terminated abnormally, or None on clean exit
    return exit_reason
