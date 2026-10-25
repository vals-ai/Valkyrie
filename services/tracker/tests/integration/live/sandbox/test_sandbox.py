"""Integration tests for live sandbox operations.

Run: uv run pytest tests/integration/live/sandbox/test_sandbox.py

Covers real provider sandbox creation, artifact movement, command streaming,
and agent execution. Add cases here when behavior must be proven against a live
sandbox rather than tracker-owned mocks.
"""

import asyncio
import io
import shlex
import zipfile
from typing import AsyncGenerator

import pytest
from benchmark_service import ImageSource, Resources, Sandbox, SandboxNotFoundError, SandboxProvider

from tests.utils import random_task_id
from tests.integration.seed_agent_artifacts import create_s3_client
from tracker import sandbox as sandbox_module
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import S3ObjectStore, get_benchmark_contract_s3_key
from tracker.runtime.artifacts import agent_bundle_key
from tracker.database.models import AgentCausedExitReason, AgentContractRequest
from tracker.egress import combine_run_egress_policies
from tracker.exceptions import AgentRunFailedError, OutputArtifactError, SandboxError
from tracker.external_service_gateway import ExternalServiceDeadlineController
from tracker.generation_plan import GenerationPlan
from tracker.sandbox import (
    SandboxCreationState,
    apply_egress_policy,
    create_sandbox,
    create_agent_error_redactor,
    DependencySetupMode,
    install_agent_dependencies,
    run_agent,
    stream_command_output,
    upload_agent_artifacts,
)


@pytest.fixture
async def test_sandbox(
    sandbox_provider: SandboxProvider,
    test_resources: Resources,
    test_image: str,
    random_sandbox_name: str,
    creation_semaphore: asyncio.Semaphore,
) -> AsyncGenerator[Sandbox, None]:
    """Create a real provider sandbox for sandbox-operation tests.

    Test cases:
    - The fixture yields a sandbox created from the configured test image.
    - Context cleanup deletes the sandbox after the dependent test completes.
    """

    async with create_sandbox(
        sandbox_provider,
        random_sandbox_name,
        ImageSource(image=test_image),
        test_resources,
        creation_semaphore,
        creation_state=SandboxCreationState(cleanup_pending=False),
    ) as sandbox:
        yield sandbox

    with pytest.raises(SandboxNotFoundError):
        await sandbox_provider.get_sandbox(sandbox.name)


@pytest.fixture
def egress_allowlist_probe_command() -> str:
    """Command that requests an allowlisted host and a blocked host during run_agent."""
    script = """
import urllib.error
import urllib.request


def can_request(url):
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "valkyrie-egress-test"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            print(f"{url} reachable: HTTP {response.status}", flush=True)
            return True
    except (OSError, urllib.error.URLError) as exc:
        print(f"{url} blocked: {type(exc).__name__}", flush=True)
        return False


allowed = can_request("https://example.com")
blocked = can_request("https://www.wikipedia.org")
print(f"allowed={allowed} blocked={blocked}", flush=True)
raise SystemExit(0 if allowed and not blocked else 1)
"""

    return f"python -c {shlex.quote(script)}"


@pytest.fixture
def restored_egress_probe_command() -> str:
    """Command that verifies unrestricted egress returns after run_agent cleanup."""
    script = """
import urllib.request


request = urllib.request.Request(
    "https://www.wikipedia.org",
    method="HEAD",
    headers={"User-Agent": "valkyrie-egress-test"},
)
with urllib.request.urlopen(request, timeout=5) as response:
    print(f"restored=True status={response.status}", flush=True)
"""

    return f"python -c {shlex.quote(script)}"


class TestSandboxOperations:
    """Integration tests for sandbox operations."""

    async def test_reported_error_survives_live_command(self, test_sandbox: Sandbox) -> None:
        """Preserve a report while excluding stdout and masking a configured secret."""
        output: list[str] = []
        command = 'sh -c \'printf "AgentError: failed with fixture-secret\\n" > "$VALKYRIE_ERROR_PATH"; echo stdout-only-sentinel; exit 1\''

        with pytest.raises(AgentRunFailedError) as failure:
            await stream_command_output(
                test_sandbox,
                command,
                on_output=output.append,
                redact_error=create_agent_error_redactor(["fixture-secret"]),
            )

        assert "AgentError: failed with [REDACTED]" in str(failure.value)
        assert "fixture-secret" not in str(failure.value)
        assert "stdout-only-sentinel" not in str(failure.value)
        assert "stdout-only-sentinel" in "".join(output)

        remaining = await test_sandbox.exec("find /tmp/.valkyrie -type f")

        assert remaining.exit_code == 0
        assert not remaining.stdout.strip()

    @pytest.mark.parametrize(
        "report_command",
        [
            "true",
            ': > "$VALKYRIE_ERROR_PATH"',
            'printf "AgentError: %2049s" x > "$VALKYRIE_ERROR_PATH"',
        ],
        ids=["missing", "empty", "oversized"],
    )
    async def test_unusable_report_keeps_exit_code(self, test_sandbox: Sandbox, report_command: str) -> None:
        """An unusable report retains the exit code and does not substitute stdout."""
        output: list[str] = []
        command = f"sh -c {shlex.quote(report_command + '; echo stdout-only-sentinel; exit 41')}"

        with pytest.raises(AgentRunFailedError) as failure:
            await stream_command_output(
                test_sandbox, command, output.append, redact_error=create_agent_error_redactor([])
            )

        assert str(failure.value) == "Sandbox error: Agent command failed with exit code 41"
        assert "stdout-only-sentinel" in "".join(output)
        remaining = await test_sandbox.exec("find /tmp/.valkyrie -type f")
        assert remaining.exit_code == 0
        assert not remaining.stdout.strip()

    async def test_install_preserves_reported_error(self, test_sandbox: Sandbox) -> None:
        """The final install attempt retains the report and masks the configured value."""
        contract = AgentContractRequest(
            name="test_agent",
            install_cmd='printf "InstallError: fixture-secret\\n" > "$VALKYRIE_ERROR_PATH"; exit 42',
            run_cmd="true",
        )
        setup = await test_sandbox.exec("mkdir -p /bundle/test_agent")
        assert setup.exit_code == 0

        with pytest.raises(AgentRunFailedError) as failure:
            await install_agent_dependencies(
                test_sandbox,
                contract,
                lambda _message: None,
                mode=DependencySetupMode.FINAL_FRESH_SANDBOX,
                redact_error=create_agent_error_redactor(["fixture-secret"]),
            )

        assert str(failure.value) == (
            "Sandbox error: Dependency installation for contract test_agent failed with exit code 42: "
            "InstallError: [REDACTED]"
        )

    async def test_run_preserves_error_when_required_artifact_is_missing(
        self,
        test_sandbox: Sandbox,
        live_aws_runtime: AWSRuntime,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Retain the agent failure and log the secondary collection failure with its context."""
        contract = AgentContractRequest(
            name="test_agent",
            install_cmd="true",
            run_cmd='sh -c \'printf "AgentError: controlled failure\\n" > "$VALKYRIE_ERROR_PATH"; exit 41\'',
            output_artifacts=["artifacts/missing.json"],
        )
        object_store = S3ObjectStore(live_aws_runtime)
        task_id = random_task_id()

        # Tracker's logger does not propagate to pytest's root capture handler.
        sandbox_module.logger.addHandler(caplog.handler)
        try:
            with pytest.raises(AgentRunFailedError) as failure:
                await run_agent(
                    test_sandbox,
                    contract,
                    "/tmp/problem.txt",
                    task_id,
                    lambda _message: None,
                    cwd="/",
                    object_store=object_store,
                    benchmark_id="live-error-preservation",
                    redact_error=create_agent_error_redactor([]),
                )
        finally:
            sandbox_module.logger.removeHandler(caplog.handler)

        assert (
            str(failure.value)
            == "Sandbox error: Agent command failed with exit code 41: AgentError: controlled failure"
        )
        collection_errors = [
            record for record in caplog.records if record.getMessage() == "Failed to collect declared agent artifacts"
        ]
        assert len(collection_errors) == 1
        record = collection_errors[0]
        assert record.exc_info is not None
        assert isinstance(record.exc_info[1], OutputArtifactError)
        assert "Required output artifact missing:" in str(record.exc_info[1])
        assert record.exc_info[1].__context__ is failure.value
        assert getattr(record, "task_id") == task_id

    async def test_create_and_cleanup_sandbox(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify sandbox context creation and cleanup through the real provider.

        Test cases:
        - The created sandbox can execute a simple command and uses the requested name prefix.
        - Exiting the context deletes the sandbox so provider lookup raises SandboxNotFoundError.
        """

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            assert sandbox.name.startswith(random_sandbox_name)
            result = await sandbox.exec("echo 'test'")
            assert result.exit_code == 0
            actual_name = sandbox.name

        with pytest.raises(SandboxNotFoundError):
            await sandbox_provider.get_sandbox(actual_name)

    async def test_upload_agent_artifacts(
        self,
        test_sandbox: Sandbox,
        live_aws_runtime: AWSRuntime,
    ) -> None:
        """Verify benchmark-scoped agent artifacts are downloaded from S3 into the sandbox.

        Test cases:
        - The frozen benchmark contract zip is extracted into /bundle.
        - Setup and agent source files are readable from their expected sandbox paths.
        """
        contract_name = "test_contract"
        benchmark_id = "test-benchmark-id-abc"
        contract = AgentContractRequest(
            name=contract_name,
            install_cmd="bash setup.sh",
            run_cmd="echo hello",
        )
        aws_runtime = live_aws_runtime
        object_store = S3ObjectStore(aws_runtime)

        agent_file = f"{contract_name}/{contract_name}/file.txt"
        setup_file = f"{contract_name}/setup.sh"

        # Create a test zip with the expected structure
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(setup_file, "#!/bin/bash\necho 'setup'")
            zf.writestr(agent_file, "hello world")
        zip_buffer.seek(0)

        # Upload zip to real S3
        s3 = create_s3_client(live_aws_runtime)
        agent_key = agent_bundle_key(contract_name)
        frozen_key = get_benchmark_contract_s3_key(benchmark_id, contract_name)
        s3.put_object(
            Bucket=live_aws_runtime.resources.s3_bucket,
            Key=agent_key,
            Body=zip_buffer.getvalue(),
        )
        # Stage the per-benchmark frozen copy that upload_agent_artifacts will now read from.
        s3.put_object(
            Bucket=live_aws_runtime.resources.s3_bucket,
            Key=frozen_key,
            Body=zip_buffer.getvalue(),
        )

        try:
            await upload_agent_artifacts(test_sandbox, contract, benchmark_id, object_store)

            # Verify files exist in sandbox
            result = await test_sandbox.exec(f"cat /bundle/{setup_file}")
            assert result.exit_code == 0
            assert "echo 'setup'" in result.stdout

            result = await test_sandbox.exec(f"cat /bundle/{agent_file}")
            assert result.exit_code == 0
            assert "hello world" in result.stdout
        finally:
            s3.delete_object(Bucket=live_aws_runtime.resources.s3_bucket, Key=agent_key)
            s3.delete_object(Bucket=live_aws_runtime.resources.s3_bucket, Key=frozen_key)

    async def test_install_agent_dependencies(self, test_sandbox: Sandbox) -> None:
        """Verify the contract install command runs from the sandbox bundle.

        Test cases:
        - install_agent_dependencies logs its installation banner.
        - Output from the setup script is streamed through the provided log callback.
        """
        logged_messages: list[str] = []

        def log_callback(message: str) -> None:
            logged_messages.append(message)

        contract_name = "test_contract"
        contract = AgentContractRequest(
            name=contract_name,
            install_cmd="bash setup.sh",
            run_cmd="echo hello",
        )

        # Create the contract directory and setup.sh directly in sandbox
        await test_sandbox.exec(f"mkdir -p /bundle/{contract_name}")
        await test_sandbox.exec(f"echo '#!/bin/bash\necho hello world' > /bundle/{contract_name}/setup.sh")

        await install_agent_dependencies(test_sandbox, contract, log_callback)

        # Verify messages were logged
        output = "\n".join(logged_messages)
        assert "Installing dependencies" in output
        assert "hello world" in output

    async def test_run_agent(
        self,
        test_sandbox: Sandbox,
        live_aws_runtime: AWSRuntime,
    ) -> None:
        """Verify run_agent streams output while executing a contract command.

        Test cases:
        - The contract run command executes from the sandbox and writes its final output artifact.
        - All emitted output lines are passed to the log callback.
        """
        logged_messages: list[str] = []

        def log_callback(message: str) -> None:
            logged_messages.append(message)

        run_cmd = 'echo line1 && sleep 1 && echo line2 && sleep 1 && echo line3 && echo \'{"result": "hello world"}\' > /tmp/agent_output.json'

        contract = AgentContractRequest(
            name="test_agent",
            install_cmd="true",
            run_cmd=run_cmd,
            final_output="/tmp/agent_output.json",
        )
        aws_runtime = live_aws_runtime
        object_store = S3ObjectStore(aws_runtime)

        # Expecting bundle directory to exist
        await test_sandbox.exec("mkdir -p /bundle/test_agent")

        await run_agent(
            test_sandbox,
            contract,
            "some problem statement",
            task_id=random_task_id(),
            log_output=log_callback,
            cwd="/",
            object_store=object_store,
            plan=GenerationPlan(allowance_seconds=None, staged=False, episode=None),
            deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=None),
        )

        output = "\n".join(logged_messages)
        assert "line1" in output
        assert "line2" in output
        assert "line3" in output

    async def test_run_policy_allowlist_then_unrestricted_transition(
        self,
        test_sandbox: Sandbox,
        live_aws_runtime: AWSRuntime,
        egress_allowlist_probe_command: str,
        restored_egress_probe_command: str,
    ) -> None:
        """Verify real provider allowlist and unrestricted stage transitions."""
        logged_messages: list[str] = []

        def log_callback(message: str) -> None:
            logged_messages.append(message)

        contract = AgentContractRequest(
            name="test_agent",
            install_cmd="true",
            run_cmd=egress_allowlist_probe_command,
            egress_allowlist=["https://example.com"],
        )

        await test_sandbox.exec("mkdir -p /bundle/test_agent")
        aws_runtime = live_aws_runtime
        object_store = S3ObjectStore(aws_runtime)

        await apply_egress_policy(
            test_sandbox,
            combine_run_egress_policies(None, contract.egress_allowlist),
        )
        await run_agent(
            test_sandbox,
            contract,
            "some problem statement",
            task_id=random_task_id(),
            log_output=log_callback,
            cwd="/",
            object_store=object_store,
            plan=GenerationPlan(allowance_seconds=None, staged=False, episode=None),
            deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=None),
        )

        output = "\n".join(logged_messages)
        assert "allowed=True blocked=False" in output

        await apply_egress_policy(test_sandbox, "*")
        restored_result = await test_sandbox.exec(restored_egress_probe_command)
        assert restored_result.exit_code == 0
        assert "restored=True" in restored_result.stdout

    async def test_deterministic_timeout_behavior(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify stream_command_output classifies timeout exits without raising.

        Test cases:
        - A command killed by the shell timeout returns AgentCausedExitReason.TIMEOUT.
        - A command that completes before the timeout returns no agent-caused exit reason.
        """

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            command = "timeout 15 sleep 70"

            exit_reason = await stream_command_output(sandbox, command, on_output=print)

            assert exit_reason == AgentCausedExitReason.TIMEOUT

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            command = "timeout 15 sleep 10"

            exit_reason = await stream_command_output(sandbox, command, on_output=print)

            assert exit_reason is None

    async def test_pty_streaming_captures_all_output(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify PTY streaming captures output across multiple command stages.

        Test cases:
        - stream_command_output does not report a timeout for a successful multi-stage command.
        - Output before, between, and after sleeps is delivered to the log callback.
        """
        logged_messages: list[str] = []

        def log_callback(message: str) -> None:
            logged_messages.append(message)

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            command = "echo 'STAGE_1' && sleep 1 && echo 'STAGE_2' && sleep 1 && echo 'STAGE_3'"

            exit_reason = await stream_command_output(sandbox, command, on_output=log_callback)

            assert exit_reason is None
            output = "\n".join(logged_messages)
            assert "STAGE_1" in output
            assert "STAGE_2" in output
            assert "STAGE_3" in output

    async def test_stream_command_raises_not_found_when_sandbox_is_deleted(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify stream_command_output preserves deleted-sandbox errors.

        Test cases:
        - A command is running while the sandbox is deleted through the provider.
        - The wrapper raises SandboxNotFoundError instead of converting it to tracker SandboxError.
        """

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            ready = asyncio.Event()

            def mark_ready(message: str) -> None:
                if "stream-ready" in message:
                    ready.set()

            stream_task = asyncio.create_task(
                stream_command_output(sandbox, "echo stream-ready && sleep 30", on_output=mark_ready)
            )

            async def destroy_sandbox_after_stream_starts() -> None:
                await asyncio.wait_for(ready.wait(), timeout=30)
                await sandbox_provider.delete_sandbox(sandbox.id)

            with pytest.raises(SandboxNotFoundError):
                await asyncio.gather(
                    stream_task,
                    destroy_sandbox_after_stream_starts(),
                )

    async def test_stream_command_raises_on_nonzero_exit(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify non-zero command exits are surfaced as tracker sandbox failures.

        Test cases:
        - A command returning exit code 1 raises SandboxError.
        - The error message includes the command exit code for user-facing diagnostics.
        """
        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            with pytest.raises(SandboxError, match="exit code 1"):
                await stream_command_output(sandbox, "false", on_output=lambda _: None)
