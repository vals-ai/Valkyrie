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

import boto3
import pytest
from benchmark_service import ImageSource, Resources, Sandbox, SandboxNotFoundError, SandboxProvider

from tests.utils import random_task_id
from tracker.aws.s3 import get_benchmark_contract_s3_key, get_contract_s3_key
from tracker.database.models import AgentContractRequest
from tracker.exceptions import SandboxError
from tracker.sandbox import (
    create_sandbox,
    install_agent_dependencies,
    run_agent,
    stream_command_output,
    upload_agent_artifacts,
)
from tracker.types import AWSCredentials, HarnessConfig


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
    ) as sandbox:
        yield sandbox


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
        live_aws_credentials: AWSCredentials,
        harness_config: HarnessConfig,
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

        agent_file = f"{contract_name}/{contract_name}/file.txt"
        setup_file = f"{contract_name}/setup.sh"

        # Create a test zip with the expected structure
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(setup_file, "#!/bin/bash\necho 'setup'")
            zf.writestr(agent_file, "hello world")
        zip_buffer.seek(0)

        # Upload zip to real S3
        s3 = boto3.client(  # type: ignore
            "s3",
            region_name=live_aws_credentials.aws_default_region,
            aws_access_key_id=live_aws_credentials.aws_access_key_id,
            aws_secret_access_key=live_aws_credentials.aws_secret_access_key,
            aws_session_token=live_aws_credentials.aws_session_token,
        )
        agent_key = get_contract_s3_key(contract_name)
        frozen_key = get_benchmark_contract_s3_key(benchmark_id, contract_name)
        s3.put_object(
            Bucket=harness_config.s3_bucket,
            Key=agent_key,
            Body=zip_buffer.getvalue(),
        )
        # Stage the per-benchmark frozen copy that upload_agent_artifacts will now read from.
        s3.put_object(
            Bucket=harness_config.s3_bucket,
            Key=frozen_key,
            Body=zip_buffer.getvalue(),
        )

        try:
            await upload_agent_artifacts(
                test_sandbox,
                contract,
                benchmark_id,
                live_aws_credentials,
                harness_config.s3_bucket,
            )

            # Verify files exist in sandbox
            result = await test_sandbox.exec(f"cat /bundle/{setup_file}")
            assert result.exit_code == 0
            assert "echo 'setup'" in result.stdout

            result = await test_sandbox.exec(f"cat /bundle/{agent_file}")
            assert result.exit_code == 0
            assert "hello world" in result.stdout
        finally:
            s3.delete_object(Bucket=harness_config.s3_bucket, Key=agent_key)
            s3.delete_object(Bucket=harness_config.s3_bucket, Key=frozen_key)

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
        live_aws_credentials: AWSCredentials,
        harness_config: HarnessConfig,
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

        # Expecting bundle directory to exist
        await test_sandbox.exec("mkdir -p /bundle/test_agent")

        await run_agent(
            test_sandbox,
            contract,
            "some problem statement",
            task_id=random_task_id(),
            log_output=log_callback,
            cwd="/",
            aws=live_aws_credentials,
            s3_bucket=harness_config.s3_bucket,
        )

        output = "\n".join(logged_messages)
        assert "line1" in output
        assert "line2" in output
        assert "line3" in output

    async def test_run_agent_applies_egress_allowlist_and_restores_egress(
        self,
        test_sandbox: Sandbox,
        live_aws_credentials: AWSCredentials,
        harness_config: HarnessConfig,
        egress_allowlist_probe_command: str,
        restored_egress_probe_command: str,
    ) -> None:
        """Verify real provider egress rules are scoped to the agent command.

        Test cases:
        - The agent can request the allowlisted URL host but not an off-list host.
        - The off-list host is reachable again after run_agent clears egress rules.
        """
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

        await run_agent(
            test_sandbox,
            contract,
            "some problem statement",
            task_id=random_task_id(),
            log_output=log_callback,
            cwd="/",
            aws=live_aws_credentials,
            s3_bucket=harness_config.s3_bucket,
        )

        output = "\n".join(logged_messages)
        assert "allowed=True blocked=False" in output

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
        ) as sandbox:
            command = "timeout 15 sleep 70"

            command_timeout, _ = await stream_command_output(sandbox, command, on_output=print)

            assert command_timeout

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
        ) as sandbox:
            command = "timeout 15 sleep 10"

            command_timeout, _ = await stream_command_output(sandbox, command, on_output=print)

            assert not command_timeout

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
        ) as sandbox:
            command = "echo 'STAGE_1' && sleep 1 && echo 'STAGE_2' && sleep 1 && echo 'STAGE_3'"

            timed_out, _ = await stream_command_output(sandbox, command, on_output=log_callback)

            assert not timed_out
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
        ) as sandbox:
            # Use `false` (returns 1) instead of `exit 1` — exit kills the writer
            # shell itself, preventing the status file from being written.
            with pytest.raises(SandboxError, match="exit code: 1"):
                await stream_command_output(sandbox, "false", on_output=lambda _: None)
