"""Integration tests for sandbox operations."""

import asyncio
import io
import zipfile
from typing import AsyncGenerator

import boto3
import pytest
from benchmark_service import ImageSource, Resources, Sandbox, SandboxNotFoundError, SandboxProvider

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
from tests.utils import random_task_id


@pytest.fixture
async def test_sandbox(
    sandbox_provider: SandboxProvider,
    test_resources: Resources,
    test_image: str,
    random_sandbox_name: str,
    creation_semaphore: asyncio.Semaphore,
) -> AsyncGenerator[Sandbox, None]:
    """Create a test sandbox with Python."""

    async with create_sandbox(
        sandbox_provider,
        random_sandbox_name,
        ImageSource(image=test_image),
        test_resources,
        creation_semaphore,
    ) as sandbox:
        yield sandbox


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
        """Test that sandbox is created and cleaned up properly."""

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
        self, test_sandbox: Sandbox, aws_credentials: AWSCredentials, harness_config: HarnessConfig
    ) -> None:
        """Test that agent artifacts are uploaded to the sandbox."""
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
            region_name=aws_credentials.aws_default_region,
            aws_access_key_id=aws_credentials.aws_access_key_id,
            aws_secret_access_key=aws_credentials.aws_secret_access_key,
            aws_session_token=aws_credentials.aws_session_token,
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
                test_sandbox, contract, benchmark_id, aws_credentials, harness_config.s3_bucket
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
        """Test that install command is correctly executed in the sandbox."""
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
        aws_credentials: AWSCredentials,
        harness_config: HarnessConfig,
    ) -> None:
        """Test that agent runs and prints output lines."""
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
            aws=aws_credentials,
            s3_bucket=harness_config.s3_bucket,
        )

        output = "\n".join(logged_messages)
        assert "line1" in output
        assert "line2" in output
        assert "line3" in output

    async def test_deterministic_timeout_behavior(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ):
        """
        Timeouts are correct caught and returned from the stream outputs method

        Test Cases:
        - Sandbox times out when we sleep past the timeout
        - Sandbox does not timeout when the sleep is less than the timeout
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
        """Test that PTY streaming captures output from a multi-stage command."""
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

    async def test_stream_command_raises_on_sandbox_crash(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that a sandbox crash during execution is detected and raised."""

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
        ) as sandbox:

            async def destroy_sandbox_after_delay() -> None:
                await asyncio.sleep(2)
                await sandbox_provider.delete_sandbox(sandbox.id)

            with pytest.raises(SandboxError, match="crashed|health"):
                await asyncio.gather(
                    stream_command_output(sandbox, "sleep 30", on_output=lambda _: None),
                    destroy_sandbox_after_delay(),
                )

    async def test_stream_command_raises_on_nonzero_exit(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that a command with non-zero exit code raises SandboxError."""
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
