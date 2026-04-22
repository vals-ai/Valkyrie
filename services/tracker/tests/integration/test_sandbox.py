"""Integration tests for sandbox operations."""

import asyncio
import io
import zipfile
from typing import AsyncGenerator

import boto3
import pytest
from benchmark_service.schemas import Resources
from daytona import AsyncDaytona, AsyncSandbox, DaytonaError

from tests.utils import random_task_id
from tracker.database.models import AgentContractRequest
from tracker.exceptions import SandboxError
from tracker.s3 import get_contract_s3_key
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
    daytona_client: AsyncDaytona,
    test_resources: Resources,
    test_image: str,
    random_sandbox_name: str,
    creation_semaphore: asyncio.Semaphore,
) -> AsyncGenerator[AsyncSandbox, None]:
    """Create a test sandbox with Python."""

    async with create_sandbox(
        daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
    ) as sandbox:
        yield sandbox


class TestSandboxOperations:
    """Integration tests for sandbox operations."""

    async def test_create_and_cleanup_sandbox(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that sandbox is created and cleaned up properly."""

        async with create_sandbox(
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:
            assert sandbox.name == random_sandbox_name
            result = await sandbox.process.exec("echo 'test'")
            assert result.exit_code == 0

        with pytest.raises(DaytonaError):
            await daytona_client.get(random_sandbox_name)

    async def test_upload_agent_artifacts(
        self, test_sandbox: AsyncSandbox, aws_credentials: AWSCredentials, harness_config: HarnessConfig
    ) -> None:
        """Test that agent artifacts are uploaded to the sandbox."""
        contract_name = "test_contract"
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
        )
        s3_key = get_contract_s3_key(contract_name)
        s3.put_object(
            Bucket=harness_config.s3_bucket,
            Key=s3_key,
            Body=zip_buffer.getvalue(),
        )

        try:
            await upload_agent_artifacts(test_sandbox, contract, aws_credentials, harness_config.s3_bucket)

            # Verify files exist in sandbox
            result = await test_sandbox.process.exec(f"cat /bundle/{setup_file}")
            assert result.exit_code == 0
            assert "echo 'setup'" in result.result

            result = await test_sandbox.process.exec(f"cat /bundle/{agent_file}")
            assert result.exit_code == 0
            assert "hello world" in result.result
        finally:
            s3.delete_object(Bucket=harness_config.s3_bucket, Key=s3_key)

    async def test_install_agent_dependencies(self, test_sandbox: AsyncSandbox) -> None:
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
        await test_sandbox.process.exec(f"mkdir -p /bundle/{contract_name}")
        await test_sandbox.process.exec(f"echo '#!/bin/bash\necho hello world' > /bundle/{contract_name}/setup.sh")

        await install_agent_dependencies(test_sandbox, contract, log_callback)

        # Verify messages were logged
        output = "\n".join(logged_messages)
        assert "Installing dependencies" in output
        assert "hello world" in output

    async def test_run_agent(
        self,
        test_sandbox: AsyncSandbox,
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
        await test_sandbox.process.exec("mkdir -p /bundle/test_agent")

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

    async def test_create_sandbox_reuse(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that create_sandbox reuses existing sandbox instead of creating new one."""

        async with create_sandbox(
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox1:
            result = await sandbox1.process.exec("echo 'test'")
            assert result.exit_code == 0
            first_id = sandbox1.id

            async with create_sandbox(
                daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
            ) as sandbox2:
                assert sandbox2.id == first_id

    async def test_deterministic_timeout_behavior(
        self,
        daytona_client: AsyncDaytona,
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
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:
            command = "timeout 15 sleep 70"

            command_timeout = await stream_command_output(sandbox, command, on_output=print)

            assert command_timeout

        async with create_sandbox(
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:
            command = "timeout 15 sleep 10"

            command_timeout = await stream_command_output(sandbox, command, on_output=print)

            assert not command_timeout

    async def test_pty_streaming_captures_all_output(
        self,
        daytona_client: AsyncDaytona,
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
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:
            command = "echo 'STAGE_1' && sleep 1 && echo 'STAGE_2' && sleep 1 && echo 'STAGE_3'"

            timed_out = await stream_command_output(sandbox, command, on_output=log_callback)

            assert not timed_out
            output = "\n".join(logged_messages)
            assert "STAGE_1" in output
            assert "STAGE_2" in output
            assert "STAGE_3" in output

    async def test_pty_reconnect_with_connect_pty_session(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that connect_pty_session can reconnect to a running PTY and receive output."""

        async with create_sandbox(
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:
            session_id = f"{sandbox.id}:pty-reconnect-test"
            before_messages: list[str] = []
            after_messages: list[str] = []

            def before_callback(data: bytes) -> None:
                before_messages.append(data.decode("utf-8", errors="replace"))

            def after_callback(data: bytes) -> None:
                after_messages.append(data.decode("utf-8", errors="replace"))

            # Create PTY session and start a long-running command
            handle = await sandbox.process.create_pty_session(
                id=session_id,
                on_data=before_callback,
                envs={"TERM": "dumb"},
            )
            await handle.send_input("stty -echo\n")
            await handle.send_input("echo 'BEFORE_DISCONNECT' && sleep 3 && echo 'AFTER_RECONNECT' && sleep 1; exit\n")

            # Wait for initial output, then disconnect
            await asyncio.sleep(2)
            await handle.disconnect()

            # Reconnect to the same PTY session with a new callback
            handle2 = await sandbox.process.connect_pty_session(session_id, after_callback)
            await handle2.wait()
            await handle2.disconnect()

            # BEFORE_DISCONNECT was captured by the first handle
            assert any("BEFORE_DISCONNECT" in msg for msg in before_messages)
            # AFTER_RECONNECT was captured by the second handle after reconnect
            assert any("AFTER_RECONNECT" in msg for msg in after_messages)

            try:
                await sandbox.process.kill_pty_session(session_id)
            except Exception:
                pass

    async def test_stream_command_raises_on_sandbox_crash(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that a sandbox crash during execution is detected and raised."""

        async with create_sandbox(
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:

            async def destroy_sandbox_after_delay() -> None:
                await asyncio.sleep(2)
                await daytona_client.delete(sandbox)

            with pytest.raises(SandboxError, match="crashed|health"):
                await asyncio.gather(
                    stream_command_output(sandbox, "sleep 30", on_output=lambda _: None),
                    destroy_sandbox_after_delay(),
                )

    async def test_stream_command_raises_on_nonzero_exit(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test that a command with non-zero exit code raises SandboxError."""
        async with create_sandbox(
            daytona_client, random_sandbox_name, test_image, test_resources, creation_semaphore
        ) as sandbox:
            # Use `false` (returns 1) instead of `exit 1` — exit kills the writer
            # shell itself, preventing the status file from being written.
            with pytest.raises(SandboxError, match="exit code: 1"):
                await stream_command_output(sandbox, "false", on_output=lambda _: None)
