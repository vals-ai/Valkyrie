"""Integration tests for sandbox operations."""

import io
import uuid
import zipfile
from typing import AsyncGenerator, Generator

import boto3
import pytest
from daytona import AsyncDaytona, AsyncSandbox, DaytonaError
from moto import mock_aws
from mypy_boto3_s3.client import S3Client

from tracker.config import AWS_S3_BUCKET
from tracker.database.models import AgentContractRequest
from tracker.s3 import get_contract_s3_key
from tracker.sandbox import (
    create_sandbox,
    install_agent_dependencies,
    run_agent,
    upload_agent_artifacts,
)
from tracker.types import Resources


@pytest.fixture
def mock_s3() -> Generator[S3Client, None, None]:
    """Mock S3 for testing."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]
        s3.create_bucket(Bucket=AWS_S3_BUCKET)
        yield s3


@pytest.fixture
def test_resources() -> Resources:
    """Create a test resources object."""
    return Resources(vcpu=2, memory=4, disk=5)


@pytest.fixture
async def test_sandbox(daytona_client: AsyncDaytona, test_resources: Resources) -> AsyncGenerator[AsyncSandbox, None]:
    """Create a test sandbox with Python."""

    sandbox_name = f"test-sandbox-{str(uuid.uuid4())}"

    async with create_sandbox(daytona_client, sandbox_name, "python:3.11-slim", test_resources) as sandbox:
        yield sandbox


class TestSandboxOperations:
    """Integration tests for sandbox operations."""

    async def test_create_and_cleanup_sandbox(self, daytona_client: AsyncDaytona, test_resources: Resources) -> None:
        """Test that sandbox is created and cleaned up properly."""
        sandbox_name = "test-cleanup-sandbox"

        async with create_sandbox(daytona_client, sandbox_name, "python:3.11-slim", test_resources) as sandbox:
            assert sandbox.name == sandbox_name
            result = await sandbox.process.exec("echo 'test'")
            assert result.exit_code == 0

        with pytest.raises(DaytonaError):
            await daytona_client.find_one(sandbox_name)

    async def test_upload_agent_artifacts(self, test_sandbox: AsyncSandbox, mock_s3: S3Client) -> None:
        """Test that agent artifacts are uploaded to the sandbox."""
        contract_name = "test_contract"
        contract = AgentContractRequest(
            name=contract_name,
            artifacts=["setup.sh", "submodules/some_dir"],
            install_cmd="bash setup.sh",
            run_cmd="echo hello",
        )

        agent_file = f"{contract_name}/submodules/{contract_name}/file.txt"
        setup_file = f"{contract_name}/setup.sh"

        # Create a mock zip with the expected structure
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(setup_file, "#!/bin/bash\necho 'setup'")
            zf.writestr(agent_file, "hello world")
        zip_buffer.seek(0)

        # Upload zip to mocked S3
        mock_s3.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=get_contract_s3_key(contract_name),
            Body=zip_buffer.getvalue(),
        )

        await upload_agent_artifacts(test_sandbox, contract)

        # Verify files exist in sandbox
        result = await test_sandbox.process.exec(f"cat /bundle/{setup_file}")
        assert result.exit_code == 0
        assert "echo 'setup'" in result.result

        result = await test_sandbox.process.exec(f"cat /bundle/{agent_file}")
        assert result.exit_code == 0
        assert "hello world" in result.result

    async def test_install_agent_dependencies(self, test_sandbox: AsyncSandbox) -> None:
        """Test that install command is correctly executed in the sandbox."""
        logged_messages: list[str] = []

        def log_callback(message: str) -> None:
            logged_messages.append(message)

        contract_name = "test_contract"
        contract = AgentContractRequest(
            name=contract_name,
            artifacts=["setup.sh"],
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
    ) -> None:
        """Test that agent runs and prints output lines."""
        logged_messages: list[str] = []

        def log_callback(message: str) -> None:
            logged_messages.append(message)

        run_cmd = 'echo line1 && sleep 1 && echo line2 && sleep 1 && echo line3 && echo \'{"result": "hello world"}\' > /tmp/agent_output.json'

        contract = AgentContractRequest(
            name="test_agent",
            artifacts=[],
            install_cmd="true",
            run_cmd=run_cmd,
            final_output="/tmp/agent_output.json",
        )

        # Expecting bundle directory to exist
        await test_sandbox.process.exec("mkdir -p /bundle/test_agent")

        final_output = await run_agent(test_sandbox, contract, "some problem statement", log_callback, cwd="/")

        assert final_output == {"result": "hello world"}

        output = "\n".join(logged_messages)
        assert "line1" in output
        assert "line2" in output
        assert "line3" in output
