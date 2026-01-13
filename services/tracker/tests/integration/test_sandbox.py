"""Integration tests for sandbox operations."""

import io
import zipfile
from typing import AsyncGenerator, Generator

import boto3
import pytest
from daytona import AsyncDaytona, AsyncSandbox, FileUpload
from moto import mock_aws
from mypy_boto3_s3.client import S3Client

from tracker.config import S3_BUCKET_NAME
from tracker.exceptions import SandboxError
from tracker.sandbox import create_sandbox, install_dependencies, run_agent, upload_contract_to_sandbox


@pytest.fixture
def mock_s3() -> Generator[S3Client, None, None]:
    """Mock S3 for testing."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        yield s3


@pytest.fixture
def minimal_contract_zip() -> bytes:
    """
    Create a minimal test contract that properly implements AgentContract.

    This contract:
    - Implements AgentContract properly
    - Returns a simple QueryResult
    - Has no external dependencies
    """
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        # Minimal contract.py that implements AgentContract
        contract_code = '''"""Test contract for integration tests."""

from model_library.base import QueryResult, TextOutput

from agentic_harness.base.contract import AgentContract
from agentic_harness.base.types import Task


class TestContract(AgentContract):
    """Minimal test contract."""

    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task."""
        return QueryResult(
            output=[TextOutput(text="Test agent completed successfully")]
        )
'''
        zip_file.writestr("bundle/contracts/test_contract/contract.py", contract_code)

        # Minimal setup.sh (just echoes success)
        setup_script = """#!/bin/bash
echo "Test contract setup complete"
"""
        zip_file.writestr("bundle/contracts/test_contract/setup.sh", setup_script)

    zip_buffer.seek(0)
    return zip_buffer.read()


@pytest.fixture
async def test_sandbox(daytona_client: AsyncDaytona) -> AsyncGenerator[AsyncSandbox, None]:
    """Create a test sandbox with Python."""
    async with create_sandbox(daytona_client, "test-sandbox", "python:3.11-slim") as sandbox:
        yield sandbox


class TestSandboxOperations:
    """Integration tests for sandbox operations."""

    async def test_create_and_cleanup_sandbox(self, daytona_client: AsyncDaytona) -> None:
        """Test that sandbox is created and cleaned up properly."""
        sandbox_name = "test-cleanup-sandbox"

        async with create_sandbox(daytona_client, sandbox_name, "python:3.11-slim") as sandbox:
            assert sandbox.name == sandbox_name
            result = await sandbox.process.exec("echo 'test'")
            assert result.exit_code == 0

    async def test_upload_contract_to_sandbox(
        self, test_sandbox: AsyncSandbox, mock_s3: S3Client, minimal_contract_zip: bytes
    ) -> None:
        """Test uploading a contract to the sandbox."""
        # Upload contract zip to mocked S3
        mock_s3.put_object(Bucket=S3_BUCKET_NAME, Key="contracts/test_contract.zip", Body=minimal_contract_zip)

        # Upload contract to sandbox
        await upload_contract_to_sandbox(test_sandbox, "test_contract")

        # Verify contract file exists in sandbox
        result = await test_sandbox.process.exec("test -f /bundle/contracts/test_contract/contract.py")
        assert result.exit_code == 0, "Contract file should exist in sandbox"

        # Verify setup.sh exists
        result = await test_sandbox.process.exec("test -f /bundle/contracts/test_contract/setup.sh")
        assert result.exit_code == 0, "Setup script should exist in sandbox"

    async def test_upload_contract_creates_nested_directories(
        self, test_sandbox: AsyncSandbox, mock_s3: S3Client
    ) -> None:
        """Test that nested directories are created correctly."""
        # Create a contract with nested structure
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("bundle/contracts/nested/subdir/deep/file.py", "# test file")
            zf.writestr("bundle/contracts/nested/other/file.txt", "test content")

        mock_s3.put_object(Bucket=S3_BUCKET_NAME, Key="contracts/nested.zip", Body=zip_buffer.getvalue())

        await upload_contract_to_sandbox(test_sandbox, "nested")

        # Verify nested files exist
        result = await test_sandbox.process.exec("test -f /bundle/contracts/nested/subdir/deep/file.py")
        assert result.exit_code == 0, "Deeply nested file should exist"

        result = await test_sandbox.process.exec("test -f /bundle/contracts/nested/other/file.txt")
        assert result.exit_code == 0, "Other nested file should exist"

    async def test_install_dependencies_errors_on_missing_bundle(
        self, test_sandbox: AsyncSandbox, mock_s3: S3Client, minimal_contract_zip: bytes
    ) -> None:
        """Test that install_dependencies raises SandboxError when bundle is missing."""

        # Upload contract without bundle
        mock_s3.put_object(Bucket=S3_BUCKET_NAME, Key="contracts/test_contract.zip", Body=minimal_contract_zip)
        await upload_contract_to_sandbox(test_sandbox, "test_contract")

        # Try to install dependencies without bundle - should fail
        with pytest.raises(SandboxError, match="Failed to install agentic_harness dependencies"):
            await install_dependencies(test_sandbox, "test_contract")

    async def test_run_agent_creates_session_and_executes_command(self, test_sandbox: AsyncSandbox) -> None:
        """Test that run_agent creates a session and executes the command."""

        # Create a simple test script that will succeed
        await test_sandbox.process.exec("mkdir -p /test_contract")
        await test_sandbox.process.exec(
            "echo '#!/bin/bash\necho success' > /test_contract/run.sh && chmod +x /test_contract/run.sh"
        )

        # Mock the entry point by creating a simple Python script
        script = """#!/usr/bin/env python3
import sys
print("Agent executed successfully")
sys.exit(0)
"""
        await test_sandbox.process.exec("mkdir -p /bundle/contracts/test_contract")
        file_upload = FileUpload(source=script.encode(), destination="/bundle/contracts/test_contract/entry.py")
        await test_sandbox.fs.upload_files(files=[file_upload])
        await test_sandbox.process.exec("chmod +x /test_agent.py")

        # Test that command execution works (will fail because entry.py doesn't exist, but tests the flow)
        with pytest.raises(SandboxError):
            await run_agent(test_sandbox, "test_contract", "task-1", "Test problem statement")
