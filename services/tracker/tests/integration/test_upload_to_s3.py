import asyncio

from benchmark_service import ImageSource, Resources, SandboxProvider

from tests.utils import random_task_id
from tracker.database.models import Benchmark
from tracker.aws.s3 import delete_from_s3, download_from_s3, get_agent_result_s3_key
from tracker.sandbox import archive_and_upload_output, create_sandbox
from tracker.types import HarnessConfig


class TestUploadToS3:
    async def test_archive_and_upload_file(
        self,
        example_benchmark_object: Benchmark,
        test_image: str,
        random_sandbox_name: str,
        test_resources: Resources,
        sandbox_provider: SandboxProvider,
        harness_config: HarnessConfig,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test creating a tar.gz from a file in sandbox and uploading to S3."""
        file_path = "/tmp/test_output.json"
        file_content = '{"result": "success", "score": 95}'
        s3_key = get_agent_result_s3_key(str(example_benchmark_object.id), random_task_id(), "test_output.json")

        try:
            async with create_sandbox(
                provider=sandbox_provider,
                sandbox_name=random_sandbox_name,
                source=ImageSource(image=test_image),
                resources=test_resources,
                creation_semaphore=creation_semaphore,
            ) as sandbox:
                await sandbox.exec(f"echo '{file_content}' > {file_path}")

                await archive_and_upload_output(
                    sandbox, file_path, s3_key, harness_config.aws, harness_config.s3_bucket
                )

                downloaded_content = download_from_s3(s3_key, harness_config.aws, harness_config.s3_bucket)

                assert len(downloaded_content) > 0
                assert downloaded_content.startswith(b"\x1f\x8b")
        finally:
            delete_from_s3(s3_key, harness_config.aws, harness_config.s3_bucket)

    async def test_archive_and_upload_directory(
        self,
        example_benchmark_object: Benchmark,
        harness_config: HarnessConfig,
        test_image: str,
        random_sandbox_name: str,
        test_resources: Resources,
        sandbox_provider: SandboxProvider,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Test creating a tar.gz from a directory in sandbox and uploading to S3."""
        dir_path = "/tmp/test_output_dir"
        s3_key = get_agent_result_s3_key(str(example_benchmark_object.id), random_task_id(), "test_output_dir")

        try:
            async with create_sandbox(
                provider=sandbox_provider,
                sandbox_name=random_sandbox_name,
                source=ImageSource(image=test_image),
                resources=test_resources,
                creation_semaphore=creation_semaphore,
            ) as sandbox:
                await sandbox.exec(f"mkdir -p {dir_path}")
                await sandbox.exec(f"echo 'file1 content' > {dir_path}/file1.txt")
                await sandbox.exec(f"echo 'file2 content' > {dir_path}/file2.txt")
                await sandbox.exec(f"mkdir -p {dir_path}/nested")
                await sandbox.exec(f"echo 'nested content' > {dir_path}/nested/file3.txt")

                await archive_and_upload_output(sandbox, dir_path, s3_key, harness_config.aws, harness_config.s3_bucket)

                downloaded_content = download_from_s3(s3_key, harness_config.aws, harness_config.s3_bucket)
                assert len(downloaded_content) > 0
                assert downloaded_content.startswith(b"\x1f\x8b")
        finally:
            delete_from_s3(s3_key, harness_config.aws, harness_config.s3_bucket)
