import uuid

from benchmark_service.schemas import Resources

from tracker.database.models import Benchmark
from tracker.s3 import delete_from_s3, download_from_s3, get_agent_result_s3_key
from tracker.sandbox import archive_and_upload_output, create_sandbox
from tracker.types import AWSCredentials


class TestUploadToS3:
    async def test_archive_and_upload_file(
        self, example_benchmark_object: Benchmark, test_aws: AWSCredentials, test_daytona_secret: str
    ) -> None:
        """Test creating a tar.gz from a file in sandbox and uploading to S3."""
        task_id = f"test-task-{uuid.uuid4().hex[:8]}"
        file_path = "/tmp/test_output.json"
        file_content = '{"result": "success", "score": 95}'
        s3_key = get_agent_result_s3_key(str(example_benchmark_object.id), task_id, "test_output.json")

        daytona_client = example_benchmark_object.benchmark_service(test_daytona_secret, test_aws).daytona_client

        try:
            async with create_sandbox(
                daytona=daytona_client,
                sandbox_name=f"test-upload-file-{uuid.uuid4().hex[:8]}",
                image="ubuntu:22.04",
                resources=Resources(vcpu=2, memory=4, disk=10),
            ) as sandbox:
                await sandbox.process.exec(f"echo '{file_content}' > {file_path}")

                await archive_and_upload_output(sandbox, file_path, s3_key)

                downloaded_content = download_from_s3(s3_key)

                assert len(downloaded_content) > 0
                assert downloaded_content.startswith(b"\x1f\x8b")
        finally:
            delete_from_s3(s3_key)

    async def test_archive_and_upload_directory(
        self, example_benchmark_object: Benchmark, test_aws: AWSCredentials, test_daytona_secret: str
    ) -> None:
        """Test creating a tar.gz from a directory in sandbox and uploading to S3."""
        task_id = f"test-task-{uuid.uuid4().hex[:8]}"
        dir_path = "/tmp/test_output_dir"
        s3_key = get_agent_result_s3_key(str(example_benchmark_object.id), task_id, "test_output_dir")

        daytona_client = example_benchmark_object.benchmark_service(test_daytona_secret, test_aws).daytona_client

        try:
            async with create_sandbox(
                daytona=daytona_client,
                sandbox_name=f"test-upload-dir-{uuid.uuid4().hex[:8]}",
                image="ubuntu:22.04",
                resources=Resources(vcpu=2, memory=4, disk=10),
            ) as sandbox:
                await sandbox.process.exec(f"mkdir -p {dir_path}")
                await sandbox.process.exec(f"echo 'file1 content' > {dir_path}/file1.txt")
                await sandbox.process.exec(f"echo 'file2 content' > {dir_path}/file2.txt")
                await sandbox.process.exec(f"mkdir -p {dir_path}/nested")
                await sandbox.process.exec(f"echo 'nested content' > {dir_path}/nested/file3.txt")

                await archive_and_upload_output(sandbox, dir_path, s3_key)

                downloaded_content = download_from_s3(s3_key)
                assert len(downloaded_content) > 0
                assert downloaded_content.startswith(b"\x1f\x8b")
        finally:
            delete_from_s3(s3_key)
