import asyncio
import io
import tarfile

from benchmark_service import ImageSource, Resources, SandboxProvider

from tests.utils import random_task_id
from tracker.aws.s3 import delete_from_s3, download_from_s3, get_agent_result_s3_key
from tracker.database.models import Benchmark
from tracker.sandbox import archive_and_upload_output, create_sandbox
from tracker.types import HarnessConfig


def _archive_members(archive_content: bytes) -> dict[str, bytes]:
    with tarfile.open(fileobj=io.BytesIO(archive_content), mode="r:gz") as archive:
        members: dict[str, bytes] = {}
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted_file = archive.extractfile(member)
            if extracted_file is not None:
                members[member.name] = extracted_file.read()
        return members


def _member_content(members: dict[str, bytes], suffix: str) -> bytes:
    matching_members = [content for name, content in members.items() if name.endswith(suffix)]
    assert len(matching_members) == 1
    return matching_members[0]


class TestUploadToS3:
    async def test_archive_and_upload_file_and_directory(
        self,
        example_benchmark_object: Benchmark,
        test_image: str,
        random_sandbox_name: str,
        test_resources: Resources,
        sandbox_provider: SandboxProvider,
        harness_config: HarnessConfig,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify sandbox output archives preserve file contents when uploaded to S3.

        Test cases:
        - A single output file is archived, uploaded, downloaded, and readable from S3.
        - A directory output preserves nested files in the uploaded archive.
        """
        file_path = "/tmp/test_output.json"
        file_content = '{"result": "success", "score": 95}'
        dir_path = "/tmp/test_output_dir"
        task_id = random_task_id()
        file_s3_key = get_agent_result_s3_key(str(example_benchmark_object.id), task_id, "test_output.json")
        dir_s3_key = get_agent_result_s3_key(str(example_benchmark_object.id), task_id, "test_output_dir")

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
                    sandbox, file_path, file_s3_key, harness_config.aws, harness_config.s3_bucket
                )

                await sandbox.exec(f"mkdir -p {dir_path}/nested")
                await sandbox.exec(f"echo 'file1 content' > {dir_path}/file1.txt")
                await sandbox.exec(f"echo 'file2 content' > {dir_path}/file2.txt")
                await sandbox.exec(f"echo 'nested content' > {dir_path}/nested/file3.txt")
                await archive_and_upload_output(
                    sandbox, dir_path, dir_s3_key, harness_config.aws, harness_config.s3_bucket
                )

                file_members = _archive_members(
                    download_from_s3(file_s3_key, harness_config.aws, harness_config.s3_bucket)
                )
                dir_members = _archive_members(
                    download_from_s3(dir_s3_key, harness_config.aws, harness_config.s3_bucket)
                )

                assert _member_content(file_members, "tmp/test_output.json") == f"{file_content}\n".encode()
                assert _member_content(dir_members, "tmp/test_output_dir/file1.txt") == b"file1 content\n"
                assert _member_content(dir_members, "tmp/test_output_dir/file2.txt") == b"file2 content\n"
                assert _member_content(dir_members, "tmp/test_output_dir/nested/file3.txt") == b"nested content\n"
        finally:
            delete_from_s3(file_s3_key, harness_config.aws, harness_config.s3_bucket)
            delete_from_s3(dir_s3_key, harness_config.aws, harness_config.s3_bucket)
