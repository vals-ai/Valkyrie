import asyncio
from uuid import uuid4

import boto3
from benchmark_service import ImageSource, Resources, SandboxProvider

from tests.utils import random_task_id
from tracker.database.models import AgentContractRequest
from tracker.aws.s3 import get_benchmark_contract_s3_key, get_contract_s3_key
from tracker.sandbox import create_sandbox, upload_agent_artifacts
from tracker.types import AWSCredentials, HarnessConfig

# Images covering the major package families
_IMAGES = [
    ("python:3.11-slim", "python-slim-apt"),
    ("python:3.11-alpine", "python-alpine-apk"),
    ("debian:bookworm-slim", "debian-apt"),
    ("ubuntu:24.04", "ubuntu-apt"),
    ("fedora:40", "fedora-dnf"),
    ("amazonlinux:2023", "amazonlinux-dnf"),
    ("node:20-slim", "node-slim-apt"),
    ("alpine:3.20", "alpine-apk"),
    ("archlinux:base", "archlinux-pacman"),
]

# 9 sandboxes are pulled and warmed up concurrently; slower images (e.g. fedora)
# occasionally exceed a 60s budget under image-pull contention on CI runners.
_PER_IMAGE_TIMEOUT_SECONDS = 120


def _format_failure(exc: BaseException) -> str:
    """Render an exception so that bare ``TimeoutError()`` doesn't show up empty."""
    text = str(exc)
    return text if text else f"{type(exc).__name__}()"


class TestUploadArtifactsAcrossImages:
    async def test_upload_all_images(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        contract: AgentContractRequest,
        aws_credentials: AWSCredentials,
        harness_config: HarnessConfig,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        """Verify agent artifact upload works across the supported Linux image families.

        Test cases:
        - The frozen benchmark contract zip is downloaded and extracted into each sandbox image.
        - The temporary zip is not left behind after extraction.
        """

        benchmark_id = f"test-benchmark-{uuid4().hex[:5]}"

        # Stage the per-benchmark frozen copy that upload_agent_artifacts will now read from.
        s3 = boto3.client(  # type: ignore
            "s3",
            region_name=aws_credentials.aws_default_region,
            aws_access_key_id=aws_credentials.aws_access_key_id,
            aws_secret_access_key=aws_credentials.aws_secret_access_key,
            aws_session_token=aws_credentials.aws_session_token,
        )
        agent_key = get_contract_s3_key(contract.name)
        frozen_key = get_benchmark_contract_s3_key(benchmark_id, contract.name)
        s3.copy_object(
            Bucket=harness_config.s3_bucket,
            CopySource={"Bucket": harness_config.s3_bucket, "Key": agent_key},
            Key=frozen_key,
        )

        async def verify_image(image: str, label: str) -> None:
            sandbox_name = random_task_id()

            async with (
                asyncio.timeout(_PER_IMAGE_TIMEOUT_SECONDS),
                create_sandbox(
                    sandbox_provider,
                    sandbox_name,
                    ImageSource(image=image),
                    test_resources,
                    creation_semaphore,
                ) as sandbox,
            ):
                await upload_agent_artifacts(sandbox, contract, benchmark_id, aws_credentials, harness_config.s3_bucket)

                dir_check = await sandbox.exec(f"test -d /bundle/{contract.name}")
                assert dir_check.exit_code == 0, f"[{label}] contract dir /bundle/{contract.name} should exist"

                leftover = await sandbox.exec(f"test -f /tmp/{contract.name}.zip")
                assert leftover.exit_code != 0, f"[{label}] temp zip should be deleted after extraction"

        results = await asyncio.gather(
            *(verify_image(image, label) for image, label in _IMAGES),
            return_exceptions=True,
        )

        failures = [
            f"[{label}] {_format_failure(result)}"
            for (_, label), result in zip(_IMAGES, results)
            if isinstance(result, BaseException)
        ]

        assert not failures, f"Failed on {len(failures)} image(s):\n" + "\n".join(failures)
