import asyncio

from benchmark_service.schemas import Resources
from daytona import AsyncDaytona

from tests.utils import random_task_id
from tracker.database.models import AgentContractRequest
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


class TestUploadArtifactsAcrossImages:
    async def test_upload_all_images(
        self,
        daytona_client: AsyncDaytona,
        test_resources: Resources,
        contract: AgentContractRequest,
        aws_credentials: AWSCredentials,
        harness_config: HarnessConfig,
    ) -> None:
        """Run upload_agent_artifacts on all images bases and confirm that the agent can successfully be uploaded"""

        async def verify_image(image: str, label: str) -> None:
            sandbox_name = random_task_id()

            async with (
                asyncio.timeout(60),
                create_sandbox(daytona_client, sandbox_name, image, test_resources) as sandbox,
            ):
                await upload_agent_artifacts(sandbox, contract, aws_credentials, harness_config.s3_bucket)

                dir_check = await sandbox.process.exec(f"test -d /bundle/{contract.name}")
                assert dir_check.exit_code == 0, f"[{label}] contract dir /bundle/{contract.name} should exist"

                leftover = await sandbox.process.exec(f"test -f /tmp/{contract.name}.zip")
                assert leftover.exit_code != 0, f"[{label}] temp zip should be deleted after extraction"

                excluded = await sandbox.process.exec("test -f /bundle/contract.py")
                assert excluded.exit_code != 0, f"[{label}] contract.py should be excluded from extraction"

        results = await asyncio.gather(
            *(verify_image(image, label) for image, label in _IMAGES),
            return_exceptions=True,
        )

        failures = [
            f"[{label}] {result}" for (_, label), result in zip(_IMAGES, results) if isinstance(result, BaseException)
        ]

        assert not failures, f"Failed on {len(failures)} image(s):\n" + "\n".join(failures)
