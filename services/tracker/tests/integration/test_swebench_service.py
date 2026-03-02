import os
from typing import Any

import pytest
from benchmark_service.client import BenchmarkServiceClient
from daytona import AsyncDaytona, AsyncSandbox
from pytest import MonkeyPatch
from requests.exceptions import ConnectTimeout

from tests.utils import build_task_environment, validate_docker_image
from tracker.types import AWSCredentials
from tracker.utils import create_benchmark_service_client, fetch_daytona_headers


@pytest.fixture
def docker_image_format() -> str:
    return "ghcr.io/epoch-research/swe-bench.eval.x86_64.{task_id}:latest"


@pytest.fixture(scope="session", autouse=True)
async def require_health_check(test_aws: AWSCredentials, test_daytona_secret: str):
    """Checks that the server is running before running the test. If its not connected it will fail"""

    service_ip = os.getenv("BENCHMARK_SERVICE_URL")
    if not service_ip:
        pytest.fail("BENCHMARK_SERVICE_URL is not set", pytrace=False)

    benchmark_service = create_benchmark_service_client(
        url=service_ip, daytona_secret_name=test_daytona_secret, aws=test_aws
    )

    try:
        _ = await benchmark_service.health_check()
    except ConnectTimeout:
        pytest.fail("Could not connect to the swebench service. Please ensure that it is running.", pytrace=False)


class TestSWEBenchmarkService:
    def test_setup_benchmark_service(
        self, monkeypatch: MonkeyPatch, test_aws: AWSCredentials, test_daytona_secret: str
    ):
        """
        Test setup of benchmark service with valid and invalid environment variables.

        Test Cases:
        - Invalid environment variables: Raises ValueError that the user sees
        - Valid environment variables: Sets environment variables and returns the correct keys
        """
        monkeypatch.setenv("DAYTONA_API_KEY", "xyz")
        monkeypatch.setenv("DAYTONA_TARGET", "us")

        # User is missing the api url environment variable
        monkeypatch.setenv("DAYTONA_API_URL", "")

        with pytest.raises(ValueError):
            _ = create_benchmark_service_client(
                url="http://test_ip:8000", daytona_secret_name=test_daytona_secret, aws=test_aws
            )

        # User sets the api url environment variable after finding out that they are missing it
        monkeypatch.setenv("DAYTONA_API_URL", "https://app.daytona.io/api")
        try:
            assert fetch_daytona_headers(test_daytona_secret, test_aws) == {
                "x-api-key": "xyz",
                "x-api-url": "https://app.daytona.io/api",
                "x-target": "us",
            }
        except Exception as e:
            pytest.fail(f"Missing environment variables: {e}", pytrace=False)

    async def test_health_check(self, benchmark_service: BenchmarkServiceClient):
        """
        Test health check of the benchmark service. Ensures that the service is running before we proceed with the tests.

        Test Cases:
        - Service is running: Returns 200 OK
        """
        try:
            response = await benchmark_service.health_check()
            assert response.status == "ok"

        except Exception as e:
            pytest.fail(f"Health check failed: {e}", pytrace=False)

    async def test_verify_task_ids(self, benchmark_service: BenchmarkServiceClient):
        """
        Test the verify task ids endpoint of the benchmark service.

        Test Cases:
        - Valid task ids: Returns 200 OK
        - Invalid task ids: Raises Exception that the user sees
        - No task ids passed in: Returns all 500 task ids to run the benchmark
        - Slice string passed in: Returns the correct amount of task ids expected for the slice
        """

        try:
            # Test case 1. Valid tasks passed in returns the same task ids in the same order passed in
            task_ids = ["astropy__astropy-12907", "django__django-11066", "django__django-12858"]
            response = await benchmark_service.verify_task_ids(task_ids=task_ids, slice_str=None)
            assert response.task_ids == task_ids

            # Test case 2. Invalid task ids passed in raises and Exception that the user sees
            with pytest.raises(Exception):
                _ = await benchmark_service.verify_task_ids(
                    task_ids=["astropy__astropy-12907", "invalid_task_id"], slice_str=None
                )

            # Test case 3. No task ids passed in returns all 500 task ids to run the benchmark
            response = await benchmark_service.verify_task_ids(task_ids=[], slice_str=None)
            assert response.task_ids
            assert len(response.task_ids) == 500

            # Test case 4. Slice string passed in returns the correct amount of task ids expected for the slice
            response = await benchmark_service.verify_task_ids(task_ids=None, slice_str="100:200")
            assert response.task_ids
            assert len(response.task_ids) == 100

        except Exception as e:
            pytest.fail(f"Verify task ids failed: {e}", pytrace=False)

    async def test_retrieve_task(self, benchmark_service: BenchmarkServiceClient, docker_image_format: str):
        """
        Test the retrieve task endpoint of the benchmark service.

        Test Cases:
        - Valid task id: Returns the task data with correct structure
        - Invalid task id: Raises Exception that the user sees
        """

        try:
            # Test case 1. Valid task returns a valid dict structure
            task_ids = ["astropy__astropy-12907", "django__django-11066", "django__django-12858"]

            for task_id in task_ids:
                task_data = await benchmark_service.retrieve_task(task_id=task_id)

                assert task_data.docker_image == docker_image_format.format(task_id=task_id)
                assert task_data.request_setup
                assert task_data.problem_statement

                # Verify docker image exists
                if not await validate_docker_image(task_data.docker_image):
                    pytest.fail(
                        f"Failed to validate docker image for task: {task_id} with image: {task_data.docker_image}"
                    )

            # Test case 2. Invalid task id raises an Exception that the user sees
            # Skip validation since some tasks don't have proper manifests but we can still pull them
            with pytest.raises(Exception):
                _ = await benchmark_service.retrieve_task(task_id="invalid_task_id", skip_validation=True)

        except Exception as e:
            pytest.fail(f"Retrieve task failed: {e}", pytrace=False)

    @staticmethod
    async def _fetch_commit(sandbox: AsyncSandbox) -> str:
        """
        Fetches the current commit inside of the sandbox
        """
        git_diff_result = await sandbox.process.exec(
            command="git rev-parse HEAD",
            cwd="/testbed",
        )

        current_commit = git_diff_result.result.split()[0]

        return current_commit

    async def test_setup_task(
        self, benchmark_service: BenchmarkServiceClient, docker_image_format: str, daytona_client: AsyncDaytona
    ):
        """
        Ensures that the setup.sh script inside of the swebench service is running inside of the container we build
        NOTE: This endpoint occurs after we setup the sandbox so we can skip to that step in the test

        Test Cases:
        - When sandbox is first created, we are on the correct commit inside of the environment
        - If a task does not start on the base commit, we checkout the correct commit after using the setup task endpoint
        - When using the setup task endpoint with a valid task id and instance id: Returns 200 OK
        - Ensure we have entered the correct commit inside of the environment after using the setup task endpoint

        Use the following url to find the base commit and task ids of [swebench verified dataset](https://huggingface.co/datasets/SWE-bench/SWE-bench_Verified/viewer/default/test?views%5B%5D=test)
        """

        task_id = "django__django-15572"
        base_commit = "0b31e024873681e187b574fe1c4afe5e48aeeecf"

        try:
            docker_image = docker_image_format.format(task_id=task_id)
            async with build_task_environment(daytona_client, task_id, docker_image) as sandbox:
                # Test case 1. We are on the correct commit inside of the environment before using the setup task endpoint
                current_commit = await self._fetch_commit(sandbox)
                assert current_commit == base_commit, "Should be the same at the start of the test"

                # Test case 2. We are not on the same commit as the base commit when we start the container
                _ = await sandbox.process.exec(
                    command="git checkout HEAD~1",
                    cwd="/testbed",
                )

                current_commit = await self._fetch_commit(sandbox)
                assert current_commit != base_commit, "Should not be the same after checking out a different commit"

                # Test case 3. When using the setup task endpoint with a valid task id and instance id: Returns 200 OK
                response = await benchmark_service.setup_task(task_id=task_id, instance_id=sandbox.id)
                assert response.status == "ok"

                # Test case 3. Ensure we have entered the correct commit inside of the environment after using the setup task endpoint
                current_commit = await self._fetch_commit(sandbox)
                assert current_commit == base_commit, "Should be the same after using the setup task endpoint"
        except Exception as e:
            pytest.fail(f"Setup task failed: {e}", pytrace=False)

    async def test_evaluate_instance(
        self, benchmark_service: BenchmarkServiceClient, docker_image_format: str, daytona_client: AsyncDaytona
    ):
        """
        Test the evaluate instance endpoint of the benchmark service.
        NOTE: end to end testing done inside of the swebench service itself, so we are just testing the endpoint itself here

        Test Cases:
        - When using evaluate instance endpoint with a valid task id and instance id: Returns 200 OK
        """
        try:
            task_id = "django__django-12325"
            docker_image = docker_image_format.format(task_id=task_id)

            async with build_task_environment(daytona_client, task_id, docker_image) as sandbox:
                response = await benchmark_service.evaluate_instance(task_id=task_id, instance_id=sandbox.id)

                # Since no solution patch was used this evaluation is going to be unresolved
                assert not response.get("resolved")

        except Exception as e:
            pytest.fail(f"Evaluate instance failed: {e}", pytrace=False)

    async def test_final_score(self, benchmark_service: BenchmarkServiceClient):
        """
        Test the final score endpoint of the benchmark service.

        Test Cases:
        - When using final score endpoint with a valid evaluation results: Correctly constructs a final score object
        """
        try:
            task_id = "astropy__astropy-12907"
            first_evaluation_result: dict[str, dict[str, Any] | None] = {
                task_id: {
                    "patch_successfully_applied": True,
                    "resolved": True,
                    "resolution_status": "FULL",
                }
            }

            final_score = await benchmark_service.final_score(evaluation_results=first_evaluation_result)

            assert final_score.tasks_evaluated == [task_id]
            assert final_score.final_score == round(100.0, 6)
            assert final_score.metadata.get("resolved_tasks", []) == [task_id]
            assert final_score.metadata.get("unresolved_tasks", []) == []

        except Exception as e:
            pytest.fail(f"Final score failed: {e}", pytrace=False)
