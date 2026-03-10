"""Client for interacting with the tracker service."""

import os
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import yaml
from dotenv import load_dotenv
from httpx._models import Response
from tracker.database.models import AgentContractRequest
from tracker.types import (
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    HarnessConfig,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkRequest,
    StopBenchmarkResponse,
)

from agentic_harness.cli.exceptions import TrackerServiceError

load_dotenv()

TRACKER_URL = os.environ.get("TRACKER_SERVICE_URL", "https://benchmark-tracker.vals.ai")
_CONFIG_LOCATION = Path("~/.config/harness/harness.yaml")
_REQUIRED_CONFIG_KEYS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "S3_BUCKET",
    "DAYTONA_SECRET_NAME",
}


class TrackerService:
    """Client for tracker service API."""

    _config_values: dict[str, str] = {}

    def __init__(self, base_url: str = TRACKER_URL, timeout: int = 120):
        """
        Initialize tracker service client.

        Args:
            base_url: Base URL of tracker service
            timeout: Request timeout in seconds
        """
        self._config_values = self.parse_config_keys()
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client = httpx.Client(timeout=timeout, headers=self._build_harness_headers())

    def __enter__(self) -> "TrackerService":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close client."""
        self.close()

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    @staticmethod
    def get_benchmark_service_url(benchmark_name: str) -> str | None:
        """
        Get custom benchmark service URL from config if it exists.

        Args:
            benchmark_name: Name of the benchmark

        Returns:
            Custom URL if configured, None otherwise
        """
        config_path = _CONFIG_LOCATION.expanduser()
        if not config_path.exists():
            return None

        with open(config_path) as f:
            harness_config = yaml.safe_load(f) or {}

        services = harness_config.get("custom_benchmark_services") or {}
        return services.get(benchmark_name)

    @staticmethod
    def parse_config_keys() -> dict[str, str]:
        """Parses expected config keys and handles edge cases"""
        config_path: Path = _CONFIG_LOCATION.expanduser()
        config_keys: dict[str, str] = {}
        if not config_path.exists():
            raise TrackerServiceError(f"Could not find the config at {_CONFIG_LOCATION}, run `harness config init`")

        with open(config_path) as f:
            harness_config: dict[str, str] = yaml.safe_load(f) or {}

        missing = _REQUIRED_CONFIG_KEYS - harness_config.keys()
        if missing:
            raise TrackerServiceError(
                f"Missing required config keys: {', '.join(sorted(missing))}. "
                "Run `harness config init` to initialize the harness config or `harness config modify` to update an existing config"
            )

        # Skip custom_benchmark_services to avoid adding them inside of the header
        for key, value in harness_config.items():
            if isinstance(value, dict):
                continue

            config_keys[key] = str(value)

        return config_keys

    def _build_harness_headers(self) -> dict[str, str]:
        """Automate building the headers from the config keys"""
        return {f"X-Harness-{re.sub(r'_', '-', key).title()}": value for key, value in self._config_values.items()}

    def _build_harness_config_payload(self) -> dict[str, Any]:
        """Build the harness config in a way that can be packed into a object"""
        flat = {key.lower(): value for key, value in self._config_values.items()}
        return {
            "aws": {
                "aws_access_key_id": flat["aws_access_key_id"],
                "aws_secret_access_key": flat["aws_secret_access_key"],
                "aws_default_region": flat["aws_default_region"],
            },
            "s3_bucket": flat["s3_bucket"],
            "log_group": flat["log_group"],
            "log_retention_policy": int(flat["log_retention_policy"]),
            "daytona_secret_name": flat["daytona_secret_name"],
        }

    def health_check(self) -> Response:
        """
        Check tracker service health.

        Returns:
            Response with health status

        Raises:
            TrackerServiceError: If health check fails
        """
        try:
            response = self._client.get(f"{self._base_url}/health")

            return response
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Health check failed: {e}") from e

    def start_benchmark(
        self,
        contract: AgentContractRequest,
        benchmark_name: str,
        concurrency: int,
        task_ids: list[str] | None,
        slice_str: str | None,
        lambda_function: str | None = None,
        dataset: str | None = None,
    ) -> Response:
        """
        Start a benchmark run on the tracker service.

        Args:
            contract: Agent contract request
            benchmark_name: Name of the benchmark
            concurrency: Number of concurrent tasks
            task_ids: Optional list of specific task IDs to run
            slice_str: Optional slice string for task selection
            lambda_function: Optional lambda function to invoke after benchmark

        Returns:
            Run response with status, message, and results

        Raises:
            TrackerServiceError: If start run fails
        """
        try:
            payload = StartBenchmarkRequest(
                contract=contract,
                benchmark_name=benchmark_name,
                concurrency=concurrency,
                task_ids=task_ids,
                slice_str=slice_str,
                lambda_function=lambda_function,
                dataset=dataset,
                harness_config=HarnessConfig.model_validate(self._build_harness_config_payload()),
                custom_benchmark_service=self.get_benchmark_service_url(benchmark_name),
            )

            body = payload.model_dump()

            response = self._client.post(f"{self._base_url}/start-benchmark", json=body)

            return response
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to start benchmark: {e}") from e

    def fetch_benchmark(self, benchmark_id: UUID) -> FetchBenchmarkResponse:
        """
        Fetch a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id

        Returns:
            FetchBenchmarkResponse with benchmark information
        """
        try:
            response = self._client.get(f"{self._base_url}/fetch-benchmark", params={"benchmark_id": str(benchmark_id)})

            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to fetch benchmark: {details}")

            return FetchBenchmarkResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch benchmark: {e}") from e

    def stream_benchmark(self, benchmark_id: UUID) -> Generator[str, None, None]:
        """
        Stream benchmark updates using a generator.

        possible values for the generator:
        - data: {FetchBenchmarkResponse}
        - event: complete: benchmark completed
        - event: error: benchmark error
        - event: disconnect: client disconnected from stream

        Args:
            benchmark_id: Benchmark id

        Yields:
            Generator[str, None, None]
        """
        try:
            with self._client.stream(
                "GET",
                f"{self._base_url}/fetch-benchmark",
                params={"benchmark_id": str(benchmark_id), "connect": "true"},
                timeout=None,
            ) as response:
                if response.status_code != 200:
                    response.read()
                    details = response.json().get("detail", response.text)
                    raise TrackerServiceError(f"Failed to stream benchmark: {details}")

                for line in response.iter_lines():
                    if line:
                        yield line
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to stream benchmark: {e}") from e

    def retrieve_results(self, benchmark_id: UUID, s3: bool) -> RetrieveResultsResponse:
        """
        Retrieve the results of a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id

        Returns:
            RetrieveResultsResponse with benchmark results
        """
        try:
            response = self._client.get(
                f"{self._base_url}/retrieve-results", params={"benchmark_id": str(benchmark_id), "s3": s3}
            )

            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to retrieve results: {details}")

            response_data = response.json()
            if not s3:
                return FinalViewResponse.model_validate(response_data)

            return S3UploadResultsResponse.model_validate(response_data)

        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to retrieve results: {e}") from e

    def check_results_exist_in_s3(self, benchmark_id: UUID) -> bool:
        """
        Check if results already exist in S3 for the given benchmark.

        Args:
            benchmark_id: Benchmark id

        Returns:
            True if results exist in S3, False otherwise

        Raises:
            TrackerServiceError if request fails
        """
        try:
            response = self._client.get(
                f"{self._base_url}/check-results-exist", params={"benchmark_id": str(benchmark_id)}
            )

            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to check S3 results: {details}")

            return response.json()["exists"]
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to check S3 results: {e}") from e

    def stop_benchmark(self, benchmark_id: UUID, force: bool) -> StopBenchmarkResponse:
        """
        Stop a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id

        Returns:
            StopBenchmarkResponse with status and message
        """
        try:
            response = self._client.post(f"{self._base_url}/stop-benchmark/{benchmark_id}", params={"force": force})
            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to stop benchmark: {details}")

            return StopBenchmarkResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to stop benchmark: {e}") from e

    def retry_or_resume_benchmark(
        self, benchmark_id: UUID, retry: bool, concurrency: int | None, task_ids: list[str]
    ) -> RetryOrResumeBenchmarkResponse:
        """
        Run a benchmark that has already been created by its benchmark id.

        Args:
            benchmark_id: Benchmark id
            retry: Whether to retry tasks with the status error
            concurrency: Optional new concurrency level to override original value
            task_ids: List of task ids to force retry

        Returns:
            RetryOrResumeBenchmarkResponse with status and message
        """
        try:
            params: dict[str, Any] = {"retry": retry}

            # NOTE: 0 is not acceptable
            if concurrency:
                params["concurrency"] = concurrency

            response = self._client.post(
                f"{self._base_url}/retry-or-resume-benchmark/{benchmark_id}",
                params=params,
                json=task_ids,
            )
            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to run benchmark: {details}")

            return RetryOrResumeBenchmarkResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to run benchmark: {e}") from e

    def fetch_benchmarks(self, request: FetchBenchmarksRequest) -> FetchBenchmarksResponse:
        """
        Fetch benchmarks based on the request parameters.

        Args:
            request: FetchBenchmarksRequest

        Returns:
            FetchBenchmarksResponse
        """
        try:
            response = self._client.get(
                f"{self._base_url}/fetch-benchmarks", params=request.model_dump(exclude_none=True, mode="json")
            )
            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to fetch benchmarks: {details}")

            return FetchBenchmarksResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch benchmarks: {e}") from e

    def fetch_agent_outputs(self, benchmark_id: UUID) -> Response:
        """
        Fetch agent outputs for a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id

        Returns:
            httpx Response with agent outputs
        """
        try:
            response = self._client.get(f"{self._base_url}/fetch-agent-outputs/{benchmark_id}")
            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to fetch agent outputs: {details}")

            return response
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch agent outputs: {e}") from e

    def fetch_benchmark_metadata(self, benchmark_id: UUID) -> FetchBenchmarkMetadataResponse:
        """
        Fetch benchmark metadata for a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id

        Returns:
            FetchBenchmarkMetadataResponse with benchmark metadata
        """
        try:
            response = self._client.get(f"{self._base_url}/fetch-benchmark-metadata/{benchmark_id}")
            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to fetch benchmark metadata: {details}")

            return FetchBenchmarkMetadataResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch benchmark metadata: {e}") from e
