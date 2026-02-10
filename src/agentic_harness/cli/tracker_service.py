"""Client for interacting with the tracker service."""

import os
from collections.abc import Generator
from typing import Any, BinaryIO
from uuid import UUID

import httpx
from dotenv import load_dotenv
from httpx._models import Response
from tracker.database.models import AgentContractRequest
from tracker.types import (
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    StartBenchmarkRequest,
    StopBenchmarkResponse,
)

from agentic_harness.cli.exceptions import TrackerServiceError

load_dotenv()

TRACKER_URL = os.environ.get("TRACKER_SERVICE_URL", "http://localhost:8000")


class TrackerService:
    """Client for tracker service API."""

    def __init__(self, base_url: str = TRACKER_URL, timeout: int = 120):
        """
        Initialize tracker service client.

        Args:
            base_url: Base URL of tracker service
            timeout: Request timeout in seconds
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    def __enter__(self) -> "TrackerService":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close client."""
        self.close()

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

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

    def upload_contract(self, contract_name: str, file_stream: BinaryIO) -> dict[str, str]:
        """
        Upload contract bundle to tracker service.

        Args:
            contract_name: Name of the contract
            file_stream: File-like object containing zipped contract bundle

        Returns:
            Upload response with status and message

        Raises:
            TrackerServiceError: If upload fails
        """
        try:
            files = {"contract": (f"{contract_name}.zip", file_stream, "application/zip")}
            response = self._client.post(f"{self._base_url}/upload", files=files, timeout=600)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Upload failed: {e}") from e

    def start_benchmark(
        self,
        contract: AgentContractRequest,
        benchmark_name: str,
        concurrency: int,
        task_ids: list[str] | None,
        slice_str: str | None,
    ) -> Response:
        """
        Start a benchmark run on the tracker service.

        Args:
            contract_name: Name of the contract
            benchmark_name: Name of the benchmark

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
            )

            response = self._client.post(f"{self._base_url}/start-benchmark", json=payload.model_dump())

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

    def retrieve_results(self, benchmark_id: UUID) -> RetrieveResultsResponse:
        """
        Retrieve the results of a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id

        Returns:
            RetrieveResultsResponse with benchmark results
        """
        try:
            response = self._client.get(
                f"{self._base_url}/retrieve-results", params={"benchmark_id": str(benchmark_id)}
            )

            if response.status_code != 200:
                details = response.json().get("detail", response.text)
                raise TrackerServiceError(f"Failed to retrieve results: {details}")

            return RetrieveResultsResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to retrieve results: {e}") from e

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
        self, benchmark_id: UUID, retry: bool, task_ids: list[str]
    ) -> RetryOrResumeBenchmarkResponse:
        """
        Run a benchmark that has already been created by its benchmark id.

        Args:
            benchmark_id: Benchmark id
            retry: Whether to retry tasks with the status error
            task_ids: List of task ids to force retry

        Returns:
            RetryOrResumeBenchmarkResponse with status and message
        """
        try:
            response = self._client.post(
                f"{self._base_url}/retry-or-resume-benchmark/{benchmark_id}",
                params={"retry": retry},
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
