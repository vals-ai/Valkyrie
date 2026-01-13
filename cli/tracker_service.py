"""Client for interacting with the tracker service."""

from collections.abc import Generator
from typing import Any, BinaryIO
from uuid import UUID

import httpx
from httpx._models import Response
from tracker.types import FetchBenchmarkResponse, RetrieveResultsResponse, StartRunRequest

from cli.config import TRACKER_URL


class TrackerServiceError(Exception):
    """Exception raised for tracker service errors."""

    pass


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
            response = self._client.post(f"{self._base_url}/upload", files=files)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Upload failed: {e}") from e

    def start_run(
        self,
        contract_name: str,
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
            payload = StartRunRequest(
                contract_name=contract_name,
                benchmark_name=benchmark_name,
                concurrency=concurrency,
                task_ids=task_ids,
                slice_str=slice_str,
            )

            response = self._client.post(f"{self._base_url}/start-run", json=payload.model_dump())

            return response
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to start run: {e}") from e

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
                raise TrackerServiceError(f"Failed to fetch benchmark: {response.text}")

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
                    raise TrackerServiceError(f"Failed to stream benchmark: {response.status_code}")

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
                raise TrackerServiceError(f"Failed to retrieve results: {response.text}")

            return RetrieveResultsResponse.model_validate(response.json())
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to retrieve results: {e}") from e
