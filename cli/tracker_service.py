"""Client for interacting with the tracker service."""

from typing import Any, BinaryIO

import httpx

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

    def health_check(self) -> dict[str, str]:
        """
        Check tracker service health.

        Returns:
            Health status response

        Raises:
            TrackerServiceError: If health check fails
        """
        try:
            response = self._client.get(f"{self._base_url}/health")
            response.raise_for_status()
            return response.json()
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

    def start_run(self, contract_name: str, benchmark_name: str) -> dict[str, Any]:
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
            payload = {
                "contract_name": contract_name,
                "benchmark_name": benchmark_name,
            }

            response = self._client.post(f"{self._base_url}/start-run", json=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to start run: {e}") from e
