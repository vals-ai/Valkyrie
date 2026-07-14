"""Client for interacting with the tracker service."""

import json
import re
from collections.abc import Generator, Iterator
from typing import Any, cast
from uuid import UUID

import httpx
import yaml
from benchmark_service.schemas import VerifyTaskIdsResponse
from dotenv import load_dotenv
from httpx._models import Response
from tracker.database.models import AgentContractRequest, RetryMode
from tracker.types import (
    BenchmarkServiceEntry,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
    BenchmarkStatusResponse,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarkTasksRequest,
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

from valkyrie.cli.exceptions import TrackerNotFoundError, TrackerServiceError
from valkyrie.cli.runtime_config import config_location, tracker_service_url

_REQUIRED_CONFIG_KEYS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "S3_BUCKET",
}
_PROVIDER_SETUP_COMMAND = "valkyrie config provider set <provider> <secret-name>"


def _resolve_tracker_url(base_url: str | None) -> str:
    """Load local environment values after CLI logging has been configured."""
    if base_url is not None:
        return base_url.rstrip("/")

    load_dotenv()
    return tracker_service_url().rstrip("/")


def _sandbox_providers(config: dict[str, Any]) -> dict[str, str]:
    raw_providers = config.get("sandbox_providers")
    if not isinstance(raw_providers, dict):
        return {}
    providers = cast(dict[object, object], raw_providers)
    return {str(name): str(secret_name) for name, secret_name in providers.items()}


def _response_error_detail(response: Response) -> Any:
    try:
        body = response.json()
    except ValueError:
        return response.text

    if isinstance(body, dict):
        return body.get("detail", response.text)
    return response.text


def _parse_response(response: Response, action: str) -> Any:
    """Parse a tracker JSON response, raising when the request failed."""
    if response.status_code != 200:
        details = _response_error_detail(response)
        raise TrackerServiceError(f"{action}: {details}")
    return response.json()


def _resolve_sandbox_provider_config(
    config: dict[str, Any], config_values: dict[str, str], provider: str | None = None
) -> tuple[str, str]:
    providers = _sandbox_providers(config)

    # Fall back to the legacy Daytona secret when named providers are not configured.
    if not providers:
        if provider is not None:
            raise TrackerServiceError(
                f"Unknown sandbox provider '{provider}'. Configure it with `{_PROVIDER_SETUP_COMMAND}`."
            )
        secret_name = config_values.get("DAYTONA_SECRET_NAME")
        if not secret_name:
            raise TrackerServiceError(f"Missing sandbox provider config. Run `{_PROVIDER_SETUP_COMMAND}`.")
        return "daytona", secret_name

    # Use the requested provider, configured default, or first configured provider.
    provider_name = str(provider or config.get("default_sandbox_provider") or next(iter(providers)))
    secret_name = providers.get(provider_name)
    if secret_name is not None:
        return provider_name, secret_name

    # Report valid provider names when the selected provider is unknown.
    raise TrackerServiceError(
        f"Unknown sandbox provider '{provider_name}'. Configured providers: {', '.join(providers)}"
    )


class TrackerService:
    """Client for tracker service API."""

    _config_values: dict[str, str] = {}

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int = 120,
        require_config: bool = True,
    ):
        """
        Initialize tracker service client.

        Args:
            base_url: Base URL of tracker service
            timeout: Request timeout in seconds
            require_config: Whether to require full harness config values
        """
        self._config = self._load_config()
        self._api_key = self._config.get("api_key")
        self._base_url = _resolve_tracker_url(base_url)
        self._timeout = timeout
        self._config_values = self.parse_config_keys() if require_config else {}
        self._client = httpx.Client(timeout=timeout, headers=self._build_auth_headers())

    def __enter__(self) -> "TrackerService":
        """Context manager entry."""
        self.require_health()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close client."""
        self.close()

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    @staticmethod
    def _load_config() -> dict[str, Any]:
        """Load the valkyrie config file if it exists."""
        config_path = config_location()
        if not config_path.exists():
            return {}

        with open(config_path) as f:
            return yaml.safe_load(f) or {}

    def _build_auth_headers(self) -> dict[str, str]:
        """Build request headers. Hosted mode adds X-Api-Key alongside X-Harness-* headers."""
        headers = self._build_harness_headers()
        if self._api_key:
            headers["X-Api-Key"] = self._api_key
        return headers

    @staticmethod
    def get_benchmark_service_url(benchmark_name: str) -> str | None:
        """
        Get custom benchmark service URL from config if it exists.

        Args:
            benchmark_name: Name of the benchmark

        Returns:
            Custom URL if configured, None otherwise
        """
        config_path = config_location()
        if not config_path.exists():
            return None

        with open(config_path) as f:
            harness_config = yaml.safe_load(f) or {}

        services = harness_config.get("custom_benchmark_services") or {}
        return services.get(benchmark_name)

    @staticmethod
    def get_benchmark_auth(benchmark_name: str) -> str | None:
        """
        Get benchmark auth credential from config if it exists.

        Args:
            benchmark_name: Name of the benchmark

        Returns:
            Auth credential if configured, None otherwise
        """
        config_path = config_location()
        if not config_path.exists():
            return None

        with open(config_path) as f:
            harness_config = yaml.safe_load(f) or {}

        auth = harness_config.get("benchmark_auth") or {}
        return auth.get(benchmark_name)

    @staticmethod
    def get_webhook_secret() -> str | None:
        """
        Get Slack webhook secret name from config if it exists.

        Returns:
            Webhook secret name if configured, None otherwise
        """
        config_path = config_location()
        if not config_path.exists():
            return None

        with open(config_path) as f:
            harness_config = yaml.safe_load(f) or {}

        secret_name = harness_config.get("webhook")
        return secret_name if secret_name else None

    @staticmethod
    def parse_config_keys() -> dict[str, str]:
        """Parses expected config keys and handles edge cases"""
        config_path = config_location()
        config_keys: dict[str, str] = {}
        if not config_path.exists():
            raise TrackerServiceError(f"Could not find the config at {config_path}, run `valkyrie config init`")

        with open(config_path) as f:
            harness_config: dict[str, Any] = yaml.safe_load(f) or {}

        missing = _REQUIRED_CONFIG_KEYS - harness_config.keys()
        if missing:
            raise TrackerServiceError(
                f"Missing required config keys: {', '.join(sorted(missing))}. "
                "Run `valkyrie config init` to initialize the Valkyrie config or `valkyrie config set` to update an existing config"
            )
        if not (_sandbox_providers(harness_config) or "DAYTONA_SECRET_NAME" in harness_config):
            raise TrackerServiceError(f"Missing sandbox provider config. Run `{_PROVIDER_SETUP_COMMAND}`.")

        # Keys that are managed separately and should not be sent as harness headers
        _SKIP_HEADER_KEYS = {"webhook", "api_key", "default_sandbox_provider"}

        # Skip custom_benchmark_services to avoid adding them inside of the header
        for key, value in harness_config.items():
            if isinstance(value, dict) or key in _SKIP_HEADER_KEYS:
                continue

            config_keys[key] = str(value)

        return config_keys

    @classmethod
    def validate_sandbox_provider(cls, provider: str | None = None) -> tuple[str, str]:
        """Validate sandbox provider config without opening a tracker client.

        Arguments
        - provider: Optional provider name supplied by the CLI.

        Returns
        - The resolved provider name and cloud secret name.

        Raises
        - TrackerServiceError: If provider config is missing or the provider is unknown.
        """
        return _resolve_sandbox_provider_config(cls._load_config(), cls.parse_config_keys(), provider)

    def _build_harness_headers(self) -> dict[str, str]:
        """Automate building the headers from the config keys"""
        return {f"X-Harness-{re.sub(r'_', '-', key).title()}": value for key, value in self._config_values.items()}

    def resolve_sandbox_provider(self, provider: str | None = None) -> tuple[str, str]:
        return _resolve_sandbox_provider_config(self._config, self._config_values, provider)

    def _build_harness_config_payload(self, sandbox_provider_secret_name: str) -> dict[str, Any]:
        """Build the Valkyrie config in a way that can be packed into a object"""
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
            "sandbox_provider_secret_name": sandbox_provider_secret_name,
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

    def require_health(self) -> None:
        """Raise if the tracker service cannot handle CLI requests."""
        try:
            response = self.health_check()
        except TrackerServiceError as e:
            raise TrackerNotFoundError(str(e)) from e

        if response.status_code == 200:
            return

        detail = _response_error_detail(response)
        if not isinstance(detail, str):
            detail = json.dumps(detail, indent=4, default=str)
        raise TrackerNotFoundError(f"Tracker service failed to respond!\n{detail}")

    def catalog_benchmark_services(self) -> list[BenchmarkServiceEntry]:
        """List catalog benchmark services visible to the configured tenant from tracker."""
        try:
            response = self._client.get(f"{self._base_url}/benchmark-services")

            response_data = _parse_response(response, "Failed to list benchmark services")
            return [BenchmarkServiceEntry.model_validate(service) for service in response_data.get("services", [])]
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to list benchmark services: {e}") from e

    def list_benchmark_services(self) -> BenchmarkServicesResponse:
        """List hosted benchmark services visible to the configured tenant."""
        services = self.catalog_benchmark_services()
        if not services:
            return BenchmarkServicesResponse(services=[])
        return self.check_benchmark_services(services)

    def check_benchmark_services(self, services: list[BenchmarkServiceEntry]) -> BenchmarkServicesResponse:
        """Health-check caller-provided benchmark services."""
        try:
            payload = BenchmarkServicesRequest(services=services)
            response = self._client.post(f"{self._base_url}/benchmark-services", json=payload.model_dump())

            return BenchmarkServicesResponse.model_validate(
                _parse_response(response, "Failed to check benchmark services")
            )
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to check benchmark services: {e}") from e

    @classmethod
    def init_org(cls, api_key: str, base_url: str | None = None) -> dict[str, str | bool]:
        """Validate a Descope API key and create/confirm the org. Does not require a full config."""
        tracker_url = _resolve_tracker_url(base_url)
        try:
            with httpx.Client(timeout=120, headers={"X-Api-Key": api_key}) as client:
                response = client.post(f"{tracker_url}/init")

                return _parse_response(response, "Failed to initialize org")
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to initialize org: {e}") from e

    def start_benchmark(
        self,
        contract: AgentContractRequest,
        benchmark_name: str,
        concurrency: int,
        ignore_custom_services: bool,
        task_ids: list[str] | None,
        slice_str: str | None,
        label: str | None = None,
        lambda_function: str | None = None,
        dataset: str | None = None,
        service_headers: dict[str, str] | None = None,
        provider: str | None = None,
        webhook_secret_name: str | None = None,
        webhook_intervals: list[int] | None = None,
    ) -> Response:
        """
        Start a benchmark run on the tracker service.

        Args:
            contract: Agent contract request
            benchmark_name: Name of the benchmark
            concurrency: Number of concurrent tasks
            task_ids: Optional list of specific task IDs to run
            slice_str: Optional slice string for task selection
            label: Optional run label
            lambda_function: Optional lambda function to invoke after benchmark

        Returns:
            Run response with status, message, and results

        Raises:
            TrackerServiceError: If start run fails
        """
        try:
            provider_name, sandbox_provider_secret_name = self.resolve_sandbox_provider(provider)
            payload = StartBenchmarkRequest(
                contract=contract,
                benchmark_name=benchmark_name,
                concurrency=concurrency,
                label=label,
                task_ids=task_ids,
                slice_str=slice_str,
                lambda_function=lambda_function,
                dataset=dataset,
                harness_config=HarnessConfig.model_validate(
                    self._build_harness_config_payload(sandbox_provider_secret_name)
                ),
                custom_benchmark_service=self.get_benchmark_service_url(benchmark_name)
                if not ignore_custom_services
                else None,
                service_headers=service_headers or {},
                sandbox_provider=provider_name,
                webhook_secret_name=webhook_secret_name,
                webhook_intervals=webhook_intervals,
            )

            body = payload.model_dump()

            response = self._client.post(f"{self._base_url}/start-benchmark", json=body)

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

            return FetchBenchmarkResponse.model_validate(_parse_response(response, "Failed to fetch run"))
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch run: {e}") from e

    def analyze_benchmark(
        self,
        benchmark_id: UUID,
        *,
        no_cache: bool,
        lambda_function: str | None,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Trigger Docent analysis. Yields ``(event_name, data)`` SSE events
        (``started``, ``heartbeat``, ``done``, ``error``) until terminal."""
        url = f"{self._base_url}/analyze-benchmark/{benchmark_id}"
        body = {"no_cache": no_cache, "lambda_function": lambda_function}

        try:
            with self._client.stream("POST", url, json=body, timeout=None) as response:
                if response.status_code != 200:
                    response.read()
                    details = _response_error_detail(response)
                    raise TrackerServiceError(f"analyze-benchmark failed: {details}")

                # Cached short-circuit returns a single JSON body; fresh
                # invocations return SSE. Normalize both to a ("done", payload)
                # yield for the caller's event loop.
                content_type = response.headers.get("content-type", "")
                if "text/event-stream" not in content_type:
                    payload = json.loads(response.read())
                    yield ("done", payload)
                    return

                event_name = ""
                for raw_line in response.iter_lines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    if line.startswith("event: "):
                        event_name = line.removeprefix("event: ").strip()
                    elif line.startswith("data: "):
                        data = json.loads(line.removeprefix("data: "))
                        yield (event_name, data)
                        if event_name in ("done", "error"):
                            return
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"analyze-benchmark failed: {e}") from e

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
                    details = _response_error_detail(response)
                    raise TrackerServiceError(f"Failed to stream run: {details}")

                for line in response.iter_lines():
                    if line:
                        yield line
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to stream run: {e}") from e

    def retrieve_results(
        self,
        benchmark_id: UUID,
        s3: bool,
        task_ids: list[str] | None = None,
    ) -> RetrieveResultsResponse:
        """
        Retrieve the results of a benchmark by its benchmark id.

        If task_ids is provided, results are filtered to that subset and the final score is
        recomputed over those tasks (does not mutate the stored FinalEvaluation).
        """
        try:
            params: dict[str, Any] = {"benchmark_id": str(benchmark_id), "s3": s3}
            if task_ids:
                params["task_ids"] = task_ids

            response = self._client.get(f"{self._base_url}/retrieve-results", params=params)

            response_data = _parse_response(response, "Failed to retrieve results")
            if not s3:
                return FinalViewResponse.model_validate(response_data)

            return S3UploadResultsResponse.model_validate(response_data)

        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to retrieve results: {e}") from e

    def fetch_benchmark_tasks(
        self,
        benchmark_name: str,
        dataset: str | None = None,
        ignore_custom_services: bool = False,
        service_headers: dict[str, str] | None = None,
    ) -> list[str]:
        """
        Fetch all task ids for a benchmark dataset.
        """
        try:
            payload = FetchBenchmarkTasksRequest(
                benchmark_name=benchmark_name,
                dataset=dataset,
                custom_benchmark_service=self.get_benchmark_service_url(benchmark_name)
                if not ignore_custom_services
                else None,
                service_headers=service_headers or {},
            )
            response = self._client.post(f"{self._base_url}/fetch-benchmark-tasks", json=payload.model_dump())

            return VerifyTaskIdsResponse.model_validate(_parse_response(response, "Failed to fetch task ids")).task_ids
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch task ids: {e}") from e

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

            return _parse_response(response, "Failed to check S3 results")["exists"]
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to check S3 results: {e}") from e

    def stop_benchmark(
        self,
        benchmark_id: UUID,
        force: bool,
        task_ids: list[str] | None = None,
    ) -> StopBenchmarkResponse:
        """
        Stop a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id
            force: Whether to stop active sandboxes immediately
            task_ids: Optional task IDs to stop without affecting other tasks

        Returns:
            StopBenchmarkResponse with status and message
        """
        try:
            response = self._client.post(
                f"{self._base_url}/stop-benchmark/{benchmark_id}",
                params={"force": force},
                json={"task_ids": task_ids},
            )

            return StopBenchmarkResponse.model_validate(_parse_response(response, "Failed to stop run"))
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to stop run: {e}") from e

    def retry_or_resume_benchmark(
        self,
        benchmark_id: UUID,
        retry: bool,
        retry_mode: RetryMode,
        concurrency: int | None,
        task_ids: list[str],
        service_headers: dict[str, str] | None = None,
        secrets: dict[str, str] | None = None,
    ) -> RetryOrResumeBenchmarkResponse:
        """
        Run a benchmark that has already been created by its benchmark id.

        Args:
            benchmark_id: Benchmark id
            retry: Whether to retry tasks with the status error
            concurrency: Optional new concurrency level to override original value
            task_ids: List of task ids to force retry. Task ids without an existing row
                are created as fresh PENDING if valid in the current dataset.
            service_headers: Optional headers for benchmark service authentication
            secrets: Optional agent secret mappings to merge into the stored contract

        Returns:
            RetryOrResumeBenchmarkResponse with status and message
        """
        try:
            params: dict[str, Any] = {"retry": retry, "retry_mode": retry_mode.value}

            # NOTE: 0 is not acceptable
            if concurrency:
                params["concurrency"] = concurrency

            body: dict[str, Any] = {"task_ids": task_ids, "service_headers": service_headers or {}}
            if secrets:
                body["secrets"] = secrets

            response = self._client.post(
                f"{self._base_url}/retry-or-resume-benchmark/{benchmark_id}",
                params=params,
                json=body,
            )

            return RetryOrResumeBenchmarkResponse.model_validate(_parse_response(response, "Failed to start run"))
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to start run: {e}") from e

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

            return FetchBenchmarksResponse.model_validate(_parse_response(response, "Failed to fetch runs"))
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch runs: {e}") from e

    def fetch_benchmark_statuses(self, benchmark_ids: list[UUID]) -> BenchmarkStatusResponse:
        """Fetch lightweight status and task counts for multiple runs."""
        try:
            response = self._client.get(
                f"{self._base_url}/benchmarks/status",
                params={"ids": ",".join(str(benchmark_id) for benchmark_id in benchmark_ids)},
            )
            return BenchmarkStatusResponse.model_validate(_parse_response(response, "Failed to fetch run statuses"))
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch run statuses: {e}") from e

    def fetch_run_outputs(self, benchmark_id: UUID, task_ids: list[str] | None = None) -> Response:
        """
        Fetch run outputs for a benchmark by its benchmark id.

        Args:
            benchmark_id: Benchmark id
            task_ids: Optional list of task ids to filter outputs

        Returns:
            httpx Response with run outputs
        """
        try:
            params: dict[str, Any] = {}
            if task_ids:
                params["task_ids"] = task_ids
            response = self._client.get(f"{self._base_url}/fetch-run-outputs/{benchmark_id}", params=params)
            if response.status_code != 200:
                details = _response_error_detail(response)
                raise TrackerServiceError(f"Failed to fetch run outputs: {details}")

            return response
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch run outputs: {e}") from e

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

            return FetchBenchmarkMetadataResponse.model_validate(
                _parse_response(response, "Failed to fetch run metadata")
            )
        except httpx.HTTPError as e:
            raise TrackerServiceError(f"Failed to fetch run metadata: {e}") from e
