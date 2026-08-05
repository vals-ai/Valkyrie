"""Run lifecycle operations for the async Valkyrie SDK."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from uuid import UUID

from valkyrie.sdk.errors import ValkyrieConfigError, ValkyrieRunError, ValkyrieStreamError, handle_httpx_stream_errors
from valkyrie.sdk.models import (
    AgentContractRequest,
    AnalyzeBenchmarkRequest,
    AnalyzeEvent,
    FetchBenchmarkResponse,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    RetrieveResultsResponse,
    ResultsExistResponse,
    RetryMode,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
)

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class RunsResource:
    """Async operations for the Valkyrie run lifecycle."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def start(
        self,
        agent: str | AgentContractRequest,
        benchmark: str,
        *,
        model: str | None = None,
        concurrency: int = 5,
        task_ids: Sequence[str] | None = None,
        slice_str: str | None = None,
        dataset: str | None = None,
        label: str | None = None,
        lambda_function: str | None = None,
        provider: str | None = None,
        agent_kwargs: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        service_headers: Mapping[str, str] | None = None,
        webhook_intervals: Sequence[int] | None = None,
        ignore_custom_services: bool = False,
    ) -> StartBenchmarkResponse:
        """Start a run from an uploaded agent name or complete contract."""
        if concurrency < 1:
            raise ValkyrieRunError("concurrency must be greater than 0")
        if not benchmark.strip():
            raise ValkyrieRunError("benchmark must not be blank")
        if task_ids and slice_str:
            raise ValkyrieRunError("task_ids and slice_str are mutually exclusive")

        contract = self._normalize_contract(agent, model=model, agent_kwargs=agent_kwargs, secrets=secrets)
        provider_name, provider_secret_name = self._sdk.config.resolve_sandbox_provider(provider)
        intervals = self._resolve_webhook_intervals(webhook_intervals)
        effective_service_headers = self._service_headers(benchmark, service_headers)

        payload = StartBenchmarkRequest(
            contract=contract,
            benchmark_name=benchmark,
            concurrency=concurrency,
            task_ids=list(task_ids) if task_ids else None,
            slice_str=slice_str,
            dataset=dataset,
            label=label,
            lambda_function=lambda_function,
            harness_config=self._sdk.config.harness_config(provider_secret_name),
            custom_benchmark_service=(
                None if ignore_custom_services else self._sdk.config.custom_benchmark_services.get(benchmark)
            ),
            service_headers=effective_service_headers,
            sandbox_provider=provider_name,
            webhook_secret_name=self._sdk.config.webhook if intervals else None,
            webhook_intervals=intervals,
        )
        return await self._sdk.request_model(
            "POST",
            "/start-benchmark",
            StartBenchmarkResponse,
            json=payload.model_dump(mode="json"),
        )

    async def fetch(self, run_id: UUID) -> FetchBenchmarkResponse:
        """Fetch the latest state of a run."""
        return await self._sdk.request_model(
            "GET",
            "/fetch-benchmark",
            FetchBenchmarkResponse,
            params={"benchmark_id": str(run_id)},
        )

    async def list(self, request: FetchBenchmarksRequest | None = None) -> FetchBenchmarksResponse:
        """List runs using typed filters and pagination."""
        resolved_request = request or FetchBenchmarksRequest()
        return await self._sdk.request_model(
            "GET",
            "/fetch-benchmarks",
            FetchBenchmarksResponse,
            params=resolved_request.model_dump(exclude_none=True, mode="json"),
        )

    @handle_httpx_stream_errors("Valkyrie stream failed")
    async def stream(self, run_id: UUID) -> AsyncIterator[FetchBenchmarkResponse]:
        """Yield typed updates until the run completes or disconnects."""
        async with self._sdk.stream_response(
            "GET",
            "/fetch-benchmark",
            params={"benchmark_id": str(run_id), "connect": "true"},
        ) as response:
            if not response.is_success:
                await response.aread()
                self._sdk.raise_for_status(response)

            event_name = ""
            data_lines: list[str] = []
            async for line in response.aiter_lines():
                if line == "":
                    if event_name or data_lines:
                        snapshot = self._parse_stream_event(event_name, data_lines)
                        if snapshot is not None:
                            yield snapshot
                        if event_name in {"complete", "disconnect"}:
                            return
                    event_name = ""
                    data_lines = []
                elif line.startswith("event:"):
                    event_name = line.removeprefix("event:").strip()
                elif line.startswith("data:"):
                    data_lines.append(line.removeprefix("data:").lstrip())

            if event_name or data_lines:
                snapshot = self._parse_stream_event(event_name, data_lines)
                if snapshot is not None:
                    yield snapshot

    @overload
    async def results(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        upload_to_s3: Literal[False] = False,
    ) -> FinalViewResponse: ...

    @overload
    async def results(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        upload_to_s3: Literal[True],
    ) -> S3UploadResultsResponse: ...

    @overload
    async def results(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        upload_to_s3: bool,
    ) -> RetrieveResultsResponse: ...

    async def results(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        upload_to_s3: bool = False,
    ) -> RetrieveResultsResponse:
        """Fetch final results or upload them and return S3 links."""
        params: dict[str, Any] = {"benchmark_id": str(run_id), "s3": upload_to_s3}
        if task_ids:
            params["task_ids"] = list(task_ids)
        response_model = S3UploadResultsResponse if upload_to_s3 else FinalViewResponse
        return await self._sdk.request_model("GET", "/retrieve-results", response_model, params=params)

    async def metadata(self, run_id: UUID) -> FetchBenchmarkMetadataResponse:
        """Fetch the stored launch metadata for a run."""
        return await self._sdk.request_model(
            "GET",
            f"/fetch-benchmark-metadata/{run_id}",
            FetchBenchmarkMetadataResponse,
        )

    async def results_exist(self, run_id: UUID) -> ResultsExistResponse:
        """Check whether the canonical result file already exists in S3."""
        return await self._sdk.request_model(
            "GET",
            "/check-results-exist",
            ResultsExistResponse,
            params={"benchmark_id": str(run_id)},
        )

    @handle_httpx_stream_errors("Valkyrie analysis stream failed")
    async def analyze(
        self,
        run_id: UUID,
        *,
        no_cache: bool = False,
        lambda_function: str | None = None,
    ) -> AsyncIterator[AnalyzeEvent]:
        """Yield typed progress events while analyzing a finished run."""
        payload = AnalyzeBenchmarkRequest(no_cache=no_cache, lambda_function=lambda_function)
        async with self._sdk.stream_response(
            "POST",
            f"/analyze-benchmark/{run_id}",
            json=payload.model_dump(mode="json"),
        ) as response:
            if not response.is_success:
                await response.aread()
                self._sdk.raise_for_status(response)

            if "text/event-stream" not in response.headers.get("content-type", ""):
                await response.aread()
                cached_payload: Any = response.json()
                if not isinstance(cached_payload, dict):
                    raise ValkyrieStreamError("Invalid Valkyrie analysis response")
                yield AnalyzeEvent(event="done", data=cast(dict[str, Any], cached_payload))
                return

            event_name = ""
            data_lines: list[str] = []
            async for line in response.aiter_lines():
                if line == "":
                    event = self._parse_analysis_event(event_name, data_lines)
                    if event is not None:
                        yield event
                        if event.event == "done":
                            return
                    event_name = ""
                    data_lines = []
                elif line.startswith("event:"):
                    event_name = line.removeprefix("event:").strip()
                elif line.startswith("data:"):
                    data_lines.append(line.removeprefix("data:").lstrip())

            event = self._parse_analysis_event(event_name, data_lines)
            if event is not None:
                yield event

    @handle_httpx_stream_errors("Valkyrie output stream failed")
    async def stream_outputs(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
    ) -> AsyncIterator[bytes]:
        """Yield chunks from the tar archive containing run outputs."""
        params: dict[str, Any] = {}
        if task_ids:
            params["task_ids"] = list(task_ids)
        async with self._sdk.stream_response(
            "GET",
            f"/fetch-run-outputs/{run_id}",
            params=params,
        ) as response:
            if not response.is_success:
                await response.aread()
                self._sdk.raise_for_status(response)
            async for chunk in response.aiter_bytes():
                yield chunk

    async def stop(
        self,
        run_id: UUID,
        *,
        force: bool = False,
        task_ids: Sequence[str] | None = None,
    ) -> StopBenchmarkResponse:
        """Stop a run or selected tasks."""
        return await self._sdk.request_model(
            "POST",
            f"/stop-benchmark/{run_id}",
            StopBenchmarkResponse,
            params={"force": force},
            json={"task_ids": list(task_ids)} if task_ids is not None else None,
        )

    async def resume(
        self,
        run_id: UUID,
        *,
        concurrency: int | None = None,
        task_ids: Sequence[str] | None = None,
        secrets: Mapping[str, str] | None = None,
        service_headers: Mapping[str, str] | None = None,
        from_scratch: bool = False,
        benchmark_url: str | None = None,
    ) -> RetryOrResumeBenchmarkResponse:
        """Resume unfinished work for a run."""
        return await self._retry_or_resume(
            run_id,
            retry=False,
            concurrency=concurrency,
            task_ids=task_ids,
            secrets=secrets,
            service_headers=service_headers,
            from_scratch=from_scratch,
            benchmark_url=benchmark_url,
        )

    async def retry(
        self,
        run_id: UUID,
        *,
        concurrency: int | None = None,
        task_ids: Sequence[str] | None = None,
        secrets: Mapping[str, str] | None = None,
        service_headers: Mapping[str, str] | None = None,
        from_scratch: bool = False,
        benchmark_url: str | None = None,
    ) -> RetryOrResumeBenchmarkResponse:
        """Retry failed or selected work for a run."""
        return await self._retry_or_resume(
            run_id,
            retry=True,
            concurrency=concurrency,
            task_ids=task_ids,
            secrets=secrets,
            service_headers=service_headers,
            from_scratch=from_scratch,
            benchmark_url=benchmark_url,
        )

    async def _retry_or_resume(
        self,
        run_id: UUID,
        *,
        retry: bool,
        concurrency: int | None,
        task_ids: Sequence[str] | None,
        secrets: Mapping[str, str] | None,
        service_headers: Mapping[str, str] | None,
        from_scratch: bool,
        benchmark_url: str | None,
    ) -> RetryOrResumeBenchmarkResponse:
        """Send a retry or resume request."""
        if concurrency is not None and concurrency < 1:
            raise ValkyrieRunError("concurrency must be greater than 0")

        run = await self.fetch(run_id)
        effective_headers = self._service_headers(run.benchmark_name, service_headers)
        params: dict[str, Any] = {
            "retry": retry,
            "retry_mode": RetryMode.FROM_SCRATCH.value if from_scratch else RetryMode.AUTO.value,
        }
        if concurrency is not None:
            params["concurrency"] = concurrency

        body: dict[str, Any] = {
            "task_ids": list(task_ids or []),
            "service_headers": effective_headers,
            "secrets": dict(secrets or {}),
        }
        if benchmark_url is not None:
            body["benchmark_url"] = benchmark_url

        return await self._sdk.request_model(
            "POST",
            f"/retry-or-resume-benchmark/{run_id}",
            RetryOrResumeBenchmarkResponse,
            params=params,
            json=body,
        )

    def _service_headers(self, benchmark: str, explicit_headers: Mapping[str, str] | None) -> dict[str, str]:
        """Merge configured and explicit service headers."""
        headers: dict[str, str] = {}
        if credential := self._sdk.config.benchmark_auth.get(benchmark):
            headers["Authorization"] = credential.get_secret_value()
        headers.update(explicit_headers or {})
        return headers

    def _resolve_webhook_intervals(self, intervals: Sequence[int] | None) -> list[int] | None:
        """Resolve and validate webhook intervals."""
        if intervals and not self._sdk.config.webhook:
            raise ValkyrieConfigError("webhook_intervals require a webhook secret in ValkyrieConfig")
        resolved = list(intervals) if intervals else ([100] if self._sdk.config.webhook else None)
        if resolved is None:
            return None
        if len(resolved) > 3:
            raise ValkyrieRunError("a maximum of 3 webhook intervals is allowed")
        if any(value < 5 or value > 100 or value % 5 != 0 for value in resolved):
            raise ValkyrieRunError("webhook intervals must be divisible by 5 and between 5 and 100")
        return resolved

    @staticmethod
    def _normalize_contract(
        agent: str | AgentContractRequest,
        *,
        model: str | None,
        agent_kwargs: Mapping[str, str] | None,
        secrets: Mapping[str, str] | None,
    ) -> AgentContractRequest:
        """Build an agent contract request."""
        if isinstance(agent, str):
            if not agent.strip():
                raise ValkyrieRunError("agent must not be blank")
            contract = AgentContractRequest(name=agent)
        else:
            contract = agent

        updates: dict[str, Any] = {
            "kwargs": {**contract.kwargs, **dict(agent_kwargs or {})},
            "secrets": {**contract.secrets, **dict(secrets or {})},
        }
        if model is not None:
            updates["model"] = model
        return contract.model_copy(update=updates)

    @staticmethod
    def _parse_stream_event(event_name: str, data_lines: Sequence[str]) -> FetchBenchmarkResponse | None:
        """Parse one server-sent event."""
        data = "\n".join(data_lines)
        if event_name == "error":
            try:
                payload: Any = json.loads(data) if data else {}
            except json.JSONDecodeError:
                payload = data
            detail = payload.get("error", payload) if isinstance(payload, dict) else payload
            raise ValkyrieStreamError(f"Valkyrie run stream returned an error: {detail}")
        if event_name in {"complete", "disconnect"}:
            return None
        if not data:
            return None
        try:
            return FetchBenchmarkResponse.model_validate_json(data)
        except (ValueError, TypeError) as exc:
            raise ValkyrieStreamError(f"Invalid Valkyrie run stream event: {data}") from exc

    @staticmethod
    def _parse_analysis_event(event_name: str, data_lines: Sequence[str]) -> AnalyzeEvent | None:
        """Parse one analysis server-sent event."""
        data = "\n".join(data_lines)
        if not event_name and not data:
            return None
        try:
            payload: Any = json.loads(data) if data else {}
        except json.JSONDecodeError as exc:
            raise ValkyrieStreamError(f"Invalid Valkyrie analysis event: {data}") from exc
        if not isinstance(payload, dict):
            raise ValkyrieStreamError(f"Invalid Valkyrie analysis event: {data}")
        if event_name == "error":
            detail = payload.get("message", payload)
            raise ValkyrieStreamError(f"Valkyrie analysis failed: {detail}")
        return AnalyzeEvent(event=event_name or "message", data=cast(dict[str, Any], payload))
