"""Run lifecycle operations for the async Valkyrie SDK."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload
from uuid import UUID

import httpx
from valkyrie.sdk.errors import ValkyrieConfigError, ValkyrieRunError, ValkyrieStreamError, ValkyrieTransportError
from valkyrie.sdk.models import (
    AgentContractRequest,
    GetRunResponse,
    ListRunsRequest,
    ListRunsResponse,
    RetrieveRunResultsResponse,
    RetryMode,
    RetryOrResumeRunResponse,
    RunResultsResponse,
    S3UploadResultsResponse,
    StartRunRequest,
    StartRunResponse,
    StopRunResponse,
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
    ) -> StartRunResponse:
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

        payload = StartRunRequest(
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
            "/runs",
            StartRunResponse,
            json=payload.model_dump(mode="json"),
            fallback_path="/start-benchmark",
        )

    async def fetch(self, run_id: UUID) -> GetRunResponse:
        """Fetch the latest state of a run."""
        return await self._sdk.request_model(
            "GET",
            f"/runs/{run_id}",
            GetRunResponse,
            fallback_path="/fetch-benchmark",
            fallback_params={"benchmark_id": str(run_id)},
        )

    async def list(self, request: ListRunsRequest | None = None) -> ListRunsResponse:
        """List runs using typed filters and pagination."""
        resolved_request = request or ListRunsRequest()
        return await self._sdk.request_model(
            "GET",
            "/runs",
            ListRunsResponse,
            params=resolved_request.model_dump(exclude_none=True, mode="json"),
            fallback_path="/fetch-benchmarks",
        )

    async def stream(self, run_id: UUID) -> AsyncIterator[GetRunResponse]:
        """Yield typed updates until the run completes or disconnects."""
        try:
            routes = (
                (f"/runs/{run_id}/events", None),
                ("/fetch-benchmark", {"benchmark_id": str(run_id), "connect": "true"}),
            )
            for route_index, (path, params) in enumerate(routes):
                async with self._sdk.stream_response("GET", path, params=params) as response:
                    if response.status_code == 404 and route_index == 0:
                        await response.aread()
                        continue
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
                    return
        except httpx.HTTPError as exc:
            raise ValkyrieTransportError(f"Valkyrie stream failed: {exc}") from exc

    @overload
    async def results(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        upload_to_s3: Literal[False] = False,
    ) -> RunResultsResponse: ...

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
    ) -> RetrieveRunResultsResponse: ...

    async def results(
        self,
        run_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        upload_to_s3: bool = False,
    ) -> RetrieveRunResultsResponse:
        """Fetch final results or upload them and return S3 links."""
        params: dict[str, Any] = {"s3": upload_to_s3}
        if task_ids:
            params["task_ids"] = list(task_ids)
        response_model = S3UploadResultsResponse if upload_to_s3 else RunResultsResponse
        fallback_params = {"benchmark_id": str(run_id), **params}
        return await self._sdk.request_model(
            "GET",
            f"/runs/{run_id}/results",
            response_model,
            params=params,
            fallback_path="/retrieve-results",
            fallback_params=fallback_params,
        )

    async def stop(self, run_id: UUID, *, force: bool = False) -> StopRunResponse:
        """Stop a run, optionally terminating active sandboxes."""
        return await self._sdk.request_model(
            "POST",
            f"/runs/{run_id}/stop",
            StopRunResponse,
            params={"force": force},
            fallback_path=f"/stop-benchmark/{run_id}",
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
    ) -> RetryOrResumeRunResponse:
        """Resume unfinished work for a run."""
        return await self._retry_or_resume(
            run_id,
            retry=False,
            concurrency=concurrency,
            task_ids=task_ids,
            secrets=secrets,
            service_headers=service_headers,
            from_scratch=from_scratch,
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
    ) -> RetryOrResumeRunResponse:
        """Retry failed or selected work for a run."""
        return await self._retry_or_resume(
            run_id,
            retry=True,
            concurrency=concurrency,
            task_ids=task_ids,
            secrets=secrets,
            service_headers=service_headers,
            from_scratch=from_scratch,
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
    ) -> RetryOrResumeRunResponse:
        """Send a retry or resume request."""
        if concurrency is not None and concurrency < 1:
            raise ValkyrieRunError("concurrency must be greater than 0")

        run = await self.fetch(run_id)
        effective_headers = self._service_headers(run.benchmark_name, service_headers)
        params: dict[str, Any] = {
            "retry_mode": RetryMode.FROM_SCRATCH.value if from_scratch else RetryMode.AUTO.value,
        }
        if concurrency is not None:
            params["concurrency"] = concurrency

        return await self._sdk.request_model(
            "POST",
            f"/runs/{run_id}/{'retry' if retry else 'resume'}",
            RetryOrResumeRunResponse,
            params=params,
            json={
                "task_ids": list(task_ids or []),
                "service_headers": effective_headers,
                "secrets": dict(secrets or {}),
            },
            fallback_path=f"/retry-or-resume-benchmark/{run_id}",
            fallback_params={"retry": retry, **params},
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
    def _parse_stream_event(event_name: str, data_lines: Sequence[str]) -> GetRunResponse | None:
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
            return GetRunResponse.model_validate_json(data)
        except (ValueError, TypeError) as exc:
            raise ValkyrieStreamError(f"Invalid Valkyrie run stream event: {data}") from exc
