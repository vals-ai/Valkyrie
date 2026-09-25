"""Parse the execution request carried by the executor host payload."""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from tracker.logging import get_logger
from tracker.types import ManagedExecutionContext, StartBenchmarkRequest

logger = get_logger(__name__)


def _parse_start_benchmark_request(payload: dict[str, Any]) -> StartBenchmarkRequest:
    """Validate a queued request without serializing credential-bearing input in errors."""
    request: StartBenchmarkRequest | None
    try:
        request = StartBenchmarkRequest.model_validate(payload)
    except ValidationError as exc:
        # Log field locations only; rendering the full error would expose input
        # values, which include AWS credentials on this payload.
        logger.warning(
            f"Queued benchmark request failed validation: {exc.errors(include_url=False, include_input=False)}"
        )
        request = None

    if request is None:
        raise ValueError("Queued benchmark request is invalid and cannot be processed.")
    return request


@dataclass(frozen=True)
class _QueuedExecution:
    request: StartBenchmarkRequest
    benchmark_id: UUID
    verified_task_ids: list[str]
    aws_managed: bool
    context_version: int | None = None


def parse_queued_execution(
    start_benchmark_request_json: dict[str, Any] | None,
    benchmark_id_str: str | None,
    verified_task_ids: list[str] | None,
    execution_context_json: dict[str, Any] | None,
) -> _QueuedExecution:
    if execution_context_json is None:
        if start_benchmark_request_json is None or benchmark_id_str is None or verified_task_ids is None:
            raise ValueError("Queued benchmark request is incomplete and cannot be processed.")
        request = _parse_start_benchmark_request(start_benchmark_request_json)
        if request.harness_config is None:
            raise ValueError("Queued access-key benchmark request has no AWS configuration.")

        if request.managed_s3_bucket is not None:
            raise ValueError("Queued execution cannot include an admission-only storage override.")

        return _QueuedExecution(
            request=request,
            benchmark_id=UUID(benchmark_id_str),
            verified_task_ids=verified_task_ids,
            aws_managed=False,
        )

    if start_benchmark_request_json is not None or benchmark_id_str is not None or verified_task_ids is not None:
        raise ValueError("Queued benchmark request mixes access-key and managed execution inputs.")
    try:
        context = ManagedExecutionContext.model_validate(execution_context_json)
    except ValidationError:
        raise ValueError("Queued managed execution context is invalid and cannot be processed.") from None
    return _QueuedExecution(
        request=context.start_benchmark_request,
        benchmark_id=context.benchmark_id,
        verified_task_ids=context.verified_task_ids,
        aws_managed=True,
        context_version=context.version,
    )
