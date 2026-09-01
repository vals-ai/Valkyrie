"""Shared Tracker-to-ExecutorHost wire contract."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Annotated, Generic, Literal, Self, TypeVar, cast
from urllib.parse import urlparse
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    RootModel,
    ValidationError,
    field_validator,
    model_validator,
)

EXECUTOR_TASK_NAME = "tracker.utils:process_benchmark"
SUPPORTED_PROTOCOL_VERSION = "2"
SUPPORTED_PROTOCOL_VERSIONS = frozenset({"1", SUPPORTED_PROTOCOL_VERSION})
MANAGED_EXECUTION_PROTOCOL_VERSION = "2"
DEFAULT_STABLE_QUEUE_NAME = "valkyrie-stable"
DEFAULT_EXECUTOR_RELEASE_PREFIX = "releases"

AccessKeyRequestT = TypeVar("AccessKeyRequestT", bound=BaseModel)
ManagedRequestT = TypeVar("ManagedRequestT", bound=BaseModel)

_PROCESS_WIRE_KEYS = frozenset(
    {
        "start_benchmark_request_json",
        "benchmark_id_str",
        "verified_task_ids",
        "execution_context_json",
        "telemetry_context_json",
        "executor_dispatch_id",
    }
)
_TASK_WIRE_KEYS = _PROCESS_WIRE_KEYS | {
    "executor_release_id",
    "executor_artifact_uri",
    "executor_artifact_digest",
    "executor_protocol_version",
}


class ExecutorDispatchStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class ExecutorJsonObject(RootModel[dict[str, JsonValue]]):
    """Hold an opaque JSON object until its owning process validates it."""


class ExecutorTelemetryContext(BaseModel):
    """Correlation context propagated independently from executor behavior."""

    model_config = ConfigDict(frozen=True)

    request_id: str = ""
    trace_headers: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_wire(cls, value: object) -> ExecutorTelemetryContext:
        """Normalize optional telemetry without making observability fatal."""
        if not isinstance(value, Mapping):
            return cls()

        request_id = value.get("request_id")
        raw_trace_headers = value.get("trace_headers")
        trace_headers = (
            {str(key): str(header) for key, header in raw_trace_headers.items()}
            if isinstance(raw_trace_headers, Mapping)
            else {}
        )
        return cls(
            request_id=str(request_id) if request_id else "",
            trace_headers=trace_headers,
        )


class ExecutorTaskPayloadValidationError(ValueError):
    """Report invalid Taskiq input with best-effort wire correlation."""

    def __init__(
        self,
        message: str,
        *,
        telemetry_context: ExecutorTelemetryContext,
        benchmark_id: str,
        dispatch_id: str,
        release_id: str,
    ) -> None:
        super().__init__(message)
        self.telemetry_context = telemetry_context
        self.benchmark_id = benchmark_id
        self.dispatch_id = dispatch_id
        self.release_id = release_id


class AccessKeyExecutorExecution(BaseModel, Generic[AccessKeyRequestT]):
    """Validated access-key executor input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["access_key"] = "access_key"
    request: AccessKeyRequestT = Field(repr=False)
    benchmark_id: UUID
    verified_task_ids: list[str]


class ExecutorManagedExecutionContext(BaseModel, Generic[ManagedRequestT]):
    """Versioned managed executor input shared across process boundaries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[2]
    benchmark_id: UUID
    verified_task_ids: list[str]
    start_benchmark_request: ManagedRequestT = Field(repr=False)


class ManagedExecutorExecution(BaseModel, Generic[ManagedRequestT]):
    """Validated managed executor input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["managed"] = "managed"
    context: ExecutorManagedExecutionContext[ManagedRequestT]

    @property
    def request(self) -> ManagedRequestT:
        return self.context.start_benchmark_request

    @property
    def benchmark_id(self) -> UUID:
        return self.context.benchmark_id

    @property
    def verified_task_ids(self) -> list[str]:
        return self.context.verified_task_ids


def _execution_wire_data(payload: Mapping[str, object]) -> dict[str, object]:
    managed_context = payload.get("execution_context_json")
    access_key_values = (
        payload.get("start_benchmark_request_json"),
        payload.get("benchmark_id_str"),
        payload.get("verified_task_ids"),
    )
    if managed_context is not None:
        if any(value is not None for value in access_key_values):
            raise ValueError("Executor payload mixes access-key and managed execution inputs")
        return {
            "mode": "managed",
            "context": managed_context,
        }

    request, benchmark_id, verified_task_ids = access_key_values
    if request is None or benchmark_id is None or verified_task_ids is None:
        raise ValueError("Executor access-key payload is incomplete")
    return {
        "mode": "access_key",
        "request": request,
        "benchmark_id": benchmark_id,
        "verified_task_ids": verified_task_ids,
    }


def _reject_unexpected_wire_keys(payload: Mapping[str, object], allowed_keys: frozenset[str]) -> None:
    unexpected_keys = sorted(set(payload) - allowed_keys)
    if unexpected_keys:
        raise ValueError(f"Executor payload contains unexpected fields: {', '.join(unexpected_keys)}")


def _process_wire_data(
    payload: Mapping[str, object],
    *,
    telemetry_context: ExecutorTelemetryContext | None = None,
) -> dict[str, object]:
    executor_dispatch_id = payload.get("executor_dispatch_id")
    if not isinstance(executor_dispatch_id, str) or not executor_dispatch_id:
        raise ValueError("executor_dispatch_id is required")
    return {
        "execution": _execution_wire_data(payload),
        "telemetry_context": telemetry_context
        or ExecutorTelemetryContext.from_wire(payload.get("telemetry_context_json")),
        "executor_dispatch_id": executor_dispatch_id,
    }


def _model_json_object(model: BaseModel) -> dict[str, object]:
    value = model.model_dump(mode="json")
    if not isinstance(value, dict):
        raise TypeError("Executor request must serialize to a JSON object")
    return cast(dict[str, object], value)


def _wire_string(value: object) -> str:
    return value if isinstance(value, str) else ""


def _wire_benchmark_id(payload: Mapping[str, object]) -> str:
    if benchmark_id := _wire_string(payload.get("benchmark_id_str")):
        return benchmark_id
    managed_context = payload.get("execution_context_json")
    if isinstance(managed_context, Mapping):
        return _wire_string(managed_context.get("benchmark_id"))
    return ""


def _validation_fields(error: ValidationError) -> str:
    return ", ".join(
        f"{'.'.join(str(part) for part in item['loc'])}: {item['msg']}" for item in error.errors(include_input=False)
    )


class ExecutorProcessPayload(BaseModel, Generic[AccessKeyRequestT, ManagedRequestT]):
    """Payload written by ExecutorHost and consumed by the executor process."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    execution: Annotated[
        AccessKeyExecutorExecution[AccessKeyRequestT] | ManagedExecutorExecution[ManagedRequestT],
        Field(discriminator="mode"),
    ]
    telemetry_context: ExecutorTelemetryContext = Field(default_factory=ExecutorTelemetryContext)
    executor_dispatch_id: str

    @property
    def benchmark_id(self) -> UUID:
        return self.execution.benchmark_id

    @property
    def verified_task_ids(self) -> list[str]:
        return self.execution.verified_task_ids

    @field_validator("executor_dispatch_id")
    @classmethod
    def validate_dispatch_id(cls, value: str) -> str:
        if not value:
            raise ValueError("Executor dispatch ID is required")
        return value

    @classmethod
    def from_wire(cls, payload: Mapping[str, object]) -> Self:
        """Validate one process-boundary JSON object."""
        _reject_unexpected_wire_keys(payload, _PROCESS_WIRE_KEYS)
        try:
            return cls.model_validate(_process_wire_data(payload))
        except ValidationError as error:
            validation_fields = _validation_fields(error)
        raise ValueError(f"Executor process payload is invalid: {validation_fields}")

    def to_wire(self) -> dict[str, object]:
        """Serialize to the deployed payload.json shape."""
        execution = self.execution
        if isinstance(execution, AccessKeyExecutorExecution):
            wire: dict[str, object] = {
                "start_benchmark_request_json": _model_json_object(execution.request),
                "benchmark_id_str": str(execution.benchmark_id),
                "verified_task_ids": execution.verified_task_ids,
            }
        else:
            wire = {"execution_context_json": _model_json_object(execution.context)}
        wire.update(
            {
                "telemetry_context_json": self.telemetry_context.model_dump(mode="json"),
                "executor_dispatch_id": self.executor_dispatch_id,
            }
        )
        return wire


class ExecutorTaskPayload(BaseModel, Generic[AccessKeyRequestT, ManagedRequestT]):
    """Taskiq envelope containing one validated executor process payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    process: ExecutorProcessPayload[AccessKeyRequestT, ManagedRequestT]
    executor_release_id: str
    executor_artifact_uri: str
    executor_artifact_digest: str
    executor_protocol_version: str

    @field_validator("executor_release_id", "executor_artifact_uri")
    @classmethod
    def validate_required_string(cls, value: str) -> str:
        if not value:
            raise ValueError("Executor dispatch metadata is required")
        return value

    @field_validator("executor_artifact_digest")
    @classmethod
    def validate_artifact_digest(cls, value: str) -> str:
        return validate_executor_digest(value)

    @field_validator("executor_protocol_version")
    @classmethod
    def validate_protocol_version(cls, value: str) -> str:
        if value not in SUPPORTED_PROTOCOL_VERSIONS:
            raise ValueError(f"Unsupported executor protocol version: {value}")
        return value

    @model_validator(mode="after")
    def validate_execution_protocol(self) -> Self:
        if (
            isinstance(self.process.execution, ManagedExecutorExecution)
            and self.executor_protocol_version != MANAGED_EXECUTION_PROTOCOL_VERSION
        ):
            raise ValueError(f"Managed execution requires executor protocol {MANAGED_EXECUTION_PROTOCOL_VERSION}")
        return self

    @classmethod
    def from_wire(cls, payload: Mapping[str, object]) -> Self:
        """Validate one deployed Taskiq message shape."""
        telemetry_context = ExecutorTelemetryContext.from_wire(payload.get("telemetry_context_json"))
        try:
            _reject_unexpected_wire_keys(payload, _TASK_WIRE_KEYS)
            return cls.model_validate(
                {
                    "process": _process_wire_data(payload, telemetry_context=telemetry_context),
                    "executor_release_id": payload.get("executor_release_id"),
                    "executor_artifact_uri": payload.get("executor_artifact_uri"),
                    "executor_artifact_digest": payload.get("executor_artifact_digest"),
                    "executor_protocol_version": payload.get("executor_protocol_version"),
                }
            )
        except ValidationError as error:
            validation_fields = _validation_fields(error)
            message = f"Executor Taskiq payload is invalid: {validation_fields}"
        except ValueError as error:
            message = str(error)
        raise ExecutorTaskPayloadValidationError(
            message,
            telemetry_context=telemetry_context,
            benchmark_id=_wire_benchmark_id(payload),
            dispatch_id=_wire_string(payload.get("executor_dispatch_id")),
            release_id=_wire_string(payload.get("executor_release_id")),
        ) from None

    def to_wire(self) -> dict[str, object]:
        """Serialize to the deployed Taskiq keyword shape."""
        return {
            **self.process.to_wire(),
            "executor_release_id": self.executor_release_id,
            "executor_artifact_uri": self.executor_artifact_uri,
            "executor_artifact_digest": self.executor_artifact_digest,
            "executor_protocol_version": self.executor_protocol_version,
        }


async def executor_task_signature(**_payload: object) -> None:
    """Provide the producer's Taskiq signature; this body never executes."""
    raise RuntimeError("Executor task signatures cannot execute in Tracker")


def validate_executor_digest(digest: str) -> str:
    normalized = digest.lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError("Executor artifact digest must be a 64-character SHA-256 digest")
    return normalized


def validate_executor_artifact_uri(uri: str, expected_bucket: str, expected_prefix: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    prefix = expected_prefix.strip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("Executor artifact URI must identify an S3 object")
    if parsed.netloc != expected_bucket or not prefix or not key.startswith(f"{prefix}/"):
        raise ValueError("Executor artifact URI is outside the configured S3 bucket and prefix")
    return parsed.netloc, key
