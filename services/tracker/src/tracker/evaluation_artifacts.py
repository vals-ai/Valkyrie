"""Crash-safe, benchmark-service-owned post-evaluation artifacts.

Benchmark services sometimes learn authoritative facts only while grading, after
the evaluated agent's ordinary output artifacts have already been collected.
They can checkpoint a small, prompt-free artifact bundle through the existing
evaluation-resume channel.  The tracker validates that bundle, uploads the
exact bytes under the task's normal S3 prefix, and only then commits the score.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from tracker.aws.s3 import get_agent_result_s3_key, upload_to_s3
from tracker.exceptions import OutputArtifactError
from tracker.types import AWSCredentials

TRUSTED_EVALUATION_BUNDLE_SCHEMA = "valkyrie_trusted_evaluation_bundle.v1"
TRUSTED_EVALUATION_BUNDLE_UPLOADED_SCHEMA = "valkyrie_trusted_evaluation_bundle_uploaded.v1"

# The benchmark-service websocket is capped at 10 MiB.  Base64 expands bytes by
# one third and the result/manifest also consume space, so decoded artifacts are
# deliberately capped at 6 MiB total.
MAX_TRUSTED_EVALUATION_BUNDLE_BYTES = 6 * 1024 * 1024

REQUIRED_TRUSTED_EVALUATION_ARTIFACTS = {
    "gateway-run-accounting.json": "application/json",
    "run-report.json": "application/json",
    "vals_format/run_config.json": "application/json",
    "vals_format/turns.jsonl": "application/x-ndjson",
}
_TRUSTED_EVALUATION_ARTIFACT_COUNT = len(REQUIRED_TRUSTED_EVALUATION_ARTIFACTS)

_FORBIDDEN_FIELD_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "cookie",
        "headers",
        "messages",
        "password",
        "prompt",
        "prompts",
        "raw_error",
        "refresh_token",
        "request_body",
        "response_body",
        "secret",
        "secrets",
        "set_cookie",
        "stack_trace",
        "traceback",
    }
)
_SAFE_ERROR_CLASSIFICATION = re.compile(r"[a-z0-9][a-z0-9_.:-]{0,127}\Z")
_GATEWAY_ATTEMPT_STATUSES = frozenset({"success", "provider_error", "gateway_error", "unresolved"})


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field is forbidden: {key}")
        result[key] = value
    return result


def _load_json(data: bytes, *, label: str) -> Any:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} is not UTF-8") from exc
    try:
        return json.loads(
            text,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON") from exc


def _strict_json_copy(value: Any, *, label: str) -> Any:
    try:
        serialized = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not strict JSON") from exc
    return _load_json(serialized.encode(), label=label)


def _walk_prompt_free(value: Any, *, label: str, in_turn: bool = False) -> None:
    if isinstance(value, dict):
        status = value.get("status")
        this_is_turn = in_turn or ("turn_index" in value and status in {"success", "error"})
        for raw_key, child in value.items():
            key = str(raw_key).lower().replace("-", "_")
            if key in _FORBIDDEN_FIELD_NAMES:
                raise ValueError(f"{label} contains forbidden field {raw_key!r}")
            if key == "error":
                if not this_is_turn or not isinstance(child, str):
                    raise ValueError(f"{label} contains a raw error field")
                if _SAFE_ERROR_CLASSIFICATION.fullmatch(child) is None:
                    raise ValueError(f"{label} error must be a short failure classification")
            _walk_prompt_free(child, label=label, in_turn=this_is_turn)
        return
    if isinstance(value, list):
        for child in value:
            _walk_prompt_free(child, label=label, in_turn=in_turn)
        return
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{label} contains a non-finite number")


def _validate_terminal_result(value: Any) -> dict[str, Any]:
    copied = _strict_json_copy(value, label="terminal result")
    if not isinstance(copied, dict):
        raise ValueError("terminal result must be an object")
    if copied.get("task") != "full_ladder":
        raise ValueError("trusted KSP result must belong to full_ladder")
    if not isinstance(copied.get("resolved"), bool):
        raise ValueError("trusted KSP result must contain boolean resolved")
    score = copied.get("score")
    if (
        not isinstance(score, (int, float))
        or isinstance(score, bool)
        or not math.isfinite(float(score))
        or not 0.0 <= float(score) <= 1.0
    ):
        raise ValueError("trusted KSP result must contain a finite score in [0, 1]")
    _walk_prompt_free(copied, label="terminal result")
    return copied


def _load_artifact_documents(path: str, data: bytes) -> list[Any]:
    if path.endswith(".json"):
        return [_load_json(data, label=path)]
    if path != "vals_format/turns.jsonl":
        raise ValueError(f"unsupported trusted artifact path: {path}")
    documents: list[Any] = []
    for line_number, line in enumerate(data.splitlines(), start=1):
        if not line.strip():
            raise ValueError(f"{path} contains a blank row at line {line_number}")
        documents.append(_load_json(line, label=f"{path} line {line_number}"))
    return documents


def _validate_finalization_documents(documents: dict[str, list[Any]]) -> None:
    accounting_docs = documents["gateway-run-accounting.json"]
    if len(accounting_docs) != 1 or not isinstance(accounting_docs[0], dict):
        raise ValueError("gateway-run-accounting.json must contain one object")
    accounting = accounting_docs[0]
    if accounting.get("final") is not True or accounting.get("finalized") is not True:
        raise ValueError("Gateway accounting is not fenced and finalized")
    attempts = accounting.get("attempts")
    if not isinstance(attempts, list):
        raise ValueError("Gateway accounting attempts must be a list")
    for attempt in attempts:
        if not isinstance(attempt, dict) or attempt.get("status") not in _GATEWAY_ATTEMPT_STATUSES:
            raise ValueError("Gateway accounting contains an invalid attempt status")
        if attempt["status"] == "unresolved":
            raise ValueError("Gateway accounting still has unresolved attempts")

    report_docs = documents["run-report.json"]
    if len(report_docs) != 1 or not isinstance(report_docs[0], dict):
        raise ValueError("run-report.json must contain one object")
    finality = report_docs[0].get("finality")
    if not isinstance(finality, dict) or finality.get("complete") is not True:
        raise ValueError("run report is not final")


def _normalized_artifact_path(value: str) -> str:
    if not value or "\\" in value:
        raise ValueError("trusted evaluation artifact paths must be non-empty POSIX paths")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or "." in path.parts or ".." in path.parts:
        raise ValueError("trusted evaluation artifact paths must be normalized relative paths")
    normalized = str(path)
    if normalized != value:
        raise ValueError("trusted evaluation artifact paths must already be normalized")
    return normalized


class _PendingArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    path: str
    media_type: Literal["application/json", "application/x-ndjson"]
    bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_base64: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _normalized_artifact_path(value)

    def decode(self) -> bytes:
        try:
            content = base64.b64decode(self.content_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise OutputArtifactError(f"Trusted evaluation artifact {self.path!r} has invalid base64") from exc
        if base64.b64encode(content).decode("ascii") != self.content_base64:
            raise OutputArtifactError(f"Trusted evaluation artifact {self.path!r} base64 is not canonical")
        if len(content) != self.bytes:
            raise OutputArtifactError(
                f"Trusted evaluation artifact {self.path!r} size mismatch: {len(content)} != {self.bytes}"
            )
        observed = hashlib.sha256(content).hexdigest()
        if observed != self.sha256:
            raise OutputArtifactError(
                f"Trusted evaluation artifact {self.path!r} SHA-256 mismatch: {observed} != {self.sha256}"
            )
        return content


class _PendingBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["valkyrie_trusted_evaluation_bundle.v1"]
    result: dict[str, Any]
    artifacts: list[_PendingArtifact] = Field(
        min_length=_TRUSTED_EVALUATION_ARTIFACT_COUNT,
        max_length=_TRUSTED_EVALUATION_ARTIFACT_COUNT,
    )

    @model_validator(mode="after")
    def validate_artifacts(self) -> _PendingBundle:
        paths = [artifact.path for artifact in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("trusted evaluation artifact paths must be unique")
        observed = {artifact.path: artifact.media_type for artifact in self.artifacts}
        if observed != REQUIRED_TRUSTED_EVALUATION_ARTIFACTS:
            raise ValueError("trusted evaluation artifact paths and media types must match the v1 manifest exactly")
        if sum(artifact.bytes for artifact in self.artifacts) > MAX_TRUSTED_EVALUATION_BUNDLE_BYTES:
            raise ValueError(
                f"trusted evaluation artifact bundle exceeds {MAX_TRUSTED_EVALUATION_BUNDLE_BYTES} decoded bytes"
            )
        return self

    @field_validator("result")
    @classmethod
    def validate_result(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _validate_terminal_result(value)


class UploadedArtifact(BaseModel):
    """Compact durable reference retained after a successful upload."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    path: str
    media_type: Literal["application/json", "application/x-ndjson"]
    bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    s3_key: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _normalized_artifact_path(value)


class UploadedEvaluationBundle(BaseModel):
    """Small checkpoint stored atomically with the finished task."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["valkyrie_trusted_evaluation_bundle_uploaded.v1"]
    result: dict[str, Any]
    artifacts: list[UploadedArtifact] = Field(
        min_length=_TRUSTED_EVALUATION_ARTIFACT_COUNT,
        max_length=_TRUSTED_EVALUATION_ARTIFACT_COUNT,
    )

    @model_validator(mode="after")
    def validate_artifacts(self) -> UploadedEvaluationBundle:
        paths = [artifact.path for artifact in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("uploaded trusted evaluation artifact paths must be unique")
        observed = {artifact.path: artifact.media_type for artifact in self.artifacts}
        if observed != REQUIRED_TRUSTED_EVALUATION_ARTIFACTS:
            raise ValueError("uploaded trusted evaluation artifacts must match the v1 manifest exactly")
        if sum(artifact.bytes for artifact in self.artifacts) > MAX_TRUSTED_EVALUATION_BUNDLE_BYTES:
            raise ValueError(
                f"uploaded trusted evaluation artifact bundle exceeds {MAX_TRUSTED_EVALUATION_BUNDLE_BYTES} bytes"
            )
        return self

    @field_validator("result")
    @classmethod
    def validate_result(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _validate_terminal_result(value)


@dataclass(frozen=True)
class PreparedEvaluationBundle:
    result: dict[str, Any]
    artifacts: tuple[tuple[_PendingArtifact, bytes], ...]


def is_pending_evaluation_bundle(value: object) -> bool:
    return isinstance(value, dict) and value.get("schema_version") == TRUSTED_EVALUATION_BUNDLE_SCHEMA


def is_uploaded_evaluation_bundle(value: object) -> bool:
    return isinstance(value, dict) and value.get("schema_version") == TRUSTED_EVALUATION_BUNDLE_UPLOADED_SCHEMA


def uploaded_evaluation_result(value: object) -> dict[str, Any] | None:
    """Return a previously committed result without calling a public replay API."""

    if not is_uploaded_evaluation_bundle(value):
        return None
    try:
        return UploadedEvaluationBundle.model_validate(value).result
    except ValidationError as exc:
        raise OutputArtifactError(f"Invalid uploaded trusted evaluation artifact bundle: {exc}") from exc


def prepare_evaluation_bundle(
    value: object,
    *,
    terminal_result: object,
) -> PreparedEvaluationBundle | None:
    """Parse and hash-check a checkpoint when it uses the reserved schema.

    Other benchmark-owned evaluation checkpoints are ignored.  Once a service
    opts into the reserved schema, malformed data fails closed instead of being
    treated as an ordinary checkpoint.
    """

    if not is_pending_evaluation_bundle(value):
        return None
    try:
        bundle = _PendingBundle.model_validate(value)
    except ValidationError as exc:
        raise OutputArtifactError(f"Invalid trusted evaluation artifact bundle: {exc}") from exc
    try:
        checked_terminal_result = _validate_terminal_result(terminal_result)
    except ValueError as exc:
        raise OutputArtifactError(f"Invalid trusted evaluation terminal result: {exc}") from exc
    if bundle.result != checked_terminal_result:
        raise OutputArtifactError("Trusted evaluation artifact bundle result does not match terminal evaluation")
    decoded = tuple((artifact, artifact.decode()) for artifact in bundle.artifacts)
    try:
        documents: dict[str, list[Any]] = {}
        for artifact, content in decoded:
            parsed = _load_artifact_documents(artifact.path, content)
            for document in parsed:
                _walk_prompt_free(document, label=artifact.path)
            documents[artifact.path] = parsed
        _validate_finalization_documents(documents)
    except ValueError as exc:
        raise OutputArtifactError(f"Invalid trusted evaluation artifact content: {exc}") from exc
    return PreparedEvaluationBundle(result=bundle.result, artifacts=decoded)


async def upload_evaluation_bundle(
    bundle: PreparedEvaluationBundle,
    *,
    benchmark_id: str,
    task_id: str,
    aws: AWSCredentials,
    s3_bucket: str,
    execution_is_current: Callable[[], bool],
) -> UploadedEvaluationBundle:
    """Upload exact checked bytes, preserving execution-authority fencing."""

    uploaded: list[UploadedArtifact] = []
    for artifact, content in bundle.artifacts:
        if not execution_is_current():
            raise OutputArtifactError("Trusted evaluation artifact upload authority was revoked")
        s3_key = get_agent_result_s3_key(benchmark_id, task_id, artifact.path)
        await upload_to_s3(content, s3_key, aws, s3_bucket)
        if not execution_is_current():
            raise OutputArtifactError("Trusted evaluation artifact upload authority was revoked")
        uploaded.append(
            UploadedArtifact(
                path=artifact.path,
                media_type=artifact.media_type,
                bytes=artifact.bytes,
                sha256=artifact.sha256,
                s3_key=s3_key,
            )
        )
    return UploadedEvaluationBundle(
        schema_version=TRUSTED_EVALUATION_BUNDLE_UPLOADED_SCHEMA,
        result=bundle.result,
        artifacts=uploaded,
    )
