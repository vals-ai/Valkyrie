"""Tests for crash-safe post-evaluation artifact bundles."""

from __future__ import annotations

import base64
import hashlib
from typing import Any

import pytest

from tracker.evaluation_artifacts import (
    MAX_TRUSTED_EVALUATION_BUNDLE_BYTES,
    REQUIRED_TRUSTED_EVALUATION_ARTIFACTS,
    TRUSTED_EVALUATION_BUNDLE_SCHEMA,
    TRUSTED_EVALUATION_BUNDLE_UPLOADED_SCHEMA,
    prepare_evaluation_bundle,
    upload_evaluation_bundle,
    uploaded_evaluation_result,
)
from tracker.exceptions import OutputArtifactError
from tracker.types import AWSCredentials


def _artifact(path: str, content: bytes, *, media_type: str = "application/json") -> dict[str, Any]:
    return {
        "path": path,
        "media_type": media_type,
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "content_base64": base64.b64encode(content).decode("ascii"),
    }


def _bundle(*artifacts: dict[str, Any], result: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": TRUSTED_EVALUATION_BUNDLE_SCHEMA,
        "result": result or _result(),
        "artifacts": list(artifacts),
    }


def _result(*, score: float = 1.0, resolved: bool = True) -> dict[str, Any]:
    return {"task": "full_ladder", "resolved": resolved, "score": score}


def _required_artifacts(*, turns: bytes | None = None) -> list[dict[str, Any]]:
    contents = {
        "gateway-run-accounting.json": b'{"final":true,"finalized":true,"attempts":[]}',
        "run-report.json": b'{"finality":{"complete":true}}',
        "vals_format/run_config.json": b'{"schema_version":"vals_run_config.v1"}',
        "vals_format/turns.jsonl": turns or b'{"type":"assistant"}\n',
    }
    return [
        _artifact(path, contents[path], media_type=media_type)
        for path, media_type in REQUIRED_TRUSTED_EVALUATION_ARTIFACTS.items()
    ]


def _aws() -> AWSCredentials:
    return AWSCredentials(
        aws_access_key_id="test-access",
        aws_secret_access_key="test-secret",
        aws_default_region="us-east-1",
    )


def test_prepare_bundle_hash_checks_exact_bytes_and_result() -> None:
    turns = b'{"input_tokens": 1, "cache_read_tokens": 2}\n'
    state = _bundle(*_required_artifacts(turns=turns))

    prepared = prepare_evaluation_bundle(state, terminal_result=_result())

    assert prepared is not None
    assert prepared.result == _result()
    assert prepared.artifacts[-1][1] == turns


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda state: state["artifacts"][0].update(path="../secret"), "normalized relative paths"),
        (lambda state: state["artifacts"][0].update(content_base64="not base64"), "invalid base64"),
        (lambda state: state["artifacts"][0].update(bytes=99), "size mismatch"),
        (lambda state: state["artifacts"][0].update(sha256="0" * 64), "SHA-256 mismatch"),
        (lambda state: state.update(extra="nope"), "extra"),
    ],
)
def test_prepare_bundle_rejects_malformed_reserved_state(mutate: Any, message: str) -> None:
    state = _bundle(*_required_artifacts())
    mutate(state)

    with pytest.raises(OutputArtifactError, match=message):
        prepare_evaluation_bundle(state, terminal_result=_result())


def test_prepare_bundle_rejects_result_mismatch_and_duplicate_paths() -> None:
    state = _bundle(
        *_required_artifacts()[:-1],
        _artifact("run-report.json", b'{"duplicate":true}'),
    )

    with pytest.raises(OutputArtifactError, match="paths must be unique"):
        prepare_evaluation_bundle(state, terminal_result=_result())

    single = _bundle(*_required_artifacts())
    with pytest.raises(OutputArtifactError, match="does not match terminal"):
        prepare_evaluation_bundle(single, terminal_result=_result(score=0.0, resolved=False))


def test_prepare_bundle_rejects_total_decoded_size_limit() -> None:
    first = b"a" * (MAX_TRUSTED_EVALUATION_BUNDLE_BYTES // 2 + 1)
    second = b"b" * (MAX_TRUSTED_EVALUATION_BUNDLE_BYTES // 2 + 1)
    artifacts = _required_artifacts()
    artifacts[0] = _artifact("gateway-run-accounting.json", first)
    artifacts[1] = _artifact("run-report.json", second)
    state = _bundle(*artifacts)

    with pytest.raises(OutputArtifactError, match="bundle exceeds"):
        prepare_evaluation_bundle(state, terminal_result=_result())


def test_prepare_bundle_allows_one_large_artifact_within_total_limit() -> None:
    turns = b'{"padding":"' + b"x" * (5 * 1024 * 1024) + b'"}\n'
    prepared = prepare_evaluation_bundle(
        _bundle(*_required_artifacts(turns=turns)),
        terminal_result=_result(),
    )

    assert prepared is not None
    assert prepared.artifacts[-1][1] == turns


def test_prepare_bundle_rejects_noncanonical_base64() -> None:
    artifacts = _required_artifacts()
    artifacts[0] = _artifact("gateway-run-accounting.json", b"{}")
    canonical = artifacts[0]["content_base64"]
    assert canonical.endswith("0=")
    # Change only unused pad bits. Python's strict decoder accepts this and
    # produces the same bytes, but the wire representation is not canonical.
    artifacts[0]["content_base64"] = canonical[:-2] + "1="

    with pytest.raises(OutputArtifactError, match="base64 is not canonical"):
        prepare_evaluation_bundle(_bundle(*artifacts), terminal_result=_result())


def test_non_reserved_checkpoint_is_ignored() -> None:
    assert prepare_evaluation_bundle({"schema_version": "other.v1"}, terminal_result={}) is None


async def test_upload_bundle_uses_deterministic_task_keys_and_returns_compact_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = b'{"finality":{"complete":true}}'
    turns = b'{"cache_read_tokens":4}\n'
    artifacts = _required_artifacts(turns=turns)
    artifacts[1] = _artifact("run-report.json", report)
    state = _bundle(*artifacts)
    prepared = prepare_evaluation_bundle(state, terminal_result=_result())
    assert prepared is not None
    uploads: list[tuple[bytes, str, str]] = []

    async def fake_upload(content: bytes, key: str, _aws: AWSCredentials, bucket: str) -> None:
        uploads.append((content, key, bucket))

    monkeypatch.setattr("tracker.evaluation_artifacts.upload_to_s3", fake_upload)

    uploaded = await upload_evaluation_bundle(
        prepared,
        benchmark_id="run-1",
        task_id="full_ladder",
        aws=_aws(),
        s3_bucket="artifact-bucket",
        execution_is_current=lambda: True,
    )

    assert uploads[1] == (report, "benchmarks/run-1/full_ladder/run-report.json", "artifact-bucket")
    assert uploads[-1] == (turns, "benchmarks/run-1/full_ladder/vals_format/turns.jsonl", "artifact-bucket")
    assert len(uploads) == 4
    dumped = uploaded.model_dump(mode="json")
    assert dumped["schema_version"] == TRUSTED_EVALUATION_BUNDLE_UPLOADED_SCHEMA
    assert "content_base64" not in str(dumped)
    assert dumped["artifacts"][1]["sha256"] == hashlib.sha256(report).hexdigest()


async def test_upload_bundle_fails_when_execution_authority_is_revoked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_evaluation_bundle(
        _bundle(*_required_artifacts()),
        terminal_result=_result(),
    )
    assert prepared is not None
    checks = iter([True, False])

    async def fake_upload(_content: bytes, _key: str, _aws: AWSCredentials, _bucket: str) -> None:
        return None

    monkeypatch.setattr("tracker.evaluation_artifacts.upload_to_s3", fake_upload)

    with pytest.raises(OutputArtifactError, match="authority was revoked"):
        await upload_evaluation_bundle(
            prepared,
            benchmark_id="run-1",
            task_id="full_ladder",
            aws=_aws(),
            s3_bucket="artifact-bucket",
            execution_is_current=lambda: next(checks),
        )


def test_prepare_bundle_requires_exact_v1_paths_and_media_types() -> None:
    missing = _bundle(*_required_artifacts()[:-1])
    with pytest.raises(OutputArtifactError, match="at least 4 items"):
        prepare_evaluation_bundle(missing, terminal_result=_result())

    wrong_media = _required_artifacts()
    wrong_media[-1]["media_type"] = "application/json"
    with pytest.raises(OutputArtifactError, match="must match the v1 manifest exactly"):
        prepare_evaluation_bundle(_bundle(*wrong_media), terminal_result=_result())

    extra = _required_artifacts()
    extra.append(_artifact("extra.json", b"{}"))
    with pytest.raises(OutputArtifactError, match="at most 4 items"):
        prepare_evaluation_bundle(_bundle(*extra), terminal_result=_result())


@pytest.mark.parametrize(
    ("path", "content", "message"),
    [
        ("run-report.json", b"\xff", "not UTF-8"),
        ("run-report.json", b'{"finality":', "not valid JSON"),
        ("run-report.json", b'{"finality":{"complete":true},"prompt":"private"}', "forbidden field"),
        ("run-report.json", b'{"finality":{"complete":true},"error":"provider_error"}', "raw error field"),
        ("run-report.json", b'{"finality":{"complete":true,"complete":true}}', "duplicate JSON field"),
        ("run-report.json", b'{"finality":{"complete":true},"score":NaN}', "non-finite JSON"),
        ("run-report.json", b"[]", "must contain one object"),
        ("run-report.json", b'{"finality":{"complete":false}}', "run report is not final"),
        ("gateway-run-accounting.json", b"[]", "must contain one object"),
        (
            "gateway-run-accounting.json",
            b'{"final":true,"finalized":true,"attempts":{}}',
            "attempts must be a list",
        ),
        (
            "gateway-run-accounting.json",
            b'{"final":true,"finalized":true,"attempts":[{"status":"unknown"}]}',
            "invalid attempt status",
        ),
        (
            "gateway-run-accounting.json",
            b'{"final":true,"finalized":true,"attempts":[{"status":"unresolved"}]}',
            "unresolved attempts",
        ),
        (
            "gateway-run-accounting.json",
            b'{"final":true,"finalized":false,"attempts":[]}',
            "not fenced and finalized",
        ),
        (
            "vals_format/turns.jsonl",
            b'{"turn_index":0,"status":"error","error":"provider returned private prose"}\n',
            "short failure classification",
        ),
        (
            "vals_format/turns.jsonl",
            b'{"turn_index":0,"status":"success"}\n\n{"turn_index":1,"status":"success"}\n',
            "blank row",
        ),
    ],
)
def test_prepare_bundle_rejects_untrusted_artifact_content(path: str, content: bytes, message: str) -> None:
    artifacts = _required_artifacts()
    media_type = REQUIRED_TRUSTED_EVALUATION_ARTIFACTS[path]
    artifacts[list(REQUIRED_TRUSTED_EVALUATION_ARTIFACTS).index(path)] = _artifact(
        path,
        content,
        media_type=media_type,
    )

    with pytest.raises(OutputArtifactError, match=message):
        prepare_evaluation_bundle(_bundle(*artifacts), terminal_result=_result())


@pytest.mark.parametrize(
    "result",
    [
        {"task": "reach_orbit", "resolved": True, "score": 1.0},
        {"task": "full_ladder", "resolved": "yes", "score": 1.0},
        {"task": "full_ladder", "resolved": True, "score": 1.1},
        {"task": "full_ladder", "resolved": True, "score": float("nan")},
        {"task": "full_ladder", "resolved": True, "score": 1.0, "api_key": "secret"},
    ],
)
def test_prepare_bundle_rejects_invalid_terminal_result(result: dict[str, Any]) -> None:
    with pytest.raises(OutputArtifactError, match="Invalid trusted evaluation artifact bundle"):
        prepare_evaluation_bundle(_bundle(*_required_artifacts(), result=result), terminal_result=result)


def test_prepare_bundle_rejects_non_object_terminal_result_from_stream() -> None:
    with pytest.raises(OutputArtifactError, match="terminal result must be an object"):
        prepare_evaluation_bundle(_bundle(*_required_artifacts()), terminal_result=[])


def test_uploaded_resume_state_fails_closed_if_tampered() -> None:
    artifacts = [
        {
            "path": path,
            "media_type": media_type,
            "bytes": 0,
            "sha256": "0" * 64,
            "s3_key": f"benchmarks/run/full_ladder/{path}",
        }
        for path, media_type in REQUIRED_TRUSTED_EVALUATION_ARTIFACTS.items()
    ]
    artifacts[-1]["path"] = artifacts[0]["path"]
    state = {
        "schema_version": TRUSTED_EVALUATION_BUNDLE_UPLOADED_SCHEMA,
        "result": _result(),
        "artifacts": artifacts,
    }

    with pytest.raises(OutputArtifactError, match="Invalid uploaded trusted evaluation artifact bundle"):
        uploaded_evaluation_result(state)
