"""Canonical V1 payloads accepted by the Tracker service models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from tracker.types import (
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sdk_api"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("name", "key", "model"),
    [
        ("start.json", "request", StartBenchmarkRequest),
        ("start.json", "response", StartBenchmarkResponse),
        ("fetch.json", "response", FetchBenchmarkResponse),
        ("list.json", "request", FetchBenchmarksRequest),
        ("list.json", "response", FetchBenchmarksResponse),
        ("results.json", "inline", FinalViewResponse),
        ("results.json", "s3", S3UploadResultsResponse),
        ("stop.json", "response", StopBenchmarkResponse),
        ("retry_resume.json", "response", RetryOrResumeBenchmarkResponse),
    ],
)
def test_tracker_accepts_canonical_sdk_fixture(name: str, key: str, model: type[BaseModel]) -> None:
    validated = model.model_validate(load_fixture(name)[key])
    assert isinstance(validated, model)
