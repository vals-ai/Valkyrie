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
from valkyrie.sdk.models import (
    FetchBenchmarkResponse as SDKFetchBenchmarkResponse,
    FetchBenchmarksRequest as SDKFetchBenchmarksRequest,
    FetchBenchmarksResponse as SDKFetchBenchmarksResponse,
    FinalViewResponse as SDKFinalViewResponse,
    RetryOrResumeBenchmarkResponse as SDKRetryResponse,
    S3UploadResultsResponse as SDKS3ResultsResponse,
    StartBenchmarkRequest as SDKStartBenchmarkRequest,
    StartBenchmarkResponse as SDKStartBenchmarkResponse,
    StopBenchmarkResponse as SDKStopBenchmarkResponse,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sdk_api"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("name", "key", "tracker_model", "sdk_model"),
    [
        ("start.json", "request", StartBenchmarkRequest, SDKStartBenchmarkRequest),
        ("start.json", "response", StartBenchmarkResponse, SDKStartBenchmarkResponse),
        ("fetch.json", "response", FetchBenchmarkResponse, SDKFetchBenchmarkResponse),
        ("list.json", "request", FetchBenchmarksRequest, SDKFetchBenchmarksRequest),
        ("list.json", "response", FetchBenchmarksResponse, SDKFetchBenchmarksResponse),
        ("results.json", "inline", FinalViewResponse, SDKFinalViewResponse),
        ("results.json", "s3", S3UploadResultsResponse, SDKS3ResultsResponse),
        ("stop.json", "response", StopBenchmarkResponse, SDKStopBenchmarkResponse),
        ("retry_resume.json", "response", RetryOrResumeBenchmarkResponse, SDKRetryResponse),
    ],
)
def test_sdk_and_tracker_accept_canonical_fixture(
    name: str,
    key: str,
    tracker_model: type[BaseModel],
    sdk_model: type[BaseModel],
) -> None:
    payload = load_fixture(name)[key]
    tracker_value = tracker_model.model_validate(payload)
    sdk_value = sdk_model.model_validate(payload)

    assert isinstance(tracker_value, tracker_model)
    assert isinstance(sdk_value, sdk_model)
