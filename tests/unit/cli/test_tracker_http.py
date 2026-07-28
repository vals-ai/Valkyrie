"""Tests for tracker HTTP response and streaming contracts.

Run: uv run pytest tests/unit/cli/test_tracker_http.py
"""

import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from uuid import UUID

import httpx
import pytest
from tracker.database.models import BenchmarkStatus
from tracker.types import FinalViewResponse, S3UploadResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService

from tests.unit.cli.factories import make_final_view

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


@contextmanager
def _tracker_with_handler(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
) -> Generator[TrackerService, None, None]:
    original_client = httpx.Client
    transport = httpx.MockTransport(handler)

    def build_client(
        *,
        timeout: int | float | httpx.Timeout | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Client:
        return original_client(transport=transport, timeout=timeout, headers=headers)

    monkeypatch.setattr(TrackerService, "_load_config", staticmethod(lambda: {}))
    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)
    tracker = TrackerService(base_url="http://tracker", require_config=False)

    try:
        yield tracker
    finally:
        tracker.close()


def _fetch_payload() -> dict[str, object]:
    return {
        "benchmark_name": "swebench",
        "benchmark_id": str(_RUN_ID),
        "details": {
            "status": "FINISHED",
            "started_at": "2026-07-17T12:00:00Z",
            "finished_at": "2026-07-17T12:05:00Z",
            "total_tasks": 2,
            "finished_tasks": 2,
            "task_breakdown": {"FINISHED": 2},
            "docent_reading_status": "IDLE",
        },
        "s3_bucket_url": "s3://bucket/run",
        "final_score": 0.75,
    }


def _metadata_payload() -> dict[str, object]:
    return {
        "benchmark_id": str(_RUN_ID),
        "benchmark_name": "swebench",
        "benchmark_arguments": {
            "contract": {"name": "agent", "install_cmd": "true", "run_cmd": "true"},
            "concurrency": 2,
            "dataset": "verified",
        },
        "started_by_email": "runner@example.com",
    }


class TestTrackerJsonEndpoints:
    """Typed parsing and request contracts for tracker JSON endpoints."""

    def test_malformed_success_response_raises_tracker_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful HTTP responses with an invalid tracker payload need a stable CLI error.

        Test cases:
        - A partial fetch response raises TrackerServiceError instead of leaking model validation internals.
        """

        def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"benchmark_name": "swebench"}, request=request)

        with _tracker_with_handler(monkeypatch, handle_request) as tracker:
            with pytest.raises(
                TrackerServiceError,
                match="Failed to fetch run: tracker returned a malformed response",
            ):
                tracker.fetch_benchmark(_RUN_ID)

    @pytest.mark.parametrize(
        ("status_code", "detail"),
        [(401, "invalid token"), (400, "invalid status request")],
        ids=["authentication", "request"],
    )
    def test_status_requests_surface_non_ok_responses(
        self,
        status_code: int,
        detail: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Connected status handling must not hide non-retryable tracker responses.

        Test cases:
        - Authentication failures surface from both fetch and stream requests.
        - Invalid requests surface from both fetch and stream requests.
        """

        def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code, json={"detail": detail}, request=request)

        with _tracker_with_handler(monkeypatch, handle_request) as tracker:
            with pytest.raises(TrackerServiceError, match=detail):
                tracker.fetch_benchmark(_RUN_ID)

            with pytest.raises(TrackerServiceError, match=detail):
                list(tracker.stream_benchmark(_RUN_ID))

    def test_run_read_endpoints_parse_real_http_responses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Run reads must preserve typed payloads, filters, and task selections over HTTP.

        Test cases:
        - Fetch and metadata responses parse into their production models.
        - Inline and S3 result variants are selected by the request mode.
        - Task export and result-existence calls send their documented payloads.
        """
        requests: list[httpx.Request] = []
        final_view = make_final_view(
            _RUN_ID,
            status=BenchmarkStatus.FINISHED,
            error_message=None,
        ).model_dump(mode="json")

        def handle_request(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/fetch-benchmark":
                return httpx.Response(200, json=_fetch_payload(), request=request)
            if request.url.path == f"/fetch-benchmark-metadata/{_RUN_ID}":
                return httpx.Response(200, json=_metadata_payload(), request=request)
            if request.url.path == "/retrieve-results":
                if request.url.params["s3"] == "true":
                    return httpx.Response(
                        200,
                        json={
                            "s3_url": "s3://bucket/results.json",
                            "presigned_url": "https://download.example/results",
                            "console_url": "https://console.example/results",
                        },
                        request=request,
                    )
                return httpx.Response(200, json=final_view, request=request)
            if request.url.path == "/fetch-benchmark-tasks":
                return httpx.Response(200, json={"task_ids": ["task-a", "task-b"]}, request=request)
            if request.url.path == "/check-results-exist":
                return httpx.Response(200, json={"exists": True}, request=request)
            return httpx.Response(404, request=request)

        with _tracker_with_handler(monkeypatch, handle_request) as tracker:
            fetched_run = tracker.fetch_benchmark(_RUN_ID)
            metadata = tracker.fetch_benchmark_metadata(_RUN_ID)
            inline_results = tracker.retrieve_results(_RUN_ID, False, task_ids=["task-a"])
            s3_results = tracker.retrieve_results(_RUN_ID, True)
            task_ids = tracker.fetch_benchmark_tasks(
                "swebench",
                dataset="verified",
                ignore_custom_services=True,
                service_headers={"Authorization": "Bearer benchmark"},
            )
            results_exist = tracker.check_results_exist_in_s3(_RUN_ID)

        assert fetched_run.final_score == 0.75
        assert metadata.benchmark_arguments.dataset == "verified"
        assert isinstance(inline_results, FinalViewResponse)
        assert isinstance(s3_results, S3UploadResultsResponse)
        assert inline_results.benchmark_id == _RUN_ID
        assert s3_results.presigned_url == "https://download.example/results"
        assert task_ids == ["task-a", "task-b"]
        assert results_exist is True

        result_request = next(
            request
            for request in requests
            if request.url.path == "/retrieve-results" and request.url.params["s3"] == "false"
        )
        assert result_request.url.params.get_list("task_ids") == ["task-a"]

        task_request = next(request for request in requests if request.url.path == "/fetch-benchmark-tasks")
        assert json.loads(task_request.content) == {
            "benchmark_name": "swebench",
            "dataset": "verified",
            "custom_benchmark_service": None,
            "service_headers": {"Authorization": "Bearer benchmark"},
        }


class TestTrackerStreams:
    """SSE parsing, cached analysis, and terminal error behavior."""

    def test_analysis_and_run_streams_preserve_events(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Cached JSON and SSE responses must normalize into stable iterator events.

        Test cases:
        - Cached analysis JSON is emitted as one done event.
        - Fresh analysis stops after its terminal done event.
        - Run status streaming forwards non-empty SSE lines in order.
        """
        analysis_requests = 0

        def handle_request(request: httpx.Request) -> httpx.Response:
            nonlocal analysis_requests
            if request.url.path == f"/analyze-benchmark/{_RUN_ID}":
                analysis_requests += 1
                if analysis_requests == 1:
                    return httpx.Response(
                        200,
                        json={"reading_plan_url": "https://docent.example/cached"},
                        request=request,
                    )
                return httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    text=(
                        'event: started\ndata: {"lambda_function": "ingest"}\n\n'
                        "event: heartbeat\ndata: {}\n\n"
                        'event: done\ndata: {"reading_plan_url": "https://docent.example/new"}\n\n'
                        'event: heartbeat\ndata: {"ignored": true}\n\n'
                    ),
                    request=request,
                )
            if request.url.path == "/fetch-benchmark":
                return httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    text='data: {"status": "IN_PROGRESS"}\n\nevent: complete\n\n',
                    request=request,
                )
            return httpx.Response(404, request=request)

        with _tracker_with_handler(monkeypatch, handle_request) as tracker:
            cached_events = list(tracker.analyze_benchmark(_RUN_ID, no_cache=False, lambda_function="ingest"))
            streamed_events = list(tracker.analyze_benchmark(_RUN_ID, no_cache=True, lambda_function="ingest"))
            run_lines = list(tracker.stream_benchmark(_RUN_ID))

        assert cached_events == [("done", {"reading_plan_url": "https://docent.example/cached"})]
        assert streamed_events == [
            ("started", {"lambda_function": "ingest"}),
            ("heartbeat", {}),
            ("done", {"reading_plan_url": "https://docent.example/new"}),
        ]
        assert run_lines == ['data: {"status": "IN_PROGRESS"}', "event: complete"]

    @pytest.mark.parametrize(
        ("content_type", "response_content", "expected_message"),
        [
            ("application/json", json.dumps({"detail": "run is not finished"}), "run is not finished"),
            ("text/plain", "gateway unavailable", "gateway unavailable"),
        ],
    )
    def test_analysis_rejects_non_ok_json_and_text_responses(
        self,
        content_type: str,
        response_content: str,
        expected_message: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Analysis failures must retain useful JSON or plain-text tracker details.

        Test cases:
        - JSON detail responses preserve the tracker explanation.
        - Plain-text upstream failures remain visible to the caller.
        """

        def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                409,
                headers={"content-type": content_type},
                content=response_content,
                request=request,
            )

        with _tracker_with_handler(monkeypatch, handle_request) as tracker:
            with pytest.raises(TrackerServiceError, match=expected_message):
                list(tracker.analyze_benchmark(_RUN_ID, no_cache=False, lambda_function="ingest"))
