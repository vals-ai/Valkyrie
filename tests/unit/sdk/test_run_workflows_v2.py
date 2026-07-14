"""SDK V2 run workflow tests.

Run: pytest tests/unit/sdk/test_run_workflows_v2.py
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import httpx
import pytest

from valkyrie.sdk import ValkyrieAPIError, ValkyrieStreamError

_STOP_RUN_ID = UUID("11111111-1111-4111-8111-111111111111")


class ChunkStream(httpx.AsyncByteStream):
    """Deterministic streaming body for output archive tests."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"first-"
        yield b"second"


async def test_metadata_returns_typed_run_metadata(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == f"/fetch-benchmark-metadata/{run_id}"
        return httpx.Response(
            200,
            json={
                "benchmark_id": str(run_id),
                "benchmark_name": "swebench",
                "benchmark_arguments": {
                    "contract": {"name": "sweagent", "model": "anthropic/claude-sonnet-4-6"},
                    "concurrency": 5,
                    "task_ids": None,
                    "slice_str": None,
                    "lambda_function": None,
                    "dataset": "default",
                    "sandbox_provider": "daytona",
                    "sandbox_provider_secret_name": None,
                },
                "started_by_email": "developer@vals.ai",
            },
        )

    async with make_client(handler) as client:
        result = await client.runs.metadata(run_id)

    assert result.benchmark_id == run_id
    assert result.benchmark_arguments.contract.name == "sweagent"


async def test_results_exist_returns_typed_s3_state(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/check-results-exist"
        assert request.url.params["benchmark_id"] == str(run_id)
        return httpx.Response(200, json={"exists": True})

    async with make_client(handler) as client:
        result = await client.runs.results_exist(run_id)

    assert result.exists is True


@pytest.mark.parametrize(
    ("task_ids", "expected_body"),
    [
        (None, None),
        (["task-1", "task-2"], {"task_ids": ["task-1", "task-2"]}),
    ],
)
async def test_stop_sends_optional_task_scope(
    make_client,
    task_ids: list[str] | None,
    expected_body: dict[str, list[str]] | None,
) -> None:
    """Check task-scoped stop bodies.

    Test cases:
    - No IDs omits the body.
    - IDs send a task selection.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        assert (request.url.path, request.url.params["force"], body) == (
            f"/stop-benchmark/{_STOP_RUN_ID}",
            "true",
            expected_body,
        )

        return httpx.Response(200, json={"status": "success"})

    async with make_client(handler) as client:
        result = await client.runs.stop(_STOP_RUN_ID, force=True, task_ids=task_ids)

    assert result.status == "success"


async def test_analyze_normalizes_cached_json_to_a_done_event(make_client) -> None:
    run_id = uuid4()
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == f"/analyze-benchmark/{run_id}"
        captured_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"status": "done", "reading_plan_url": "https://analysis.test/plan"},
        )

    async with make_client(handler) as client:
        events = [event async for event in client.runs.analyze(run_id)]

    assert captured_body == {"no_cache": False, "lambda_function": None}
    assert [(event.event, event.data) for event in events] == [
        ("done", {"status": "done", "reading_plan_url": "https://analysis.test/plan"})
    ]


async def test_analyze_parses_sse_progress_events(make_client) -> None:
    run_id = uuid4()

    def handler(_request: httpx.Request) -> httpx.Response:
        content = (
            'event: started\ndata: {"lambda_function": "analyze-fn"}\n\n'
            "event: heartbeat\ndata: {}\n\n"
            'event: done\ndata: {"reading_plan_url":\n'
            'data: "https://analysis.test/plan"}\n\n'
        )
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=content)

    async with make_client(handler) as client:
        events = [
            event
            async for event in client.runs.analyze(
                run_id,
                no_cache=True,
                lambda_function="analyze-fn",
            )
        ]

    assert [event.event for event in events] == ["started", "heartbeat", "done"]
    assert events[-1].data == {"reading_plan_url": "https://analysis.test/plan"}


async def test_analyze_stops_after_the_terminal_done_event(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(
                'event: done\ndata: {"reading_plan_url": "https://analysis.test/plan"}\n\n'
                "event: heartbeat\ndata: {}\n\n"
            ),
        )

    async with make_client(handler) as client:
        events = [event async for event in client.runs.analyze(uuid4(), lambda_function="analyze-fn")]

    assert [event.event for event in events] == ["done"]


async def test_analyze_raises_for_an_sse_error_event(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content='event: error\ndata: {"message": "lambda failed"}\n\n',
        )

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieStreamError, match="lambda failed"):
            _ = [event async for event in client.runs.analyze(uuid4(), lambda_function="analyze-fn")]


async def test_analyze_raises_api_errors_before_streaming(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "run must be FINISHED"})

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieAPIError, match="run must be FINISHED"):
            _ = [event async for event in client.runs.analyze(uuid4())]


async def test_stream_outputs_yields_archive_chunks_and_repeated_task_filters(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == f"/fetch-run-outputs/{run_id}"
        assert request.url.params.get_list("task_ids") == ["task-1", "task-2"]
        return httpx.Response(200, headers={"content-type": "application/x-tar"}, stream=ChunkStream())

    async with make_client(handler) as client:
        chunks = [
            chunk
            async for chunk in client.runs.stream_outputs(
                run_id,
                task_ids=["task-1", "task-2"],
            )
        ]

    assert chunks == [b"first-", b"second"]


async def test_stream_outputs_raises_api_errors_before_streaming(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "No outputs found"})

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieAPIError, match="No outputs found"):
            _ = [chunk async for chunk in client.runs.stream_outputs(uuid4())]
