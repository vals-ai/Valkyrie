"""Run with `uv run pytest tests/unit/middleware/test_run_request_size.py`."""

from collections.abc import Iterator

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from tracker.middleware.run_request_size import MAX_RUN_REQUEST_BYTES, RunRequestSizeLimitMiddleware


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.add_middleware(RunRequestSizeLimitMiddleware)

    async def echo_size(request: Request) -> dict[str, int]:
        return {"bytes": len(await request.body())}

    for path in ("/start-benchmark", "/start-benchmark-with-storage", "/retry-or-resume-benchmark/run-1", "/other"):
        app.add_api_route(path, echo_size, methods=["POST"])
    return TestClient(app)


def _chunks(total: int) -> Iterator[bytes]:
    chunk = b"x" * 65536
    for _ in range(total // len(chunk)):
        yield chunk
    yield b"x" * (total % len(chunk))


@pytest.mark.parametrize(
    "path", ["/start-benchmark", "/start-benchmark-with-storage", "/retry-or-resume-benchmark/run-1"]
)
def test_run_request_at_limit_passes_and_one_byte_over_is_rejected(client: TestClient, path: str) -> None:
    accepted = client.post(path, content=b"x" * MAX_RUN_REQUEST_BYTES)
    rejected = client.post(path, content=b"x" * (MAX_RUN_REQUEST_BYTES + 1))

    assert accepted.status_code == 200
    assert accepted.json() == {"bytes": MAX_RUN_REQUEST_BYTES}
    assert rejected.status_code == 413


def test_streamed_run_request_without_content_length_is_rejected_once_over_limit(client: TestClient) -> None:
    response = client.post("/start-benchmark", content=_chunks(MAX_RUN_REQUEST_BYTES + 1))

    assert response.status_code == 413


def test_other_routes_are_not_limited(client: TestClient) -> None:
    response = client.post("/other", content=b"x" * (MAX_RUN_REQUEST_BYTES + 1))

    assert response.status_code == 200
