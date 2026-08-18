"""Tests for hosted benchmark and task inspection workflows."""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from valkyrie.sdk.models import FetchTasksRequest, Order, TaskStatus


async def test_fetch_returns_typed_benchmark_detail(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == f"/benchmarks/{run_id}"
        return httpx.Response(
            200,
            json={
                "id": str(run_id),
                "name": "swebench",
                "agent_name": "sweagent",
                "model": "anthropic/claude-sonnet-4-6",
                "started_at": "2026-07-08T12:00:00Z",
                "finished_at": None,
                "status": "IN_PROGRESS",
                "total_tasks": 2,
                "finished_tasks": 1,
                "task_state_counts": {"FINISHED": 1, "IN_PROGRESS": 1},
                "started_by_email": "developer@vals.ai",
                "final_score": None,
                "error_message": None,
                "cloudwatch_url": "https://logs.test",
                "s3_bucket_url": "s3://runs-bucket/benchmarks/run",
            },
        )

    async with make_client(handler) as client:
        result = await client.benchmarks.fetch(run_id)

    assert result.id == run_id
    assert result.agent_name == "sweagent"
    assert result.task_state_counts == {"FINISHED": 1, "IN_PROGRESS": 1}


async def test_statuses_serializes_ids_as_csv_and_accepts_an_empty_list(make_client) -> None:
    first_id = uuid4()
    second_id = uuid4()
    requested_ids: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/benchmarks/status"
        requested_ids.append(request.url.params["ids"])
        entries = []
        if request.url.params["ids"]:
            entries = [
                {
                    "id": str(first_id),
                    "status": "FINISHED",
                    "finished_at": "2026-07-08T13:00:00Z",
                    "total_tasks": 2,
                    "finished_tasks": 2,
                    "task_state_counts": {"FINISHED": 2},
                }
            ]
        return httpx.Response(200, json={"entries": entries})

    async with make_client(handler) as client:
        populated = await client.benchmarks.statuses([first_id, second_id])
        empty = await client.benchmarks.statuses([])

    assert requested_ids == [f"{first_id},{second_id}", ""]
    assert populated.entries[0].id == first_id
    assert empty.entries == []


async def test_tasks_serializes_typed_filters_and_pagination(make_client) -> None:
    run_id = uuid4()
    task_row_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/benchmarks/{run_id}/tasks"
        assert dict(request.url.params) == {
            "status": "ERROR,FINISHED",
            "task_id_search": "repo__issue",
            "sort": "status",
            "sort_dir": "asc",
            "limit": "25",
            "offset": "50",
        }
        return httpx.Response(
            200,
            json={
                "tasks": [
                    {
                        "id": str(task_row_id),
                        "task_id": "repo__issue-1",
                        "status": "ERROR",
                        "started_at": "2026-07-08T12:00:00Z",
                        "finished_at": "2026-07-08T12:05:00Z",
                        "error_message": "agent failed",
                    }
                ],
                "total_count": 1,
            },
        )

    request = FetchTasksRequest(
        status=[TaskStatus.ERROR, TaskStatus.FINISHED],
        task_id_search="repo__issue",
        sort="status",
        sort_dir=Order.ASC,
        limit=25,
        offset=50,
    )
    async with make_client(handler) as client:
        result = await client.benchmarks.tasks(run_id, request)

    assert result.total_count == 1
    assert result.tasks[0].status is TaskStatus.ERROR


async def test_task_and_artifacts_escape_task_id_path_segment(make_client) -> None:
    run_id = uuid4()
    task_row_id = uuid4()
    raw_paths: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        raw_paths.append(request.url.raw_path)
        if request.url.path.endswith("/artifacts"):
            return httpx.Response(
                200,
                json={
                    "cloudwatch_url": "https://logs.test/task",
                    "agent_output_url": "https://download.test/output",
                    "agent_output_expires_in": 300,
                },
            )
        return httpx.Response(
            200,
            json={
                "id": str(task_row_id),
                "task_id": "task one",
                "status": "FINISHED",
                "started_at": "2026-07-08T12:00:00Z",
                "finished_at": "2026-07-08T12:05:00Z",
                "error_message": None,
                "evaluation_result": {"score": 1.0},
                "agent_caused_exit_reason": None,
            },
        )

    async with make_client(handler) as client:
        task = await client.benchmarks.task(run_id, "task one")
        artifacts = await client.benchmarks.artifacts(run_id, "task one")

    assert raw_paths == [
        f"/benchmarks/{run_id}/tasks/task%20one".encode(),
        f"/benchmarks/{run_id}/tasks/task%20one/artifacts".encode(),
    ]
    assert task.evaluation_result == {"score": 1.0}
    assert artifacts.agent_output_expires_in == 300


@pytest.mark.parametrize("method_name", ["task", "artifacts"])
async def test_task_methods_reject_blank_task_ids(make_client, method_name: str) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        method = getattr(client.benchmarks, method_name)
        with pytest.raises(ValueError, match="task_id must not be blank"):
            await method(uuid4(), "  ")


@pytest.mark.parametrize("method_name", ["task", "artifacts"])
async def test_task_methods_reject_path_separators(make_client, method_name: str) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        method = getattr(client.benchmarks, method_name)
        with pytest.raises(ValueError, match="task_id must not contain '/'"):
            await method(uuid4(), "suite/task")


@pytest.mark.parametrize("method_name", ["task", "artifacts"])
@pytest.mark.parametrize("task_id", [".", ".."])
async def test_task_methods_reject_normalized_dot_segments(make_client, method_name: str, task_id: str) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        method = getattr(client.benchmarks, method_name)
        with pytest.raises(ValueError, match=r"task_id must not be '\.' or '\.\.'"):
            await method(uuid4(), task_id)
