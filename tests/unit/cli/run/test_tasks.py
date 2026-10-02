"""JSON output for run task scores."""

import json
from importlib import import_module
from uuid import UUID

import pytest
from click.testing import CliRunner
from valkyrie.sdk.models import FetchTasksRequest, TasksResponse

tasks_module = import_module("valkyrie.cli.run.tasks")


@pytest.mark.parametrize("score", [0.0, 0.75, None])
def test_tasks_json_includes_score(monkeypatch: pytest.MonkeyPatch, score: float | None) -> None:
    run_id = UUID("11111111-1111-4111-8111-111111111111")

    async def fetch_tasks(request_run_id: UUID, request: FetchTasksRequest) -> TasksResponse:
        assert request_run_id == run_id
        assert request.limit == 50
        return TasksResponse.model_validate(
            {
                "tasks": [
                    {
                        "id": str(run_id),
                        "task_id": "task-1",
                        "status": "FINISHED",
                        "started_at": "2026-10-01T12:00:00+00:00",
                        "finished_at": "2026-10-01T12:01:00+00:00",
                        "score": score,
                    }
                ],
                "total_count": 1,
            }
        )

    monkeypatch.setattr(tasks_module, "_tasks", fetch_tasks)
    result = CliRunner().invoke(tasks_module.tasks, [str(run_id), "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["tasks"][0]["score"] == score
