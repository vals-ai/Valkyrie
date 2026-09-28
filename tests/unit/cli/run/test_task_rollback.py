"""Tests for `valk run task-history` and `valk run rollback-task`.

Run: uv run pytest tests/unit/cli/run/test_task_rollback.py
"""

import json
from datetime import UTC, datetime
from importlib import import_module
from uuid import UUID

import pytest
from click.testing import CliRunner
from valkyrie.sdk import ValkyrieSDKError
from valkyrie.sdk.models import RollbackTaskResponse, TaskResultEntry, TaskResultsResponse, TaskStatus

tasks_module = import_module("valkyrie.cli.run.tasks")

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_OLD_ID = UUID("00000000-0000-0000-0000-000000000001")
_NEW_ID = UUID("00000000-0000-0000-0000-000000000002")
_RESTORED_ID = UUID("00000000-0000-0000-0000-000000000003")


def _history() -> TaskResultsResponse:
    return TaskResultsResponse(
        task_id="task-1",
        status=TaskStatus.FINISHED,
        results=[
            TaskResultEntry(
                id=_NEW_ID,
                created_at=datetime(2026, 9, 2, tzinfo=UTC),
                current=True,
                agent_caused_exit_reason=None,
                result={"score": 1.0},
            ),
            TaskResultEntry(
                id=_OLD_ID,
                created_at=datetime(2026, 9, 1, tzinfo=UTC),
                current=False,
                agent_caused_exit_reason="TIMEOUT",
                result={"score": 0.0},
            ),
        ],
    )


def _rollback(*, versioned: bool) -> RollbackTaskResponse:
    return RollbackTaskResponse(
        task_id="task-1",
        status=TaskStatus.FINISHED,
        restored_from_result_id=_OLD_ID,
        result_id=_RESTORED_ID,
        artifacts_versioned=versioned,
        restored_artifacts=["agent_output.tar.gz"] if versioned else [],
        removed_artifacts=[],
    )


def test_task_history_lists_attempts_and_marks_current(monkeypatch: pytest.MonkeyPatch) -> None:
    """Text output shows one row per kept attempt with the current one flagged; JSON returns the full payload.

    Test cases:
    - Text table lists both result IDs, newest first, with only the newest marked current.
    - JSON output round-trips result IDs.
    - SDK errors become a non-zero ClickException.
    """
    calls: list[tuple[UUID, str]] = []

    async def fake_history(run_id: UUID, task_id: str) -> TaskResultsResponse:
        calls.append((run_id, task_id))
        return _history()

    monkeypatch.setattr(tasks_module, "_task_history", fake_history)
    runner = CliRunner()

    result = runner.invoke(tasks_module.task_history, [str(_RUN_ID), "task-1"])

    assert result.exit_code == 0, result.output
    assert calls == [(_RUN_ID, "task-1")]
    lines = result.output.splitlines()
    new_line = next(line for line in lines if str(_NEW_ID) in line)
    old_line = next(line for line in lines if str(_OLD_ID) in line)
    assert lines.index(new_line) < lines.index(old_line)
    assert "yes" in new_line and "yes" not in old_line

    as_json = runner.invoke(tasks_module.task_history, [str(_RUN_ID), "task-1", "--format", "json"])
    assert as_json.exit_code == 0, as_json.output
    assert [entry["id"] for entry in json.loads(as_json.output)["results"]] == [str(_NEW_ID), str(_OLD_ID)]

    async def failing_history(run_id: UUID, task_id: str) -> TaskResultsResponse:
        raise ValkyrieSDKError("tracker said no")

    monkeypatch.setattr(tasks_module, "_task_history", failing_history)
    failed = runner.invoke(tasks_module.task_history, [str(_RUN_ID), "task-1"])
    assert failed.exit_code != 0
    assert "tracker said no" in failed.output


def test_rollback_task_passes_result_id_and_reports_next_step(monkeypatch: pytest.MonkeyPatch) -> None:
    """The command forwards the chosen result, prints the response, and tells the operator to resume.

    Test cases:
    - Without --result-id the SDK receives None; with it, the parsed UUID.
    - Text output includes the restored result and the resume hint; unversioned buckets add a warning.
    - JSON output is the raw response without hints.
    """
    calls: list[tuple[UUID, str, UUID | None]] = []
    versioned = True

    async def fake_rollback(run_id: UUID, task_id: str, result_id: UUID | None) -> RollbackTaskResponse:
        calls.append((run_id, task_id, result_id))
        return _rollback(versioned=versioned)

    monkeypatch.setattr(tasks_module, "_rollback_task", fake_rollback)
    runner = CliRunner()

    default = runner.invoke(tasks_module.rollback_task, [str(_RUN_ID), "task-1"])
    assert default.exit_code == 0, default.output
    assert calls == [(_RUN_ID, "task-1", None)]
    assert f"restored_from_result_id: {_OLD_ID}" in default.output
    assert f"valk run resume {_RUN_ID}" in default.output
    assert "not versioned" not in default.output

    explicit = runner.invoke(tasks_module.rollback_task, [str(_RUN_ID), "task-1", "--result-id", str(_OLD_ID)])
    assert explicit.exit_code == 0, explicit.output
    assert calls[-1] == (_RUN_ID, "task-1", _OLD_ID)

    versioned = False
    unversioned = runner.invoke(tasks_module.rollback_task, [str(_RUN_ID), "task-1"])
    assert unversioned.exit_code == 0, unversioned.output
    assert "not versioned" in unversioned.output

    as_json = runner.invoke(tasks_module.rollback_task, [str(_RUN_ID), "task-1", "--format", "json"])
    assert as_json.exit_code == 0, as_json.output
    assert json.loads(as_json.output)["result_id"] == str(_RESTORED_ID)

    async def failing_rollback(run_id: UUID, task_id: str, result_id: UUID | None) -> RollbackTaskResponse:
        raise ValkyrieSDKError("run is still in progress")

    monkeypatch.setattr(tasks_module, "_rollback_task", failing_rollback)
    failed = runner.invoke(tasks_module.rollback_task, [str(_RUN_ID), "task-1"])
    assert failed.exit_code != 0
    assert "run is still in progress" in failed.output
