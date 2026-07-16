"""Unit tests for ECS task-protection reference counting.

Run: uv run pytest tests/unit/middleware/test_task_protection.py
"""

from typing import Any, cast
from unittest.mock import Mock

import pytest
from taskiq import TaskiqMessage, TaskiqResult

import tracker.middleware.task_protection as task_protection_module
from tracker.middleware.task_protection import TaskProtectionMiddleware


async def test_task_protection_spans_all_concurrent_benchmarks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deployment protection must remain enabled until every active benchmark exits.

    Test cases:
    - The first task enables protection and a second task only increments the reference count.
    - A successful completion keeps protection while another task is active.
    - An error on the final task releases protection.
    """
    protection_changes: list[bool] = []

    async def record_protection(*, enabled: bool) -> None:
        protection_changes.append(enabled)

    monkeypatch.setattr(task_protection_module, "_set_task_protection", record_protection)
    middleware = TaskProtectionMiddleware()
    first_message = cast(TaskiqMessage, Mock())
    second_message = cast(TaskiqMessage, Mock())
    result = cast(TaskiqResult[Any], Mock())

    assert await middleware.pre_execute(first_message) is first_message
    assert await middleware.pre_execute(second_message) is second_message
    assert protection_changes == [True]

    await middleware.post_execute(first_message, result)
    assert protection_changes == [True]

    await middleware.on_error(second_message, result, RuntimeError("worker failed"))
    assert protection_changes == [True, False]
