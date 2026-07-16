from uuid import uuid4


def random_task_id() -> str:
    """Return a task ID that will not collide across live test runs."""
    return f"test-task-{uuid4().hex[:5]}"
