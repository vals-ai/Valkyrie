from uuid import uuid4


def random_task_id():
    """Prevent collisions across different machines running tests"""
    return f"test-task-{uuid4().hex[:5]}"
