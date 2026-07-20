from collections.abc import AsyncIterator, Iterable
from typing import TypeVar
from uuid import UUID, uuid4

_Item = TypeVar("_Item")


# Match the default organization seeded by the database fixture.
TEST_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")


def random_task_id() -> str:
    """Return a task ID that will not collide across live test runs."""
    return f"test-task-{uuid4().hex[:5]}"


async def async_iterator(items: Iterable[_Item]) -> AsyncIterator[_Item]:
    """Yield test values through an async iterator."""
    for item in items:
        yield item
