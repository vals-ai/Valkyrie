from collections.abc import AsyncIterator, Iterable
from typing import Any, TypeVar
from uuid import UUID, uuid4

from descope.exceptions import AuthException

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


class FakeAccessKeys:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.missing: set[str] = set()
        self.failing: set[str] = set()
        self.fail_create = False

    def create(self, name: str, expire_time: int = 0, **kwargs: Any) -> dict[str, Any]:
        if self.fail_create:
            raise RuntimeError("Descope is unavailable")
        key_id = f"K{len(self.created) + 1}"
        self.created.append({"id": key_id, "name": name, "expire_time": expire_time, **kwargs})
        return {"key": {"id": key_id}, "cleartext": f"run-secret-{key_id}"}

    def delete(self, id: str) -> None:
        if id in self.failing:
            raise RuntimeError("Descope is unavailable")
        if id in self.missing:
            raise AuthException(404, "E", "not found")
        self.deleted.append(id)
