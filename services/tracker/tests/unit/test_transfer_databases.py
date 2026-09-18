"""Paired transfer test databases use explicit local URLs or disposable CI databases."""

from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest

from tests.integration.local.database.test_run_transfer import pair


@pytest.mark.parametrize(
    "source,destination",
    [
        (None, "postgresql://localhost/tracker_transfer_test_one"),
        ("postgresql://localhost/tracker_transfer_test_one", None),
    ],
)
def test_partial_override_fails_before_docker(
    monkeypatch: pytest.MonkeyPatch, source: str | None, destination: str | None
) -> None:
    for side, value in (("SOURCE", source), ("DESTINATION", destination)):
        name = f"TRANSFER_TEST_{side}_DATABASE_URL"
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    class UnavailableDocker:
        def getfixturevalue(self, name: str) -> None:
            raise AssertionError("Docker must not be resolved for explicit overrides")

    iterator = cast(Callable[[Any], Iterator[Any]], getattr(pair, "__wrapped__"))(UnavailableDocker())
    with pytest.raises(ValueError, match="both"):
        next(iterator)
