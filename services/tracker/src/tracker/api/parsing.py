"""Shared parsing helpers for API query params."""

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def parse_csv(raw: str, convert: Callable[[str], T]) -> list[T]:
    """Parse a comma-separated query param into a list.

    Orval emits array query params as comma-joined strings. Blank tokens and
    tokens that ``convert`` rejects (``ValueError``) are skipped.
    """
    parsed: list[T] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            parsed.append(convert(token))
        except ValueError:
            continue
    return parsed
