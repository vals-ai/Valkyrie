"""Safe URL path segments for SDK resource routes."""

from urllib.parse import quote


def encode_path_segment(value: str, *, label: str = "name") -> str:
    """Quote one path segment without allowing values that URL normalization changes."""
    if value in {"", ".", ".."}:
        raise ValueError(f"Invalid {label}: {value!r}")
    return quote(value, safe="")
