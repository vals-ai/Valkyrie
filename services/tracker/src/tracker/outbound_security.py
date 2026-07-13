"""Validation for caller-controlled benchmark-service destinations and headers.

This module intentionally implements Tracker's reject-only policy before HTTP
transport. Generic URL and header containers normalize or accept inputs forbidden
here, which would change persisted/API values or defer failures until request time.
"""

from __future__ import annotations

from collections.abc import Mapping
import re
from urllib.parse import urlsplit

_BENCHMARK_NAME_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_HEADER_NAME_RE = re.compile(r"[-!#$%&'*+.^_`|~0-9A-Za-z]+")
_HEADER_VALUE_RE = re.compile(r"(?:[\x21-\x7e]+(?:[ \t]+[\x21-\x7e]+)*)?")
_FORBIDDEN_HEADER_NAMES = {
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "x-api-key",
    "x-real-ip",
}
_FORBIDDEN_HEADER_PREFIXES = ("forwarded", "proxy-", "sec-websocket-", "x-forwarded-")


def validate_benchmark_name(value: str) -> str:
    """Require the DNS-label grammar used to derive hosted benchmark-service URLs."""
    if not _BENCHMARK_NAME_RE.fullmatch(value):
        raise ValueError("Invalid benchmark name")
    return value


def validate_service_headers(headers: Mapping[str, str]) -> None:
    """Reject malformed headers and names that alter HTTP routing or protocol handling."""
    for name, value in headers.items():
        normalized = name.lower()
        if (
            not _HEADER_NAME_RE.fullmatch(name)
            or not _HEADER_VALUE_RE.fullmatch(value)
            or normalized in _FORBIDDEN_HEADER_NAMES
            or normalized.startswith(_FORBIDDEN_HEADER_PREFIXES)
        ):
            raise ValueError("Unsupported benchmark service header")


def validate_service_url_syntax(value: str) -> str:
    """Validate and normalize a caller-provided benchmark-service base URL."""
    if (
        any(character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        or "\\" in value
    ):
        raise ValueError("Invalid benchmark service URL")

    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Invalid benchmark service URL")
    if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        raise ValueError("Invalid benchmark service URL")

    try:
        parsed.port
    except ValueError as exc:
        raise ValueError("Invalid benchmark service URL") from exc

    return value.rstrip("/")
