"""Validation for caller-controlled benchmark-service destinations and headers.

This module intentionally implements Tracker's reject-only policy before HTTP
transport. Generic URL and header containers normalize or accept inputs forbidden
here, which would change persisted/API values or defer failures until request time.
"""

from __future__ import annotations

from collections.abc import Mapping
from ipaddress import ip_address
import re
from socket import inet_aton
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
    "x-benchmark-dataset-version",
    "x-real-ip",
}
_FORBIDDEN_HEADER_PREFIXES = ("forwarded", "proxy-", "sec-websocket-", "x-forwarded-")
_VALS_DOMAIN = "vals.ai"
_RESTRICTED_HOSTNAMES = {"internal", "local", "localhost"}
_RESTRICTED_HOSTNAME_SUFFIXES = (".localhost", ".local", ".internal")
_IDNA_SEPARATOR_TRANSLATION = str.maketrans({"。": ".", "．": ".", "｡": "."})
_CUSTOM_DESTINATION_DENIED = "Custom benchmark destination is not allowed"
_INVALID_URL_CHARACTERS_RE = re.compile(r"[\s\x00-\x1f\x7f\\]")
_INVALID_SERVICE_URL = "Invalid benchmark service URL"


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


def validate_service_header_name(name: str) -> str:
    """Accept a header name that validate_service_headers allows."""
    validate_service_headers({name: ""})
    return name


def validate_custom_service_destination(
    value: str,
    *,
    org_name: str,
    auth_required: bool,
    restrict_vals_hosts: bool = True,
) -> None:
    """Keep private and optionally Vals-owned custom destinations limited to trusted callers."""
    from tracker.secret_access import may_use_shared_secrets

    if not auth_required or may_use_shared_secrets(org_name):
        return

    hostname = urlsplit(value).hostname
    assert hostname is not None
    normalized_host = hostname.translate(_IDNA_SEPARATOR_TRANSLATION).lower().rstrip(".")
    if (
        (restrict_vals_hosts and (normalized_host == _VALS_DOMAIN or normalized_host.endswith(f".{_VALS_DOMAIN}")))
        or normalized_host in _RESTRICTED_HOSTNAMES
        or normalized_host.endswith(_RESTRICTED_HOSTNAME_SUFFIXES)
    ):
        raise ValueError(_CUSTOM_DESTINATION_DENIED)

    try:
        parsed_ip = ip_address(normalized_host)
    except ValueError:
        try:
            parsed_ip = ip_address(inet_aton(normalized_host))
        except OSError:
            return
    if not parsed_ip.is_global or parsed_ip.is_multicast:
        raise ValueError(_CUSTOM_DESTINATION_DENIED)


def validate_service_url_syntax(value: str) -> str:
    """Validate and normalize a caller-provided benchmark-service base URL."""
    if _INVALID_URL_CHARACTERS_RE.search(value):
        raise ValueError(_INVALID_SERVICE_URL)

    parsed = urlsplit(value)
    try:
        parsed.port
    except ValueError as exc:
        raise ValueError(_INVALID_SERVICE_URL) from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(_INVALID_SERVICE_URL)

    return value.rstrip("/")
