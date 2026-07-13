"""Compatibility exports for legacy harness-header dependencies."""

from tracker.aws.resolver import (
    HarnessHeaderState,
    fetch_harness_config,
    inspect_harness_headers,
    parse_log_retention_policy,
    resolve_start_harness_config,
    try_fetch_harness_config,
)

_parse_log_retention_policy = parse_log_retention_policy

__all__ = [
    "HarnessHeaderState",
    "_parse_log_retention_policy",
    "fetch_harness_config",
    "inspect_harness_headers",
    "resolve_start_harness_config",
    "try_fetch_harness_config",
]
