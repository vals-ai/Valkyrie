"""Deprecated import path for harness-header helpers.

Import from tracker.aws.resolver instead.
"""

from tracker.aws.resolver import (
    HarnessHeaderInspection,
    fetch_harness_config,
    inspect_harness_headers,
    parse_log_retention_policy,
    try_fetch_harness_config,
)

_parse_log_retention_policy = parse_log_retention_policy

__all__ = [
    "HarnessHeaderInspection",
    "_parse_log_retention_policy",
    "fetch_harness_config",
    "inspect_harness_headers",
    "try_fetch_harness_config",
]
