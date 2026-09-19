"""Transfer policy settings. Deployed values come only from the environment.

The settings object is frozen. A test lowers the interval by replacing the module
attribute with `dataclasses.replace`, never by mutating the deployed one.
"""

import os
from dataclasses import dataclass
from datetime import timedelta

LOG_QUIET_INTERVAL_FLOOR_HOURS = 24


@dataclass(frozen=True)
class TransferSettings:
    log_quiet_interval_hours: int = LOG_QUIET_INTERVAL_FLOOR_HOURS

    @property
    def log_quiet_interval(self) -> timedelta:
        return timedelta(hours=self.log_quiet_interval_hours)


def load_settings() -> TransferSettings:
    hours = int(os.environ.get("TRANSFER_LOG_QUIET_INTERVAL_HOURS", LOG_QUIET_INTERVAL_FLOOR_HOURS))
    if hours < LOG_QUIET_INTERVAL_FLOOR_HOURS:
        raise ValueError("TRANSFER_LOG_QUIET_INTERVAL_HOURS must be at least 24")

    return TransferSettings(log_quiet_interval_hours=hours)


SETTINGS = load_settings()
