"""Shim — exceptions now live in tracker.exceptions."""

from tracker.exceptions import BundlerError, ContractValidationError, TrackerServiceError


class TrackerNotFoundError(TrackerServiceError):
    """Tracker service is unreachable or unhealthy."""


__all__ = ["BundlerError", "ContractValidationError", "TrackerNotFoundError", "TrackerServiceError"]
