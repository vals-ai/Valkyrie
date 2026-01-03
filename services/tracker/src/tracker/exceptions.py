"""Custom exceptions for the tracker service."""


class TrackerServiceError(Exception):
    """Base exception for all tracker service errors."""

    pass


class SandboxError(TrackerServiceError):
    """Exception raised for sandbox-related errors."""

    pass


class BenchmarkServiceError(TrackerServiceError):
    """Exception raised for benchmark service communication errors."""

    pass


class S3Error(TrackerServiceError):
    """Exception raised for S3 storage operation errors."""

    pass
