"""Shared exceptions used by the CLI and tracker service."""


class TrackerServiceError(Exception):
    """Base exception for all tracker service errors."""

    pass


class S3Error(TrackerServiceError):
    """Exception raised for S3 storage operation errors."""

    def __str__(self) -> str:
        return "S3 error: " + super().__str__()
