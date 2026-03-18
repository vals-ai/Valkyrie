"""Custom exceptions for the tracker service."""


class TrackerServiceError(Exception):
    """Base exception for all tracker service errors."""

    pass


class SandboxError(TrackerServiceError):
    """Exception raised for sandbox-related errors."""

    def __str__(self) -> str:
        return "Sandbox error: " + super().__str__()


class InvalidSandboxConfigurationError(SandboxError):
    """Exception raised for deterministic sandbox configuration errors."""


class S3Error(TrackerServiceError):
    """Exception raised for S3 storage operation errors."""

    def __str__(self) -> str:
        return "S3 error: " + super().__str__()


class CloudWatchError(TrackerServiceError):
    """Exception raised for CloudWatch operation errors."""

    def __str__(self) -> str:
        return "CloudWatch error: " + super().__str__()


class LambdaError(TrackerServiceError):
    """Exception raised for Lambda operation errors."""

    def __str__(self) -> str:
        return "Lambda error: " + super().__str__()


class SecretsError(TrackerServiceError):
    """Exception raised for Secret operation errors."""

    def __str__(self) -> str:
        return "Secret error: " + super().__str__()
