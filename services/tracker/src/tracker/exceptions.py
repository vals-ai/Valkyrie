"""Custom exceptions for the tracker service.

Base exceptions (TrackerServiceError, S3Error) are defined in tracker_shared
and re-exported here for backward compatibility. Tracker-specific exceptions
extend the shared base.
"""

from tracker_shared.exceptions import S3Error, TrackerServiceError

__all__ = [
    "TrackerServiceError",
    "S3Error",
    "SandboxError",
    "InvalidSandboxConfigurationError",
    "AgentRunFailedError",
    "SandboxSetupError",
    "PtyCreationError",
    "SSLConnectionError",
    "CloudWatchError",
    "LambdaError",
    "SecretsError",
]


class SandboxError(TrackerServiceError):
    """Exception raised for sandbox-related errors."""

    def __str__(self) -> str:
        return "Sandbox error: " + super().__str__()


class InvalidSandboxConfigurationError(SandboxError):
    """Exception raised for deterministic sandbox configuration errors."""


class AgentRunFailedError(SandboxError):
    """Exception raised when the agent process inside a healthy sandbox exits non-zero.

    Distinct from infra-caused SandboxErrors. Sandbox retries don't help these
    and they should be triaged separately from real infra failures.
    """


class SandboxSetupError(SandboxError):
    """Exception raised when sandbox setup fails after all retry attempts — triggers a new sandbox."""


class PtyCreationError(SandboxSetupError):
    """Exception raised when PTY session creation fails after all retry attempts."""


class SSLConnectionError(SandboxSetupError):
    """Exception raised when a sandbox command fails due to an SSL/TLS connection error (curl exit code 35)."""


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
