"""Custom exceptions for the tracker service."""

from pydantic import ValidationError


class TrackerServiceError(Exception):
    """Base exception for all tracker service errors."""

    pass


class ExecutionAuthorityRevoked(TrackerServiceError):
    """The executor dispatch no longer owns benchmark execution."""


class BundlerError(Exception):
    """Exception raised for agent contract bundler errors."""

    pass


class ContractValidationError(ValueError):
    """Formats a Pydantic ValidationError into user-friendly messages."""

    FORMATTERS: dict[str, str] = {
        "missing": "'{field}' is required but was not provided",
        "literal_error": "'{field}' has an invalid value. {msg}",
    }

    def __init__(self, validation_error: ValidationError, context: str = "") -> None:
        messages: list[str] = []
        for err in validation_error.errors():
            field = ".".join(str(loc) for loc in err["loc"])
            template = self.FORMATTERS.get(err["type"])
            if template:
                messages.append(f"  - {template.format(field=field, msg=err['msg'])}")
            else:
                messages.append(f"  - '{field}': {err['msg']}")

        detail = "\n".join(messages)
        super().__init__(f"{context}\n{detail}" if context else detail)


class SandboxError(TrackerServiceError):
    """Exception raised for sandbox-related errors."""

    def __str__(self) -> str:
        return "Sandbox error: " + super().__str__()


class InvalidSandboxConfigurationError(SandboxError):
    """Exception raised for deterministic sandbox configuration errors."""


class OutputArtifactError(TrackerServiceError):
    """Exception raised when a declared output artifact is missing or invalid."""

    def __str__(self) -> str:
        return "Output artifact error: " + super().__str__()


class AgentRunFailedError(SandboxError):
    """Exception raised when the agent process inside a healthy sandbox exits non-zero.

    Distinct from infra-caused SandboxErrors. Sandbox retries don't help these
    and they should be triaged separately from real infra failures.
    """


class SandboxSetupError(SandboxError):
    """Exception raised when sandbox setup fails after all retry attempts — triggers a new sandbox."""


class DependencySetupExhaustedError(SandboxSetupError):
    """Dependency setup exhausted in-place retries and needs one clean sandbox."""


class SSLConnectionError(SandboxSetupError):
    """Exception raised when a sandbox command fails due to an SSL/TLS connection error (curl exit code 35)."""


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
