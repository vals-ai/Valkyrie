"""Exceptions raised by the Valkyrie SDK."""

from typing import Any

from pydantic import ValidationError

_FIELD_ERROR_TEMPLATES = {
    "missing": "'{field}' is required but was not provided",
    "literal_error": "'{field}' has an invalid value. {msg}",
}


class ValkyrieSDKError(Exception):
    """Base class for all SDK errors."""


class ValkyrieConfigError(ValkyrieSDKError):
    """A saved Valkyrie login could not be read, or signing in or refreshing it failed."""


class ValkyrieRunError(ValkyrieSDKError):
    """A run operation received invalid input."""


class ValkyrieTransportError(ValkyrieSDKError):
    """A request could not reach the Valkyrie service."""


class ValkyrieAPIError(ValkyrieSDKError):
    """The Valkyrie API returned a non-success response."""

    def __init__(self, status_code: int, detail: Any):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Valkyrie API request failed ({status_code}): {detail}")


class ValkyrieStreamError(ValkyrieSDKError):
    """A run update stream returned an error or invalid event."""


class ValkyrieContractError(ValkyrieSDKError):
    """Raised when an agent contract, or the kwargs passed to it, fails validation."""

    @classmethod
    def from_validation_error(cls, error: ValidationError, context: str = "") -> "ValkyrieContractError":
        """Build one from a pydantic ValidationError: `context` first when given, then one line per invalid field.

        Fields are worded the way Tracker words them, so the CLI and the server report a bad contract alike.
        """
        lines: list[str] = []
        for detail in error.errors():
            field = ".".join(str(location) for location in detail["loc"])
            template = _FIELD_ERROR_TEMPLATES.get(detail["type"], "'{field}': {msg}")
            lines.append(f"  - {template.format(field=field, msg=detail['msg'])}")

        message = "\n".join(lines)
        return cls(f"{context}\n{message}" if context else message)
