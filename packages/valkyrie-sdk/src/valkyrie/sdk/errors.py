"""Exceptions raised by the Valkyrie SDK."""

from typing import Any


class ValkyrieSDKError(Exception):
    """Base class for all SDK errors."""


class ValkyrieConfigError(ValkyrieSDKError):
    """The supplied Valkyrie configuration is missing or invalid."""


class ValkyrieRunError(ValkyrieSDKError):
    """A run operation received invalid client-side input."""


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
