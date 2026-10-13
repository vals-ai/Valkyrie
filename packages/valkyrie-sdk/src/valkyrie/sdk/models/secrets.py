"""Organization secrets stored through Tracker."""

from valkyrie.sdk.models._base import ResponseModel


class SecretEntry(ResponseModel):
    """One stored secret: its short name, and the reference runs use to read it."""

    name: str
    reference: str


class SecretsResponse(ResponseModel):
    """Secrets stored for the configured tenant, without their values."""

    secrets: list[SecretEntry]
