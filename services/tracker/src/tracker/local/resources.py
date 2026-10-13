"""Persisted resource locations for local execution."""

from pathlib import Path

from pydantic import BaseModel, field_validator, model_validator


class LocalResources(BaseModel, frozen=True):
    data_root: Path
    secrets_file: Path | None = None
    secrets_manager_region: str | None = None

    @field_validator("data_root", "secrets_file")
    @classmethod
    def validate_absolute_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Local resource paths must be absolute")
        return value.resolve() if value is not None else None

    @model_validator(mode="after")
    def validate_one_secrets_source(self) -> "LocalResources":
        if self.secrets_file is not None and self.secrets_manager_region is not None:
            raise ValueError("Choose secrets_file or secrets_manager_region, not both")
        return self


def reads_secrets_file(properties: object) -> bool:
    """Return whether a run reads secret values from a local file instead of named secrets in a store."""
    return isinstance(properties, LocalResources) and properties.secrets_manager_region is None
