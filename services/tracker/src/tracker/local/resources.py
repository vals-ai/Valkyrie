"""Persisted resource locations for local execution."""

from pathlib import Path

from pydantic import BaseModel, field_validator


class LocalResources(BaseModel, frozen=True):
    data_root: Path
    secrets_file: Path | None = None

    @field_validator("data_root", "secrets_file")
    @classmethod
    def validate_absolute_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Local resource paths must be absolute")
        return value.resolve() if value is not None else None
