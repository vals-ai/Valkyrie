"""Nested configuration models sent to the Valkyrie API."""

from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class AWSResources(BaseModel, frozen=True):
    region: str = Field(min_length=1)
    s3_bucket: str = Field(min_length=1)
    log_group: str = Field(min_length=1)
    log_retention_days: int = Field(gt=0)


class LocalResources(BaseModel, frozen=True):
    data_root: Path
    secrets_file: Path | None = None

    @field_validator("data_root", "secrets_file")
    @classmethod
    def validate_absolute_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Local resource paths must be absolute")
        return value
