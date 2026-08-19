"""Typed AWS resource bindings stored with benchmark runs."""

from pydantic import BaseModel, ConfigDict, field_validator


class RunAWSResources(BaseModel):
    """AWS destinations permanently bound to a run."""

    model_config = ConfigDict(frozen=True)

    region: str
    s3_bucket: str
    log_group: str
    log_retention_days: int

    @field_validator("region", "s3_bucket")
    @classmethod
    def validate_resource_name(cls, value: str) -> str:
        if not value:
            raise ValueError("AWS run resource names cannot be empty")
        return value

    @field_validator("log_retention_days")
    @classmethod
    def validate_log_retention_days(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("AWS log retention must be positive")
        return value
