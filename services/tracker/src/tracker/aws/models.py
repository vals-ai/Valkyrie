"""Typed AWS resource bindings stored with benchmark runs."""

from pydantic import BaseModel, ConfigDict, field_validator


class RunAWSResources(BaseModel):
    """AWS resource namespace and locations permanently bound to a run."""

    model_config = ConfigDict(frozen=True)

    account_id: str
    region: str
    s3_bucket: str
    log_group: str
    log_retention_days: int

    @field_validator("account_id")
    @classmethod
    def validate_account_id(cls, value: str) -> str:
        if len(value) != 12 or not value.isdigit():
            raise ValueError("AWS account ID must contain exactly 12 digits")
        return value

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

    def mismatched_locations(self, candidate: "RunAWSResources") -> tuple[str, ...]:
        """Return resource locations that would redirect work for this run."""
        fields = ("account_id", "region", "s3_bucket", "log_group")
        return tuple(field for field in fields if getattr(self, field) != getattr(candidate, field))
