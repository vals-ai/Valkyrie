"""Nested configuration models sent to the Valkyrie API."""

from pydantic import BaseModel, Field


class AWSCredentials(BaseModel, frozen=True):
    """AWS credentials forwarded in the run harness configuration."""

    aws_access_key_id: str
    aws_secret_access_key: str = Field(repr=False)
    aws_default_region: str
    aws_session_token: str | None = Field(default=None, repr=False)


class HarnessConfig(BaseModel):
    """Harness configuration required to start a run."""

    aws: AWSCredentials
    s3_bucket: str
    log_group: str
    log_retention_policy: int
    sandbox_provider_secret_name: str
