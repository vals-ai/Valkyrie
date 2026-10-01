"""Typed configuration for the Valkyrie SDK."""

from pathlib import Path
from typing import Literal, TypeVar, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator

from valkyrie.sdk.errors import ValkyrieConfigError

DEFAULT_CONFIG_PATH = Path("~/.config/valkyrie/valkyrie.yaml")
TRACKER_URLS: dict[str, str] = {
    "bench": "https://benchmark-tracker.vals.ai",
    "prod": "https://benchmark-tracker-prod.vals.ai",
    "dev": "https://benchmark-tracker-dev.vals.ai",
}
ConfigT = TypeVar("ConfigT", bound="ValkyrieConfig")


class AWSConfig(BaseModel):
    """AWS resources used by local operations."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", hide_input_in_errors=True)

    aws_default_region: str = Field(alias="AWS_DEFAULT_REGION")
    s3_bucket: str = Field(alias="S3_BUCKET")
    log_group: str = Field(default="benchmarks", alias="LOG_GROUP")
    log_retention_policy: int = Field(default=365, alias="LOG_RETENTION_POLICY", gt=0)

    @field_validator(
        "aws_default_region",
        "s3_bucket",
        "log_group",
    )
    @classmethod
    def reject_blank_required_values(cls, value: str) -> str:
        """Reject blank required values."""
        if not value.strip():
            raise ValueError("must not be blank")
        return value


class ValkyrieConfig(BaseModel):
    """Validated SDK configuration for API-key-authenticated runs."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", hide_input_in_errors=True)

    environment: Literal["bench", "prod", "dev"] = "bench"
    tracker_url_override: str | None = Field(default=None, alias="tracker_url")
    api_key: SecretStr | None = Field(default=None, repr=False)
    aws: AWSConfig | None = None
    custom_benchmark_services: dict[str, str] = Field(default_factory=dict)
    benchmark_auth: dict[str, SecretStr] = Field(default_factory=dict, repr=False)

    @property
    def tracker_url(self) -> str:
        """Tracker base URL for the configured environment."""
        if self.tracker_url_override:
            return self.tracker_url_override.rstrip("/")
        if self.environment not in TRACKER_URLS:
            raise ValkyrieConfigError(
                f"Unsupported environment {self.environment!r}; expected one of: {', '.join(TRACKER_URLS)}"
            )
        return TRACKER_URLS[self.environment]

    @field_validator("custom_benchmark_services")
    @classmethod
    def normalize_service_urls(cls, services: dict[str, str]) -> dict[str, str]:
        """Remove trailing slashes from service URLs."""
        return {name: url.rstrip("/") for name, url in services.items()}

    @classmethod
    def from_yaml(cls: type[ConfigT], path: str | Path = DEFAULT_CONFIG_PATH) -> ConfigT:
        """Load and validate a CLI-compatible YAML config."""
        config_path = Path(path).expanduser()
        try:
            with config_path.open(encoding="utf-8") as config_file:
                raw_config = cast(object, yaml.safe_load(config_file))
        except OSError as exc:
            raise ValkyrieConfigError(f"Could not read Valkyrie config at {config_path}: {exc}") from exc
        except yaml.YAMLError as exc:
            raise ValkyrieConfigError(f"Invalid YAML in Valkyrie config at {config_path}") from exc

        if not isinstance(raw_config, dict):
            raise ValkyrieConfigError(f"Valkyrie config at {config_path} must contain a YAML mapping")
        try:
            return cls.model_validate(raw_config)
        except ValidationError as exc:
            raise ValkyrieConfigError(f"Invalid Valkyrie config at {config_path}: {exc}") from exc

    def request_headers(self) -> dict[str, str]:
        """Build the API-key header for tracker requests."""
        headers: dict[str, str] = {}
        if self.api_key and (api_key := self.api_key.get_secret_value()):
            headers["X-Api-Key"] = api_key
        return headers
