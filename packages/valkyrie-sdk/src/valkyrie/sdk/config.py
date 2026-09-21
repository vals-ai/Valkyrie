"""Typed configuration for the Valkyrie SDK."""

from pathlib import Path
from typing import Literal, TypeVar, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator, model_validator

from valkyrie.sdk.errors import ValkyrieConfigError
from valkyrie.sdk.models import AWSCredentials, HarnessConfig

DEFAULT_CONFIG_PATH = Path("~/.config/valkyrie/valkyrie.yaml")
TRACKER_URLS: dict[str, str] = {
    "bench": "https://benchmark-tracker.vals.ai",
    "prod": "https://benchmark-tracker-prod.vals.ai",
    "dev": "https://benchmark-tracker-dev.vals.ai",
}
# Top-level keys from the flat config layout and the nested path that replaced each one.
LEGACY_CONFIG_KEYS: dict[str, tuple[str, ...]] = {
    "AWS_ACCESS_KEY_ID": ("aws", "credentials", "AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": ("aws", "credentials", "AWS_SECRET_ACCESS_KEY"),
    "AWS_SESSION_TOKEN": ("aws", "credentials", "AWS_SESSION_TOKEN"),
    "AWS_DEFAULT_REGION": ("aws", "AWS_DEFAULT_REGION"),
    "S3_BUCKET": ("aws", "S3_BUCKET"),
    "LOG_GROUP": ("aws", "LOG_GROUP"),
    "LOG_RETENTION_POLICY": ("aws", "LOG_RETENTION_POLICY"),
    "DAYTONA_SECRET_NAME": ("sandbox_providers", "daytona"),
}
ConfigT = TypeVar("ConfigT", bound="ValkyrieConfig")


class AWSAccessKeys(BaseModel):
    """Static AWS credentials."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", hide_input_in_errors=True)

    aws_access_key_id: SecretStr = Field(alias="AWS_ACCESS_KEY_ID", repr=False)
    aws_secret_access_key: SecretStr = Field(alias="AWS_SECRET_ACCESS_KEY", repr=False)
    aws_session_token: SecretStr | None = Field(default=None, alias="AWS_SESSION_TOKEN", repr=False)

    @field_validator("aws_access_key_id", "aws_secret_access_key")
    @classmethod
    def reject_blank_required_secrets(cls, value: SecretStr) -> SecretStr:
        """Reject blank required secret values."""
        if not value.get_secret_value().strip():
            raise ValueError("must not be blank")
        return value


class AWSConfig(BaseModel):
    """AWS resources with optional static credentials."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", hide_input_in_errors=True)

    credentials: AWSAccessKeys | None = None
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

    def harness_config(self, provider_secret_name: str | None) -> HarnessConfig | None:
        """Build the nested harness config expected by the tracker."""
        if self.credentials is None:
            return None
        if provider_secret_name is None:
            raise ValkyrieConfigError("AWS execution requires a sandbox provider secret")
        return HarnessConfig(
            aws=AWSCredentials(
                aws_access_key_id=self.credentials.aws_access_key_id.get_secret_value(),
                aws_secret_access_key=self.credentials.aws_secret_access_key.get_secret_value(),
                aws_default_region=self.aws_default_region,
                aws_session_token=(
                    self.credentials.aws_session_token.get_secret_value()
                    if self.credentials.aws_session_token
                    else None
                ),
            ),
            s3_bucket=self.s3_bucket,
            log_group=self.log_group,
            log_retention_policy=self.log_retention_policy,
            sandbox_provider_secret_name=provider_secret_name,
        )


class ValkyrieConfig(BaseModel):
    """Validated SDK configuration with optional caller-supplied AWS access."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", hide_input_in_errors=True)

    environment: Literal["bench", "prod", "dev"] = "bench"
    tracker_url_override: str | None = Field(default=None, alias="tracker_url")
    api_key: SecretStr | None = Field(default=None, repr=False)
    aws: AWSConfig | None = None
    sandbox_providers: dict[str, str] = Field(default_factory=dict, repr=False)
    default_sandbox_provider: str | None = None
    custom_benchmark_services: dict[str, str] = Field(default_factory=dict)
    benchmark_auth: dict[str, SecretStr] = Field(default_factory=dict, repr=False)
    webhook: str | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def validate_access_key_configuration(self) -> "ValkyrieConfig":
        if self.aws is not None and self.aws.credentials is not None and not self.sandbox_providers:
            raise ValueError("sandbox_providers are required with AWS configuration")
        return self

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
            raise ValkyrieConfigError(f"Invalid YAML in Valkyrie config at {config_path}: {exc}") from exc

        if not isinstance(raw_config, dict):
            raise ValkyrieConfigError(f"Valkyrie config at {config_path} must contain a YAML mapping")
        if legacy_keys := [key for key in LEGACY_CONFIG_KEYS if key in raw_config]:
            raise ValkyrieConfigError(
                f"Invalid Valkyrie config at {config_path}: legacy top-level keys {', '.join(legacy_keys)} "
                "are no longer supported. Run `valkyrie config init` to migrate them."
            )

        try:
            return cls.model_validate(raw_config)
        except ValidationError as exc:
            raise ValkyrieConfigError(f"Invalid Valkyrie config at {config_path}: {exc}") from exc

    def resolve_sandbox_provider(self, provider: str | None = None) -> tuple[str | None, str | None]:
        """Resolve the selected sandbox provider and secret name."""
        if not self.sandbox_providers:
            return provider or self.default_sandbox_provider, None
        provider_name = provider or self.default_sandbox_provider or next(iter(self.sandbox_providers))
        secret_name = self.sandbox_providers.get(provider_name)
        if secret_name is None:
            configured = ", ".join(self.sandbox_providers)
            raise ValkyrieConfigError(f"Unknown sandbox provider '{provider_name}'. Configured providers: {configured}")
        return provider_name, secret_name

    def request_headers(self) -> dict[str, str]:
        """Build API-key and harness headers for tracker requests."""
        headers: dict[str, str] = {}
        if self.aws is not None and self.aws.credentials is not None:
            values: dict[str, str | None] = {
                "AWS_ACCESS_KEY_ID": self.aws.credentials.aws_access_key_id.get_secret_value(),
                "AWS_SECRET_ACCESS_KEY": self.aws.credentials.aws_secret_access_key.get_secret_value(),
                "AWS_DEFAULT_REGION": self.aws.aws_default_region,
                "AWS_SESSION_TOKEN": self.aws.credentials.aws_session_token.get_secret_value()
                if self.aws.credentials.aws_session_token
                else None,
                "S3_BUCKET": self.aws.s3_bucket,
                "LOG_GROUP": self.aws.log_group,
                "LOG_RETENTION_POLICY": str(self.aws.log_retention_policy),
            }
            headers.update(
                {
                    f"X-Harness-{key.replace('_', '-').title()}": value
                    for key, value in values.items()
                    if value is not None
                }
            )
        if self.api_key and (api_key := self.api_key.get_secret_value()):
            headers["X-Api-Key"] = api_key
        return headers
