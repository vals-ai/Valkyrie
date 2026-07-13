"""Typed configuration for the Valkyrie SDK."""

from pathlib import Path
from typing import TypeVar, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator
from tracker.types import AWSCredentials, HarnessConfig

from valkyrie.sdk.errors import ValkyrieConfigError

DEFAULT_CONFIG_PATH = Path("~/.config/valkyrie/valkyrie.yaml")
ConfigT = TypeVar("ConfigT", bound="ValkyrieConfig")


class ValkyrieConfig(BaseModel):
    """Validated SDK configuration using ``valkyrie.yaml`` field aliases."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    api_key: SecretStr | None = Field(default=None, repr=False)
    aws_access_key_id: SecretStr = Field(alias="AWS_ACCESS_KEY_ID", repr=False)
    aws_secret_access_key: SecretStr = Field(alias="AWS_SECRET_ACCESS_KEY", repr=False)
    aws_default_region: str = Field(alias="AWS_DEFAULT_REGION")
    aws_session_token: SecretStr | None = Field(default=None, alias="AWS_SESSION_TOKEN", repr=False)
    s3_bucket: str = Field(alias="S3_BUCKET")
    log_group: str = Field(default="benchmarks", alias="LOG_GROUP")
    log_retention_policy: int = Field(default=365, alias="LOG_RETENTION_POLICY", gt=0)
    sandbox_providers: dict[str, str] = Field(min_length=1, repr=False)
    default_sandbox_provider: str | None = None
    custom_benchmark_services: dict[str, str] = Field(default_factory=dict)
    benchmark_auth: dict[str, SecretStr] = Field(default_factory=dict, repr=False)
    webhook: str | None = Field(default=None, repr=False)

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

    @field_validator("aws_access_key_id", "aws_secret_access_key")
    @classmethod
    def reject_blank_required_secrets(cls, value: SecretStr) -> SecretStr:
        """Reject blank required secret values."""
        if not value.get_secret_value().strip():
            raise ValueError("must not be blank")
        return value

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

        try:
            return cls.model_validate(cast(dict[str, object], raw_config))
        except ValidationError as exc:
            raise ValkyrieConfigError(f"Invalid Valkyrie config at {config_path}: {exc}") from exc

    def resolve_sandbox_provider(self, provider: str | None = None) -> tuple[str, str]:
        """Resolve the selected sandbox provider and secret name."""
        provider_name = provider or self.default_sandbox_provider or next(iter(self.sandbox_providers))
        secret_name = self.sandbox_providers.get(provider_name)
        if secret_name is None:
            configured = ", ".join(self.sandbox_providers)
            raise ValkyrieConfigError(f"Unknown sandbox provider '{provider_name}'. Configured providers: {configured}")
        return provider_name, secret_name

    def harness_config(self, provider_secret_name: str) -> HarnessConfig:
        """Build the nested harness config expected by the tracker."""
        return HarnessConfig(
            aws=AWSCredentials(
                aws_access_key_id=self.aws_access_key_id.get_secret_value(),
                aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
                aws_default_region=self.aws_default_region,
                aws_session_token=(self.aws_session_token.get_secret_value() if self.aws_session_token else None),
            ),
            s3_bucket=self.s3_bucket,
            log_group=self.log_group,
            log_retention_policy=self.log_retention_policy,
            sandbox_provider_secret_name=provider_secret_name,
        )

    def request_headers(self) -> dict[str, str]:
        """Build API-key and harness headers for tracker requests."""
        values: dict[str, str | None] = {
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id.get_secret_value(),
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key.get_secret_value(),
            "AWS_DEFAULT_REGION": self.aws_default_region,
            "AWS_SESSION_TOKEN": self.aws_session_token.get_secret_value() if self.aws_session_token else None,
            "S3_BUCKET": self.s3_bucket,
            "LOG_GROUP": self.log_group,
            "LOG_RETENTION_POLICY": str(self.log_retention_policy),
        }
        headers = {
            f"X-Harness-{key.replace('_', '-').title()}": value for key, value in values.items() if value is not None
        }
        if self.api_key and (api_key := self.api_key.get_secret_value()):
            headers["X-Api-Key"] = api_key
        return headers
