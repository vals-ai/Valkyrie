"""Typed configuration for the Valkyrie SDK."""

import copy
import warnings
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator, model_validator

from valkyrie.sdk.errors import ValkyrieConfigError

DEFAULT_CONFIG_PATH = Path("~/.config/valkyrie/valkyrie.yaml")
TRACKER_URLS: dict[str, str] = {
    "bench": "https://benchmark-tracker.vals.ai",
    "prod": "https://benchmark-tracker-prod.vals.ai",
    "dev": "https://benchmark-tracker-dev.vals.ai",
}
# Top-level keys from the flat config layout and the nested path that replaced each one.
LEGACY_CONFIG_KEYS: dict[str, tuple[str, ...]] = {
    "AWS_DEFAULT_REGION": ("aws", "AWS_DEFAULT_REGION"),
    "S3_BUCKET": ("aws", "S3_BUCKET"),
    "LOG_GROUP": ("aws", "LOG_GROUP"),
    "LOG_RETENTION_POLICY": ("aws", "LOG_RETENTION_POLICY"),
}
# Flat keys the SDK model accepted before the nested layout; code callers may still pass them.
_FLAT_SDK_KEYS = frozenset(LEGACY_CONFIG_KEYS)
# Flat keys retired with client-supplied AWS credentials and provider secrets.
RETIRED_CONFIG_KEYS: frozenset[str] = frozenset(
    {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "DAYTONA_SECRET_NAME"}
)
ConfigT = TypeVar("ConfigT", bound="ValkyrieConfig")


def _nests_into_plain_dicts(config: dict[Any, Any], legacy_key: str) -> bool:
    target = config
    for parent in LEGACY_CONFIG_KEYS[legacy_key][:-1]:
        child = target.get(parent, {})
        if not isinstance(child, dict):
            return False
        target = cast(dict[Any, Any], child)
    return True


def migrate_legacy_config_keys(config: dict[str, Any], keys: Iterable[str] = LEGACY_CONFIG_KEYS) -> None:
    """Move flat config keys to their nested paths, keeping any value already set there."""
    for legacy_key in keys:
        if legacy_key not in config:
            continue
        *parents, key = LEGACY_CONFIG_KEYS[legacy_key]
        target = config
        for parent in parents:
            target = target.setdefault(parent, {})
        target.setdefault(key, config.pop(legacy_key))


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
    webhook: str | None = Field(default=None, repr=False)

    @model_validator(mode="before")
    @classmethod
    def accept_flat_aws_keys(cls, data: object) -> object:
        """Nest the flat AWS keys, by alias or field name, that SDK callers passed before the `aws` layout."""
        if not isinstance(data, dict):
            return data
        config = copy.deepcopy(cast(dict[Any, Any], data))
        field_name_keys = [
            key for key in config if isinstance(key, str) and key != key.upper() and key.upper() in _FLAT_SDK_KEYS
        ]
        for key in field_name_keys:
            config[key.upper()] = config.pop(key)
        flat_keys = _FLAT_SDK_KEYS.intersection(config)
        # Typed models in the nested path can't take the flat values, so validation reports them as extra keys.
        if not flat_keys or not all(_nests_into_plain_dicts(config, key) for key in flat_keys):
            return data
        warnings.warn("Flat AWS config keys are deprecated; nest them under `aws`.", DeprecationWarning, stacklevel=2)
        migrate_legacy_config_keys(config, _FLAT_SDK_KEYS)
        return config

    @model_validator(mode="before")
    @classmethod
    def reject_retired_config_keys(cls, data: object) -> object:
        """Reject credential and provider-secret settings retired by deployment-managed resolution."""
        if not isinstance(data, dict):
            return data
        retired = [
            key for key in data if isinstance(key, str) and key.upper() in RETIRED_CONFIG_KEYS
        ]
        aws = data.get("aws")
        if isinstance(aws, dict) and "credentials" in aws:
            retired.append("aws.credentials")
        for key in ("sandbox_providers", "default_sandbox_provider"):
            if key in data:
                retired.append(key)
        if retired:
            raise ValkyrieConfigError(
                f"Invalid Valkyrie config: {', '.join(retired)} are no longer supported. "
                "Runs resolve AWS resources and the sandbox provider from the Vals deployment; "
                "remove them or re-run `valkyrie config init`."
            )
        return data

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
        if retired_keys := [key for key in RETIRED_CONFIG_KEYS if key in raw_config]:
            raise ValkyrieConfigError(
                f"Invalid Valkyrie config at {config_path}: {', '.join(retired_keys)} are no longer supported. "
                "Remove them or re-run `valkyrie config init`."
            )
        if legacy_keys := [key for key in LEGACY_CONFIG_KEYS if key in raw_config]:
            migrations = ", ".join(f"{key} -> {'.'.join(LEGACY_CONFIG_KEYS[key])}" for key in legacy_keys)
            raise ValkyrieConfigError(
                f"Invalid Valkyrie config at {config_path}: legacy top-level keys {', '.join(legacy_keys)} "
                f"are no longer supported in YAML. Edit this file to move the existing values: {migrations}. "
                "Remove the old top-level entries and keep other settings unchanged. "
                "See https://docs.valkyrie.vals.ai/get-started/configuration#migrate-an-existing-configuration"
            )

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
