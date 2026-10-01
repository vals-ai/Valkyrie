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


class ValkyrieConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid", hide_input_in_errors=True)

    environment: Literal["bench", "prod", "dev"] = "bench"
    tracker_url_override: str | None = Field(default=None, alias="tracker_url")
    api_key: SecretStr | None = Field(default=None, repr=False)
    sandbox_providers: dict[str, str] = Field(default_factory=dict, repr=False)
    default_sandbox_provider: str | None = None
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
        headers: dict[str, str] = {}
        if self.api_key and (api_key := self.api_key.get_secret_value()):
            headers["X-Api-Key"] = api_key
        return headers
