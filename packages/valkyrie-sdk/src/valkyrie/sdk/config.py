"""Typed configuration for the Valkyrie SDK."""

import os
from typing import Self

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from valkyrie.sdk.login import ApiKeyAuth, LoginSession, SessionAuth

DEFAULT_TRACKER_URL = "https://benchmark-tracker.vals.ai"


class ValkyrieConfig(BaseModel):
    """Which Tracker to call and how to authenticate; unset values come from the environment and `valkyrie login`."""

    model_config = ConfigDict(hide_input_in_errors=True)

    url: str = ""
    api_key: SecretStr | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def _fill_from_environment(self) -> Self:
        if self.api_key is None and (key := os.environ.get("VALKYRIE_API_KEY", "").strip()):
            self.api_key = SecretStr(key)
        if not self.url:
            self.url = os.environ.get("VALKYRIE_URL", "")
        if not self.url and not self.api_key:
            self.url = LoginSession.remembered_tracker() or ""
        self.url = (self.url or DEFAULT_TRACKER_URL).rstrip("/")
        return self

    def http_auth(self) -> httpx.Auth | None:
        if self.api_key:
            return ApiKeyAuth(self.api_key.get_secret_value())
        session = LoginSession.load(self.url)
        return SessionAuth(session) if session is not None else None
