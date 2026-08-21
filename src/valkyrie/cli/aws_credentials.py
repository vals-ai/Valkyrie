"""Static AWS credential classification for CLI config consumers."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StaticAWSCredentials:
    """Normalized static credentials from the selected CLI config."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None


def resolve_static_aws_credentials(config: Mapping[str, Any]) -> StaticAWSCredentials | None:
    """Return normalized static credentials, or None when every credential value is blank."""

    def credential_value(key: str) -> str | None:
        value = config.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a string.")
        return value.strip() or None

    access_key_id = credential_value("AWS_ACCESS_KEY_ID")
    secret_access_key = credential_value("AWS_SECRET_ACCESS_KEY")
    session_token = credential_value("AWS_SESSION_TOKEN")
    if access_key_id is None and secret_access_key is None:
        if session_token is not None:
            raise ValueError("AWS_SESSION_TOKEN requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        return None
    if access_key_id is None or secret_access_key is None:
        raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together.")

    return StaticAWSCredentials(access_key_id, secret_access_key, session_token)
