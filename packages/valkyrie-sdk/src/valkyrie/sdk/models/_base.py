"""Shared SDK model behavior."""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict


class ResponseModel(BaseModel):
    """Forward-compatible model for data returned by the Valkyrie API."""

    model_config = ConfigDict(extra="ignore")


def serialize_utc(value: datetime | None) -> str | None:
    """Serialize datetimes with an explicit UTC offset."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
