"""Stable wire models, independent of Tracker's ORM and configuration."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class DispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claimant_id: UUID


class ClaimRequest(DispatchRequest):
    benchmark_id: UUID
    executor_release_id: str = Field(min_length=1)
    executor_artifact_uri: str = Field(min_length=1)
    executor_artifact_digest: str = Field(pattern="^[0-9a-f]{64}$")
    executor_protocol_version: str = Field(min_length=1)


class FailRequest(DispatchRequest):
    error_message: str = Field(min_length=1, max_length=4096)


class LeaseResponse(BaseModel):
    dispatch_id: UUID
    claimant_id: UUID
    lease_expires_at: datetime


class AuthorityResponse(BaseModel):
    current: bool


class TerminalResponse(BaseModel):
    dispatch_id: UUID
    status: Literal["FINISHED", "FAILED"]
    finished_at: datetime
