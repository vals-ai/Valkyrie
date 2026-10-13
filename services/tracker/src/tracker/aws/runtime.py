"""AWS resources and authentication selected for one tracker operation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from pydantic import Field

from tracker.aws.clients import AWSClientProvider


@dataclass(frozen=True)
class AWSResources:
    region: Annotated[str, Field(min_length=1)]
    s3_bucket: Annotated[str, Field(min_length=1)]
    log_group: str
    log_retention_days: Annotated[int, Field(gt=0)]


@dataclass(frozen=True)
class AWSRuntime:
    resources: AWSResources
    clients: AWSClientProvider
    expected_bucket_owner: str | None = None
