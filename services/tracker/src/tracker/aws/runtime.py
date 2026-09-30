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

    def with_resources(self, resources: AWSResources | None) -> AWSRuntime:
        """Use persisted locations while retaining the resolved credential source."""
        if resources is None or resources == self.resources:
            return self
        return AWSRuntime(
            resources=resources,
            clients=self.clients.with_region(resources.region),
            expected_bucket_owner=self.expected_bucket_owner,
        )
