"""Strict stored archive reference, with no database or lifecycle imports."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Positive = Annotated[int, Field(gt=0, strict=True)]
Nonempty = Annotated[str, Field(min_length=1, strict=True)]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ArchiveObject(ContractModel):
    key: Nonempty
    version_id: Nonempty
    sha256: Digest
    size_bytes: Positive

    @model_validator(mode="after")
    def immutable_version(self) -> "ArchiveObject":
        if self.version_id == "null":
            raise ValueError("immutable version required")

        return self


class LogHistoryReference(ContractModel):
    format_version: Literal[1] = 1
    run_id: UUID
    operation_id: UUID
    parent_plan_sha256: Digest
    manifest: ArchiveObject

    @property
    def prefix(self) -> str:
        return f"benchmarks/{self.run_id}/log-history/{self.operation_id}/v1/"

    @model_validator(mode="after")
    def manifest_key(self) -> "LogHistoryReference":
        if self.manifest.key != f"{self.prefix}manifest.json":
            raise ValueError("manifest key does not match run and operation")

        return self
