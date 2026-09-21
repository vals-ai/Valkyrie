"""Strict provider operations, restricted to saved resources and exact run scope."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from benchmark_service import SandboxNotFoundError, SandboxQuery
from botocore.exceptions import ClientError
from pydantic import AwareDatetime, Field, model_validator

from tracker.aws.clients import AWSClientProvider
from tracker.aws.managed_storage import (
    ManagedStoragePolicy,
    validate_managed_storage_bucket,
    validate_managed_storage_bucket_versioning,
)
from tracker.aws.runtime import AWSRuntime
from tracker.aws.secrets import SecretsManagerStore
from tracker.lifecycle import ContractModel, Digest, LifecycleConflict, OperationIdentity, Verification, unverified
from tracker.run_purge.contracts import PurgeRun
from tracker.utils.resources import fetch_sandbox_provider_config


class OwnerDeletionFence(ContractModel):
    Sid: str
    Effect: Literal["Deny"]
    Principal: Literal["*"]
    Action: tuple[Literal["s3:PutObject"], Literal["s3:DeleteObject"]]
    Resource: str


class WriteProbe(ContractModel):
    key: str
    outcome: Literal["AccessDenied"]


class FenceReceipt(ContractModel):
    identity: OperationIdentity
    bucket: str
    policy_sha256: Digest
    observed_at: AwareDatetime
    write_probe: WriteProbe

    @model_validator(mode="after")
    def validate_probe(self) -> "FenceReceipt":
        if self.write_probe.key != f".valsmith-owner-deletion/{self.identity.operation_id}/write-probe":
            raise ValueError("Write probe does not match exact operation key")
        if self.observed_at.utcoffset() != timedelta(0):
            raise ValueError("Fence receipt time must use UTC")
        return self


FenceReceipts = Annotated[tuple[FenceReceipt, ...], Field(min_length=1, json_schema_extra={"uniqueItems": True})]


def policy_digest(policy: Any) -> str:
    return hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class AWSProviderBoundary:
    def __init__(self, clients: AWSClientProvider, *, fence_receipts: tuple[FenceReceipt, ...] = ()) -> None:
        self.clients = clients
        self.fence_receipts = fence_receipts

    async def validate(self, identity: OperationIdentity, run: PurgeRun) -> None:
        resources = run.scope.original_resources
        clients = self.clients.with_region(resources.region)
        account = await asyncio.to_thread(lambda: clients.sts_client().get_caller_identity()["Account"])
        if account != identity.source_aws_account_id:
            raise LifecycleConflict("AWS caller account does not match source account")
        runtime = AWSRuntime(resources, clients, identity.source_aws_account_id)
        await validate_managed_storage_bucket(
            runtime,
            org_id=identity.org_id,
            bucket_name=resources.s3_bucket,
            policy=ManagedStoragePolicy(
                identity.source_aws_account_id, {identity.org_id: frozenset({identity.environment})}
            ),
        )
        await validate_managed_storage_bucket_versioning(runtime, bucket_name=resources.s3_bucket)
        async with clients.s3_client() as client:
            tags = await client.get_bucket_tagging(
                Bucket=resources.s3_bucket, ExpectedBucketOwner=identity.source_aws_account_id
            )
        owners = [tag["Value"] for tag in tags.get("TagSet", []) if tag.get("Key") == "valsmith:owner-account-id"]
        if owners != [str(identity.github_owner_id)]:
            raise LifecycleConflict("Bucket owner tag does not match exact parent owner")

    async def verify_fence(self, identity: OperationIdentity, run: PurgeRun) -> str:
        bucket = run.scope.original_resources.s3_bucket
        matches = [
            receipt for receipt in self.fence_receipts if receipt.identity == identity and receipt.bucket == bucket
        ]
        if len(matches) != 1 or matches[0].observed_at > datetime.now(UTC):
            raise LifecycleConflict("Exact owner write fence receipt is required")
        async with self.clients.with_region(identity.region).s3_client() as client:
            response = await client.get_bucket_policy(Bucket=bucket, ExpectedBucketOwner=identity.source_aws_account_id)
        policy = json.loads(response["Policy"])
        expected = OwnerDeletionFence(
            Sid="ValSmithOwnerDeletion" + identity.operation_id.hex,
            Effect="Deny",
            Principal="*",
            Action=("s3:PutObject", "s3:DeleteObject"),
            Resource=f"arn:aws:s3:::{bucket}/*",
        ).model_dump(mode="json")
        statements = policy.get("Statement", [])
        found = [statement for statement in statements if statement.get("Sid") == expected["Sid"]]
        digest = policy_digest(policy)
        if found != [expected] or digest != matches[0].policy_sha256:
            raise LifecycleConflict("Current owner write fence differs from exact operation receipt")
        return digest

    async def _sandboxes(self, run: PurgeRun, *, delete: bool, verify: Verification) -> None:
        clients = self.clients.with_region(run.scope.original_resources.region)
        configuration = await asyncio.to_thread(
            fetch_sandbox_provider_config, run.provider.secret_name, SecretsManagerStore(clients), run.provider.kind
        )
        provider = configuration.create_provider()
        try:
            async for sandbox in provider.list_sandboxes(SandboxQuery(labels={"Id": str(run.scope.run_id)})):
                if (sandbox.labels or {}).get("Id") != str(run.scope.run_id):
                    raise LifecycleConflict("Provider sandbox inventory is outside exact run scope")
                if not delete:
                    raise LifecycleConflict("Sandboxes remain for the held run")

                verify()
                try:
                    await provider.delete_sandbox(sandbox.id)
                except SandboxNotFoundError:
                    pass
        finally:
            await provider.close()

    async def cleanup_sandboxes(self, run: PurgeRun, *, verify: Verification) -> None:
        await self._sandboxes(run, delete=True, verify=verify)

    async def verify_absence(self, run: PurgeRun) -> None:
        await self._sandboxes(run, delete=False, verify=unverified)

    async def _inventory(
        self, client: Any, identity: OperationIdentity, run: PurgeRun, *, uploads: bool
    ) -> list[dict[str, str]]:
        request: dict[str, Any] = {
            "Bucket": run.scope.original_resources.s3_bucket,
            "Prefix": run.scope.object_prefix,
            "ExpectedBucketOwner": identity.source_aws_account_id,
        }
        found: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        while True:
            response = await (
                client.list_multipart_uploads(**request) if uploads else client.list_object_versions(**request)
            )
            records = (
                response.get("Uploads", [])
                if uploads
                else response.get("Versions", []) + response.get("DeleteMarkers", [])
            )
            for record in records:
                key, identifier = record.get("Key"), record.get("UploadId" if uploads else "VersionId")
                if (
                    not isinstance(key, str)
                    or not key.startswith(run.scope.object_prefix)
                    or not isinstance(identifier, str)
                    or not identifier
                ):
                    raise LifecycleConflict("Provider inventory is outside exact run prefix or lacks version identity")
                found.append({"Key": key, "UploadId" if uploads else "VersionId": identifier})
            truncated = response.get("IsTruncated")
            if not isinstance(truncated, bool):
                raise LifecycleConflict("Provider inventory pagination proof is missing")
            if not truncated:
                return found
            key_marker = response.get("NextKeyMarker")
            value_marker = response.get("NextUploadIdMarker" if uploads else "NextVersionIdMarker")
            if (
                not isinstance(key_marker, str)
                or not isinstance(value_marker, str)
                or (key_marker, value_marker) in seen
            ):
                raise LifecycleConflict("Provider inventory pagination is incomplete")
            seen.add((key_marker, value_marker))
            request.update({"KeyMarker": key_marker, "UploadIdMarker" if uploads else "VersionIdMarker": value_marker})

    async def purge_objects(self, identity: OperationIdentity, run: PurgeRun, *, verify: Verification) -> None:
        async with self.clients.with_region(identity.region).s3_client() as client:
            versions = await self._inventory(client, identity, run, uploads=False)
            uploads = await self._inventory(client, identity, run, uploads=True)
            arguments = {
                "Bucket": run.scope.original_resources.s3_bucket,
                "ExpectedBucketOwner": identity.source_aws_account_id,
            }
            for version in versions:
                verify()
                await client.delete_object(**arguments, **version)
            for upload in uploads:
                verify()
                try:
                    await client.abort_multipart_upload(**arguments, **upload)
                except ClientError as error:
                    if error.response.get("Error", {}).get("Code") != "NoSuchUpload":
                        raise
            if await self._inventory(client, identity, run, uploads=False) or await self._inventory(
                client, identity, run, uploads=True
            ):
                raise LifecycleConflict("Object versions or multipart uploads remain")

    async def _log_exists(self, run: PurgeRun) -> bool:
        client = self.clients.with_region(run.scope.original_resources.region).cloudwatch_logs_client()
        arguments: dict[str, str] = {"logGroupNamePrefix": run.scope.log_group}
        tokens: set[str] = set()
        while True:
            response = await asyncio.to_thread(client.describe_log_groups, **arguments)
            if any(group.get("logGroupName") == run.scope.log_group for group in response.get("logGroups", [])):
                return True
            token = response.get("nextToken")
            if token is None:
                return False
            if not isinstance(token, str) or token in tokens:
                raise LifecycleConflict("Log inventory pagination is incomplete")
            tokens.add(token)
            arguments["nextToken"] = token

    async def purge_logs(self, run: PurgeRun, *, verify: Verification) -> None:
        if await self._log_exists(run):
            client = self.clients.with_region(run.scope.original_resources.region).cloudwatch_logs_client()
            verify()
            try:
                await asyncio.to_thread(client.delete_log_group, logGroupName=run.scope.log_group)
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                    raise
        if await self._log_exists(run):
            raise LifecycleConflict("Exact run log group remains")

    async def verify_storage_absence(self, identity: OperationIdentity, run: PurgeRun) -> None:
        async with self.clients.with_region(identity.region).s3_client() as client:
            if await self._inventory(client, identity, run, uploads=False) or await self._inventory(
                client, identity, run, uploads=True
            ):
                raise LifecycleConflict("Object versions or multipart uploads remain")
        if await self._log_exists(run):
            raise LifecycleConflict("Exact run log group remains")
