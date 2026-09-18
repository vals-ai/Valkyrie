"""Read-only full-version proof and strict sandbox cleanup for relocation."""

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

from botocore.exceptions import ClientError

from tracker.aws.managed_storage import ManagedStoragePolicy, validate_managed_storage_bucket
from tracker.aws.runtime import AWSRuntime
from tracker.lifecycle import LifecycleConflict, OperationIdentity
from tracker.run_purge.contracts import PurgeRun
from tracker.run_purge.providers import AWSProviderBoundary
from tracker.storage_migration_exchange import (
    ExecutionReference,
    ObjectTransformation,
    RelocationRun,
    TrackerRequest,
    canonical_digest,
)


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def rewrite_json(content: bytes, transformation: ObjectTransformation) -> bytes:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise LifecycleConflict("Ambiguous JSON object has duplicate keys")
            result[key] = value
        return result

    def invalid_constant(_value: str) -> Any:
        raise LifecycleConflict("Nonfinite JSON constant is not permitted")

    if len(content) != transformation.original_size or _digest(content) != transformation.original_sha256:
        raise LifecycleConflict("Transformation source bytes changed")
    try:
        document: Any = json.loads(content.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid_constant)
        for edit in transformation.edits:
            if not edit.pointer.startswith("/") or re.search(r"~(?![01])", edit.pointer):
                raise LifecycleConflict("Invalid JSON pointer")
            tokens = [token.replace("~1", "/").replace("~0", "~") for token in edit.pointer[1:].split("/")]
            current = document
            for index, token in enumerate(tokens):
                key: str | int = token
                if isinstance(current, list):
                    if re.fullmatch(r"0|[1-9][0-9]*", token) is None:
                        raise LifecycleConflict("Invalid array pointer")
                    key = int(token)
                elif not isinstance(current, dict):
                    raise LifecycleConflict("Pointer does not name a JSON container")
                container = cast(Any, current)
                value: Any = container[key]
                if index == len(tokens) - 1:
                    if value != edit.original or not isinstance(value, str):
                        raise LifecycleConflict("Original JSON locator changed")
                    container[key] = edit.replacement
                else:
                    current = value
        rewritten = json.dumps(document, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (UnicodeError, ValueError, KeyError, IndexError, TypeError) as error:
        raise LifecycleConflict("Invalid planned JSON transformation") from error
    if len(rewritten) != transformation.rewritten_size or _digest(rewritten) != transformation.rewritten_sha256:
        raise LifecycleConflict("Rewritten JSON does not match immutable plan")
    return rewritten


@dataclass(frozen=True)
class Version:
    key: str
    version_id: str
    marker: bool
    size: int
    current: bool
    modified: datetime
    sha256: str | None


class RelocationAWSBoundary(AWSProviderBoundary):
    async def validate_source(self, identity: OperationIdentity, run: PurgeRun) -> None:
        resources = run.scope.original_resources
        clients = self.clients.with_region(resources.region)
        if clients.credential_source != "managed" or resources.region != identity.region:
            raise LifecycleConflict("Source authority or saved region differs")
        account = await asyncio.to_thread(lambda: clients.sts_client().get_caller_identity()["Account"])
        if account != identity.source_aws_account_id:
            raise LifecycleConflict("Source caller account differs")
        arguments = {"Bucket": resources.s3_bucket, "ExpectedBucketOwner": identity.source_aws_account_id}
        tags: list[dict[str, str]]
        async with clients.s3_client() as client:
            head = await client.head_bucket(**arguments)
            if head.get("BucketRegion") != resources.region:
                raise LifecycleConflict("Source bucket region differs from saved scope")
            try:
                tags = (await client.get_bucket_tagging(**arguments)).get("TagSet", [])
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != "NoSuchTagSet":
                    raise
                tags = []
            versioning = await client.get_bucket_versioning(**arguments)
        if (
            set(versioning) - {"Status", "MFADelete", "ResponseMetadata"}
            or ("Status" in versioning and versioning["Status"] not in {"Enabled", "Suspended"})
            or ("MFADelete" in versioning and versioning["MFADelete"] not in {"Enabled", "Disabled"})
        ):
            raise LifecycleConflict("Unknown source versioning state")
        owners = [tag["Value"] for tag in tags if tag.get("Key") == "valsmith:owner-account-id"]
        organizations = [tag["Value"] for tag in tags if tag.get("Key") == "valsmith:valkyrie-org-id"]
        if organizations and organizations != [str(identity.org_id)]:
            raise LifecycleConflict("Source organization tag differs")
        if resources.s3_bucket.startswith("vs-") or owners:
            if owners != [str(identity.github_owner_id)]:
                raise LifecycleConflict("Managed source owner differs; legacy fallback is forbidden")
            await validate_managed_storage_bucket(
                AWSRuntime(resources, clients, identity.source_aws_account_id),
                org_id=identity.org_id,
                bucket_name=resources.s3_bucket,
                policy=ManagedStoragePolicy(
                    identity.source_aws_account_id, {identity.org_id: frozenset({identity.environment})}
                ),
            )

    async def _bytes(self, client: Any, request: TrackerRequest, bucket: str, key: str, version_id: str) -> bytes:
        response = await client.get_object(
            Bucket=bucket, Key=key, VersionId=version_id, ExpectedBucketOwner=request.source_aws_account_id
        )
        returned_version = response.get("VersionId")
        if returned_version != version_id and not (version_id == "null" and returned_version is None):
            raise LifecycleConflict("Provider returned another object version")
        async with response["Body"] as body:
            content = await body.read()
        if not isinstance(content, bytes) or len(content) != response.get("ContentLength"):
            raise LifecycleConflict("Object body length does not match exact version")
        return content

    async def _versions(self, client: Any, request: TrackerRequest, bucket: str, prefix: str) -> tuple[Version, ...]:
        arguments: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "ExpectedBucketOwner": request.source_aws_account_id,
        }
        values: list[Version] = []
        seen: set[tuple[str, str]] = set()
        pages: set[tuple[str, str]] = set()
        while True:
            response = await client.list_object_versions(**arguments)
            for marker, records in ((False, response.get("Versions", [])), (True, response.get("DeleteMarkers", []))):
                for record in records:
                    key, version_id = record.get("Key"), record.get("VersionId")
                    if (
                        not isinstance(key, str)
                        or not key.startswith(prefix)
                        or not isinstance(version_id, str)
                        or not version_id
                        or (key, version_id) in seen
                        or not isinstance(record.get("IsLatest"), bool)
                        or not isinstance(record.get("LastModified"), datetime)
                    ):
                        raise LifecycleConflict("Incomplete or duplicate version identity")
                    seen.add((key, version_id))
                    content = None if marker else await self._bytes(client, request, bucket, key, version_id)
                    size = 0 if marker else record.get("Size")
                    if not isinstance(size, int) or (content is not None and len(content) != size):
                        raise LifecycleConflict("Version size does not match bytes")
                    values.append(
                        Version(
                            key,
                            version_id,
                            marker,
                            size,
                            record["IsLatest"],
                            record["LastModified"],
                            None if content is None else _digest(content),
                        )
                    )
            truncated = response.get("IsTruncated")
            if not isinstance(truncated, bool):
                raise LifecycleConflict("Version pagination proof is missing")
            if not truncated:
                break
            marker = (response.get("NextKeyMarker"), response.get("NextVersionIdMarker"))
            if not all(isinstance(item, str) and item for item in marker) or marker in pages:
                raise LifecycleConflict("Version pagination is incomplete")
            pages.add(marker)
            arguments.update(KeyMarker=marker[0], VersionIdMarker=marker[1])
        uploads = await client.list_multipart_uploads(
            Bucket=bucket, Prefix=prefix, ExpectedBucketOwner=request.source_aws_account_id
        )
        if uploads.get("Uploads") or uploads.get("IsTruncated") is not False:
            raise LifecycleConflict("Multipart upload or incomplete upload inventory remains")
        for key in {item.key for item in values}:
            group = [item for item in values if item.key == key]
            if sum(item.current for item in group) != 1:
                raise LifecycleConflict("Exactly one current object or marker is required per key")

            if len({item.modified for item in group}) != len(group):
                raise LifecycleConflict("Version ordering is ambiguous")
        return tuple(values)

    async def verify_objects(
        self, request: TrackerRequest, run: RelocationRun, *, source_removed: bool = False, source_partial: bool = False
    ) -> None:
        run_id = run.scope.run_id
        source_bucket, destination_bucket = run.scope.original_resources.s3_bucket, run.destination_resources.s3_bucket
        if source_bucket == destination_bucket:
            raise LifecycleConflict("Relocation requires distinct buckets")
        copies = [item for item in request.copied_objects if item.run_id == run_id]
        history = [item for item in request.destination_versions if item.run_id == run_id]
        if any(item.run_id not in request.run_ids for item in (*request.copied_objects, *request.destination_versions)):
            raise LifecycleConflict("Copy evidence is outside the exact run set")
        if len({item.source_version_id + "\0" + item.key for item in copies}) != len(copies) or len(
            {item.destination_version_id + "\0" + item.key for item in copies}
        ) != len(copies):
            raise LifecycleConflict("Duplicate source or destination copy identity")
        if len({(item.key, item.version_id) for item in history}) != len(history):
            raise LifecycleConflict("Duplicate destination version identity")
        for item in copies:
            if (
                item.source_bucket != source_bucket
                or item.destination_bucket != destination_bucket
                or not item.source_version_id
                or item.destination_version_id in {"", "null"}
            ):
                raise LifecycleConflict("Copy account or bucket scope differs")
        for item in history:
            if (
                item.bucket != destination_bucket
                or not item.key.startswith(run.scope.object_prefix)
                or item.version_id in {"", "null"}
            ):
                raise LifecycleConflict("Destination history is outside exact scope")
            if item.is_delete_marker and (item.sha256 is not None or item.size != 0):
                raise LifecycleConflict("Delete marker has byte content")
            if not item.is_delete_marker and item.sha256 is None:
                raise LifecycleConflict("Live destination version lacks a checksum")
        transforms = {canonical_digest(item.model_dump(mode="json")): item for item in run.transformations}
        async with self.clients.with_region(request.region).s3_client() as client:
            if request.plan is None:
                raise LifecycleConflict("Copy proof requires an immutable operation plan")
            policy_response = await client.get_bucket_policy(
                Bucket=source_bucket, ExpectedBucketOwner=request.source_aws_account_id
            )
            policy = json.loads(policy_response["Policy"])
            expected = {
                "Sid": "ValSmithOwnerMigration" + request.plan.identity.operation_id.hex,
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["s3:PutObject", "s3:DeleteObject"],
            }
            statements = [
                statement for statement in policy.get("Statement", []) if statement.get("Sid") == expected["Sid"]
            ]
            if len(statements) != 1:
                raise LifecycleConflict("Exact source migration fence is missing or ambiguous")
            statement = dict(statements[0])
            resources = statement.pop("Resource", None)
            if (
                statement != expected
                or not isinstance(resources, list)
                or f"arn:aws:s3:::{source_bucket}/{run.scope.object_prefix}*" not in resources
            ):
                raise LifecycleConflict("Source migration fence differs from exact operation scope")
            source = await self._versions(client, request, source_bucket, run.scope.object_prefix)
            destination = await self._versions(client, request, destination_bucket, run.scope.object_prefix)
            source_map = {(item.key, item.version_id): item for item in source}
            destination_map = {(item.key, item.version_id): item for item in destination}
            copies_map = {(item.key, item.source_version_id): item for item in copies}
            if source_removed and source:
                raise LifecycleConflict("Source cleanup is incomplete")
            if not source_removed and (
                (not source_partial and set(source_map) != set(copies_map)) or not set(source_map).issubset(copies_map)
            ):
                raise LifecycleConflict("Complete source copy proof is missing or source changed")
            if set(destination_map) != {(item.key, item.version_id) for item in history}:
                raise LifecycleConflict("Destination history has missing or unknown versions")
            for item in history:
                actual = destination_map[item.key, item.version_id]
                if (actual.marker, actual.size, actual.sha256, actual.current) != (
                    item.is_delete_marker,
                    item.size,
                    item.sha256,
                    item.is_current,
                ):
                    raise LifecycleConflict("Destination version bytes, marker or current state changed")
            for key in {item.key for item in history}:
                ordered = [destination_map[item.key, item.version_id] for item in history if item.key == key]
                if not ordered[0].current or any(
                    left.modified < right.modified for left, right in zip(ordered, ordered[1:])
                ):
                    raise LifecycleConflict("Destination version history is out of order")
                # S3 timestamps cannot establish relative order for tied historical versions.
                if any(left.modified == right.modified for left, right in zip(ordered, ordered[1:])):
                    raise LifecycleConflict("Destination version ordering is ambiguous")
            proof_by_destination = {(item.key, item.destination_version_id): item for item in copies}
            for item in copies:
                original = source_map.get((item.key, item.source_version_id))
                if original is not None and (original.marker, original.size, original.sha256) != (
                    item.is_delete_marker,
                    item.source_size,
                    item.source_sha256,
                ):
                    raise LifecycleConflict("Source bytes differ from copied proof")
                matching = [
                    version
                    for version in history
                    if (version.key, version.version_id) == (item.key, item.destination_version_id)
                ]
                if (
                    len(matching) != 1
                    or matching[0].provenance != "copied"
                    or matching[0].transformation_sha256 != item.transformation_sha256
                    or (matching[0].is_delete_marker, matching[0].size, matching[0].sha256, matching[0].is_current)
                    != (item.is_delete_marker, item.destination_size, item.destination_sha256, item.is_current)
                ):
                    raise LifecycleConflict("Copy proof differs from complete destination history")
                if item.transformation_sha256 is None:
                    if item.source_sha256 != item.destination_sha256 or item.source_size != item.destination_size:
                        raise LifecycleConflict("Changed bytes have no immutable transformation")
                else:
                    transformation = transforms.get(item.transformation_sha256)
                    if transformation is None or (
                        transformation.source_bucket,
                        transformation.key,
                        transformation.source_version_id,
                        transformation.original_sha256,
                        transformation.original_size,
                        transformation.rewritten_sha256,
                        transformation.rewritten_size,
                    ) != (
                        source_bucket,
                        item.key,
                        item.source_version_id,
                        item.source_sha256,
                        item.source_size,
                        item.destination_sha256,
                        item.destination_size,
                    ):
                        raise LifecycleConflict("Transformation does not match exact source and destination")
                    if original is not None:
                        rewrite_json(
                            await self._bytes(client, request, source_bucket, item.key, item.source_version_id),
                            transformation,
                        )
                    elif not source_partial and not source_removed:
                        raise LifecycleConflict("Transformation original is missing")
            for key in {item.key for item in history}:
                ordered_history = [item for item in history if item.key == key]
                copied_history = [item for item in ordered_history if item.provenance == "copied"]
                if copied_history and ordered_history[0].provenance == "existing":
                    raise LifecycleConflict("Copied versions require an explicit current restoration")
                source_order = [
                    source_map[(item.key, proof_by_destination[item.key, item.version_id].source_version_id)]
                    for item in copied_history
                    if (item.key, proof_by_destination[item.key, item.version_id].source_version_id) in source_map
                ]
                if not source_partial and not source_removed and source_order and not source_order[0].current:
                    raise LifecycleConflict("Copied history does not preserve the source current version")
                if any(left.modified < right.modified for left, right in zip(source_order, source_order[1:])):
                    raise LifecycleConflict("Copied history inverts source version order")
            used_transforms = {item.transformation_sha256 for item in copies if item.transformation_sha256 is not None}
            for item in history:
                if item.provenance == "copied":
                    if (
                        item.key,
                        item.version_id,
                    ) not in proof_by_destination or item.restored_from_version_id is not None:
                        raise LifecycleConflict("Destination copied version lacks source proof")
                elif item.provenance == "existing":
                    if item.restored_from_version_id is not None or item.transformation_sha256 is not None:
                        raise LifecycleConflict("Existing history cannot be rewritten")
                else:
                    originals = [
                        version
                        for version in history
                        if version.key == item.key
                        and version.version_id == item.restored_from_version_id
                        and version.provenance == "existing"
                    ]
                    if len(originals) != 1:
                        raise LifecycleConflict("Restoration requires exact retained original version")
                    original = originals[0]
                    if item.transformation_sha256 is None:
                        if (item.is_delete_marker, item.size, item.sha256) != (
                            original.is_delete_marker,
                            original.size,
                            original.sha256,
                        ):
                            raise LifecycleConflict("Restoration bytes differ from original")
                    else:
                        transformation = transforms.get(item.transformation_sha256)
                        if transformation is None or (
                            transformation.source_bucket,
                            transformation.key,
                            transformation.source_version_id,
                            transformation.original_sha256,
                            transformation.original_size,
                            transformation.rewritten_sha256,
                            transformation.rewritten_size,
                        ) != (
                            destination_bucket,
                            item.key,
                            original.version_id,
                            original.sha256,
                            original.size,
                            item.sha256,
                            item.size,
                        ):
                            raise LifecycleConflict("Restored transformation differs from plan")
                        rewrite_json(
                            await self._bytes(client, request, destination_bucket, item.key, original.version_id),
                            transformation,
                        )
                        used_transforms.add(item.transformation_sha256)
            if set(transforms) != used_transforms:
                raise LifecycleConflict("Planned transformations have incomplete copy proof")

    async def execution_references(
        self, arguments: dict[str, Any], request: TrackerRequest, run: RelocationRun | None
    ) -> tuple[ExecutionReference, ...]:
        references: list[ExecutionReference] = []
        source_bucket = None if run is None else run.scope.original_resources.s3_bucket
        values: list[tuple[str, Any]] = [("/dataset", arguments.get("dataset"))]
        if arguments.get("lambda_function") is not None:
            values.append(("/lambda_function", arguments["lambda_function"]))

        def scan(value: Any, pointer: str) -> None:
            if isinstance(value, dict):
                for key, child in cast(dict[str, Any], value).items():
                    scan(child, pointer + "/" + key.replace("~", "~0").replace("/", "~1"))
            elif isinstance(value, list):
                for index, child in enumerate(cast(list[Any], value)):
                    scan(child, pointer + "/" + str(index))
            elif isinstance(value, str) and ("s3://" in value or "https://" in value or "http://" in value):
                values.append((pointer, value))

        scan(arguments.get("contract"), "/contract")
        for pointer, value in values:
            reference = ExecutionReference(pointer=pointer, value_sha256=canonical_digest(value), kind="unknown")
            if isinstance(value, str) and value.startswith("s3://"):
                parsed = urlsplit(value)
                key = parsed.path.lstrip("/")
                if parsed.netloc == source_bucket:
                    reference = reference.model_copy(update={"kind": "retired_source"})
                elif parsed.netloc and key and not parsed.fragment:
                    versions = parse_qs(parsed.query, keep_blank_values=True)
                    if (
                        set(versions) != {"versionId"}
                        or len(versions["versionId"]) != 1
                        or versions["versionId"][0] in {"", "null"}
                    ):
                        references.append(reference)
                        continue
                    async with self.clients.with_region(request.region).s3_client() as client:
                        options = {
                            "Bucket": parsed.netloc,
                            "Key": key,
                            "ExpectedBucketOwner": request.source_aws_account_id,
                        }
                        if "versionId" in versions:
                            options["VersionId"] = versions["versionId"][0]
                        response = await client.get_object(**options)
                        identifier = response.get("VersionId")
                        async with response["Body"] as body:
                            content = await body.read()
                        if not isinstance(content, bytes) or len(content) != response.get("ContentLength"):
                            raise LifecycleConflict("Retained execution object bytes are incomplete")
                        requested_version = options.get("VersionId")
                        if (
                            requested_version is not None
                            and identifier != requested_version
                            and not (requested_version == "null" and identifier is None)
                        ):
                            raise LifecycleConflict("Retained execution object returned another version")

                        if not isinstance(identifier, str) or identifier in {"", "null"}:
                            references.append(reference)
                            continue
                    reference = ExecutionReference(
                        pointer=pointer,
                        value_sha256=canonical_digest(value),
                        kind="retained_s3_object",
                        bucket=parsed.netloc,
                        key=key,
                        version_id=identifier,
                        sha256=_digest(content),
                    )
            references.append(reference)
        return tuple(references)
