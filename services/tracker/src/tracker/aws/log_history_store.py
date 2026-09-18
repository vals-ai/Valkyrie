"""Exact-version archive storage and durable, exclusive upload intent journal."""

import base64
import fcntl
import hashlib
import json
import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from pydantic import BaseModel

from tracker.runtime.log_history import ArchiveError, ArchiveLocation, ArchiveObject, ScanEvidence


def encode(value: BaseModel | dict[str, Any] | list[str]) -> bytes:
    data = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def verified_client(session: Any, service: str, account: str, region: str) -> tuple[Any, str]:
    identity = session.client("sts", region_name=region).get_caller_identity()
    principal = identity.get("Arn", "")
    if identity.get("Account") != account or f":{account}:" not in principal:
        raise ArchiveError("AWS authority account mismatch")

    client = session.client(service, region_name=region)
    if client.meta.region_name != region:
        raise ArchiveError("AWS authority region mismatch")

    return client, principal


class ArchiveVersionStore:
    def __init__(self, session: Any, location: ArchiveLocation) -> None:
        self.client, self.principal = verified_client(session, "s3", location.account_id, location.region)
        self.location = location
        self.request = {"Bucket": location.bucket, "ExpectedBucketOwner": location.account_id}
        bucket = self.client.head_bucket(**self.request)
        if bucket.get("BucketRegion") != location.region:
            raise ArchiveError("bucket region mismatch")

        if self.client.get_bucket_versioning(**self.request).get("Status") != "Enabled":
            raise ArchiveError("bucket versioning required")

        tags = {item["Key"]: item["Value"] for item in self.client.get_bucket_tagging(**self.request).get("TagSet", [])}
        if (
            tags.get("valsmith:owner-account-id") != str(location.github_owner_id)
            or tags.get("valsmith:environment") != location.environment
            or tags.get("valsmith:valkyrie-org-id") != str(location.org_id)
            or tags.get("valsmith:backup") != "true"
        ):
            raise ArchiveError("bucket owner scope mismatch")

        rules = self.client.get_bucket_ownership_controls(**self.request).get("OwnershipControls", {}).get("Rules", [])
        if rules != [{"ObjectOwnership": "BucketOwnerEnforced"}]:
            raise ArchiveError("bucket owner enforcement required")

    def read(self, reference: ArchiveObject, maximum_bytes: int) -> bytes:
        if reference.size_bytes > maximum_bytes or not reference.key.startswith(
            f"benchmarks/{self.location.run_id}/log-history/"
        ):
            raise ArchiveError("object scope or byte limit mismatch")

        response = self.client.get_object(**self.request, Key=reference.key, VersionId=reference.version_id)
        body = response["Body"]
        try:
            if (
                response.get("VersionId") != reference.version_id
                or response.get("ContentLength") != reference.size_bytes
                or response.get("ServerSideEncryption") != "AES256"
                or response.get("ContentEncoding") not in (None, "identity")
            ):
                raise ArchiveError("object content verification failed")

            content = body.read(reference.size_bytes + 1)
        finally:
            body.close()

        if len(content) != reference.size_bytes or digest(content) != reference.sha256:
            raise ArchiveError("object content verification failed")

        return content

    def write(self, key: str, content: bytes) -> str:
        if not key.startswith(f"benchmarks/{self.location.run_id}/log-history/"):
            raise ArchiveError("object write scope mismatch")

        response = self.client.put_object(
            **self.request,
            Key=key,
            Body=content,
            ServerSideEncryption="AES256",
            ContentType="application/json",
            IfNoneMatch="*",
            ChecksumSHA256=base64.b64encode(hashlib.sha256(content).digest()).decode("ascii"),
        )
        version = response.get("VersionId")
        if not isinstance(version, str) or not version or version == "null":
            raise ArchiveError("immutable version missing after upload")

        return version


class UploadJournal:
    """A local durable volume is required. Unknown acceptance is never adopted."""

    def __init__(self, directory: Path, scope_sha256: str, prefix: str, max_chunks: int) -> None:
        self.directory = directory
        self.scope_sha256 = scope_sha256
        self.prefix = prefix
        self.max_chunks = max_chunks
        self.objects: dict[str, ArchiveObject] = {}
        self.inventory: ScanEvidence | None = None

    @property
    def manifest(self) -> ArchiveObject | None:
        return self.objects.get(f"{self.prefix}manifest.json")

    def _read_json(self, path: Path) -> dict[str, Any]:
        with path.open("rb") as source:
            content = source.read(16_385)

        if len(content) > 16_384:
            raise ArchiveError("journal size limit exceeded")

        value = json.loads(content)
        if not isinstance(value, dict):
            raise ArchiveError("invalid journal record")

        return cast(dict[str, Any], value)

    def _validate_key(self, key: str) -> None:
        if key == f"{self.prefix}manifest.json":
            return

        match = re.fullmatch(re.escape(self.prefix) + r"chunks/([0-9]{8})\.json", key)
        if match is None or int(match[1]) >= self.max_chunks:
            raise ArchiveError("unexplained journal object scope")

    def _load(self) -> None:
        binding = {"journal_version": 1, "scope_sha256": self.scope_sha256, "prefix": self.prefix}
        binding_path = self.directory / "scope.json"
        if binding_path.exists():
            if self._read_json(binding_path) != binding:
                raise ArchiveError("journal identity conflict")
        else:
            if any(path.name != "lock" for path in self.directory.iterdir()):
                raise ArchiveError("unbound prior journal state requires reconciliation")

            self._persist(binding_path, binding)

        self.objects.clear()
        self.inventory = None
        for count, path in enumerate(self.directory.iterdir(), start=1):
            if count > self.max_chunks + 4:
                raise ArchiveError("journal entry count limit exceeded")

            if path.name in {"lock", "scope.json"}:
                continue

            if path.name == "inventory.json":
                saved = self._read_json(path)
                if set(saved) != {"scope_sha256", "scan"} or saved["scope_sha256"] != self.scope_sha256:
                    raise ArchiveError("journal inventory identity conflict")

                self.inventory = ScanEvidence.model_validate(saved["scan"])
                continue

            if not re.fullmatch(r"[0-9a-f]{64}\.json", path.name):
                raise ArchiveError("unexplained prior journal state requires reconciliation")

            saved = self._read_json(path)
            if saved.get("scope_sha256") != self.scope_sha256:
                raise ArchiveError("journal identity conflict")

            if not saved.get("version_id"):
                raise ArchiveError("unresolved upload intent requires operator reconciliation")

            reference = ArchiveObject.model_validate(
                {key: value for key, value in saved.items() if key != "scope_sha256"}
            )
            self._validate_key(reference.key)
            if path.name != f"{digest(reference.key.encode())}.json" or reference.key in self.objects:
                raise ArchiveError("journal object identity conflict")

            self.objects[reference.key] = reference

    def remember_inventory(self, evidence: ScanEvidence) -> None:
        if self.inventory is not None:
            if not same_inventory(self.inventory, evidence):
                raise ArchiveError("source inventory conflicts with prior journal scan")

            return

        self._persist(
            self.directory / "inventory.json",
            {"scope_sha256": self.scope_sha256, "scan": evidence.model_dump(mode="json")},
        )
        self.inventory = evidence

    def require_chunks(self, references: list[ArchiveObject]) -> None:
        expected = {reference.key: reference for reference in references}
        actual = {key: reference for key, reference in self.objects.items() if key != f"{self.prefix}manifest.json"}
        if len(expected) != len(references) or actual != expected:
            raise ArchiveError("unexplained prior journal chunks differ from current inventory")

    @contextmanager
    def locked(self) -> Generator[None]:
        self.directory.mkdir(exist_ok=True, mode=0o700)
        descriptor = os.open(self.directory.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        with (self.directory / "lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ArchiveError("archive journal already in use") from None

            try:
                self._load()
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _persist(self, path: Path, data: dict[str, Any]) -> None:
        temporary = self.directory / f".{uuid4()}.tmp"
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(encode(data))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        descriptor = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def put(self, store: ArchiveVersionStore, key: str, content: bytes, maximum_bytes: int) -> ArchiveObject:
        if len(content) > maximum_bytes:
            raise ArchiveError("archive object byte limit exceeded")

        self._validate_key(key)
        checksum = digest(content)
        reference = self.objects.get(key)
        if reference is not None:
            if reference.sha256 != checksum or reference.size_bytes != len(content):
                raise ArchiveError("journal identity or content conflict")
        else:
            if self.manifest is not None:
                raise ArchiveError("published archive inventory cannot gain objects")

            path = self.directory / f"{digest(key.encode())}.json"
            expected = {"scope_sha256": self.scope_sha256, "key": key, "sha256": checksum, "size_bytes": len(content)}
            self._persist(path, expected)
            version = store.write(key, content)
            self._persist(path, {**expected, "version_id": version})
            reference = ArchiveObject(key=key, version_id=version, sha256=checksum, size_bytes=len(content))
            self.objects[key] = reference

        store.read(reference, maximum_bytes)
        return reference


def same_inventory(first: ScanEvidence, second: ScanEvidence) -> bool:
    fields = {"group_pages", "stream_pages", "event_pages"}
    return first.model_dump(exclude=fields) == second.model_dump(exclude=fields)
