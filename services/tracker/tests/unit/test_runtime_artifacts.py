"""Artifact freezing and organization-scoped alias policy.

Run: uv run pytest tests/unit/test_runtime_artifacts.py
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from tests.utils import TEST_ORG_ID
from tracker.local.storage import FilesystemObjectStore

from tracker.runtime.artifacts import agent_bundle_key, benchmark_agent_bundle_key, copy_agent_to_benchmark, list_agents
from tracker.runtime.storage import ObjectCopier, ObjectStore, StoredObject, StoredObjectCopy


class RecordingStore:
    """Small artifact-store double that exposes policy inputs and outputs."""

    def __init__(self, *, exists: bool, objects: list[StoredObject] | None = None) -> None:
        self._exists = exists
        self._objects = objects or []
        self.exists_keys: list[str] = []
        self.copies: list[tuple[str, str]] = []
        self.listed_prefixes: list[str] = []

    async def exists(self, key: str) -> bool:
        self.exists_keys.append(key)
        return self._exists

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        self.copies.append((source_key, destination_key))
        return StoredObjectCopy(deletion_token="copied-version")

    async def list_objects(self, prefix: str):  # type: ignore[no-untyped-def]
        self.listed_prefixes.append(prefix)
        for stored_object in self._objects:
            yield stored_object


class RecordingCopier:
    """Small cross-store copy double with an opaque destination identity."""

    def __init__(self) -> None:
        self.copies: list[tuple[str, str]] = []

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        self.copies.append((source_key, destination_key))

        return StoredObjectCopy(deletion_token="destination-version")


async def test_copy_agent_to_benchmark_does_not_replace_existing_bundle() -> None:
    recording_store = RecordingStore(exists=True)
    recording_copier = RecordingCopier()

    copied = await copy_agent_to_benchmark(
        cast(ObjectStore, recording_store),
        benchmark_id="benchmark-1",
        agent_name="agent-a",
        org_id=TEST_ORG_ID,
        copier=cast(ObjectCopier, recording_copier),
    )

    assert copied is None
    assert recording_store.exists_keys == ["benchmarks/benchmark-1/agent-a.zip"]
    assert recording_store.copies == []
    assert recording_copier.copies == []


async def test_copy_agent_to_benchmark_freezes_missing_bundle_with_provider_token() -> None:
    recording_store = RecordingStore(exists=False)

    copied = await copy_agent_to_benchmark(
        cast(ObjectStore, recording_store), benchmark_id="benchmark-1", agent_name="agent-a", org_id=TEST_ORG_ID
    )

    assert copied == StoredObjectCopy(deletion_token="copied-version")
    assert recording_store.exists_keys == ["benchmarks/benchmark-1/agent-a.zip"]
    assert recording_store.copies == [(f"agents/{TEST_ORG_ID}/agent-a.zip", "benchmarks/benchmark-1/agent-a.zip")]


async def test_copy_agent_to_benchmark_uses_supplied_copier_for_missing_bundle() -> None:
    destination_store = RecordingStore(exists=False)
    recording_copier = RecordingCopier()

    copied = await copy_agent_to_benchmark(
        cast(ObjectStore, destination_store),
        benchmark_id="run-1",
        agent_name="agent-a",
        org_id=TEST_ORG_ID,
        copier=cast(ObjectCopier, recording_copier),
    )

    assert destination_store.exists_keys == ["benchmarks/run-1/agent-a.zip"]
    assert recording_copier.copies == [(f"agents/{TEST_ORG_ID}/agent-a.zip", "benchmarks/run-1/agent-a.zip")]
    assert copied == StoredObjectCopy(deletion_token="destination-version")
    assert destination_store.copies == []


async def test_list_agents_keeps_zip_bundles_and_last_modified_metadata() -> None:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    recording_store = RecordingStore(
        exists=False,
        objects=[
            StoredObject(key=f"agents/{TEST_ORG_ID}/alpha.zip", last_modified=timestamp, size=0),
            StoredObject(key=f"agents/{TEST_ORG_ID}/notes.txt", last_modified=timestamp, size=0),
            StoredObject(key=f"agents/{TEST_ORG_ID}/nested/beta.zip", size=0),
        ],
    )

    agents = await list_agents(cast(ObjectStore, recording_store), org_id=TEST_ORG_ID)

    assert agents == [("alpha", timestamp)]
    assert recording_store.listed_prefixes == [f"agents/{TEST_ORG_ID}/"]


class TestTenantAgentFreeze:
    """A frozen run reads only its owner's current alias and stays immutable."""

    async def test_freezes_each_tenants_alias_without_shared_fallback(self, tmp_path: Path) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        orgs = (TEST_ORG_ID, UUID("00000000-0000-0000-0000-000000000002"))
        await store.put_bytes("agents/demo.zip", b"unscoped bundle")
        for index, org_id in enumerate(orgs):
            await store.put_bytes(agent_bundle_key("demo", org_id=org_id), f"tenant {index}".encode())

        for index, org_id in enumerate(orgs):
            await copy_agent_to_benchmark(store, f"run-{index}", "demo", org_id=org_id)

            frozen_key = benchmark_agent_bundle_key(f"run-{index}", "demo")
            assert await store.get_bytes(frozen_key) == f"tenant {index}".encode()

            await store.delete(agent_bundle_key("demo", org_id=org_id))
            await copy_agent_to_benchmark(store, f"run-{index}", "demo", org_id=org_id)

            assert await store.get_bytes(frozen_key) == f"tenant {index}".encode()
            with pytest.raises(FileNotFoundError):
                await copy_agent_to_benchmark(store, f"new-run-{index}", "demo", org_id=org_id)
