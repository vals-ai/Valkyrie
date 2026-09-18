"""Behavioral tests for provider-neutral artifact policy."""

from datetime import UTC, datetime
from typing import cast

from tracker.runtime.artifacts import copy_agent_to_benchmark, list_agents
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
        copier=cast(ObjectCopier, recording_copier),
    )

    assert copied is None
    assert recording_store.exists_keys == ["benchmarks/benchmark-1/agent-a.zip"]
    assert recording_store.copies == []
    assert recording_copier.copies == []


async def test_copy_agent_to_benchmark_freezes_missing_bundle_with_provider_token() -> None:
    recording_store = RecordingStore(exists=False)

    copied = await copy_agent_to_benchmark(
        cast(ObjectStore, recording_store), benchmark_id="benchmark-1", agent_name="agent-a"
    )

    assert copied == StoredObjectCopy(deletion_token="copied-version")
    assert recording_store.exists_keys == ["benchmarks/benchmark-1/agent-a.zip"]
    assert recording_store.copies == [("agents/agent-a.zip", "benchmarks/benchmark-1/agent-a.zip")]


async def test_copy_agent_to_benchmark_uses_supplied_copier_for_missing_bundle() -> None:
    destination_store = RecordingStore(exists=False)
    recording_copier = RecordingCopier()

    copied = await copy_agent_to_benchmark(
        cast(ObjectStore, destination_store),
        benchmark_id="run-1",
        agent_name="agent-a",
        copier=cast(ObjectCopier, recording_copier),
    )

    assert destination_store.exists_keys == ["benchmarks/run-1/agent-a.zip"]
    assert recording_copier.copies == [("agents/agent-a.zip", "benchmarks/run-1/agent-a.zip")]
    assert copied == StoredObjectCopy(deletion_token="destination-version")
    assert destination_store.copies == []


async def test_list_agents_keeps_zip_bundles_and_last_modified_metadata() -> None:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    recording_store = RecordingStore(
        exists=False,
        objects=[
            StoredObject(key="agents/alpha.zip", last_modified=timestamp),
            StoredObject(key="agents/notes.txt", last_modified=timestamp),
            StoredObject(key="agents/nested/beta.zip"),
        ],
    )

    agents = await list_agents(cast(ObjectStore, recording_store))

    assert agents == [("alpha", timestamp), ("nested/beta", None)]
    assert recording_store.listed_prefixes == ["agents/"]
