"""Artifact key and lifecycle policy."""

from datetime import datetime
from uuid import UUID

from tracker.runtime.storage import ObjectCopier, ObjectStore, StoredObjectCopy

AGENTS_PREFIX = "agents"
BENCHMARKS_PREFIX = "benchmarks"


def agent_library_prefix(org_id: UUID | None) -> str:
    return f"{AGENTS_PREFIX}/{org_id}/" if org_id is not None else f"{AGENTS_PREFIX}/"


def agent_bundle_key(agent_name: str, *, org_id: UUID | None) -> str:
    return f"{agent_library_prefix(org_id)}{agent_name}.zip"


def benchmark_prefix(benchmark_id: str) -> str:
    return f"{BENCHMARKS_PREFIX}/{benchmark_id}/"


def benchmark_agent_bundle_key(benchmark_id: str, agent_name: str) -> str:
    return f"{benchmark_prefix(benchmark_id)}{agent_name}.zip"


def task_artifact_key(benchmark_id: str, task_id: str, output_name: str) -> str:
    return f"{benchmark_prefix(benchmark_id)}{task_id}/{output_name}"


async def copy_agent_to_benchmark(
    store: ObjectStore,
    benchmark_id: str,
    agent_name: str,
    *,
    org_id: UUID | None,
    copier: ObjectCopier | None = None,
) -> StoredObjectCopy | None:
    """Freeze an agent bundle for a benchmark unless it already exists."""
    source_key = agent_bundle_key(agent_name, org_id=org_id)
    destination_key = benchmark_agent_bundle_key(benchmark_id, agent_name)
    if await store.exists(destination_key):
        return None

    return await (copier or store).copy(source_key, destination_key)


async def list_agents(store: ObjectStore, *, org_id: UUID | None) -> list[tuple[str, datetime | None]]:
    agents: list[tuple[str, datetime | None]] = []
    prefix = agent_library_prefix(org_id)
    async for stored_object in store.list_objects(prefix):
        tail = stored_object.key.removeprefix(prefix)
        if "/" not in tail and tail.endswith(".zip"):
            agents.append((tail.removesuffix(".zip"), stored_object.last_modified))
    return agents
