"""Local service composition uses persistent files and transient credentials."""

from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest

from tracker.exceptions import SecretsError
from tracker.runtime.secrets import resolve_secrets
from tracker.local.runtime import LocalRuntimeFactory
from tracker.local.secrets import InMemorySecretStore


async def test_local_runtime_scopes_files_and_clears_secrets(tmp_path: Path) -> None:
    """Persist artifacts without allowing another organization to read them."""
    org_id = uuid4()
    references = {"API_KEY": "agent-key"}
    with closing(InMemorySecretStore(references, {"API_KEY": "transient-value"})) as secrets:
        runtime = LocalRuntimeFactory.create_runtime(tmp_path, org_id, secrets=secrets)
        await runtime.objects.put_bytes("agent.zip", b"agent")
        assert await resolve_secrets(references, runtime.secrets) == {"API_KEY": "transient-value"}
        assert runtime.artifacts.object_location("agent.zip") == str(
            tmp_path / "orgs" / str(org_id) / "objects/agent.zip"
        )
        assert (await runtime.get_sandbox_provider_config()).type == "docker"
    with pytest.raises(SecretsError, match="no values"):
        await resolve_secrets(references, runtime.secrets)
    reopened = LocalRuntimeFactory.create_runtime(tmp_path, org_id)
    assert await reopened.objects.get_bytes("agent.zip") == b"agent"
    other = LocalRuntimeFactory.create_runtime(tmp_path, uuid4())
    assert not await other.objects.exists("agent.zip")
    assert all(b"transient-value" not in path.read_bytes() for path in tmp_path.rglob("*") if path.is_file())


async def test_executor_reads_only_declared_credentials_fresh_for_each_dispatch(tmp_path: Path) -> None:
    from contextlib import AsyncExitStack

    from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Org
    from tracker.executor.dependencies import get_execution_runtime
    from tracker.local.resources import LocalResources
    from tracker.types import StartBenchmarkRequest

    source = tmp_path / "existing.env"
    root = tmp_path / "artifacts"
    org = Org(id=uuid4(), name="local")
    contract = AgentContractRequest(name="agent", secrets={"MODEL_KEY": "model-key"})
    properties = LocalResources(data_root=root, secrets_file=source)
    request = StartBenchmarkRequest(
        environment="local", sandbox_provider="docker", contract=contract, benchmark_name="test"
    )
    benchmark = Benchmark(
        org_id=org.id,
        name="test",
        aws_managed=False,
        arguments=BenchmarkArguments(
            environment="local", properties=properties, contract=contract, concurrency=1, sandbox_provider="docker"
        ),
    )
    for value in ("first-key", "rotated-key"):
        source.write_text(f"MODEL_KEY={value}\nUNRELATED_KEY=must-not-inject\n", encoding="utf-8")
        async with AsyncExitStack() as stack:
            runtime = await get_execution_runtime(request, benchmark, org, runtime_stack=stack)
            assert await resolve_secrets(contract.secrets, runtime.secrets) == {"MODEL_KEY": value}
            with pytest.raises(SecretsError, match="no values"):
                await runtime.secrets.get("UNRELATED_KEY")
        with pytest.raises(SecretsError, match="no values"):
            await resolve_secrets(contract.secrets, runtime.secrets)
        assert value not in benchmark.model_dump_json()
        assert all(value.encode() not in path.read_bytes() for path in root.rglob("*") if path.is_file())
    assert source.read_text(encoding="utf-8") == "MODEL_KEY=rotated-key\nUNRELATED_KEY=must-not-inject\n"
    source.write_text("UNRELATED_KEY=present\n", encoding="utf-8")
    async with AsyncExitStack() as stack:
        with pytest.raises(SecretsError, match="missing keys: MODEL_KEY"):
            await get_execution_runtime(request, benchmark, org, runtime_stack=stack)
