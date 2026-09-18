"""Exercise the local runtime and agent transfer against the real Docker daemon."""

import asyncio
import io
import zipfile
from pathlib import Path
from uuid import uuid4

from benchmark_service import ImageSource, Resources, SandboxQuery

from tracker.database.models import AgentContractRequest
from tracker.local.runtime import LocalRuntimeFactory
from tracker.runtime.artifacts import benchmark_agent_bundle_key
from tracker.sandbox import create_sandbox, upload_agent_artifacts


async def test_local_runtime_transfers_and_executes_frozen_agent(tmp_path: Path) -> None:
    """Execute a frozen local bundle without S3, curl, unzip, or package downloads."""
    org_id = uuid4()
    benchmark_id = str(uuid4())
    contract = AgentContractRequest(name="local-test", run_cmd="/bundle/local-test/run.sh")
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "local-test/contract.yaml",
            "name: local-test\ninstall_cmd: ''\nrun_cmd: /bundle/local-test/run.sh {problem_statement_path}\n",
        )
        executable = zipfile.ZipInfo("local-test/run.sh")
        executable.external_attr = 0o100755 << 16
        archive.writestr(executable, '#!/bin/sh\nprintf "local-result" > /tmp/result.txt\n')
    async with LocalRuntimeFactory.open(tmp_path, org_id) as runtime:
        await runtime.objects.put_bytes(benchmark_agent_bundle_key(benchmark_id, contract.name), stream.getvalue())
        provider_config = await runtime.get_sandbox_provider_config()
        async with runtime.get_sandbox_provider(provider_config) as provider:
            async with create_sandbox(
                provider,
                "local-transfer",
                ImageSource(image="python:3.12-slim"),
                Resources(vcpu=1, memory=1, disk=1),
                asyncio.Semaphore(1),
            ) as sandbox:
                sandbox_id = sandbox.id
                await upload_agent_artifacts(sandbox, contract, benchmark_id, runtime.objects)
                result = await sandbox.exec(contract.run_cmd)
                assert result.exit_code == 0
                assert await sandbox.download_file("/tmp/result.txt") == b"local-result"
            assert sandbox_id not in [sandbox.id async for sandbox in provider.list_sandboxes(SandboxQuery(labels={}))]
