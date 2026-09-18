"""Exercise the local runtime and agent transfer against the real Docker daemon."""

import asyncio
import io
import tarfile
from contextlib import closing, nullcontext
from datetime import UTC, datetime
import zipfile
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from benchmark_service import ImageSource, Resources, SandboxQuery

from tracker.database.models import AgentContractRequest, OutputArtifact
from tracker.exceptions import AgentRunFailedError
from tracker.runtime.secrets import resolve_secrets
from tracker.local.runtime import LocalRuntimeFactory
from tracker.local.secrets import InMemorySecretStore
from tracker.runtime.artifacts import agent_bundle_key, copy_agent_to_benchmark, task_artifact_key
from tracker.runtime.logs import TaskLogReference, task_log_stream_name
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.sandbox import create_sandbox, run_agent, upload_agent_artifacts


@pytest.mark.parametrize("exit_code", [0, 7])
async def test_local_runtime_transfers_and_executes_frozen_agent(tmp_path: Path, exit_code: int) -> None:
    """Execute a frozen local bundle without S3, curl, unzip, or package downloads."""
    org_id = uuid4()
    run_id = uuid4()
    benchmark_id = str(run_id)
    started = datetime.now(UTC)
    contract = AgentContractRequest(
        name="local-test",
        install_cmd="printf installed > /tmp/installed",
        run_cmd="/bundle/local-test/run.sh {problem_statement_path}",
        final_output="/tmp/final-output",
        secrets={"LOCAL_TEST_KEY": "local-test-key"},
        output_artifacts=[OutputArtifact(path="result.txt", source="/tmp/final-output/result.txt")],
    )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "local-test/contract.yaml",
            yaml.safe_dump(contract.model_dump(exclude_none=True, exclude={"inference_settings_attested"})),
        )
        archive.writestr("local-test/empty/", "")
        executable = zipfile.ZipInfo("local-test/run.sh")
        executable.external_attr = 0o100755 << 16
        archive.writestr(
            executable,
            "#!/bin/sh\nset -e\ntest -f /tmp/installed\nmkdir -p /tmp/final-output\n"
            'test "$LOCAL_TEST_KEY" = transient-value\n'
            'cat "$1" > /tmp/final-output/result.txt\nprintf "agent completed\\n"\n'
            f"exit {exit_code}\n",
        )
    with closing(InMemorySecretStore(contract.secrets, {"LOCAL_TEST_KEY": "transient-value"})) as secrets:
        runtime = LocalRuntimeFactory.create_runtime(tmp_path, org_id, secrets=secrets)
        await runtime.objects.put_bytes(agent_bundle_key(contract.name), stream.getvalue())
        await copy_agent_to_benchmark(runtime.objects, benchmark_id, contract.name)
        await runtime.objects.put_bytes(agent_bundle_key(contract.name), b"replacement bundle")
        await runtime.logs.create_benchmark(benchmark_id, retention_days=0)
        provider_config = await runtime.get_sandbox_provider_config()
        async with runtime.get_sandbox_provider(provider_config) as provider:
            async with create_sandbox(
                provider,
                "local-transfer",
                ImageSource(image="python:3.12-slim"),
                Resources(vcpu=1, memory=1, disk=1),
                asyncio.Semaphore(1),
                env_vars=await resolve_secrets(contract.secrets, runtime.secrets),
            ) as sandbox:
                sandbox_id = sandbox.id
                await upload_agent_artifacts(sandbox, contract, benchmark_id, runtime.objects)
                await sandbox.upload_file("/tmp/problem.txt", b"local-result")
                result = await sandbox.exec(
                    "test -d /bundle/local-test/empty && test ! -x /bundle/local-test/contract.yaml"
                )
                assert result.exit_code == 0
                output_key = task_artifact_key(benchmark_id, "task", "output.tar.gz")
                stream_key = f"{benchmark_id}:{task_log_stream_name('task', started)}"
                async with TaskLogBuffer(runtime.logs, stream_key) as logs:
                    with pytest.raises(AgentRunFailedError) if exit_code else nullcontext():
                        exit_reason, duration = await run_agent(
                            sandbox,
                            contract,
                            "/tmp/problem.txt",
                            "task",
                            logs.write,
                            "/tmp/work",
                            runtime.objects,
                            agent_output_s3_key=output_key,
                            benchmark_id=benchmark_id,
                        )
                        assert exit_reason is None
                        assert duration >= 0
                assert (
                    await runtime.objects.get_bytes(task_artifact_key(benchmark_id, "task", "result.txt"))
                    == b"local-result"
                )
                with tarfile.open(fileobj=io.BytesIO(await runtime.objects.get_bytes(output_key))) as archive:
                    result_file = archive.extractfile("tmp/final-output/result.txt")
                    assert result_file is not None
                    assert result_file.read() == b"local-result"
                page = await runtime.log_reader.fetch(TaskLogReference(run_id, "task", started))
                assert "agent completed" in "".join(event.message for event in page.events)
            assert sandbox_id not in [sandbox.id async for sandbox in provider.list_sandboxes(SandboxQuery(labels={}))]
