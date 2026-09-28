"""Real runner subprocess against disposable PostgreSQL and cached artifact."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine
import pytest
from testcontainers.postgres import PostgresContainer

import main
from tests.factories import make_benchmark
from tracker.database.models import (
    AgentContractRequest, BenchmarkStatus, ExecutorAdmission, ExecutorDispatch,
    ExecutorDispatchPayload, ExecutorDispatchStatus, ExecutorRelease, Org,
)
from tracker.executor import launcher
from tracker.executor.release_control import promote_release, register_release
from tracker.types import StartBenchmarkRequest


def test_runner_claims_cached_artifact_and_delivers_admission_trace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "child-observed.json"
    script = (
        "import json,sys\n"
        f"from pathlib import Path\nPath({str(output)!r}).write_text(Path(sys.argv[1]).read_text())\n"
    ).encode()
    digest = hashlib.sha256(script).hexdigest()
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / f"{digest}.pex").write_bytes(script)

    with PostgresContainer("postgres:16-alpine") as postgres:
        engine: Engine = create_engine(postgres.get_connection_url())
        SQLModel.metadata.create_all(engine)
        try:
            dispatch_id = uuid4()
            with Session(engine) as session:
                session.add(ExecutorAdmission())
                org = Org(id=uuid4(), name=f"runner-process-{uuid4()}")
                session.add(org)
                session.flush()
                release = ExecutorRelease(
                    id=f"runner-process-{uuid4()}", artifact_uri="s3://artifacts/executors/cached.pex",
                    artifact_digest=digest, protocol_version="3", readiness_verified=True,
                    created_at=datetime.now(UTC),
                )
                register_release(session, release)
                promote_release(session, release.id)
                benchmark = make_benchmark(
                    name="runner-process", org_id=org.id,
                    contract=AgentContractRequest(name="runner-agent", install_cmd="true", run_cmd="true"),
                    status=BenchmarkStatus.IN_PROGRESS,
                )
                session.commit()

            traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
            local_key = base64.b64encode(b"k" * 32).decode()
            monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
            monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", local_key)
            request = StartBenchmarkRequest(
                benchmark_name=benchmark.name, contract=benchmark.arguments.contract, concurrency=1,
            )
            _, admission = main._commit_start(
                engine, benchmark.model_dump_json(), request, dispatch_id, [], None,
                # Tracker's composite propagator injects both W3C and Sentry headers for the same trace.
                {
                    "request_id": "admitted-request",
                    "trace_headers": {
                        "traceparent": traceparent,
                        "sentry-trace": "0123456789abcdef0123456789abcdef-0123456789abcdef-1",
                    },
                },
            )
            assert admission.dispatch_json
            with Session(engine) as session:
                dispatch = session.get(ExecutorDispatch, dispatch_id)
                assert dispatch is not None
                assert session.get(ExecutorDispatchPayload, dispatch_id) is not None

            url = engine.url
            assert url.host and url.port and url.database and url.username and url.password
            repo_root = Path(__file__).resolve().parents[5]
            env = {
                "PYTHONPATH": os.pathsep.join((str(repo_root / "services/tracker/src"), str(repo_root))),
                "EXECUTOR_CACHE_DIR": str(cache), "EXECUTOR_RELEASE_BUCKET": "artifacts",
                "EXECUTOR_RELEASE_PREFIX": "executors",
                "DB_HOST": url.host, "DB_PORT": str(url.port), "DB_NAME": url.database,
                "DB_USERNAME": url.username, "DB_PASSWORD": url.password,
                "SENTRY_DSN": "",
            }
            for key, value in env.items():
                monkeypatch.setenv(key, value)
            processes: list[subprocess.Popen[bytes]] = []
            real_popen = subprocess.Popen

            def capture_spawn(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
                process = real_popen(*args, **kwargs)
                processes.append(process)
                return process

            with monkeypatch.context() as spawn_patch:
                spawn_patch.setattr(launcher.subprocess, "Popen", capture_spawn)
                asyncio.run(launcher.launch_dispatch(dispatch))
            assert len(processes) == 1
            assert processes[0].wait(timeout=60) == 0
            child_payload = json.loads(output.read_text())
            assert child_payload["start_benchmark_request_json"]["benchmark_name"] == benchmark.name
            assert child_payload["telemetry_context_json"]["request_id"] == "admitted-request"
            child_headers = child_payload["telemetry_context_json"]["trace_headers"]
            # The runner may convert W3C traceparent to sentry-trace; either must carry the admitted trace ID.
            child_trace_id = (
                child_headers["sentry-trace"].split("-")[0]
                if "sentry-trace" in child_headers
                else child_headers["traceparent"].split("-")[1]
            )
            assert child_trace_id == traceparent.split("-")[1]
            with Session(engine) as session:
                assert session.get(ExecutorDispatch, dispatch_id).status == ExecutorDispatchStatus.FINISHED
                assert session.get(ExecutorDispatchPayload, dispatch_id) is None
        finally:
            SQLModel.metadata.drop_all(engine)
            engine.dispose()
