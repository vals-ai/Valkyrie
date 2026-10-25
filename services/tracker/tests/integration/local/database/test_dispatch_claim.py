"""Dispatch claim fencing against disposable PostgreSQL.

Run: uv run pytest tests/integration/local/database/test_dispatch_claim.py
"""

import os
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
)
from tracker.executor import dispatch_recovery
from tracker.executor.dispatch_control import ClaimedDispatch, claim_dispatch
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release


@pytest.mark.parametrize("task_arn", [None, "arn:aws:ecs:us-east-1:123456789012:task/test/task-1"])
def test_concurrent_claims_admit_exactly_one_executor(
    postgres_engine: Engine, postgres_session: Session, task_arn: str | None
) -> None:
    org = Org(id=uuid4(), name=f"dispatch-claim-{uuid4()}")
    benchmark = make_benchmark(
        name="dispatch-claim",
        org_id=org.id,
        contract=AgentContractRequest(name="claim-agent", install_cmd="true", run_cmd="true"),
    )
    release = ExecutorRelease(
        id="dispatch-claim-release",
        artifact_uri="source:///artifacts/dispatch-claim",
        artifact_digest="a" * 64,
        protocol_version="1",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
    )
    payload = {"benchmark_id_str": str(benchmark.id)}
    sealed = seal_payload(dispatch.id, payload, generate_payload_key(dispatch.id))
    postgres_session.add_all(
        [
            benchmark,
            dispatch,
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            ),
        ]
    )
    postgres_session.commit()

    first_holds_row_lock = threading.Event()
    first_may_commit = threading.Event()
    results: dict[str, ClaimedDispatch | None] = {}

    def first_executor() -> None:
        with Session(postgres_engine) as session:
            results["first"] = claim_dispatch(session, dispatch.id, task_arn=task_arn)
            first_holds_row_lock.set()
            first_may_commit.wait(10)
            session.commit()

    def second_executor() -> None:
        first_holds_row_lock.wait(10)
        with Session(postgres_engine) as session:
            results["second"] = claim_dispatch(session, dispatch.id, task_arn=task_arn)
            session.commit()

    first = threading.Thread(target=first_executor)
    second = threading.Thread(target=second_executor)
    first.start()
    second.start()
    second.join(1)
    assert second.is_alive(), "second claim must block on the row lock until the first claim commits"
    first_may_commit.set()
    first.join(10)
    second.join(10)

    assert results["first"] is not None
    assert results["first"].payload == payload
    assert results["second"] is None
    postgres_session.expire_all()
    stored = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert stored is not None
    assert stored.status == ExecutorDispatchStatus.RUNNING
    assert stored.ecs_task_arn == task_arn
    assert (stored.lease_expires_at is not None) == (task_arn is None)
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None


@pytest.mark.parametrize("outcome", ["malformed", "corrupt", "complete"])
def test_executor_process_records_outcome(
    postgres_engine: Engine, postgres_session: Session, tmp_path: Path, outcome: str
) -> None:
    org = Org(id=uuid4(), name=f"executor-process-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"executor-process-{uuid4()}",
        artifact_uri="source:///local",
        artifact_digest="a" * 64,
        protocol_version="5",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    sealed = seal_payload(
        dispatch.id,
        {"benchmark_id_str": str(benchmark.id)} if outcome == "complete" else {},
        generate_payload_key(dispatch.id),
    )
    postgres_session.add_all(
        [
            benchmark,
            dispatch,
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext + (b"corrupt" if outcome == "corrupt" else b""),
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            ),
        ]
    )
    postgres_session.commit()

    # Replace only benchmark work. The child owns the real claim and terminal transaction.
    if outcome == "complete":
        (tmp_path / "sitecustomize.py").write_text(
            "from uuid import UUID\nfrom sqlmodel import Session\n"
            "from tracker.database.session import engine\n"
            "from tracker.database.models import Benchmark, BenchmarkStatus\n"
            "import tracker.utils.run_orchestration as orchestration\n"
            "async def complete(benchmark_id_str, **kwargs):\n"
            "    with Session(engine) as session:\n"
            "        benchmark = session.get(Benchmark, UUID(benchmark_id_str))\n"
            "        benchmark.status = BenchmarkStatus.FINISHED\n"
            "        session.add(benchmark)\n        session.commit()\n"
            "orchestration.process_benchmark = complete\n"
        )
    result = subprocess.run(
        [sys.executable, "-m", "tracker.executor.entrypoint", "--dispatch-id", str(dispatch.id)],
        env={
            **os.environ,
            "DATABASE_URL": postgres_engine.url.render_as_string(hide_password=False),
            "SENTRY_DSN": "",
            "PYTHONPATH": str(tmp_path) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert (result.returncode == 0) == (outcome == "complete"), result.stderr
    postgres_session.expire_all()
    stored_dispatch = postgres_session.get(ExecutorDispatch, dispatch.id)
    stored_benchmark = postgres_session.get(Benchmark, benchmark.id)
    assert stored_dispatch is not None and stored_benchmark is not None
    if outcome == "complete":
        assert stored_dispatch.status == ExecutorDispatchStatus.FINISHED
        assert stored_benchmark.status == BenchmarkStatus.FINISHED
        assert stored_dispatch.finished_at is not None
    else:
        assert stored_dispatch.status == ExecutorDispatchStatus.FAILED
        assert stored_benchmark.status == BenchmarkStatus.ERROR
        assert stored_dispatch.failure_reason == (
            "PAYLOAD_DECRYPT_FAILED" if outcome == "corrupt" else "EXECUTOR_FAILED"
        )
    if outcome != "corrupt":
        assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None


@pytest.mark.parametrize("termination", ["revoke", "kill"])
def test_local_process_recovers_after_stop_or_crash(
    postgres_engine: Engine,
    postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    termination: str,
) -> None:
    org = Org(id=uuid4(), name=f"local-process-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"local-process-{uuid4()}",
        artifact_uri="source:///local",
        artifact_digest="a" * 64,
        protocol_version="5",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    sealed = seal_payload(dispatch.id, {}, generate_payload_key(dispatch.id))
    postgres_session.add_all(
        [
            benchmark,
            dispatch,
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            ),
        ]
    )
    postgres_session.commit()
    # Hold benchmark work at its boundary while the real process owns its claim and lease.
    (tmp_path / "sitecustomize.py").write_text(
        "import asyncio\nimport tracker.utils.run_orchestration as orchestration\n"
        "async def wait_for_stop(**kwargs):\n    await asyncio.Future()\n"
        "orchestration.process_benchmark = wait_for_stop\n"
    )
    environment = {
        **os.environ,
        "DATABASE_URL": postgres_engine.url.render_as_string(hide_password=False),
        "SENTRY_DSN": "",
        "PYTHONPATH": str(tmp_path) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    with subprocess.Popen(
        [sys.executable, "-m", "tracker.executor.entrypoint", "--dispatch-id", str(dispatch.id)],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            deadline = monotonic() + 20
            while monotonic() < deadline:
                postgres_session.refresh(dispatch)
                if dispatch.status == ExecutorDispatchStatus.RUNNING:
                    break
                assert process.poll() is None, process.communicate()[1]
                sleep(0.05)
            else:
                pytest.fail("Executor did not claim the dispatch")
            assert dispatch.lease_expires_at is not None
            if termination == "revoke":
                dispatch.status = ExecutorDispatchStatus.FAILED
                benchmark.status = BenchmarkStatus.STOPPED
                postgres_session.add_all([dispatch, benchmark])
                postgres_session.commit()
                assert process.wait(timeout=20) == 0
            else:
                process.kill()
                process.wait(timeout=10)
                dispatch.lease_expires_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=1)
                postgres_session.add(dispatch)
                postgres_session.commit()
                monkeypatch.setattr(dispatch_recovery, "engine", postgres_engine)
                assert dispatch_recovery.reconcile_dispatches_once() == 1
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
    postgres_session.expire_all()
    stored_dispatch = postgres_session.get(ExecutorDispatch, dispatch.id)
    stored_benchmark = postgres_session.get(Benchmark, benchmark.id)
    assert stored_dispatch is not None and stored_benchmark is not None
    assert stored_dispatch.status == ExecutorDispatchStatus.FAILED
    assert stored_benchmark.status == (BenchmarkStatus.STOPPED if termination == "revoke" else BenchmarkStatus.ERROR)
