import asyncio
import threading
from typing import Any

import pytest
from fastapi import HTTPException
from sqlmodel import Session, select
from starlette.requests import Request

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID, FakeAccessKeys
from tracker.config import BenchmarkServiceDestination
from tracker.database.models import (
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    Org,
    RunAccessKey,
)
from tracker.run_access_keys import delete_finished_run_keys, issued_run_keys, run_service_key


@pytest.mark.parametrize("creation_fails", [False, True])
@pytest.mark.parametrize("cancellations", [1, 2])
async def test_cancelled_creation_finishes_before_request_cleanup(
    database_session: Session,
    fake_access_keys: FakeAccessKeys,
    monkeypatch: pytest.MonkeyPatch,
    creation_fails: bool,
    cancellations: int,
) -> None:
    started = asyncio.Event()
    finished = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original_create = fake_access_keys.create
    fake_access_keys.fail_create = creation_fails

    def slow_create(*args: Any, **kwargs: Any) -> dict[str, Any]:
        loop.call_soon_threadsafe(started.set)
        assert release.wait(timeout=5)
        try:
            return original_create(*args, **kwargs)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(fake_access_keys, "create", slow_create)

    async def request() -> None:
        dependency = issued_run_keys(database_session)
        issued = await anext(dependency)
        try:
            await run_service_key(
                Request({"type": "http", "headers": []}), _org(), BenchmarkServiceDestination.HOSTED, None, issued
            )
        finally:
            await dependency.aclose()

    task = asyncio.create_task(request())
    await asyncio.wait_for(started.wait(), timeout=5)
    for _ in range(cancellations):
        task.cancel()
        await asyncio.sleep(0)
    await asyncio.sleep(0)
    try:
        assert not task.done()
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), timeout=5)
        with pytest.raises(asyncio.CancelledError):
            await task

    assert fake_access_keys.deleted == ([] if creation_fails else ["K1"])


def _org() -> Org:
    return Org(id=TEST_ORG_ID, name="default")


def _run_with_key(
    session: Session, status: BenchmarkStatus, key_id: str, dispatch_status: ExecutorDispatchStatus | None = None
) -> None:
    benchmark = make_benchmark(status=status)
    session.add(benchmark)
    session.add(RunAccessKey(key_id=key_id, benchmark_id=benchmark.id))
    if dispatch_status is not None:
        session.add(
            ExecutorDispatch(
                benchmark_id=benchmark.id,
                kind=ExecutorDispatchKind.START,
                status=dispatch_status,
                executor_release_id="sweep-test-release",
                executor_artifact_uri="s3://artifacts/sweep.pex",
                executor_artifact_digest="a" * 64,
                executor_protocol_version=SUPPORTED_PROTOCOL_VERSION,
            )
        )
    session.commit()


def _remaining(session: Session) -> set[str]:
    session.expire_all()
    return set(session.exec(select(RunAccessKey.key_id)).all())


def test_sweep_deletes_keys_of_runs_that_have_ended(
    database_session: Session, fake_access_keys: FakeAccessKeys
) -> None:
    _run_with_key(database_session, BenchmarkStatus.FINISHED, "K-finished")
    _run_with_key(database_session, BenchmarkStatus.FINISHED, "K-gone")
    _run_with_key(database_session, BenchmarkStatus.ERROR, "K-error", ExecutorDispatchStatus.FAILED)
    _run_with_key(database_session, BenchmarkStatus.STOPPED, "K-stopped")
    _run_with_key(database_session, BenchmarkStatus.IN_PROGRESS, "K-running", ExecutorDispatchStatus.RUNNING)
    _run_with_key(database_session, BenchmarkStatus.STOPPED, "K-draining", ExecutorDispatchStatus.RUNNING)
    _run_with_key(database_session, BenchmarkStatus.STOPPING, "K-stopping")
    fake_access_keys.missing.add("K-gone")

    delete_finished_run_keys(database_session.get_bind())

    assert sorted(fake_access_keys.deleted) == ["K-error", "K-finished", "K-stopped"]
    assert _remaining(database_session) == {"K-running", "K-draining", "K-stopping"}


def test_failed_deletions_move_to_the_back_of_the_queue(
    database_session: Session, fake_access_keys: FakeAccessKeys, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("tracker.run_access_keys._SWEEP_BATCH_SIZE", 1)
    fake_access_keys.failing.add("K-stuck")
    _run_with_key(database_session, BenchmarkStatus.FINISHED, "K-stuck")
    delete_finished_run_keys(database_session.get_bind())
    _run_with_key(database_session, BenchmarkStatus.FINISHED, "K-next")

    delete_finished_run_keys(database_session.get_bind())

    assert fake_access_keys.deleted == ["K-next"]
    assert _remaining(database_session) == {"K-stuck"}


@pytest.mark.usefixtures("without_descope_management")
async def test_access_key_runs_forward_the_callers_key_without_descope_management() -> None:
    request = Request({"type": "http", "headers": [(b"x-api-key", b"caller-key")]})

    key = await run_service_key(request, _org(), BenchmarkServiceDestination.HOSTED, None, [])

    assert key == "caller-key"


@pytest.mark.usefixtures("without_descope_management")
@pytest.mark.parametrize("headers", [[], [(b"x-api-key", b"")]], ids=["missing", "empty"])
async def test_login_runs_need_descope_management(headers: list[tuple[bytes, bytes]]) -> None:
    request = Request({"type": "http", "headers": headers})

    with pytest.raises(HTTPException) as exc_info:
        await run_service_key(request, _org(), BenchmarkServiceDestination.HOSTED, None, [])

    assert exc_info.value.status_code == 503
