"""Admission HTTP regressions with bounded, deterministic external-work barriers."""

import asyncio
import threading
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi import Depends, HTTPException
from sqlmodel import Session, select

import main as main_module
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tests.unit.utils.task_execution_support import MockKicker
from tests.utils import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.executor import release_control
from tracker.executor.release_control import promote_release
from tracker.logging import benchmark_id_var
from tracker.types import HarnessConfig, StartBenchmarkRequest


@pytest.fixture
def recovery_run(database_session: Session, example_benchmark_object: Benchmark) -> tuple[Benchmark, Task]:
    release = ExecutorRelease(
        id="barrier-release",
        artifact_uri="s3://artifacts/barrier-release.pex",
        artifact_digest="barrier-digest",
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        readiness_verified=True,
    )
    database_session.add(release)
    database_session.commit()
    promote_release(database_session, release.id)
    benchmark = example_benchmark_object
    benchmark.status = BenchmarkStatus.STOPPED
    benchmark.executor_release_id = release.id
    benchmark.current_execution_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version
    task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="task_0", status=TaskStatus.STOPPED)
    database_session.add_all([benchmark, task])
    database_session.commit()
    return benchmark, task


@pytest.fixture
def observed_sessions(database_session: Session, monkeypatch: pytest.MonkeyPatch) -> list[tuple[Session, int]]:
    """Use real auth read transactions and observe thread-owned admission Sessions."""
    sessions: list[tuple[Session, int]] = []

    class ObservedSession(Session):
        def __enter__(self) -> "ObservedSession":
            sessions.append((self, threading.get_ident()))
            return super().__enter__()

    async def request_session() -> AsyncGenerator[Session, None]:
        with ObservedSession(database_session.get_bind()) as session:
            yield session

    async def current_org(session: Session = Depends(main_module.get_session)) -> Org:
        org = session.get(Org, TEST_ORG_ID)
        assert org is not None
        return org

    monkeypatch.setattr(main_module, "Session", ObservedSession)
    monkeypatch.setitem(main_module.app.dependency_overrides, main_module.get_session, request_session)
    monkeypatch.setitem(main_module.app.dependency_overrides, get_current_org, current_org)
    monkeypatch.setattr(main_module, "check_database_connection", lambda: True)
    return sessions


def _start_body(benchmark: Benchmark, harness_config: HarnessConfig) -> dict[str, Any]:
    return StartBenchmarkRequest(
        benchmark_name=benchmark.name,
        contract=benchmark.arguments.contract,
        task_ids=["task_0"],
        harness_config=harness_config,
    ).model_dump(mode="json")


@pytest.mark.parametrize("contender", ["retry", "start"])
async def test_verification_releases_transactions_and_admission_for_other_requests(
    contender: str,
    recovery_run: tuple[Benchmark, Task],
    observed_sessions: list[tuple[Session, int]],
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
    harness_config: HarnessConfig,
    mock_kicker: MockKicker,
) -> None:
    benchmark, _ = recovery_run
    url = f"/retry-or-resume-benchmark/{benchmark.id}"
    entered, release = asyncio.Event(), asyncio.Event()
    loop_thread = threading.get_ident()
    admission_lock = Mock(wraps=main_module.lock_executor_admission)
    monkeypatch.setattr(main_module, "lock_executor_admission", admission_lock)

    async def verify(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        if not entered.is_set():
            entered.set()
            await release.wait()
        return VerifyTaskIdsResponse(task_ids=["task_0"])

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app), base_url="http://test") as client:
        first = asyncio.create_task(client.post(url, json={}, headers=harness_headers))
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert len(observed_sessions) == 2  # Auth and prepare, each already closed.
            assert all(not session.in_transaction() for session, _ in observed_sessions)
            assert observed_sessions[0][1] == loop_thread
            assert observed_sessions[1][1] != loop_thread
            admission_lock.assert_not_called()
            assert (await asyncio.wait_for(client.get("/health"), timeout=2)).status_code == 200
            second_url = url if contender == "retry" else "/start-benchmark"
            body = {} if contender == "retry" else _start_body(benchmark, harness_config)
            second = await asyncio.wait_for(client.post(second_url, json=body, headers=harness_headers), timeout=2)
            assert second.status_code == 200, second.text
            assert not first.done()
        finally:
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(first, return_exceptions=True), timeout=2)
        response = responses[0]
        assert isinstance(response, httpx.Response)
        assert response.status_code == (409 if contender == "retry" else 200), response.text
        assert len(mock_kicker.queued_calls) == (1 if contender == "retry" else 2)
        assert all(not session.in_transaction() for session, _ in observed_sessions)


@pytest.mark.parametrize("operation", ["retry", "start"])
async def test_blocking_admission_wait_does_not_block_health(
    operation: str,
    recovery_run: tuple[Benchmark, Task],
    observed_sessions: list[tuple[Session, int]],
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
    harness_config: HarnessConfig,
    mock_kicker: MockKicker,
) -> None:
    benchmark, _ = recovery_run
    entered, release = threading.Event(), threading.Event()
    lock_threads: list[int] = []
    loop_thread = threading.get_ident()
    get_admission = release_control._get_admission

    def blocked_lock(session: Session, *, for_update: bool) -> Any:
        if for_update:
            lock_threads.append(threading.get_ident())
            entered.set()
            assert release.wait(timeout=5), "admission blocked the request event loop"
        return get_admission(session, for_update=for_update)

    async def verify(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task_0"])

    monkeypatch.setattr(release_control, "_get_admission", blocked_lock)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify)
    url = f"/retry-or-resume-benchmark/{benchmark.id}" if operation == "retry" else "/start-benchmark"
    body = {} if operation == "retry" else _start_body(benchmark, harness_config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app), base_url="http://test") as client:
        request = asyncio.create_task(client.post(url, json=body, headers=harness_headers))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            assert (await asyncio.wait_for(client.get("/health"), timeout=2)).status_code == 200
            assert not request.done()
            assert lock_threads and all(thread != loop_thread for thread in lock_threads)
        finally:
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=2)
        response = responses[0]
        assert isinstance(response, httpx.Response)
        assert response.status_code == 200, response.text
        assert len(mock_kicker.queued_calls) == 1
        assert all(not session.in_transaction() for session, _ in observed_sessions)


@pytest.mark.parametrize("operation", ["retry", "start"])
@pytest.mark.parametrize("commit_error", [False, True])
async def test_cancellation_during_commit_observes_outcome_before_propagating(
    operation: str,
    commit_error: bool,
    recovery_run: tuple[Benchmark, Task],
    database_session: Session,
    observed_sessions: list[tuple[Session, int]],
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
    harness_config: HarnessConfig,
) -> None:
    benchmark, _ = recovery_run
    commit_entered = threading.Event()
    release_commit = threading.Event()
    commit_completed = threading.Event()
    enqueued_dispatch_ids: list[object] = []
    commit_name = "_commit_recovery" if operation == "retry" else "_commit_start"
    original_commit = getattr(main_module, commit_name)
    cleanup_failed_start = AsyncMock(wraps=main_module._rollback_failed_start_admission)

    def blocked_commit(*args: Any, **kwargs: Any) -> Any:
        commit_entered.set()
        try:
            assert release_commit.wait(timeout=5), "cancelled admission did not release the commit barrier"
            if commit_error:
                raise RuntimeError("admission failed")
            return original_commit(*args, **kwargs)
        finally:
            commit_completed.set()

    async def enqueue(dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        with Session(database_session.get_bind()) as checked:
            assert checked.get(ExecutorDispatch, dispatch.id) is not None
        if operation == "start":
            # Enqueue is a child task and must inherit the request's benchmark correlation.
            assert benchmark_id_var.get() == str(dispatch.benchmark_id)
        enqueued_dispatch_ids.append(dispatch.id)

    async def verify(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task_0"])

    monkeypatch.setattr(main_module, commit_name, blocked_commit)
    monkeypatch.setattr(main_module, "_rollback_failed_start_admission", cleanup_failed_start)
    monkeypatch.setattr(main_module, "_enqueue_executor_dispatch", enqueue)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify)
    url = f"/retry-or-resume-benchmark/{benchmark.id}" if operation == "retry" else "/start-benchmark"
    body = {} if operation == "retry" else _start_body(benchmark, harness_config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app), base_url="http://test") as client:
        request = asyncio.create_task(client.post(url, json=body, headers=harness_headers))
        assert await asyncio.to_thread(commit_entered.wait, 2)
        request.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        assert not commit_completed.is_set()
        release_commit.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(request, timeout=2)
        assert commit_completed.is_set()

    assert len(enqueued_dispatch_ids) == (0 if commit_error else 1)
    if operation == "start" and commit_error:
        cleanup_failed_start.assert_awaited_once()
    else:
        cleanup_failed_start.assert_not_awaited()
    assert all(not session.in_transaction() for session, _ in observed_sessions)


@pytest.mark.parametrize("enqueue_error", [False, True])
async def test_start_cancellation_enqueues_after_bind_failure_and_chains_cause(
    enqueue_error: bool,
    recovery_run: tuple[Benchmark, Task],
    database_session: Session,
    observed_sessions: list[tuple[Session, int]],
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
    harness_config: HarnessConfig,
) -> None:
    benchmark, _ = recovery_run
    commit_entered = threading.Event()
    release_commit = threading.Event()
    original_commit = main_module._commit_start
    bind_error = RuntimeError("failed to bind benchmark context")
    enqueue_failure = HTTPException(status_code=503, detail="resolved enqueue failure")
    enqueued_dispatch_ids: list[object] = []
    bind_failure_log = Mock()

    def blocked_commit(*args: Any, **kwargs: Any) -> Any:
        commit_entered.set()
        assert release_commit.wait(timeout=5), "cancelled admission did not release the commit barrier"
        return original_commit(*args, **kwargs)

    async def fail_bind(_benchmark_id: object) -> None:
        raise bind_error

    async def enqueue(dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        with Session(database_session.get_bind()) as checked:
            assert checked.get(ExecutorDispatch, dispatch.id) is not None
        enqueued_dispatch_ids.append(dispatch.id)
        if enqueue_error:
            raise enqueue_failure

    async def verify(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task_0"])

    monkeypatch.setattr(main_module, "_commit_start", blocked_commit)
    monkeypatch.setattr(main_module, "bind_benchmark_id", fail_bind)
    monkeypatch.setattr(main_module.logger, "exception", bind_failure_log)
    monkeypatch.setattr(main_module, "_enqueue_executor_dispatch", enqueue)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify)
    body = _start_body(benchmark, harness_config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app), base_url="http://test") as client:
        request = asyncio.create_task(client.post("/start-benchmark", json=body, headers=harness_headers))
        assert await asyncio.to_thread(commit_entered.wait, 2)
        request.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        release_commit.set()
        with pytest.raises(asyncio.CancelledError) as cancellation:
            await asyncio.wait_for(request, timeout=2)

    assert len(enqueued_dispatch_ids) == 1
    expected_cause = enqueue_failure if enqueue_error else bind_error
    assert cancellation.value.__cause__ is expected_cause
    bind_failure_log.assert_called_once()
    assert all(not session.in_transaction() for session, _ in observed_sessions)


@pytest.mark.parametrize("operation", ["retry", "start"])
@pytest.mark.parametrize("enqueue_error", [False, True])
async def test_cancellation_during_enqueue_waits_for_completion(
    operation: str,
    enqueue_error: bool,
    recovery_run: tuple[Benchmark, Task],
    observed_sessions: list[tuple[Session, int]],
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
    harness_config: HarnessConfig,
) -> None:
    benchmark, _ = recovery_run
    enqueue_entered = asyncio.Event()
    release_enqueue = asyncio.Event()
    enqueue_finished = asyncio.Event()

    async def enqueue(*_args: Any, **_kwargs: Any) -> None:
        enqueue_entered.set()
        await release_enqueue.wait()
        enqueue_finished.set()
        if enqueue_error:
            raise HTTPException(status_code=503, detail="resolved enqueue failure")

    async def verify(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task_0"])

    monkeypatch.setattr(main_module, "_enqueue_executor_dispatch", enqueue)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify)
    url = f"/retry-or-resume-benchmark/{benchmark.id}" if operation == "retry" else "/start-benchmark"
    body = {} if operation == "retry" else _start_body(benchmark, harness_config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app), base_url="http://test") as client:
        request = asyncio.create_task(client.post(url, json=body, headers=harness_headers))
        await asyncio.wait_for(enqueue_entered.wait(), timeout=2)
        request.cancel()
        assert not request.done()
        release_enqueue.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(request, timeout=2)

    assert enqueue_finished.is_set()
    assert all(not session.in_transaction() for session, _ in observed_sessions)


@pytest.mark.parametrize(
    "change", ["execution", "status", "attempt", "selection", "dataset", "destination", "stopping", "unrelated"]
)
async def test_recovery_rechecks_verified_state_before_mutating_or_dispatching(
    change: str,
    recovery_run: tuple[Benchmark, Task],
    database_session: Session,
    observed_sessions: list[tuple[Session, int]],
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
    mock_kicker: MockKicker,
) -> None:
    benchmark, task = recovery_run
    benchmark_id, task_id = benchmark.id, task.id
    entered, release = asyncio.Event(), asyncio.Event()

    async def verify(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        entered.set()
        await release.wait()
        return VerifyTaskIdsResponse(task_ids=["task_0"])

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app), base_url="http://test") as client:
        request = asyncio.create_task(
            client.post(
                f"/retry-or-resume-benchmark/{benchmark_id}",
                json={"secrets": {"REQUEST_SECRET": "new"}},
                headers=harness_headers,
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert all(not session.in_transaction() for session, _ in observed_sessions)
            with Session(database_session.get_bind(), expire_on_commit=False) as concurrent:
                fresh_run = concurrent.get(Benchmark, benchmark_id)
                fresh_task = concurrent.get(Task, task_id)
                assert fresh_run is not None and fresh_task is not None
                if change == "execution":
                    concurrent.add(
                        ExecutorDispatch(
                            benchmark_id=benchmark_id,
                            kind=ExecutorDispatchKind.RESUME,
                            executor_release_id="barrier-release",
                            executor_artifact_uri="s3://artifacts/barrier-release.pex",
                            executor_artifact_digest="barrier-digest",
                            executor_protocol_version=SUPPORTED_PROTOCOL_VERSION,
                        )
                    )
                elif change == "status":
                    fresh_run.status = BenchmarkStatus.ERROR
                elif change == "attempt":
                    fresh_task.started_at += timedelta(microseconds=1)
                elif change == "selection":
                    fresh_task.status = TaskStatus.FINISHED
                elif change == "dataset":
                    fresh_run.arguments = fresh_run.arguments.model_copy(update={"dataset": "changed-dataset"})
                elif change == "destination":
                    fresh_run.custom_benchmark_service = "https://changed.example"
                elif change == "stopping":
                    fresh_run.status = BenchmarkStatus.STOPPING
                else:
                    contract = fresh_run.arguments.contract.model_copy(
                        update={"secrets": {"CONCURRENT_SECRET": "keep"}}
                    )
                    fresh_run.arguments = fresh_run.arguments.model_copy(
                        update={"concurrency": 9, "contract": contract}
                    )
                    fresh_run.label = "concurrent-label"
                concurrent.add_all([fresh_run, fresh_task])
                concurrent.commit()
                concurrent.refresh(fresh_run)
                concurrent.refresh(fresh_task)
                expected_run = fresh_run.model_dump(mode="json")
                expected_task = fresh_task.model_dump(mode="json")
                expected_dispatches = [
                    row.model_dump(mode="json") for row in concurrent.exec(select(ExecutorDispatch)).all()
                ]
        finally:
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=2)
        response = responses[0]
        assert isinstance(response, httpx.Response)
        expected_status = 200 if change == "unrelated" else 400 if change == "stopping" else 409
        assert response.status_code == expected_status, response.text
        with Session(database_session.get_bind()) as checked:
            persisted_run = checked.get(Benchmark, benchmark_id)
            persisted_task = checked.get(Task, task_id)
            assert persisted_run is not None and persisted_task is not None
            if change == "unrelated":
                assert persisted_run.arguments.concurrency == 9
                assert persisted_run.arguments.contract.secrets == {
                    "CONCURRENT_SECRET": "keep",
                    "REQUEST_SECRET": "new",
                }
                assert persisted_run.label == "concurrent-label"
                payload = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
                assert payload["concurrency"] == 9
                assert payload["contract"]["secrets"] == persisted_run.arguments.contract.secrets
                assert len(mock_kicker.queued_calls) == 1
            else:
                assert persisted_run.model_dump(mode="json") == expected_run
                assert persisted_task.model_dump(mode="json") == expected_task
                assert [
                    row.model_dump(mode="json") for row in checked.exec(select(ExecutorDispatch)).all()
                ] == expected_dispatches
                assert mock_kicker.queued_calls == []
